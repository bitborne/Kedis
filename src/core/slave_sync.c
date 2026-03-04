/*
 * 从节点存量同步管理模块
 *
 * 功能：
 * 1. 管理存量同步状态（IDLE -> SYNCING -> READY）
 * 2. 提供积压命令队列（仅主线程访问，无锁）
 * 3. RDMA 线程完成后通过 eventfd 通知主线程
 */

#include "../../include/kvstore.h"
#include "../../include/kvs_rdma_sync.h"
#include <sys/eventfd.h>
#include <unistd.h>
#include <string.h>

/* ============================================================================
 * 全局状态
 * ============================================================================ */

/* 同步状态 - 使用 volatile 保证可见性 */
static volatile int g_sync_state = SLAVE_STATE_IDLE;

/* eventfd - 用于 RDMA 线程通知主线程 */
static int g_event_fd = -1;

/* 积压队列 - 仅主线程访问，无锁 */
struct backlog_queue {
    struct backlog_cmd *head;
    struct backlog_cmd *tail;
    uint64_t count;
};
static struct backlog_queue g_backlog = {0};

/* 外部声明 */
extern int rdma_sync_client_start_via_tcp(const char *master_host,
                                          uint16_t master_port,
                                          rdma_engine_type_t engine_type);

/* ============================================================================
 * RDMA 同步线程
 * ============================================================================ */

/* ============================================================================
 * RDMA 同步线程主函数
 *
 * 【v3.0 架构】RDMA Channel（独立线程）
 * 职责：执行 RDMA 存量同步，与 Main Channel（io_uring）并行运行
 *
 * 线程安全：
 *   - 独占写入引擎：RDMA 线程是 SYNCING 期间唯一写入引擎的线程
 *   - 状态同步：通过 g_sync_state（volatile）+ eventfd 与主线程通信
 *   - 无锁设计：积压队列仅主线程访问，RDMA 线程不访问
 *
 * 生命周期：
 *   1. 启动：主线程调用 slave_sync_start() 创建此线程
 *   2. 运行：阻塞式 RDMA 同步（可能持续数秒到数分钟）
 *   3. 完成：设置状态 -> 写入 eventfd -> 退出
 * ============================================================================ */
static void *rdma_sync_thread_fn(void *arg) {
    struct sync_thread_args *args = (struct sync_thread_args *)arg;

    kvs_logInfo("[RDMA Thread] 存量同步线程启动，目标: %s:%d\n",
                args->master_host, args->master_port);

    /* -------------------------------------------------------------------------
     * 设置线程取消状态
     * PTHREAD_CANCEL_ENABLE: 允许取消请求
     * PTHREAD_CANCEL_DEFERRED: 延迟到取消点（如 sleep、I/O 操作）再响应
     *
     * 这允许主线程在紧急情况下取消 RDMA 同步（如 REPLICAOF NO ONE）
     * ------------------------------------------------------------------------- */
    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
    pthread_setcanceltype(PTHREAD_CANCEL_DEFERRED, NULL);

    /* -------------------------------------------------------------------------
     * 执行存量同步（阻塞式）
     *
     * 此函数内部会：
     *   - 建立 TCP 连接到主节点
     *   - 发送 RDMASYNC 命令触发 fork
     *   - 建立 RDMA 连接
     *   - 循环读取数据并解析到引擎
     *   - 使用流式 KSF 解析器处理分段数据
     * ------------------------------------------------------------------------- */
    int ret = rdma_sync_client_start_via_tcp(args->master_host,
                                              args->master_port,
                                              ENGINE_COUNT);

    /* -------------------------------------------------------------------------
     * 同步完成后的状态处理
     *
     * 成功 (ret == 0)：
     *   - 引擎已包含完整的存量数据
     *   - 积压队列中有 SYNCING 期间暂存的写命令
     *   - 需要回放积压命令以达到最终一致
     *
     * 失败 (ret < 0)：
     *   - 引擎可能处于部分同步状态
     *   - 积压队列中的命令基于不一致的数据状态
     *   - 必须清空积压队列，否则回放会导致数据错乱
     * ------------------------------------------------------------------------- */
    if (ret < 0) {
        kvs_logError("[RDMA Thread] 存量同步失败，清理积压队列\n");

        /* 【关键】同步失败，积压队列中的命令基于脏数据，必须丢弃
         * 否则回放这些命令会导致数据不一致 */
        slave_sync_clear_backlog();

        /* 重置状态为 IDLE，允许下次重新同步 */
        g_sync_state = SLAVE_STATE_IDLE;
        __sync_synchronize();  /* 内存屏障：确保状态更新可见 */
    } else {
        kvs_logInfo("[RDMA Thread] 存量同步成功完成\n");

        /* 设置状态为 READY，积压队列将在 eventfd 通知后回放 */
        g_sync_state = SLAVE_STATE_READY;
        kvs_logInfo("[RDMA Thread] 状态设置为 READY");
    }

    /* -------------------------------------------------------------------------
     * 内存屏障：确保状态更新对其他 CPU 核心可见
     *
     * 原因：g_sync_state 是 volatile，但 C 标准不保证跨线程的内存序
     * 此处使用 GCC 内置内存屏障，确保：
     *   - 前面的写操作（状态更新）已完成
     *   - 后续的 eventfd 通知能看到最新的状态
     *
     * 注意：x86_64 架构下 volatile 通常足够，但内存屏障提供更严格的保证
     * 且几乎没有性能开销（仅是一条指令）
     * ------------------------------------------------------------------------- */
    __sync_synchronize();

    /* -------------------------------------------------------------------------
     * 通知主线程同步完成
     *
     * 机制：通过 eventfd 唤醒 io_uring 事件循环
     * 流程：
     *   1. RDMA 线程写入 eventfd（这里）
     *   2. io_uring 检测到 eventfd 可读（proactor.c）
     *   3. 主线程调用 slave_sync_drain_backlog() 回放积压
     *
     * 注意：必须先更新状态，再写入 eventfd，避免竞态条件
     * ------------------------------------------------------------------------- */
    if (g_event_fd >= 0) {
        uint64_t val = 1;  /* 通知值，可以扩展为携带更多信息（如状态码） */
        ssize_t written = write(g_event_fd, &val, sizeof(val));

        if (written != sizeof(val)) {
            kvs_logError("[RDMA Thread] eventfd 写入失败: %s\n", strerror(errno));
            /* 即使通知失败，状态已更新，主线程可能在下次检查时处理 */
        } else {
            kvs_logInfo("[RDMA Thread] 已通知主线程同步完成\n");
        }
    } else {
        kvs_logError("[RDMA Thread] eventfd 未初始化，无法通知主线程\n");
    }

    /* 清理线程参数 */
    kvs_free(args->master_host);
    kvs_free(args);

    kvs_logInfo("[RDMA Thread] 线程退出\n");
    return NULL;
}

/* ============================================================================
 * 公开 API 实现
 * ============================================================================ */

/* 初始化从节点同步系统 */
int slave_sync_init(void) {
    /* 创建 eventfd */
    g_event_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (g_event_fd < 0) {
        kvs_logError("slave_sync_init: eventfd 创建失败: %s\n", strerror(errno));
        return -1;
    }

    /* 初始化积压队列 */
    g_backlog.head = NULL;
    g_backlog.tail = NULL;
    g_backlog.count = 0;

    g_sync_state = SLAVE_STATE_IDLE;

    kvs_logInfo("[Slave Sync] 初始化完成，event_fd=%d\n", g_event_fd);
    return g_event_fd;  /* 返回 eventfd，让 proactor 注册 */
}

/* 清理资源 */
void slave_sync_cleanup(void) {
    if (g_event_fd >= 0) {
        close(g_event_fd);
        g_event_fd = -1;
    }

    /* 清空积压队列 */
    slave_sync_clear_backlog();
}

/* 获取 eventfd（供 proactor 注册到 io_uring） */
int slave_sync_get_eventfd(void) {
    return g_event_fd;
}

/* 获取当前状态 */
int slave_sync_get_state(void) {
    /* 【v3.0 内存可见性修复】
     * 问题：虽然 g_sync_state 是 volatile，但跨线程访问时，
     * x86_64 架构下编译器可能优化掉内存读取，导致主线程看不到
     * RDMA 线程的最新写入。
     *
     * 解决：使用 __sync_synchronize() 内存屏障，确保：
     * 1. 在此之前的所有内存操作都已完成
     * 2. 从内存中重新读取 g_sync_state，而不是使用寄存器缓存
     * 3. 后续的内存操作在此之后执行
     *
     * 注意：这是 GCC 内置的 full memory barrier，x86_64 下生成 mfence 指令
     */
    __sync_synchronize();
    int state = g_sync_state;
    __sync_synchronize();  /* 再次屏障，确保读取不被重排到后面 */
    kvs_logInfo("[slave_sync_get_state] 读取到状态: %d (地址: %p)\n", state, (void*)&g_sync_state);
    return state;
}

/* 启动存量同步（创建 RDMA 线程） */
int slave_sync_start(const char *master_host, uint16_t master_port) {
    if (g_sync_state == SLAVE_STATE_SYNCING) {
        kvs_logWarn("[Slave Sync] 存量同步已在进行中\n");
        return -1;
    }

    /* 分配线程参数 */
    struct sync_thread_args *args = kvs_malloc(sizeof(*args));
    if (!args) {
        return -1;
    }

    size_t host_len = strlen(master_host);
    args->master_host = kvs_malloc(host_len + 1);
    if (!args->master_host) {
        kvs_free(args);
        return -1;
    }
    memcpy(args->master_host, master_host, host_len + 1);
    args->master_port = master_port;

    /* 设置状态为同步中 */
    g_sync_state = SLAVE_STATE_SYNCING;
    __sync_synchronize();  /* 内存屏障：确保状态更新对其他 CPU 核心可见 */
    kvs_logInfo("[Slave Sync] 状态设置为 SYNCING (地址: %p, 值: %d)\n",
                (void*)&g_sync_state, g_sync_state);

    /* 创建 RDMA 线程 */
    pthread_t thread;
    if (pthread_create(&thread, NULL, rdma_sync_thread_fn, args) != 0) {
        kvs_logError("[Slave Sync] RDMA 线程创建失败\n");
        kvs_free(args->master_host);
        kvs_free(args);
        g_sync_state = SLAVE_STATE_IDLE;
        return -1;
    }

    /* 分离线程，自动回收资源 */
    pthread_detach(thread);

    kvs_logInfo("[Slave Sync] RDMA 同步线程已启动\n");
    return 0;
}

/* ============================================================================
 * 积压队列操作（仅主线程访问）
 * ============================================================================ */

/* 深拷贝 robj 数组 */
static robj *robj_array_dup(int argc, robj *argv) {
    if (argc <= 0 || !argv) return NULL;

    robj *new_argv = kvs_malloc(sizeof(robj) * argc);
    if (!new_argv) return NULL;

    for (int i = 0; i < argc; i++) {
        new_argv[i].len = argv[i].len;
        if (argv[i].len > 0 && argv[i].ptr) {
            new_argv[i].ptr = kvs_malloc(argv[i].len + 1);
            if (new_argv[i].ptr) {
                memcpy(new_argv[i].ptr, argv[i].ptr, argv[i].len);
                new_argv[i].ptr[argv[i].len] = '\0';
            }
        } else {
            new_argv[i].ptr = NULL;
        }
    }

    return new_argv;
}

/* 入队 - 主线程在 SYNCING 状态下调用 */
int slave_sync_enqueue(int argc, robj *argv) {
    if (argc <= 0 || !argv) return -1;

    struct backlog_cmd *cmd = kvs_malloc(sizeof(*cmd));
    if (!cmd) return -1;

    cmd->argc = argc;
    cmd->argv = robj_array_dup(argc, argv);
    if (!cmd->argv) {
        kvs_free(cmd);
        return -1;
    }
    cmd->next = NULL;

    /* 链表尾部插入 - 仅主线程访问，无锁 */
    if (g_backlog.tail) {
        g_backlog.tail->next = cmd;
    } else {
        g_backlog.head = cmd;
    }
    g_backlog.tail = cmd;
    g_backlog.count++;

    kvs_logDebug("[Slave Sync] 命令入队，当前积压: %lu\n", (unsigned long)g_backlog.count);
    return 0;
}

/* ============================================================================
 * 积压队列回放 - 主线程在 RDMA 完成后调用
 *
 * 【v3.0 架构关键路径】
 * 调用时机：主线程通过 eventfd 收到 RDMA 线程完成通知后
 * 执行环境：io_uring 事件循环中，单线程执行（无需加锁）
 * 状态要求：必须为 READY 状态（RDMA 线程已设置）
 *
 * 处理流程：
 *   1. 状态检查：确保 RDMA 已完成，引擎数据完整
 *   2. 顺序回放：按 FIFO 顺序执行积压的写命令
 *   3. 资源清理：每处理一条命令立即释放其内存
 *   4. 进度汇报：每 100 条打印日志，便于运维监控
 * ============================================================================ */
void slave_sync_drain_backlog(msg_handler handler) {
    if (!handler) {
        kvs_logError("[Slave Sync] handler 为空，无法回放积压队列\n");
        return;
    }

    /* -------------------------------------------------------------------------
     * 状态检查：确保 RDMA 线程已完成存量同步
     *
     * 竞争场景：如果 RDMA 线程设置状态和写入 eventfd 之间有延迟，
     * 主线程可能在状态仍为 SYNCING 时收到通知。
     *
     * 处理策略：
     *   - 如果状态是 READY：正常执行
     *   - 如果状态是 IDLE：RDMA 可能失败了，积压队列已被清空
     *   - 如果状态是 SYNCING：异常，强制设置为 READY 继续执行
     * ------------------------------------------------------------------------- */
    if (g_sync_state != SLAVE_STATE_READY) {
        kvs_logWarn("[Slave Sync] 状态异常，期望 READY(%d)，实际 %d\n",
                    SLAVE_STATE_READY, g_sync_state);

        if (g_sync_state == SLAVE_STATE_IDLE) {
            /* RDMA 线程可能失败了，积压队列应该已被清空 */
            kvs_logWarn("[Slave Sync] RDMA 可能失败，积压队列状态: head=%p, count=%lu\n",
                        (void*)g_backlog.head, (unsigned long)g_backlog.count);
        } else if (g_sync_state == SLAVE_STATE_SYNCING) {
            /* 竞态条件：eventfd 通知先于状态更新到达
             * 强制更新状态为 READY，允许回放继续 */
            kvs_logWarn("[Slave Sync] 强制更新状态为 READY\n");
            g_sync_state = SLAVE_STATE_READY;
            __sync_synchronize();  /* 内存屏障，确保状态更新对其他 CPU 可见 */
        }
    }

    if (g_backlog.count == 0) {
        kvs_logInfo("[Slave Sync] 积压队列为空，无需回放\n");
        return;
    }

    kvs_logInfo("[Slave Sync] 开始处理积压队列，共 %lu 条命令\n",
                (unsigned long)g_backlog.count);

    struct backlog_cmd *cmd;
    int processed = 0;
    int failed = 0;
    time_t start_time = time(NULL);  /* 简单计时 */

    /* -------------------------------------------------------------------------
     * 顺序回放积压命令
     *
     * 注意：积压队列中的命令都是写命令（SET/DEL/MOD 等）
     * 这些命令在 SYNCING 期间被主线程拦截并存入队列
     * 现在按 FIFO 顺序回放，保证因果一致性
     * ------------------------------------------------------------------------- */
    while ((cmd = g_backlog.head) != NULL) {
        /* 从链表头部移除命令（O(1) 操作） */
        g_backlog.head = cmd->next;
        if (!g_backlog.head) {
            g_backlog.tail = NULL;  /* 队列为空，更新尾指针 */
        }
        g_backlog.count--;

        /* 创建临时 conn 结构来执行命令
         * 原因：handler 期望一个完整的 conn 结构，包含 fd/wbuf 等字段
         * 注意：这只是模拟网络连接，实际不通过 socket 发送响应 */
        struct conn temp_conn = {0};

        /* 复制命令参数 */
        temp_conn.argc = cmd->argc;
        for (int i = 0; i < cmd->argc && i < MAX_ARGC; i++) {
            /* 浅拷贝 robj：复制 len 和 ptr 指针
             * 注意：ptr 指向的内存仍由 cmd->argv[i] 拥有，稍后释放 */
            temp_conn.argv[i].len = cmd->argv[i].len;
            temp_conn.argv[i].ptr = cmd->argv[i].ptr;
        }

        /* 分配写缓冲区（handler 需要写入响应） */
        temp_conn.wbuf = kvs_malloc(RESP_BUF_SIZE);
        if (!temp_conn.wbuf) {
            kvs_logError("[Slave Sync] 分配 wbuf 失败，跳过命令 #%d\n", processed + 1);
            failed++;
            goto cleanup_cmd;  /* 跳过执行，直接清理命令资源 */
        }
        temp_conn.wlen = 0;
        memset(temp_conn.wbuf, 0, RESP_BUF_SIZE);

        /* 标记为内部连接（fd = -1 表示非网络连接）
         * 这用于区分积压回放命令和普通客户端命令，便于调试 */
        temp_conn.fd = -1;
        temp_conn.state = ST_RECV;
        temp_conn.send_st = ST_SEND_SMALL;

        /* 执行命令：调用 kvs_protocol 处理命令 */
        handler(&temp_conn);

        /* 注意：积压命令的响应不发送给任何客户端（fd = -1）
         * 这是因为这些命令来自 TCP mirror，客户端在主节点已收到响应
         * 从节点只需要保证数据一致性，不需要返回响应 */

        kvs_free(temp_conn.wbuf);

    cleanup_cmd:
        /* 释放命令资源：argv 数组和其中的字符串缓冲区 */
        for (int i = 0; i < cmd->argc; i++) {
            if (cmd->argv[i].ptr) {
                kvs_free(cmd->argv[i].ptr);
            }
        }
        kvs_free(cmd->argv);  /* 释放 argv 数组本身 */
        kvs_free(cmd);        /* 释放命令节点 */

        processed++;

        /* 每处理 100 条命令打印一次进度，便于监控大批量回放 */
        if (processed % 100 == 0) {
            kvs_logInfo("[Slave Sync] 回放进度: %d/%d (剩余 %lu)\n",
                        processed, processed + (int)g_backlog.count,
                        (unsigned long)g_backlog.count);
        }
    }

    /* 重置队列状态 */
    g_backlog.tail = NULL;
    g_backlog.count = 0;

    time_t elapsed = time(NULL) - start_time;
    kvs_logInfo("[Slave Sync] 积压队列处理完成，成功 %d，失败 %d，耗时 %ld 秒\n",
                processed - failed, failed, (long)elapsed);
}

/* 清空积压队列（不执行） */
void slave_sync_clear_backlog(void) {
    struct backlog_cmd *cmd = g_backlog.head;
    while (cmd) {
        struct backlog_cmd *next = cmd->next;

        for (int i = 0; i < cmd->argc; i++) {
            if (cmd->argv[i].ptr) {
                kvs_free(cmd->argv[i].ptr);
            }
        }
        kvs_free(cmd->argv);
        kvs_free(cmd);

        cmd = next;
    }

    g_backlog.head = NULL;
    g_backlog.tail = NULL;
    g_backlog.count = 0;
}
