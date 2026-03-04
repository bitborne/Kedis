/*
 * RDMA 存量同步实现
 *
 * 基于 RDMA Read 操作实现主从节点间的全量数据同步
 * 设计要点：
 * 1. 主节点将引擎数据序列化为 KSF 格式，mmap 后注册为 RDMA MR
 * 2. 从节点通过 RDMA Read 直接读取主节点内存
 * 3. TCP 连接用于传输控制信息和暂存增量命令
 */

#include "../../include/kvs_rdma_sync.h"
#include "../../include/kvstore.h"
#include "../../include/kvs_ksf.h"
#include "../../include/kvs_log.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <arpa/inet.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <pthread.h>
#include <ctype.h>  // isspace, isdigit 等字符处理函数
#include <dirent.h> // 用于子进程遍历/proc/self/fd关闭继承的fd
#include <signal.h> // 用于子进程忽略SIGCHLD避免僵尸进程

/* ============================================================================
 * 外部引擎变量声明（来自 kvstore.c）
 * ============================================================================ */
#if ENABLE_MULTI_ENGINE
extern kvs_array_t array_engine;
extern kvs_rbtree_t rbtree_engine;
extern kvs_hash_t hash_engine;
extern kvs_skiplist_t skiplist_engine;
#else
extern kvs_rbtree_t global_main_engine;
#endif

/* ============================================================================
 * 全局变量 - 方案C重构说明
 * ============================================================================
 *
 * 方案C变更:
 *   删除常驻服务器相关全局变量，改为fork时临时创建
 *   原因: 避免主进程常驻资源占用，简化架构
 *
 * 保留变量:
 *   - g_client_ctx: 从节点客户端上下文（仍需要）
 *   - g_engine_names: 引擎名称映射（通用工具）
 *
 * 删除变量:
 *   - g_cm_channel:    常驻事件通道（子进程临时创建）
 *   - g_listener:      常驻监听ID（子进程临时创建）
 *   - g_server_poll_thread: 常驻轮询线程（不再需要）
 *   - g_server_running: 服务器运行标志（不再需要）
 *   - g_slaves_head:   从节点链表（子进程单连接处理）
 *   - g_slaves_lock:   链表锁（不再需要）
 */

/* 【保留】从节点全局客户端上下文 */
static struct rdma_client_context *g_client_ctx = NULL;

/* 【废弃-方案A/B】主节点常驻服务器状态 - 已注释
 * 这些变量在方案C中不再使用，保留注释以便代码考古
static struct rdma_event_channel *g_cm_channel = NULL;
static struct rdma_cm_id *g_listener = NULL;
static pthread_t g_server_poll_thread;
static volatile int g_server_running = 0;
static struct rdma_sync_context *g_slaves_head = NULL;
static pthread_mutex_t g_slaves_lock = PTHREAD_MUTEX_INITIALIZER;
 */

/* 引擎名称映射表 */
static const char *g_engine_names[] = {
    "array", "rbtree", "hash", "skiplist"
};

/* ============================================================================
 * 工具函数
 * ============================================================================ */

/**
 * @brief 将引擎类型转换为字符串名称
 *
 * @param type 引擎类型枚举值
 * @return 引擎名称字符串
 */
const char* rdma_sync_engine_name(rdma_engine_type_t type) {
    if (type >= 0 && type < ENGINE_COUNT) {
        return g_engine_names[type];
    }
    return "unknown";
}

/* ============================================================================
 * 主节点（Server）实现
 * ============================================================================ */

/**
 * @brief 创建 RDMA 连接资源
 *
 * 为新连接的从节点创建保护域、完成队列、队列对等资源
 *
 * @param ctx 同步上下文（已分配 cm_id）
 * @return 0 成功，-1 失败
 */
static int setup_connection_resources(struct rdma_sync_context *ctx) {
    struct ibv_qp_init_attr qp_attr = {0};

    /* 1. 创建保护域（Protection Domain）
     * PD 是资源的容器，QP 和 MR 都关联到 PD
     * 一个从节点一个 PD，资源隔离 */
    ctx->pd = ibv_alloc_pd(ctx->cm_id->verbs);
    if (!ctx->pd) {
        kvs_logError("ibv_alloc_pd 失败: %s\n", strerror(errno));
        return -1;
    }

    /* 2. 创建完成事件通道
     * 用于异步接收完成队列事件 */
    ctx->comp_channel = ibv_create_comp_channel(ctx->cm_id->verbs);
    if (!ctx->comp_channel) {
        kvs_logError("ibv_create_comp_channel 失败\n");
        goto err_pd;
    }

    /* 3. 创建完成队列（Completion Queue）
     * CQ 存储已完成的工作请求（WR）状态
     * 发送和接收共享一个 CQ（简单模型） */
    ctx->cq = ibv_create_cq(ctx->cm_id->verbs, RDMA_CQ_CAPACITY,
                             NULL, ctx->comp_channel, 0);
    if (!ctx->cq) {
        kvs_logError("ibv_create_cq 失败\n");
        goto err_comp_channel;
    }

    /* 4. 请求 CQ 事件通知
     * 当有新完成事件时，通过 channel 通知 */
    if (ibv_req_notify_cq(ctx->cq, 0)) {
        kvs_logError("ibv_req_notify_cq 失败\n");
        goto err_cq;
    }

    /* 5. 配置队列对（Queue Pair）参数 */
    qp_attr.qp_type = IBV_QPT_RC;           /* Reliable Connection 类型 */
    qp_attr.sq_sig_all = 1;                  /* 所有发送请求都产生完成事件 */
    qp_attr.send_cq = ctx->cq;               /* 发送队列关联的 CQ */
    qp_attr.recv_cq = ctx->cq;               /* 接收队列关联的 CQ */

    /* 发送队列属性 */
    qp_attr.cap.max_send_wr = RDMA_MAX_WR;   /* 最大发送 WR 数 */
    qp_attr.cap.max_send_sge = RDMA_MAX_SGE; /* 每个 WR 最大 SGE 数 */

    /* 接收队列属性 */
    qp_attr.cap.max_recv_wr = RDMA_MAX_WR;   /* 最大接收 WR 数 */
    qp_attr.cap.max_recv_sge = RDMA_MAX_SGE; /* 每个 WR 最大 SGE 数 */

    /* 6. 创建 QP */
    if (rdma_create_qp(ctx->cm_id, ctx->pd, &qp_attr)) {
        kvs_logError("rdma_create_qp 失败: %s\n", strerror(errno));
        goto err_cq;
    }
    ctx->qp = ctx->cm_id->qp;

    /* 7. 分配控制消息缓冲区 */
    ctx->ctrl_buf = kvs_calloc(1, sizeof(struct rdma_ctrl_msg));
    if (!ctx->ctrl_buf) {
        kvs_logError("分配控制缓冲区失败\n");
        goto err_qp;
    }

    /* 8. 注册控制缓冲区为 MR
     * 权限：本地写（接收数据）+ 远程读（发送响应） */
    ctx->mr_ctrl = ibv_reg_mr(ctx->pd, ctx->ctrl_buf,
                               sizeof(struct rdma_ctrl_msg),
                               IBV_ACCESS_LOCAL_WRITE |
                               IBV_ACCESS_REMOTE_READ);
    if (!ctx->mr_ctrl) {
        kvs_logError("ibv_reg_mr (ctrl) 失败\n");
        goto err_ctrl_buf;
    }

    /* 注意: 接收请求在同步循环中按需投递，不在此处预投递 */

    kvs_logInfo("RDMA 连接资源创建成功\n");
    return 0;

err_ctrl_buf:
    free(ctx->ctrl_buf);
err_qp:
    rdma_destroy_qp(ctx->cm_id);
err_cq:
    ibv_destroy_cq(ctx->cq);
err_comp_channel:
    ibv_destroy_comp_channel(ctx->comp_channel);
err_pd:
    ibv_dealloc_pd(ctx->pd);
    return -1;
}

/**
 * @brief 清理连接资源
 */
static void cleanup_connection_resources(struct rdma_sync_context *ctx) {
    if (ctx->mr_data) {
        ibv_dereg_mr(ctx->mr_data);
        ctx->mr_data = NULL;
    }
    if (ctx->data_buf) {
        munmap(ctx->data_buf, ctx->data_buf_size);
        ctx->data_buf = NULL;
    }
    if (ctx->temp_fd >= 0) {
        close(ctx->temp_fd);
        unlink(ctx->temp_path);
        ctx->temp_fd = -1;
    }
    if (ctx->mr_ctrl) {
        ibv_dereg_mr(ctx->mr_ctrl);
    }
    if (ctx->ctrl_buf) {
        free(ctx->ctrl_buf);
    }
    if (ctx->qp) {
        rdma_destroy_qp(ctx->cm_id);
    }
    if (ctx->cq) {
        ibv_destroy_cq(ctx->cq);
    }
    if (ctx->comp_channel) {
        ibv_destroy_comp_channel(ctx->comp_channel);
    }
    if (ctx->pd) {
        ibv_dealloc_pd(ctx->pd);
    }
}

/**
 * @brief 为指定引擎创建 KSF 快照
 *
 * 将引擎数据写入临时文件，然后 mmap 并注册为 RDMA MR
 *
 * @param ctx 同步上下文
 * @param engine_type 引擎类型
 * @return 0 成功，-1 失败
 */
int rdma_sync_master_prepare_engine(struct rdma_sync_context *ctx,
                                    rdma_engine_type_t engine_type) {
    char path[256];
    int fd = -1;
    void *mmap_addr = NULL;
    struct ibv_mr *mr = NULL;

    /* 确保临时目录存在 */
    struct stat st = {0};
    if (stat(RDMA_SYNC_TEMP_DIR, &st) == -1) {
        if (mkdir(RDMA_SYNC_TEMP_DIR, 0755) == -1) {
            kvs_logError("创建临时目录失败: %s\n", strerror(errno));
            /* 尝试创建父目录 */
            if (mkdir("./data", 0755) == -1 && errno != EEXIST) {
                kvs_logError("创建父目录失败: %s\n", strerror(errno));
            } else {
                if (mkdir(RDMA_SYNC_TEMP_DIR, 0755) == -1 && errno != EEXIST) {
                    kvs_logError("创建临时目录仍然失败: %s\n", strerror(errno));
                }
            }
        }
    }

    /* 构建临时文件路径 */
    snprintf(path, sizeof(path), "%s/" RDMA_SYNC_TEMP_FILE,
             RDMA_SYNC_TEMP_DIR, rdma_sync_engine_name(engine_type));

    kvs_logInfo("准备引擎 %s 的快照: %s\n",
                rdma_sync_engine_name(engine_type), path);

    /* 创建临时文件 */
    fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        kvs_logError("创建临时文件失败: %s\n", strerror(errno));
        return -1;
    }

    /* 将引擎数据写入文件
     * 复用 ksf.c 中的写入函数（使用全局引擎变量） */
    int ret = -1;
    switch (engine_type) {
    case ENGINE_ARRAY:
        ret = ksfWriteArray(fd);
        break;
    case ENGINE_RBTREE:
        ret = ksfWriteRbtree(fd);
        break;
    case ENGINE_HASH:
        ret = ksfWriteHash(fd);
        break;
    case ENGINE_SKIPLIST:
        ret = ksfWriteSkiplist(fd);
        break;
    default:
        kvs_logError("未知的引擎类型\n");
        goto err_close;
    }

    if (ret < 0) {
        kvs_logError("写入引擎数据失败\n");
        goto err_close;
    }

    /* 获取文件大小 */
    off_t file_size = lseek(fd, 0, SEEK_END);
    if (file_size < 0) {
        kvs_logError("获取文件大小失败\n");
        goto err_close;
    }

    if (file_size == 0) {
        kvs_logInfo("引擎 %s 为空\n", rdma_sync_engine_name(engine_type));
        /* 空引擎也是合法的 - 跳过 mmap 和 MR 注册 */
        ctx->data_buf = NULL;
        ctx->data_buf_size = 0;
        ctx->mr_data = NULL;
        ctx->temp_fd = fd;
        strncpy(ctx->temp_path, path, sizeof(ctx->temp_path));
        ctx->current_engine = engine_type;
        ctx->state = SYNC_STATE_PREPARE;
        kvs_logInfo("引擎 %s 快照准备完成 (空引擎)\n",
                    rdma_sync_engine_name(engine_type));
        return 0;
    }

    kvs_logInfo("引擎 %s 快照大小: %ld bytes\n",
                rdma_sync_engine_name(engine_type), (long)file_size);

    /* mmap 文件到内存 */
    mmap_addr = mmap(NULL, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (mmap_addr == MAP_FAILED) {
        kvs_logError("mmap 失败: %s\n", strerror(errno));
        goto err_close;
    }

    /* 注册为 RDMA Memory Region
     * 权限：REMOTE_READ - 允许远程节点读取
     * 注意：mmap 区域可以安全地注册为 MR */
    mr = ibv_reg_mr(ctx->pd, mmap_addr, file_size, IBV_ACCESS_REMOTE_READ);
    if (!mr) {
        kvs_logError("ibv_reg_mr 失败: %s\n", strerror(errno));
        goto err_mmap;
    }

    /* 保存到上下文 */
    ctx->data_buf = mmap_addr;
    ctx->data_buf_size = file_size;
    ctx->mr_data = mr;
    ctx->temp_fd = fd;
    strncpy(ctx->temp_path, path, sizeof(ctx->temp_path));
    ctx->current_engine = engine_type;
    ctx->state = SYNC_STATE_PREPARE;

    kvs_logInfo("引擎 %s 快照准备完成，MR rkey=%u\n",
                rdma_sync_engine_name(engine_type), mr->rkey);

    return 0;

err_mmap:
    munmap(mmap_addr, file_size);
err_close:
    close(fd);
    unlink(path);
    return -1;
}

/**
 * @brief 清理主节点的同步资源
 */
void rdma_sync_master_cleanup(struct rdma_sync_context *ctx) {
    if (ctx->mr_data) {
        ibv_dereg_mr(ctx->mr_data);
        ctx->mr_data = NULL;
    }
    if (ctx->data_buf) {
        munmap(ctx->data_buf, ctx->data_buf_size);
        ctx->data_buf = NULL;
        ctx->data_buf_size = 0;
    }
    if (ctx->temp_fd >= 0) {
        close(ctx->temp_fd);
        ctx->temp_fd = -1;
    }
    if (ctx->temp_path[0]) {
        unlink(ctx->temp_path);
        ctx->temp_path[0] = '\0';
    }
    ctx->state = SYNC_STATE_IDLE;
}

/*
 * ============================================================================
 * 【方案C废弃】常驻服务器相关函数
 * ============================================================================
 *
 * 以下函数为方案A/B的常驻RDMA服务器实现，方案C中不再使用。
 * 方案C使用TCP触发fork子进程的方式，子进程中临时创建RDMA服务器。
 *
 * 保留这些函数定义（用#if 0包裹）以便：
 *   1. 代码考古和参考
 *   2. 未来如需回退到常驻服务器方案
 *   3. 理解两种架构的差异
 */
#if 0

/**
 * @brief 处理新的连接请求
 *
 * 在 RDMA_CM_EVENT_CONNECT_REQUEST 事件时调用
 */
static void handle_connect_request(struct rdma_cm_id *id) {
    struct rdma_sync_context *ctx = kvs_calloc(1, sizeof(struct rdma_sync_context));
    if (!ctx) {
        kvs_logError("分配同步上下文失败\n");
        rdma_reject(id, NULL, 0);
        return;
    }

    ctx->cm_id = id;
    ctx->state = SYNC_STATE_IDLE;
    ctx->temp_fd = -1;
    id->context = ctx;

    /* 创建连接资源 */
    if (setup_connection_resources(ctx) < 0) {
        kvs_logError("设置连接资源失败\n");
        free(ctx);
        rdma_reject(id, NULL, 0);
        return;
    }

    /* 接受连接 */
    struct rdma_conn_param conn_param = {
        .responder_resources = 1,
        .initiator_depth = 1,
        .retry_count = 5
    };

    if (rdma_accept(id, &conn_param)) {
        kvs_logError("rdma_accept 失败\n");
        cleanup_connection_resources(ctx);
        free(ctx);
        return;
    }

    /* 添加到从节点链表 */
    pthread_mutex_lock(&g_slaves_lock);
    ctx->next = g_slaves_head;
    g_slaves_head = ctx;
    pthread_mutex_unlock(&g_slaves_lock);

    kvs_logInfo("接受从节点 RDMA 连接\n");
}

/**
 * @brief 处理断开连接
 */
static void handle_disconnect(struct rdma_cm_id *id) {
    struct rdma_sync_context *ctx = id->context;

    /* 从链表中移除 */
    pthread_mutex_lock(&g_slaves_lock);
    struct rdma_sync_context **curr = &g_slaves_head;
    while (*curr) {
        if (*curr == ctx) {
            *curr = ctx->next;
            break;
        }
        curr = &(*curr)->next;
    }
    pthread_mutex_unlock(&g_slaves_lock);

    /* 清理资源 */
    cleanup_connection_resources(ctx);
    rdma_destroy_id(id);
    free(ctx);

    kvs_logInfo("从节点 RDMA 连接已断开\n");
}

/**
 * @brief 服务器轮询线程
 *
 * 处理 CM 事件和数据传输完成事件
 */
static void* server_poll_loop(void *arg) {
    struct rdma_cm_event *event;

    while (g_server_running) {
        /* 等待 CM 事件（阻塞） */
        if (rdma_get_cm_event(g_cm_channel, &event) < 0) {
            if (errno == EINTR) continue;
            break;
        }

        switch (event->event) {
        case RDMA_CM_EVENT_CONNECT_REQUEST:
            handle_connect_request(event->id);
            break;

        case RDMA_CM_EVENT_ESTABLISHED:
            kvs_logInfo("RDMA 连接已建立\n");
            break;

        case RDMA_CM_EVENT_DISCONNECTED:
            handle_disconnect(event->id);
            break;

        default:
            kvs_logWarn("未处理的 CM 事件: %d\n", event->event);
            break;
        }

        rdma_ack_cm_event(event);
    }

    return NULL;
}

#endif /* 方案C: 禁用常驻服务器逻辑 */

/**
 * @brief 初始化 RDMA 同步服务器
 */
/**
 * @brief 【方案C废弃】初始化 RDMA 同步服务器
 *
 * 原功能: 创建常驻RDMA监听线程
 * 新行为: 直接返回0，不执行任何操作
 *
 * 原因:
 *   方案C使用TCP触发fork子进程的方式，不再需要在主进程中
 *   常驻监听RDMA连接。子进程在需要时临时创建RDMA服务器。
 *
 * 兼容性:
 *   保留此函数以便现有代码编译通过，sync_module_init()仍可调用
 */
int rdma_sync_server_init(uint16_t listen_port) {
    (void)listen_port; /* 参数未使用，避免编译警告 */
    kvs_logInfo("[方案C] rdma_sync_server_init: 常驻服务器已废弃，使用TCP触发fork\n");
    return 0; /* 假装成功，实际功能由子进程实现 */
}

/**
 * @brief 【方案C废弃】停止 RDMA 同步服务器
 *
 * 原功能: 停止常驻服务器，断开所有从节点
 * 新行为: 直接返回，不执行任何操作
 *
 * 原因:
 *   方案C中子进程退出即自动清理资源，无需显式停止。
 *   每个同步会话由独立子进程处理，资源随进程终止而释放。
 */
void rdma_sync_server_stop(void) {
    /* 方案C: 子进程自动清理，此函数为空操作 */
    kvs_logInfo("[方案C] rdma_sync_server_stop: 子进程自动清理，无需显式停止\n");
}

/* ============================================================================
 * 方案C子进程服务器实现 (TCP-Triggered Fork)
 * ============================================================================
 *
 * 核心设计:
 *   主进程收到RDMASYNC命令时fork子进程，子进程负责完整的RDMA同步会话。
 *   子进程通过继承的TCP fd发送控制信号，独立管理RDMA连接生命周期。
 */

/**
 * @brief 子进程关闭除指定fd外的所有文件描述符
 *
 * 功能: 防止子进程继承的fd与父进程冲突，特别是监听fd和epoll/io_uring fd
 *
 * 实现方式:
 *   遍历/proc/self/fd/目录，关闭所有非必要fd
 *
 * 注意:
 *   - 必须在fork后立即调用，在任何可能分配fd的操作之前
 *   - 保留stdin(0), stdout(1), stderr(2)以便日志输出
 *   - 保留dirfd(dir)本身，避免关闭正在遍历的目录
 *
 * @param keep_fd 需要保留的fd（TCP连接）
 */
static void child_close_all_fds_except(int keep_fd) {
    DIR *dir = opendir("/proc/self/fd");
    if (!dir) {
        /* 如果无法打开/proc/self/fd，使用备用方案：关闭常见范围的fd */
        for (int fd = 3; fd < 256; fd++) {
            if (fd != keep_fd) {
                close(fd);
            }
        }
        return;
    }

    struct dirent *entry;
    int dir_fd = dirfd(dir); /* 获取目录自身的fd，避免关闭 */

    while ((entry = readdir(dir)) != NULL) {
        /* 跳过.和.. */
        if (entry->d_name[0] == '.') continue;

        int fd = atoi(entry->d_name);

        /* 保留标准IO、keep_fd和目录fd */
        if (fd > 2 && fd != keep_fd && fd != dir_fd) {
            close(fd);
        }
    }

    closedir(dir);
}

/**
 * @brief 子进程通过TCP发送响应到从节点
 *
 * 功能: 在RDMA准备就绪或完成时通过原TCP连接通知从节点
 *
 * 协议格式:
 *   +RDMA_READY <rdma_port>\r\n  - RDMA服务器就绪，携带动态端口
 *   +RDMA_DONE\r\n              - 同步完成
 *   -ERR <message>\r\n          - 发生错误
 *
 * @param tcp_fd    从节点TCP连接fd
 * @param status    状态字符串 ("RDMA_READY", "RDMA_DONE", "ERR")
 * @param rdma_port RDMA端口(status为RDMA_READY时有效，否则为0)
 * @return 0成功，-1失败
 */
static int child_send_tcp_response(int tcp_fd, const char *status,
                                    uint16_t rdma_port) {
    char buf[128];
    int len;

    if (strcmp(status, "RDMA_READY") == 0 && rdma_port > 0) {
        len = snprintf(buf, sizeof(buf), "+RDMA_READY %d\r\n", rdma_port);
    } else if (strcmp(status, "RDMA_DONE") == 0) {
        len = snprintf(buf, sizeof(buf), "+RDMA_DONE\r\n");
    } else if (strcmp(status, "ERR") == 0) {
        len = snprintf(buf, sizeof(buf), "-ERR RDMA sync failed\r\n");
    } else {
        len = snprintf(buf, sizeof(buf), "+%s\r\n", status);
    }

    /* 确保完整发送 */
    int sent = 0;
    while (sent < len) {
        int n = write(tcp_fd, buf + sent, len - sent);
        if (n < 0) {
            if (errno == EINTR) continue;
            kvs_logError("[子进程] 发送TCP响应失败: %s\n", strerror(errno));
            return -1;
        }
        sent += n;
    }

    return 0;
}

/**
 * @brief 【方案C核心】子进程RDMA服务器主函数
 *
 * 功能: 处理单个从节点的完整RDMA存量同步会话
 *
 * 生命周期:
 *   1. fork后立即调用此函数
 *   2. 完成同步后exit()，不返回父进程
 *
 * @param tcp_fd      从节点TCP连接fd（用于控制信号）
 * @param engine_type 需要同步的引擎类型（预留扩展）
 * @return 不会返回，通过exit(code)结束进程
 */
int rdma_sync_child_server(int tcp_fd, rdma_engine_type_t engine_type) {
    /*
     * ============================================================
     * 阶段1: 进程隔离与fd清理
     * ============================================================
     */
    kvs_logInfo("[子进程] RDMA子服务器启动，处理引擎类型: %d\n", engine_type);

    /* 关闭继承的所有fd，只保留TCP连接fd */
    child_close_all_fds_except(tcp_fd);

    /* 忽略SIGCHLD，避免成为僵尸进程 */
    signal(SIGCHLD, SIG_IGN);

    /* 设置进程标题（便于ps查看） */
    // prctl(PR_SET_NAME, "kvstore-rdma-child", 0, 0, 0); /* Linux特有 */

    /*
     * ============================================================
     * 阶段2: 创建RDMA监听资源
     * ============================================================
     */
    struct rdma_event_channel *cm_channel = rdma_create_event_channel();
    if (!cm_channel) {
        kvs_logError("[子进程] 创建RDMA事件通道失败: %s (errno=%d)\n", strerror(errno), errno);
        child_send_tcp_response(tcp_fd, "ERR", 0);
        exit(1);
    }

    struct rdma_cm_id *listener = NULL;
    if (rdma_create_id(cm_channel, &listener, NULL, RDMA_PS_TCP)) {
        kvs_logError("[子进程] 创建RDMA ID失败\n");
        goto err_channel;
    }

    /* 绑定到任意地址，让系统分配动态端口 */
    struct sockaddr_in addr = {
        .sin_family = AF_INET,
        .sin_port = 0,  /* 0表示动态分配端口 */
        .sin_addr.s_addr = INADDR_ANY
    };

    if (rdma_bind_addr(listener, (struct sockaddr *)&addr)) {
        kvs_logError("[子进程] RDMA绑定失败: %s\n", strerror(errno));
        goto err_id;
    }

    if (rdma_listen(listener, 1)) {  /* 只接受1个连接 */
        kvs_logError("[子进程] RDMA监听失败\n");
        goto err_id;
    }

    /* 获取实际分配的端口 */
    struct sockaddr *local_addr = rdma_get_local_addr(listener);
    uint16_t rdma_port = ntohs(((struct sockaddr_in *)local_addr)->sin_port);
    kvs_logInfo("[子进程] RDMA服务器监听动态端口 %d\n", rdma_port);

    /*
     * ============================================================
     * 阶段3: 通知从节点RDMA就绪
     * ============================================================
     */
    if (child_send_tcp_response(tcp_fd, "RDMA_READY", rdma_port) < 0) {
        kvs_logError("[子进程] 发送READY通知失败\n");
        goto err_id;
    }

    /*
     * ============================================================
     * 阶段4: 等待从节点RDMA连接
     * ============================================================
     */
    struct rdma_cm_event *event;
    if (rdma_get_cm_event(cm_channel, &event)) {
        kvs_logError("[子进程] 获取CM事件失败\n");
        goto err_id;
    }

    if (event->event != RDMA_CM_EVENT_CONNECT_REQUEST) {
        kvs_logError("[子进程] 非连接请求事件: %d\n", event->event);
        rdma_ack_cm_event(event);
        goto err_id;
    }

    struct rdma_cm_id *cm_id = event->id;  /* 新连接的ID */
    rdma_ack_cm_event(event);

    /* 创建连接资源 */
    struct rdma_sync_context ctx = {0};
    ctx.cm_id = cm_id;

    if (setup_connection_resources(&ctx) < 0) {
        kvs_logError("[子进程] 设置连接资源失败\n");
        goto err_id;
    }

    /* 接受连接
     * 注意: 连接参数必须与客户端匹配
     * responder_resources >= initiator_depth */
    struct rdma_conn_param conn_param = {
        .responder_resources = 1,  /* 响应方资源 */
        .initiator_depth = 1,       /* 发起方深度 */
        .retry_count = 5,
        .rnr_retry_count = 5        /* RNR (Receiver Not Ready) 重试次数 */
    };

    if (rdma_accept(cm_id, &conn_param)) {
        kvs_logError("[子进程] RDMA接受连接失败\n");
        goto err_conn;
    }

    /* 等待连接建立 */
    if (rdma_get_cm_event(cm_channel, &event)) {
        goto err_conn;
    }

    if (event->event != RDMA_CM_EVENT_ESTABLISHED) {
        kvs_logError("[子进程] 连接建立失败: %d\n", event->event);
        rdma_ack_cm_event(event);
        goto err_conn;
    }
    rdma_ack_cm_event(event);

    kvs_logInfo("[子进程] RDMA连接已建立\n");
    kvs_logInfo("[子进程] QP状态检查: qp=%p, pd=%p, cq=%p\n",
                (void*)ctx.qp, (void*)ctx.pd, (void*)ctx.cq);

    /* 验证 QP 状态 */
    struct ibv_qp_attr qp_attr;
    struct ibv_qp_init_attr init_attr;
    if (ibv_query_qp(ctx.qp, &qp_attr, IBV_QP_STATE, &init_attr) == 0) {
        const char *state_str = "UNKNOWN";
        switch (qp_attr.qp_state) {
            case IBV_QPS_RESET: state_str = "RESET"; break;
            case IBV_QPS_INIT: state_str = "INIT"; break;
            case IBV_QPS_RTR: state_str = "RTR"; break;
            case IBV_QPS_RTS: state_str = "RTS"; break;
            case IBV_QPS_SQD: state_str = "SQD"; break;
            case IBV_QPS_SQE: state_str = "SQE"; break;
            case IBV_QPS_ERR: state_str = "ERR"; break;
            default: state_str = "UNKNOWN"; break;
        }
        kvs_logInfo("[子进程] QP状态: %s (%d)\n", state_str, qp_attr.qp_state);
        if (qp_attr.qp_state != IBV_QPS_RTS) {
            kvs_logError("[子进程] QP未处于RTS状态，无法传输数据!\n");
        }
    } else {
        kvs_logError("[子进程] 查询QP状态失败: %s\n", strerror(errno));
    }

    /*
     * ============================================================
     * 阶段5: 引擎同步循环
     * ============================================================
     *
     * 控制流循环:
     *   1. 接收从节点的 PREPARE 命令 (RDMA Send/Recv)
     *   2. 准备引擎快照 (ksfWrite -> mmap -> ibv_reg_mr)
     *   3. 发送 READY 响应，携带 MR 元数据 (addr, rkey, length)
     *   4. 等待从节点通过 RDMA Read 读取数据
     *   5. 接收 COMPLETE 命令
     *   6. 清理当前引擎资源，继续下一个
     *
     * 注意: setup_connection_resources 已经分配了 ctx.ctrl_buf 和 ctx.mr_ctrl
     *       并且已经预投递了接收请求，我们直接使用这些资源。
     */

    /* 确定要同步的引擎范围 */
    int engine_start = (engine_type == ENGINE_COUNT) ? 0 : engine_type;
    int engine_end = (engine_type == ENGINE_COUNT) ? ENGINE_COUNT : engine_type + 1;

    for (int eng = engine_start; eng < engine_end; eng++) {
        rdma_engine_type_t current_engine = (rdma_engine_type_t)eng;
        kvs_logInfo("[子进程] 等待同步引擎: %s\n",
                    rdma_sync_engine_name(current_engine));

        /*
         * 步骤1: 投递接收请求并等待 PREPARE 命令
         */
        struct ibv_wc wc;
        int ret;

        /* 投递接收请求以接收 PREPARE 命令 */
        memset(ctx.ctrl_buf, 0, sizeof(struct rdma_ctrl_msg));
        struct ibv_recv_wr recv_wr = {
            .wr_id = 1,
            .num_sge = 1,
            .sg_list = &(struct ibv_sge){
                .addr = (uint64_t)ctx.ctrl_buf,
                .length = sizeof(struct rdma_ctrl_msg),
                .lkey = ctx.mr_ctrl->lkey
            }
        };
        struct ibv_recv_wr *bad_recv_wr;
        kvs_logInfo("[子进程] 正在投递接收请求, qp=%p, buf=%p, lkey=%u\n",
                    (void*)ctx.qp, (void*)ctx.ctrl_buf, ctx.mr_ctrl->lkey);
        if (ibv_post_recv(ctx.qp, &recv_wr, &bad_recv_wr)) {
            kvs_logError("[子进程] 投递 PREPARE 接收请求失败: %s\n", strerror(errno));
            goto err_conn;
        }
        kvs_logInfo("[子进程] 接收请求投递成功, wr_id=1\n");

        kvs_logInfo("[子进程] 开始轮询 CQ 等待 PREPARE...\n");
        do {
            ret = ibv_poll_cq(ctx.cq, 1, &wc);
        } while (ret == 0);

        if (ret < 0 || wc.status != IBV_WC_SUCCESS) {
            kvs_logError("[子进程] 接收 PREPARE 失败, status=%d\n", wc.status);
            goto err_conn;
        }

        if (wc.opcode != IBV_WC_RECV) {
            kvs_logError("[子进程] 意外的 WC opcode: %d\n", wc.opcode);
            goto err_conn;
        }

        /* 解析控制消息 - 从 ctx.ctrl_buf 读取 */
        struct rdma_ctrl_msg *req = (struct rdma_ctrl_msg *)ctx.ctrl_buf;
        if (req->cmd != CTRL_CMD_PREPARE) {
            kvs_logError("[子进程] 预期 PREPARE 命令, 收到: %d\n", req->cmd);
            /* 重新投递接收请求并继续等待 */
            memset(ctx.ctrl_buf, 0, sizeof(struct rdma_ctrl_msg));
            struct ibv_recv_wr retry_wr = {
                .wr_id = 1,
                .num_sge = 1,
                .sg_list = &(struct ibv_sge){
                    .addr = (uint64_t)ctx.ctrl_buf,
                    .length = sizeof(struct rdma_ctrl_msg),
                    .lkey = ctx.mr_ctrl->lkey
                }
            };
            struct ibv_recv_wr *bad_retry_wr;
            ibv_post_recv(ctx.qp, &retry_wr, &bad_retry_wr);
            eng--;
            continue;
        }

        kvs_logInfo("[子进程] 收到 PREPARE 命令，引擎: %s\n",
                    rdma_sync_engine_name(req->engine_type));

        /*
         * 步骤2: 准备引擎数据
         */
        if (rdma_sync_master_prepare_engine(&ctx, current_engine) < 0) {
            kvs_logError("[子进程] 准备引擎 %s 失败\n",
                         rdma_sync_engine_name(current_engine));
            /* 发送错误响应 */
            struct rdma_ctrl_msg resp = {
                .cmd = CTRL_RESP_ERROR,
                .engine_type = current_engine,
                .error_code = 1
            };
            snprintf(resp.payload.error_msg, sizeof(resp.payload.error_msg),
                     "Failed to prepare engine %s", rdma_sync_engine_name(current_engine));
            goto err_conn;
        }

        /*
         * 步骤3: 发送 READY 响应
         */
        struct rdma_ctrl_msg resp = {
            .cmd = CTRL_RESP_READY,
            .engine_type = current_engine
        };
        resp.payload.buf_attr.addr = (uint64_t)ctx.data_buf;
        resp.payload.buf_attr.length = (uint32_t)ctx.data_buf_size;
        resp.payload.buf_attr.rkey = ctx.mr_data->rkey;

        /* 使用 ctx.ctrl_buf 作为发送缓冲区（复用） */
        memcpy(ctx.ctrl_buf, &resp, sizeof(resp));

        struct ibv_sge send_sge = {
            .addr = (uint64_t)ctx.ctrl_buf,
            .length = sizeof(resp),
            .lkey = ctx.mr_ctrl->lkey
        };
        struct ibv_send_wr send_wr = {
            .wr_id = 2,
            .opcode = IBV_WR_SEND,
            .send_flags = IBV_SEND_SIGNALED,
            .num_sge = 1,
            .sg_list = &send_sge
        };
        struct ibv_send_wr *bad_send_wr;

        if (ibv_post_send(ctx.qp, &send_wr, &bad_send_wr)) {
            kvs_logError("[子进程] 发送 READY 失败\n");
            rdma_sync_master_cleanup(&ctx);
            goto err_conn;
        }

        /* 等待发送完成 */
        do {
            ret = ibv_poll_cq(ctx.cq, 1, &wc);
        } while (ret == 0);

        if (ret < 0 || wc.status != IBV_WC_SUCCESS) {
            kvs_logError("[子进程] 发送 READY 完成失败\n");
            rdma_sync_master_cleanup(&ctx);
            goto err_conn;
        }

        kvs_logInfo("[子进程] 已发送 READY: addr=%lx, rkey=%u, len=%zu\n",
                    (uint64_t)ctx.data_buf, ctx.mr_data->rkey, ctx.data_buf_size);

        /*
         * 步骤4: 等待 COMPLETE 命令
         *
         * 从节点收到 READY 后会:
         *   1. 执行 RDMA Read 读取数据 (服务器端无感知)
         *   2. 解析并加载 KSF 数据
         *   3. 发送 COMPLETE 命令
         *
         * 注意: 如果 data_len == 0 (空引擎), 从节点仍会发送 COMPLETE
         */

        /* 重新投递接收请求 */
        memset(ctx.ctrl_buf, 0, sizeof(struct rdma_ctrl_msg));
        struct ibv_recv_wr complete_wr = {
            .wr_id = 1,
            .num_sge = 1,
            .sg_list = &(struct ibv_sge){
                .addr = (uint64_t)ctx.ctrl_buf,
                .length = sizeof(struct rdma_ctrl_msg),
                .lkey = ctx.mr_ctrl->lkey
            }
        };
        struct ibv_recv_wr *bad_complete_wr;
        if (ibv_post_recv(ctx.qp, &complete_wr, &bad_complete_wr)) {
            kvs_logError("[子进程] 重新投递接收请求失败\n");
            rdma_sync_master_cleanup(&ctx);
            goto err_conn;
        }

        /* 等待 COMPLETE */
        do {
            ret = ibv_poll_cq(ctx.cq, 1, &wc);
        } while (ret == 0);

        if (ret < 0 || wc.status != IBV_WC_SUCCESS) {
            kvs_logError("[子进程] 接收 COMPLETE 失败\n");
            rdma_sync_master_cleanup(&ctx);
            goto err_conn;
        }

        req = (struct rdma_ctrl_msg *)ctx.ctrl_buf;
        if (req->cmd != CTRL_CMD_COMPLETE) {
            kvs_logError("[子进程] 预期 COMPLETE 命令, 收到: %d\n", req->cmd);
            rdma_sync_master_cleanup(&ctx);
            goto err_conn;
        }

        kvs_logInfo("[子进程] 收到 COMPLETE 命令，引擎 %s 同步完成\n",
                    rdma_sync_engine_name(current_engine));

        /*
         * 步骤5: 清理当前引擎资源
         */
        rdma_sync_master_cleanup(&ctx);

        /* 检查是否是最后一个引擎，如果是则等待 DONE 命令 */
        if (eng == engine_end - 1) {
            /* 重新投递接收请求，等待 DONE 命令 */
            memset(ctx.ctrl_buf, 0, sizeof(struct rdma_ctrl_msg));
            struct ibv_recv_wr done_wr = {
                .wr_id = 1,
                .num_sge = 1,
                .sg_list = &(struct ibv_sge){
                    .addr = (uint64_t)ctx.ctrl_buf,
                    .length = sizeof(struct rdma_ctrl_msg),
                    .lkey = ctx.mr_ctrl->lkey
                }
            };
            struct ibv_recv_wr *bad_done_wr;
            if (ibv_post_recv(ctx.qp, &done_wr, &bad_done_wr)) {
                kvs_logError("[子进程] 投递接收请求失败\n");
                goto err_conn;
            }

            do {
                ret = ibv_poll_cq(ctx.cq, 1, &wc);
            } while (ret == 0);

            if (ret > 0 && wc.status == IBV_WC_SUCCESS && wc.opcode == IBV_WC_RECV) {
                req = (struct rdma_ctrl_msg *)ctx.ctrl_buf;
                if (req->cmd == CTRL_CMD_DONE) {
                    kvs_logInfo("[子进程] 收到 DONE 命令，所有引擎同步完成\n");
                }
            }
        }
    }

    /*
     * ============================================================
     * 阶段6: 完成同步，发送DONE通知
     * ============================================================
     */
    child_send_tcp_response(tcp_fd, "RDMA_DONE", 0);
    close(tcp_fd);

    /*
     * ============================================================
     * 阶段7: 清理资源并退出
     * ============================================================
     */
    rdma_disconnect(cm_id);
    cleanup_connection_resources(&ctx);
    rdma_destroy_id(cm_id);
    rdma_destroy_id(listener);
    rdma_destroy_event_channel(cm_channel);

    kvs_logInfo("[子进程] RDMA同步完成，退出\n");
    exit(0);  /* 子进程正常退出 */

err_conn:
    cleanup_connection_resources(&ctx);
    if (cm_id) rdma_destroy_id(cm_id);
err_id:
    if (listener) rdma_destroy_id(listener);
err_channel:
    if (cm_channel) rdma_destroy_event_channel(cm_channel);
    close(tcp_fd);
    exit(1);  /* 子进程异常退出 */
}

/**
 * @brief 【方案C新增】客户端通过TCP触发RDMA同步
 *
 * 功能: 先建立TCP连接发送RDMASYNC命令，触发主节点fork，
 *       然后连接动态分配的RDMA端口执行存量同步
 *
 * 调用时序:
 *   1. 从节点配置REPLICAOF或执行SYNC命令时调用
 *   2. 此函数内部调用rdma_sync_client_connect()建立RDMA连接
 *   3. 然后调用rdma_sync_perform_full_sync()执行数据同步
 *
 * @param master_host  主节点地址
 * @param master_port  主节点TCP端口
 * @param engine_type  需要同步的引擎类型
 * @return 0成功，-1失败
 */
int rdma_sync_client_start_via_tcp(const char *master_host,
                                    uint16_t master_port,
                                    rdma_engine_type_t engine_type) {
    /*
     * ============================================================
     * 阶段1: 建立TCP连接到主节点
     * ============================================================
     */
    int tcp_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (tcp_fd < 0) {
        kvs_logError("[客户端] 创建TCP socket失败\n");
        return -1;
    }

    struct sockaddr_in addr = {
        .sin_family = AF_INET,
        .sin_port = htons(master_port)
    };

    if (inet_pton(AF_INET, master_host, &addr.sin_addr) != 1) {
        kvs_logError("[客户端] 无效的主节点地址: %s\n", master_host);
        close(tcp_fd);
        return -1;
    }

    kvs_logInfo("[客户端] 连接主节点TCP %s:%d...\n", master_host, master_port);

    if (connect(tcp_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        kvs_logError("[客户端] TCP连接失败: %s\n", strerror(errno));
        close(tcp_fd);
        return -1;
    }

    kvs_logInfo("[客户端] TCP连接已建立\n");

    /*
     * ============================================================
     * 阶段2: 发送RDMASYNC命令
     * ============================================================
     */
    char cmd_buf[64];
    int cmd_len = snprintf(cmd_buf, sizeof(cmd_buf),
                           "*2\r\n$8\r\nRDMASYNC\r\n$1\r\n%d\r\n",
                           engine_type);

    if (write(tcp_fd, cmd_buf, cmd_len) != cmd_len) {
        kvs_logError("[客户端] 发送RDMASYNC命令失败\n");
        close(tcp_fd);
        return -1;
    }

    kvs_logInfo("[客户端] 已发送RDMASYNC命令\n");

    /*
     * ============================================================
     * 阶段3: 等待+RDMA_READY响应
     * ============================================================
     */
    char resp_buf[256];
    int total_read = 0;
    struct timeval tv = {.tv_sec = 10, .tv_usec = 0};  /* 10秒超时 */
    setsockopt(tcp_fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    /* 读取直到遇到\r\n */
    while (total_read < sizeof(resp_buf) - 1) {
        int n = read(tcp_fd, resp_buf + total_read, 1);
        if (n <= 0) {
            kvs_logError("[客户端] 等待RDMA_READY超时或错误\n");
            close(tcp_fd);
            return -1;
        }
        total_read += n;
        if (total_read >= 2 &&
            resp_buf[total_read-2] == '\r' &&
            resp_buf[total_read-1] == '\n') {
            break;
        }
    }
    resp_buf[total_read] = '\0';

    kvs_logInfo("[客户端] 收到响应: %s", resp_buf);

    /* 解析响应 */
    uint16_t rdma_port = 0;
    if (strncmp(resp_buf, "+FORKED", 7) == 0) {
        /* 主节点已fork，继续等待RDMA_READY */
        total_read = 0;
        while (total_read < sizeof(resp_buf) - 1) {
            int n = read(tcp_fd, resp_buf + total_read, 1);
            if (n <= 0) {
                kvs_logError("[客户端] 等待RDMA_READY超时\n");
                close(tcp_fd);
                return -1;
            }
            total_read += n;
            if (total_read >= 2 &&
                resp_buf[total_read-2] == '\r' &&
                resp_buf[total_read-1] == '\n') {
                break;
            }
        }
        resp_buf[total_read] = '\0';
        kvs_logInfo("[客户端] 收到响应: %s", resp_buf);
    }

    if (sscanf(resp_buf, "+RDMA_READY %hu", &rdma_port) != 1 || rdma_port == 0) {
        kvs_logError("[客户端] 无效的RDMA_READY响应: %s\n", resp_buf);
        close(tcp_fd);
        return -1;
    }

    kvs_logInfo("[客户端] 主节点RDMA就绪，端口: %d\n", rdma_port);

    /*
     * ============================================================
     * 阶段4: 建立RDMA连接
     * ============================================================
     */
    if (rdma_sync_client_connect(master_host, rdma_port, engine_type) < 0) {
        kvs_logError("[客户端] RDMA连接失败\n");
        close(tcp_fd);
        return -1;
    }

    /*
     * ============================================================
     * 阶段5: 执行存量同步
     * ============================================================
     */
    kvs_logInfo("[客户端] 开始执行存量同步...\n");

    if (rdma_sync_perform_full_sync() < 0) {
        kvs_logError("[客户端] 存量同步失败\n");
        rdma_sync_client_disconnect();
        close(tcp_fd);
        return -1;
    }

    kvs_logInfo("[客户端] 存量同步完成\n");

    /*
     * ============================================================
     * 阶段6: 等待RDMA_DONE并关闭TCP连接
     * ============================================================
     */
    total_read = 0;
    while (total_read < sizeof(resp_buf) - 1) {
        int n = read(tcp_fd, resp_buf + total_read, 1);
        if (n <= 0) break;  /* 允许提前关闭 */
        total_read += n;
        if (total_read >= 2 &&
            resp_buf[total_read-2] == '\r' &&
            resp_buf[total_read-1] == '\n') {
            break;
        }
    }
    if (total_read > 0) {
        resp_buf[total_read] = '\0';
        kvs_logInfo("[客户端] 收到最终响应: %s", resp_buf);
    }

    close(tcp_fd);
    kvs_logInfo("[客户端] TCP控制连接已关闭\n");

    return 0;
}

/* ============================================================================
 * 从节点（Client）实现
 * ============================================================================ */

/**
 * @brief 初始化 RDMA 同步客户端
 *
 * 分配资源，但不建立连接
 */
int rdma_sync_client_init(void) {
    if (g_client_ctx) {
        kvs_logWarn("RDMA 客户端已初始化\n");
        return 0;
    }

    g_client_ctx = kvs_calloc(1, sizeof(struct rdma_client_context));
    if (!g_client_ctx) {
        kvs_logError("分配客户端上下文失败\n");
        return -1;
    }

    /* 先分配缓冲区，成功后初始化互斥锁
     * 这样如果分配失败，无需清理互斥锁 */
    g_client_ctx->full_sync_done = 0;
    g_client_ctx->state = SYNC_STATE_IDLE;
    g_client_ctx->cm_channel = NULL;
    g_client_ctx->cm_id = NULL;
    g_client_ctx->recv_buf = NULL;

    /* 预分配接收缓冲区（256MB） */
    g_client_ctx->recv_buf_size = RDMA_BUFFER_SIZE;
    g_client_ctx->recv_buf = aligned_alloc(4096, RDMA_BUFFER_SIZE);
    if (!g_client_ctx->recv_buf) {
        kvs_logError("分配接收缓冲区失败\n");
        free(g_client_ctx);
        g_client_ctx = NULL;
        return -1;
    }

    /* 缓冲区分配成功后才初始化互斥锁 */
    pthread_mutex_init(&g_client_ctx->cmd_queue_lock, NULL);

    kvs_logInfo("RDMA 同步客户端初始化完成\n");
    return 0;
}

/**
 * @brief 连接到主节点的 RDMA 服务
 *
 * 【方案C修改】添加 engine_type 参数
 * 用途: 记录当前同步的引擎类型，用于调试和后续扩展
 *
 * @param master_host  主节点地址
 * @param master_port  主节点RDMA端口（动态分配）
 * @param engine_type  【新增】目标引擎类型
 * @return 0 成功，-1 失败
 */
int rdma_sync_client_connect(const char *master_host,
                              uint16_t master_port,
                              rdma_engine_type_t engine_type) {
    if (!g_client_ctx) {
        kvs_logError("客户端未初始化\n");
        return -1;
    }

    strncpy(g_client_ctx->master_host, master_host,
            sizeof(g_client_ctx->master_host) - 1);
    g_client_ctx->master_rdma_port = master_port;

    /* 如果已有连接，先断开 */
    if (g_client_ctx->cm_id) {
        rdma_sync_client_disconnect();
    }

    /* 创建 CM 事件通道 */
    g_client_ctx->cm_channel = rdma_create_event_channel();
    if (!g_client_ctx->cm_channel) {
        kvs_logError("rdma_create_event_channel 失败: %s\n", strerror(errno));
        return -1;
    }

    /* 创建 CM ID */
    struct rdma_cm_id *cm_id;
    if (rdma_create_id(g_client_ctx->cm_channel, &cm_id, NULL, RDMA_PS_TCP)) {
        kvs_logError("rdma_create_id 失败: %s\n", strerror(errno));
        goto err_channel;
    }

    /* 解析地址 */
    struct sockaddr_in addr = {
        .sin_family = AF_INET,
        .sin_port = htons(master_port)
    };

    if (inet_pton(AF_INET, master_host, &addr.sin_addr) != 1) {
        kvs_logError("无效的地址: %s\n", master_host);
        goto err_id;
    }

    kvs_logInfo("正在解析 RDMA 地址: %s:%d\n", master_host, master_port);

    /* 发起连接 */
    if (rdma_resolve_addr(cm_id, NULL, (struct sockaddr *)&addr, 2000)) {
        kvs_logError("rdma_resolve_addr 失败: %s (errno=%d)\n", strerror(errno), errno);
        kvs_logError("提示: 如果使用本地环回(127.0.0.1)，请确保已加载 SoftRoCE 内核模块(rxe_rdma)\n");
        goto err_id;
    }

    /* 等待地址解析完成 */
    struct rdma_cm_event *event;
    if (rdma_get_cm_event(g_client_ctx->cm_channel, &event)) {
        kvs_logError("rdma_get_cm_event (addr) 失败: %s\n", strerror(errno));
        goto err_id;
    }

    if (event->event != RDMA_CM_EVENT_ADDR_RESOLVED) {
        kvs_logError("地址解析失败\n");
        rdma_ack_cm_event(event);
        goto err_id;
    }
    rdma_ack_cm_event(event);

    /* 解析路由 */
    kvs_logInfo("正在解析 RDMA 路由 (超时: 2秒)...\n");
    if (rdma_resolve_route(cm_id, 2000)) {
        kvs_logError("rdma_resolve_route 失败: %s (errno=%d)\n", strerror(errno), errno);
        kvs_logError("可能原因:\n");
        kvs_logError("  1. 未找到可用的 RDMA 设备 (检查 ibv_devices)\n");
        kvs_logError("  2. SoftRoCE 未正确配置 (尝试: rxe_cfg add eth0 或 rdma link add rxe0 type rxe netdev lo)\n");
        kvs_logError("  3. 网络不可达或防火墙阻止\n");
        goto err_id;
    }

    kvs_logInfo("等待路由解析事件...\n");
    if (rdma_get_cm_event(g_client_ctx->cm_channel, &event)) {
        kvs_logError("rdma_get_cm_event (route) 失败: %s\n", strerror(errno));
        goto err_id;
    }

    if (event->event != RDMA_CM_EVENT_ROUTE_RESOLVED) {
        kvs_logError("路由解析失败，事件类型: %d\n", event->event);
        switch (event->event) {
        case RDMA_CM_EVENT_ROUTE_ERROR:
            kvs_logError("原因: 路由错误\n");
            break;
        case RDMA_CM_EVENT_UNREACHABLE:
            kvs_logError("原因: 目标不可达\n");
            break;
        default:
            kvs_logError("原因: 未知事件\n");
            break;
        }
        rdma_ack_cm_event(event);
        goto err_id;
    }
    kvs_logInfo("路由解析成功\n");
    rdma_ack_cm_event(event);

    /* 创建连接资源（类似服务器端） */
    struct ibv_qp_init_attr qp_attr = {0};

    /* 1. 创建保护域 */
    g_client_ctx->pd = ibv_alloc_pd(cm_id->verbs);
    if (!g_client_ctx->pd) {
        kvs_logError("ibv_alloc_pd 失败\n");
        goto err_id;
    }

    /* 2. 创建完成事件通道 */
    g_client_ctx->comp_channel = ibv_create_comp_channel(cm_id->verbs);
    if (!g_client_ctx->comp_channel) {
        kvs_logError("ibv_create_comp_channel 失败\n");
        goto err_pd;
    }

    /* 3. 创建完成队列 */
    g_client_ctx->cq = ibv_create_cq(cm_id->verbs, RDMA_CQ_CAPACITY,
                                      NULL, g_client_ctx->comp_channel, 0);
    if (!g_client_ctx->cq) {
        kvs_logError("ibv_create_cq 失败\n");
        goto err_comp_channel;
    }

    /* 4. 请求 CQ 事件通知 */
    if (ibv_req_notify_cq(g_client_ctx->cq, 0)) {
        kvs_logError("ibv_req_notify_cq 失败\n");
        goto err_cq;
    }

    /* 5. 配置队列对 */
    qp_attr.qp_type = IBV_QPT_RC;
    qp_attr.sq_sig_all = 1;
    qp_attr.send_cq = g_client_ctx->cq;
    qp_attr.recv_cq = g_client_ctx->cq;
    qp_attr.cap.max_send_wr = RDMA_MAX_WR;
    qp_attr.cap.max_send_sge = RDMA_MAX_SGE;
    qp_attr.cap.max_recv_wr = RDMA_MAX_WR;
    qp_attr.cap.max_recv_sge = RDMA_MAX_SGE;

    /* 6. 创建 QP */
    if (rdma_create_qp(cm_id, g_client_ctx->pd, &qp_attr)) {
        kvs_logError("rdma_create_qp 失败\n");
        goto err_cq;
    }
    g_client_ctx->qp = cm_id->qp;

    /* 7. 分配控制消息缓冲区 */
    g_client_ctx->ctrl_send_buf = kvs_calloc(1, sizeof(struct rdma_ctrl_msg));
    g_client_ctx->ctrl_recv_buf = kvs_calloc(1, sizeof(struct rdma_ctrl_msg));
    if (!g_client_ctx->ctrl_send_buf || !g_client_ctx->ctrl_recv_buf) {
        kvs_logError("分配控制缓冲区失败\n");
        goto err_qp;
    }

    /* 8. 注册 MR
     * 发送缓冲区: 需要本地访问权限
     * 接收缓冲区: 需要 LOCAL_WRITE 权限以便网卡写入
     */
    g_client_ctx->mr_ctrl_send = ibv_reg_mr(g_client_ctx->pd,
                                            g_client_ctx->ctrl_send_buf,
                                            sizeof(struct rdma_ctrl_msg),
                                            IBV_ACCESS_LOCAL_WRITE);
    g_client_ctx->mr_ctrl_recv = ibv_reg_mr(g_client_ctx->pd,
                                            g_client_ctx->ctrl_recv_buf,
                                            sizeof(struct rdma_ctrl_msg),
                                            IBV_ACCESS_LOCAL_WRITE);
    if (!g_client_ctx->mr_ctrl_send || !g_client_ctx->mr_ctrl_recv) {
        kvs_logError("ibv_reg_mr 失败\n");
        goto err_ctrl_buf;
    }

    /* 9. 预投递接收请求 */
    struct ibv_sge sge = {
        .addr = (uint64_t)g_client_ctx->ctrl_recv_buf,
        .length = sizeof(struct rdma_ctrl_msg),
        .lkey = g_client_ctx->mr_ctrl_recv->lkey
    };
    struct ibv_recv_wr wr = {
        .wr_id = 0,
        .num_sge = 1,
        .sg_list = &sge
    };
    struct ibv_recv_wr *bad_wr;
    if (ibv_post_recv(g_client_ctx->qp, &wr, &bad_wr)) {
        kvs_logError("ibv_post_recv 失败\n");
        goto err_mr_ctrl;
    }

    /* 10. 发送连接请求
     * 注意: 连接参数必须与服务器端匹配 */
    struct rdma_conn_param conn_param = {
        .responder_resources = 1,
        .initiator_depth = 1,
        .retry_count = 5,
        .rnr_retry_count = 5
    };

    if (rdma_connect(cm_id, &conn_param)) {
        kvs_logError("rdma_connect 失败\n");
        goto err_mr_ctrl;
    }

    /* 11. 等待连接建立 */
    if (rdma_get_cm_event(g_client_ctx->cm_channel, &event)) {
        kvs_logError("rdma_get_cm_event (connect) 失败: %s\n", strerror(errno));
        goto err_mr_ctrl;
    }

    if (event->event != RDMA_CM_EVENT_ESTABLISHED) {
        kvs_logError("连接建立失败，事件类型: %d\n", event->event);
        switch (event->event) {
        case RDMA_CM_EVENT_ROUTE_ERROR:
            kvs_logError("原因: 路由错误 (ROUTE_ERROR)\n");
            break;
        case RDMA_CM_EVENT_UNREACHABLE:
            kvs_logError("原因: 目标不可达 (UNREACHABLE)\n");
            break;
        case RDMA_CM_EVENT_REJECTED:
            kvs_logError("原因: 连接被拒绝 (REJECTED)\n");
            break;
        case RDMA_CM_EVENT_CONNECT_ERROR:
            kvs_logError("原因: 连接错误 (CONNECT_ERROR)\n");
            break;
        default:
            kvs_logError("原因: 未知事件 %d\n", event->event);
            break;
        }
        rdma_ack_cm_event(event);
        goto err_mr_ctrl;
    }
    rdma_ack_cm_event(event);

    g_client_ctx->cm_id = cm_id;
    g_client_ctx->state = SYNC_STATE_CONNECTING;

    /* 【方案C新增】记录引擎类型，用于调试和后续扩展 */
    /* 注意: 当前仅在日志中使用，未来可支持单引擎同步 */
    (void)engine_type; /* 避免未使用警告，实际用于扩展 */
    kvs_logInfo("RDMA 连接已建立到 %s:%d (引擎类型: %d)\n",
                master_host, master_port, engine_type);

    /* 验证 QP 状态 */
    struct ibv_qp_attr query_attr;
    struct ibv_qp_init_attr query_init_attr;
    if (ibv_query_qp(g_client_ctx->qp, &query_attr, IBV_QP_STATE, &query_init_attr) == 0) {
        enum ibv_qp_state state = query_attr.qp_state;
        const char *state_str = "UNKNOWN";
        switch (state) {
            case IBV_QPS_RESET: state_str = "RESET"; break;
            case IBV_QPS_INIT: state_str = "INIT"; break;
            case IBV_QPS_RTR: state_str = "RTR"; break;
            case IBV_QPS_RTS: state_str = "RTS"; break;
            case IBV_QPS_SQD: state_str = "SQD"; break;
            case IBV_QPS_SQE: state_str = "SQE"; break;
            case IBV_QPS_ERR: state_str = "ERR"; break;
            default: state_str = "UNKNOWN"; break;
        }
        kvs_logInfo("[客户端] QP状态: %s (%d)\n", state_str, state);
        if (state != IBV_QPS_RTS) {
            kvs_logError("[客户端] QP未处于RTS状态，无法传输数据!\n");
        }
    } else {
        kvs_logError("[客户端] 查询QP状态失败: %s\n", strerror(errno));
    }

    /* 预注册接收缓冲区的 MR
     * 这样可以避免在大数据量传输时动态注册 MR 失败
     * 注册整个 256MB 缓冲区，使用 LOCAL_WRITE 权限 */
    g_client_ctx->mr_recv = ibv_reg_mr(g_client_ctx->pd,
                                        g_client_ctx->recv_buf,
                                        g_client_ctx->recv_buf_size,
                                        IBV_ACCESS_LOCAL_WRITE);
    if (!g_client_ctx->mr_recv) {
        kvs_logError("ibv_reg_mr (recv_buf) 失败: %s (errno=%d)\n",
                     strerror(errno), errno);
        kvs_logError("  尝试注册 %zu bytes 失败\n", g_client_ctx->recv_buf_size);
        goto err_mr_ctrl;
    }
    kvs_logInfo("[客户端] 接收缓冲区 MR 注册成功，lkey=%u，size=%zu\n",
                g_client_ctx->mr_recv->lkey, g_client_ctx->recv_buf_size);

    return 0;

err_mr_ctrl:
    /* 清理已注册的内存区域 */
    if (g_client_ctx->mr_ctrl_send) {
        ibv_dereg_mr(g_client_ctx->mr_ctrl_send);
        g_client_ctx->mr_ctrl_send = NULL;
    }
    if (g_client_ctx->mr_ctrl_recv) {
        ibv_dereg_mr(g_client_ctx->mr_ctrl_recv);
        g_client_ctx->mr_ctrl_recv = NULL;
    }
err_ctrl_buf:
    /* 清理控制缓冲区 */
    if (g_client_ctx->ctrl_send_buf) {
        kvs_free(g_client_ctx->ctrl_send_buf);
        g_client_ctx->ctrl_send_buf = NULL;
    }
    if (g_client_ctx->ctrl_recv_buf) {
        kvs_free(g_client_ctx->ctrl_recv_buf);
        g_client_ctx->ctrl_recv_buf = NULL;
    }
err_qp:
    /* 销毁队列对 - 只有在成功创建后才需要 */
    if (g_client_ctx->qp) {
        rdma_destroy_qp(cm_id);
        g_client_ctx->qp = NULL;
    }
err_cq:
    /* 销毁完成队列 */
    if (g_client_ctx->cq) {
        ibv_destroy_cq(g_client_ctx->cq);
        g_client_ctx->cq = NULL;
    }
err_comp_channel:
    /* 销毁完成事件通道 */
    if (g_client_ctx->comp_channel) {
        ibv_destroy_comp_channel(g_client_ctx->comp_channel);
        g_client_ctx->comp_channel = NULL;
    }
err_pd:
    /* 释放保护域 */
    if (g_client_ctx->pd) {
        ibv_dealloc_pd(g_client_ctx->pd);
        g_client_ctx->pd = NULL;
    }
err_id:
    /* 销毁CM ID - cm_id是局部变量，需要检查 */
    if (cm_id) {
        rdma_destroy_id(cm_id);
    }
err_channel:
    /* 销毁事件通道 */
    if (g_client_ctx && g_client_ctx->cm_channel) {
        rdma_destroy_event_channel(g_client_ctx->cm_channel);
        g_client_ctx->cm_channel = NULL;
    }
    return -1;
}

/**
 * @brief 发送控制消息
 *
 * 通过 RDMA Send 操作发送控制消息到对端
 */
int rdma_sync_send_ctrl_msg(struct rdma_client_context *ctx,
                            const struct rdma_ctrl_msg *msg) {
    if (!ctx || !ctx->qp) {
        kvs_logError("无效的上下文\n");
        return -1;
    }

    /* 复制消息到发送缓冲区 */
    memcpy(ctx->ctrl_send_buf, msg, sizeof(struct rdma_ctrl_msg));

    /* 构建 SGE */
    struct ibv_sge sge = {
        .addr = (uint64_t)ctx->ctrl_send_buf,
        .length = sizeof(struct rdma_ctrl_msg),
        .lkey = ctx->mr_ctrl_send->lkey
    };

    /* 构建 Send WR */
    struct ibv_send_wr wr = {
        .wr_id = 0,
        .opcode = IBV_WR_SEND,
        .send_flags = IBV_SEND_SIGNALED,
        .num_sge = 1,
        .sg_list = &sge
    };

    /* 发送 */
    struct ibv_send_wr *bad_wr;
    kvs_logInfo("[客户端] ibv_post_send: qp=%p, buf=%p, lkey=%u, len=%zu\n",
                (void*)ctx->qp, (void*)ctx->ctrl_send_buf, ctx->mr_ctrl_send->lkey, sizeof(struct rdma_ctrl_msg));
    if (ibv_post_send(ctx->qp, &wr, &bad_wr)) {
        kvs_logError("ibv_post_send 失败: %s\n", strerror(errno));
        return -1;
    }
    kvs_logInfo("[客户端] ibv_post_send 成功, 开始轮询 CQ...\n");

    /* 等待发送完成 */
    struct ibv_wc wc;
    int ret;
    do {
        ret = ibv_poll_cq(ctx->cq, 1, &wc);
    } while (ret == 0);

    if (ret < 0 || wc.status != IBV_WC_SUCCESS) {
        kvs_logError("发送完成失败, status=%d (opcode=%d, wr_id=%lu)\n",
                     wc.status, wc.opcode, (unsigned long)wc.wr_id);
        kvs_logError("  可能原因: 对端接收队列空 / 连接断开 / QP错误\n");
        return -1;
    }
    kvs_logInfo("[客户端] 发送完成成功\n");

    return 0;
}

/**
 * @brief 接收控制消息
 *
 * 从 CQ 中轮询接收完成，获取控制消息
 */
int rdma_sync_recv_ctrl_msg(struct rdma_client_context *ctx,
                            struct rdma_ctrl_msg *msg,
                            int timeout_ms) {
    if (!ctx || !ctx->cq) {
        kvs_logError("无效的上下文\n");
        return -1;
    }

    struct ibv_wc wc;
    int ret;
    int waited = 0;

    /* 轮询等待接收完成 */
    do {
        ret = ibv_poll_cq(ctx->cq, 1, &wc);
        if (ret == 0) {
            /* 短暂休眠避免忙等 */
            usleep(1000);  /* 1ms */
            waited += 1;
            if (timeout_ms > 0 && waited >= timeout_ms) {
                kvs_logError("接收超时\n");
                return -1;
            }
        }
    } while (ret == 0);

    if (ret < 0) {
        kvs_logError("ibv_poll_cq 失败\n");
        return -1;
    }

    if (wc.status != IBV_WC_SUCCESS) {
        kvs_logError("接收失败, status=%d\n", wc.status);
        return -1;
    }

    /* 复制接收到的消息 */
    memcpy(msg, ctx->ctrl_recv_buf, sizeof(struct rdma_ctrl_msg));

    /* 重新投递接收请求 */
    struct ibv_sge sge = {
        .addr = (uint64_t)ctx->ctrl_recv_buf,
        .length = sizeof(struct rdma_ctrl_msg),
        .lkey = ctx->mr_ctrl_recv->lkey
    };
    struct ibv_recv_wr wr = {
        .wr_id = 0,
        .num_sge = 1,
        .sg_list = &sge
    };
    struct ibv_recv_wr *bad_wr;
    if (ibv_post_recv(ctx->qp, &wr, &bad_wr)) {
        kvs_logError("ibv_post_recv 失败\n");
        return -1;
    }

    return 0;
}

/**
 * @brief 执行 RDMA Read 操作
 *
 * 【修改】使用预注册的 MR (ctx->mr_recv)，避免大数据量时动态注册失败
 */
int rdma_sync_post_read(struct rdma_client_context *ctx,
                        uint64_t remote_addr,
                        uint32_t remote_rkey,
                        void *local_buf,
                        size_t length) {
    /* 使用预注册的 MR */
    if (!ctx->mr_recv) {
        kvs_logError("mr_recv 未注册\n");
        return -1;
    }

    /* 检查数据是否在预注册缓冲区内 */
    if (local_buf < ctx->recv_buf ||
        (char*)local_buf + length > (char*)ctx->recv_buf + ctx->recv_buf_size) {
        kvs_logError("local_buf 超出预注册缓冲区范围\n");
        return -1;
    }

    /* 构建 SGE，使用预注册 MR 的 lkey */
    struct ibv_sge sge = {
        .addr = (uint64_t)local_buf,
        .length = length,
        .lkey = ctx->mr_recv->lkey
    };

    /* 构建 RDMA Read WR */
    struct ibv_send_wr wr = {
        .wr_id = 1,                  /* 使用简单 wr_id */
        .opcode = IBV_WR_RDMA_READ,  /* RDMA Read 操作 */
        .send_flags = IBV_SEND_SIGNALED,
        .num_sge = 1,
        .sg_list = &sge,
        .wr.rdma.remote_addr = remote_addr,
        .wr.rdma.rkey = remote_rkey
    };

    struct ibv_send_wr *bad_wr;
    if (ibv_post_send(ctx->qp, &wr, &bad_wr)) {
        kvs_logError("ibv_post_send (RDMA Read) 失败: %s\n", strerror(errno));
        return -1;
    }

    return 0;
}

/**
 * @brief 等待工作完成
 */
int rdma_sync_poll_completion(struct rdma_client_context *ctx,
                              struct ibv_wc *wc,
                              int max_wc) {
    int ret;

    do {
        ret = ibv_poll_cq(ctx->cq, max_wc, wc);
    } while (ret == 0);  /* 忙等待，直到有完成事件 */

    if (ret < 0) {
        kvs_logError("ibv_poll_cq 失败\n");
        return -1;
    }

    /* 检查完成状态 */
    if (wc->status != IBV_WC_SUCCESS) {
        kvs_logError("Work Completion 失败，状态: %d (%s)\n",
                     wc->status, ibv_wc_status_str(wc->status));
        return -1;
    }

    /* 注意: 我们使用预注册的 MR，不需要在这里释放
     * 原来用于动态注册 MR 的清理代码已移除 */

    return ret;
}

/* ============================================================================
 * TCP 队列处理
 * ============================================================================ */

/*
 * 从 RESP 协议数据中提取整数（用于解析 *<argc> 和 $<len>）
 * @param p 指向数字开始位置的指针
 * @param end 指向数据结尾的指针
 * @param out_val 输出解析后的整数值
 * @return 成功返回下一个字符位置，失败返回 NULL
 */
static const char* parse_resp_integer(const char *p, const char *end, int *out_val) {
    /* 跳过空白字符 */
    while (p < end && isspace((unsigned char)*p)) p++;
    if (p >= end) return NULL;

    /* 解析数字 */
    int val = 0;
    int sign = 1;
    if (*p == '-') {
        sign = -1;
        p++;
    }
    if (p >= end || !isdigit((unsigned char)*p)) return NULL;

    while (p < end && isdigit((unsigned char)*p)) {
        val = val * 10 + (*p - '0');
        p++;
    }

    *out_val = val * sign;
    return p;
}

/*
 * 执行积压的 RESP 命令
 * 专用解析器，假设数据是完整的 RESP Array 格式，不处理 TCP 粘包
 *
 * 输入格式示例:
 *   *3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n
 *
 * @param data RESP 协议数据
 * @param len 数据长度
 * @return 0成功，-1失败
 */
static int rdma_sync_execute_cmd(const char *data, size_t len) {
    const char *p = data;
    const char *end = data + len;
    int ret = -1; /* 初始化为-1，确保所有路径都有确定的返回值 */

    /* 步骤1: 解析 Array 头部 (*<argc>\r\n) */
    if (p >= end || *p != '*') {
        kvs_logError("[TCP Queue] 无效 RESP: 期望 '*' 开头\n");
        return -1;
    }
    p++; /* 跳过 '*' */

    int argc = 0;
    p = parse_resp_integer(p, end, &argc);
    if (!p || argc <= 0 || argc > 8) { /* 最多支持 8 个参数 */
        kvs_logError("[TCP Queue] 无效 RESP: argc=%d\n", argc);
        return -1;
    }

    /* 跳过 \r\n */
    if (p + 2 > end || p[0] != '\r' || p[1] != '\n') {
        kvs_logError("[TCP Queue] 无效 RESP: 期望 \\r\\n 在 argc 后\n");
        return -1;
    }
    p += 2;

    /* 步骤2: 解析每个 Bulk String 参数 ($<len>\r\n<data>\r\n) */
    char *argv[8] = {NULL};  /* 最多 8 个参数 */
    size_t argv_len[8] = {0};
    int i;

    for (i = 0; i < argc; i++) {
        if (p >= end || *p != '$') {
            kvs_logError("[TCP Queue] 无效 RESP: 期望 '$' 开头 (arg %d)\n", i);
            goto cleanup;
        }
        p++; /* 跳过 '$' */

        int str_len = 0;
        p = parse_resp_integer(p, end, &str_len);
        if (!p || str_len < 0) {
            kvs_logError("[TCP Queue] 无效 RESP: 无效长度 (arg %d)\n", i);
            goto cleanup;
        }

        /* 跳过 \r\n */
        if (p + 2 > end || p[0] != '\r' || p[1] != '\n') {
            kvs_logError("[TCP Queue] 无效 RESP: 期望 \\r\\n 在长度后 (arg %d)\n", i);
            goto cleanup;
        }
        p += 2;

        /* 检查数据是否完整 */
        if (p + str_len + 2 > end) {
            kvs_logError("[TCP Queue] 无效 RESP: 数据不完整 (arg %d)\n", i);
            goto cleanup;
        }

        /* 复制参数数据（引擎需要以 null 结尾的字符串） */
        argv[i] = kvs_malloc(str_len + 1);
        if (!argv[i]) {
            kvs_logError("[TCP Queue] 内存分配失败\n");
            goto cleanup;
        }
        memcpy(argv[i], p, str_len);
        argv[i][str_len] = '\0';
        argv_len[i] = str_len;

        p += str_len;

        /* 跳过 \r\n */
        if (p[0] != '\r' || p[1] != '\n') {
            kvs_logError("[TCP Queue] 无效 RESP: 期望 \\r\\n 在数据后 (arg %d)\n", i);
            goto cleanup;
        }
        p += 2;
    }

    /* 步骤3: 执行命令 */
    if (argc < 1) {
        kvs_logError("[TCP Queue] 无效命令: 空参数\n");
        goto cleanup;
    }

    const char *cmd = argv[0];
    /* ret已在函数开头初始化为-1 */

    kvs_logDebug("[TCP Queue] 执行命令: %s, argc=%d\n", cmd, argc);

#if ENABLE_MULTI_ENGINE
    /* 多引擎模式: 根据命令前缀选择引擎 */
    if (strcasecmp(cmd, "SET") == 0 && argc >= 3) {
        ret = kvs_rbtree_set(&rbtree_engine,
                             &(robj){.ptr = argv[1], .len = argv_len[1]},
                             &(robj){.ptr = argv[2], .len = argv_len[2]});
    } else if (strcasecmp(cmd, "GET") == 0 && argc >= 2) {
        char *val = kvs_rbtree_get(&rbtree_engine,
                                   &(robj){.ptr = argv[1], .len = argv_len[1]});
        if (val) kvs_free(val); /* 引擎返回的数据需要释放 */
        ret = (val != NULL) ? 0 : -1;
    } else if (strcasecmp(cmd, "DEL") == 0 && argc >= 2) {
        ret = kvs_rbtree_del(&rbtree_engine,
                             &(robj){.ptr = argv[1], .len = argv_len[1]});
    } else if (strcasecmp(cmd, "MOD") == 0 && argc >= 3) {
        ret = kvs_rbtree_mod(&rbtree_engine,
                             &(robj){.ptr = argv[1], .len = argv_len[1]},
                             &(robj){.ptr = argv[2], .len = argv_len[2]});
    }
    /* Array 引擎命令 */
    else if (strcasecmp(cmd, "ASET") == 0 && argc >= 3) {
        ret = kvs_array_set(&array_engine,
                            &(robj){.ptr = argv[1], .len = argv_len[1]},
                            &(robj){.ptr = argv[2], .len = argv_len[2]});
    } else if (strcasecmp(cmd, "ADEL") == 0 && argc >= 2) {
        ret = kvs_array_del(&array_engine,
                            &(robj){.ptr = argv[1], .len = argv_len[1]});
    } else if (strcasecmp(cmd, "AMOD") == 0 && argc >= 3) {
        ret = kvs_array_mod(&array_engine,
                            &(robj){.ptr = argv[1], .len = argv_len[1]},
                            &(robj){.ptr = argv[2], .len = argv_len[2]});
    }
    /* Hash 引擎命令 */
    else if (strcasecmp(cmd, "HSET") == 0 && argc >= 3) {
        ret = kvs_hash_set(&hash_engine,
                           &(robj){.ptr = argv[1], .len = argv_len[1]},
                           &(robj){.ptr = argv[2], .len = argv_len[2]});
    } else if (strcasecmp(cmd, "HDEL") == 0 && argc >= 2) {
        ret = kvs_hash_del(&hash_engine,
                           &(robj){.ptr = argv[1], .len = argv_len[1]});
    } else if (strcasecmp(cmd, "HMOD") == 0 && argc >= 3) {
        ret = kvs_hash_mod(&hash_engine,
                           &(robj){.ptr = argv[1], .len = argv_len[1]},
                           &(robj){.ptr = argv[2], .len = argv_len[2]});
    }
    /* Skiplist 引擎命令 */
    else if (strcasecmp(cmd, "SSET") == 0 && argc >= 3) {
        ret = kvs_skiplist_set(&skiplist_engine,
                               &(robj){.ptr = argv[1], .len = argv_len[1]},
                               &(robj){.ptr = argv[2], .len = argv_len[2]});
    } else if (strcasecmp(cmd, "SDEL") == 0 && argc >= 2) {
        ret = kvs_skiplist_del(&skiplist_engine,
                               &(robj){.ptr = argv[1], .len = argv_len[1]});
    } else if (strcasecmp(cmd, "SMOD") == 0 && argc >= 3) {
        ret = kvs_skiplist_mod(&skiplist_engine,
                               &(robj){.ptr = argv[1], .len = argv_len[1]},
                               &(robj){.ptr = argv[2], .len = argv_len[2]});
    }
#else
    /* 单引擎模式 */
    if (strcasecmp(cmd, "SET") == 0 && argc >= 3) {
        ret = kvs_main_set(&global_main_engine,
                           &(robj){.ptr = argv[1], .len = argv_len[1]},
                           &(robj){.ptr = argv[2], .len = argv_len[2]});
    } else if (strcasecmp(cmd, "GET") == 0 && argc >= 2) {
        char *val = kvs_main_get(&global_main_engine,
                                 &(robj){.ptr = argv[1], .len = argv_len[1]});
        if (val) kvs_free(val);
        ret = (val != NULL) ? 0 : -1;
    } else if (strcasecmp(cmd, "DEL") == 0 && argc >= 2) {
        ret = kvs_main_del(&global_main_engine,
                           &(robj){.ptr = argv[1], .len = argv_len[1]});
    } else if (strcasecmp(cmd, "MOD") == 0 && argc >= 3) {
        ret = kvs_main_mod(&global_main_engine,
                           &(robj){.ptr = argv[1], .len = argv_len[1]},
                           &(robj){.ptr = argv[2], .len = argv_len[2]});
    }
#endif
    else {
        kvs_logWarn("[TCP Queue] 未知或不支持的命令: %s\n", cmd);
        ret = -1; /* 明确设置返回值，避免编译器警告 */
    }

    if (ret != 0) {
        kvs_logDebug("[TCP Queue] 命令执行失败: %s (ret=%d)\n", cmd, ret);
    }

    /* 步骤4: 清理分配的内存 */
cleanup:
    for (i = 0; i < argc; i++) {
        if (argv[i]) {
            kvs_free(argv[i]);
        }
    }

    return (ret == 0) ? 0 : -1;
}

void rdma_sync_enqueue_tcp_cmd(const char *data, size_t len) {
    if (!g_client_ctx) {
        return;
    }

    /* 如果存量已完成，直接处理 */
    if (g_client_ctx->full_sync_done) {
        /* 直接执行命令 */
        return;
    }

    /* 分配节点 */
    struct cmd_buffer *cmd = kvs_malloc(sizeof(struct cmd_buffer));
    if (!cmd) {
        return;
    }

    cmd->data = kvs_malloc(len);
    if (!cmd->data) {
        kvs_free(cmd);
        return;
    }

    memcpy(cmd->data, data, len);
    cmd->len = len;
    cmd->next = NULL;

    /* 添加到队列 */
    pthread_mutex_lock(&g_client_ctx->cmd_queue_lock);

    if (g_client_ctx->cmd_queue_tail) {
        g_client_ctx->cmd_queue_tail->next = cmd;
    } else {
        g_client_ctx->cmd_queue_head = cmd;
    }
    g_client_ctx->cmd_queue_tail = cmd;

    pthread_mutex_unlock(&g_client_ctx->cmd_queue_lock);
}

/* ============================================================================
 * 【v3.0 废弃】旧积压队列处理函数
 *
 * 废弃原因：
 *   新架构使用 slave_sync.c 中的 g_backlog 链表队列，通过 eventfd
 *   通知主线程回放。此函数使用的 g_client_ctx->cmd_queue 已不再使用。
 *
 * 保留目的：
 *   兼容性，避免链接错误。函数内部仅打印日志，不执行实际操作。
 * ============================================================================ */
void rdma_sync_drain_tcp_queue(void) {
    if (!g_client_ctx) {
        return;
    }

    kvs_logWarn("[废弃] rdma_sync_drain_tcp_queue 被调用，请使用 slave_sync_drain_backlog\n");

    /* v3.0: 仅清理旧队列内存，不执行命令（命令已由主线程的 g_backlog 处理） */
    int count = 0;
    pthread_mutex_lock(&g_client_ctx->cmd_queue_lock);

    struct cmd_buffer *cmd = g_client_ctx->cmd_queue_head;
    while (cmd) {
        struct cmd_buffer *next = cmd->next;
        kvs_free(cmd->data);
        kvs_free(cmd);
        cmd = next;
        count++;
    }
    g_client_ctx->cmd_queue_head = NULL;
    g_client_ctx->cmd_queue_tail = NULL;

    pthread_mutex_unlock(&g_client_ctx->cmd_queue_lock);

    if (count > 0) {
        kvs_logWarn("[废弃] 清理 %d 条旧队列命令（未执行）\n", count);
    }
}

/* ============================================================================
 * 集成接口
 * ============================================================================ */

/**
 * @brief 执行完整的存量同步（从节点入口）
 */
int rdma_sync_perform_full_sync(void) {
    if (!g_client_ctx) {
        kvs_logError("客户端未初始化\n");
        return -1;
    }

    /* 引擎同步顺序：Array -> RBTree -> Hash -> SkipList */
    rdma_engine_type_t engines[] = {
        ENGINE_ARRAY, ENGINE_RBTREE, ENGINE_HASH, ENGINE_SKIPLIST
    };

    kvs_logInfo("开始 RDMA 存量同步...\n");
    g_client_ctx->state = SYNC_STATE_TRANSFERRING;

    for (int i = 0; i < 4; i++) {
        rdma_engine_type_t engine = engines[i];
        const char *engine_name = rdma_sync_engine_name(engine);

        kvs_logInfo("同步引擎 [%d/4]: %s\n", i + 1, engine_name);

        /* 1. 发送 PREPARE 命令 */
        struct rdma_ctrl_msg req = {
            .cmd = CTRL_CMD_PREPARE,
            .engine_type = engine
        };

        if (rdma_sync_send_ctrl_msg(g_client_ctx, &req) < 0) {
            kvs_logError("发送 PREPARE 失败\n");
            return -1;
        }

        /* 2. 等待 READY 响应 */
        struct rdma_ctrl_msg resp = {0};
        if (rdma_sync_recv_ctrl_msg(g_client_ctx, &resp, 60000) < 0) {
            kvs_logError("接收 READY 超时\n");
            return -1;
        }

        if (resp.cmd != CTRL_RESP_READY) {
            kvs_logError("主节点返回错误: %s\n", resp.payload.error_msg);
            return -1;
        }

        uint64_t remote_addr = resp.payload.buf_attr.addr;
        uint32_t remote_rkey = resp.payload.buf_attr.rkey;
        uint32_t data_len = resp.payload.buf_attr.length;

        kvs_logInfo("接收到 %s 快照元数据: addr=%lx, rkey=%u, len=%u\n",
                    engine_name, remote_addr, remote_rkey, data_len);

        if (data_len > 0) {
            /* 3. 分段接收数据（支持大数据量）
             * 使用流式 KSF 解析器，边接收边解析 */
            uint32_t total_received = 0;
            uint32_t remaining = data_len;

            /* 初始化流式解析器 */
            struct ksf_stream_parser *parser = ksf_stream_parser_init(engine);
            if (!parser) {
                kvs_logError("流式解析器初始化失败\n");
                return -1;
            }

            if (data_len > g_client_ctx->recv_buf_size) {
                kvs_logInfo("数据 %u bytes 超过缓冲区 %zu bytes，使用分段传输\n",
                            data_len, g_client_ctx->recv_buf_size);
            }

            g_client_ctx->state = SYNC_STATE_TRANSFERRING;

            while (remaining > 0) {
                uint32_t chunk_size = (remaining > g_client_ctx->recv_buf_size)
                                      ? g_client_ctx->recv_buf_size
                                      : remaining;

                kvs_logInfo("RDMA Read 分段: offset=%u, chunk=%u, remaining=%u\n",
                            total_received, chunk_size, remaining);

                /* 4. 执行 RDMA Read */
                if (rdma_sync_post_read(g_client_ctx,
                                        remote_addr + total_received,
                                        remote_rkey,
                                        g_client_ctx->recv_buf,
                                        chunk_size) < 0) {
                    kvs_logError("RDMA Read 失败 (offset=%u, chunk=%u)\n",
                                 total_received, chunk_size);
                    ksf_stream_parser_free(parser);
                    return -1;
                }

                /* 5. 等待 Read 完成 */
                struct ibv_wc wc;
                if (rdma_sync_poll_completion(g_client_ctx, &wc, 1) < 0) {
                    kvs_logError("等待 RDMA Read 完成失败\n");
                    ksf_stream_parser_free(parser);
                    return -1;
                }

                kvs_logInfo("RDMA Read 分段完成，读取 %u bytes\n", chunk_size);

                /* 6. 【流式解析】将数据块喂入解析器
                 * 解析器自动处理跨块的不完整 entry */
                g_client_ctx->state = SYNC_STATE_LOADING;

                if (ksf_stream_parser_feed(parser,
                                           g_client_ctx->recv_buf,
                                           chunk_size) < 0) {
                    kvs_logError("流式解析数据失败 (offset=%u)\n", total_received);
                    ksf_stream_parser_free(parser);
                    return -1;
                }

                g_client_ctx->state = SYNC_STATE_TRANSFERRING;

                total_received += chunk_size;
                remaining -= chunk_size;
            }

            /* 检查解析器是否完成 */
            if (ksf_stream_parser_finish(parser) < 0) {
                kvs_logError("流式解析器完成检查失败\n");
                ksf_stream_parser_free(parser);
                return -1;
            }

            kvs_logInfo("RDMA Read 全部完成，共解析 %lu 个 entry\n",
                        (unsigned long)ksf_stream_parser_get_count(parser));

            ksf_stream_parser_free(parser);
        }

        /* 7. 发送 COMPLETE 命令 */
        struct rdma_ctrl_msg complete = {
            .cmd = CTRL_CMD_COMPLETE,
            .engine_type = engine
        };
        rdma_sync_send_ctrl_msg(g_client_ctx, &complete);

        kvs_logInfo("引擎 %s 同步完成\n", engine_name);
    }

    /* 发送 DONE 命令 */
    struct rdma_ctrl_msg done = {
        .cmd = CTRL_CMD_DONE
    };
    rdma_sync_send_ctrl_msg(g_client_ctx, &done);

    g_client_ctx->state = SYNC_STATE_COMPLETE;
    g_client_ctx->full_sync_done = 1;

    kvs_logInfo("RDMA 存量同步全部完成！\n");

    /* -------------------------------------------------------------------------
     * 【v3.0 架构变更】积压队列处理已移至主线程
     *
     * 旧架构：在此处直接调用 rdma_sync_drain_tcp_queue() 处理积压
     * 新架构：通过 eventfd 通知主线程，由主线程调用 slave_sync_drain_backlog()
     *
     * 原因：
     *   1. 线程安全：积压队列 (g_backlog) 仅主线程访问，无锁设计
     *   2. 状态一致性：主线程检查 READY 状态后才回放，避免竞态
     *   3. 架构清晰：RDMA 线程只负责同步，命令执行回归主线程
     *
     * 实际流程：
     *   RDMA 线程完成 -> 写入 eventfd -> 主线程收到通知 -> 回放积压队列
     * ------------------------------------------------------------------------- */

    return 0;
}

/**
 * @brief 断开 RDMA 连接
 */
void rdma_sync_client_disconnect(void) {
    if (!g_client_ctx) {
        return;
    }

    /* 断开 RDMA 连接 - 只有状态为 CONNECTING 或更高时才需要断开
     * 如果连接从未建立，跳过断开操作以避免段错误 */
    if (g_client_ctx->cm_id && g_client_ctx->state >= SYNC_STATE_CONNECTING) {
        rdma_disconnect(g_client_ctx->cm_id);
    }

    /* 清理资源 - 按照依赖关系的逆序 */
    if (g_client_ctx->mr_ctrl_send) {
        ibv_dereg_mr(g_client_ctx->mr_ctrl_send);
        g_client_ctx->mr_ctrl_send = NULL;
    }
    if (g_client_ctx->mr_ctrl_recv) {
        ibv_dereg_mr(g_client_ctx->mr_ctrl_recv);
        g_client_ctx->mr_ctrl_recv = NULL;
    }
    if (g_client_ctx->mr_recv) {
        ibv_dereg_mr(g_client_ctx->mr_recv);
        g_client_ctx->mr_recv = NULL;
    }
    if (g_client_ctx->ctrl_send_buf) {
        kvs_free(g_client_ctx->ctrl_send_buf);
        g_client_ctx->ctrl_send_buf = NULL;
    }
    if (g_client_ctx->ctrl_recv_buf) {
        kvs_free(g_client_ctx->ctrl_recv_buf);
        g_client_ctx->ctrl_recv_buf = NULL;
    }
    if (g_client_ctx->recv_buf) {
        free(g_client_ctx->recv_buf);
        g_client_ctx->recv_buf = NULL;
    }
    if (g_client_ctx->qp) {
        rdma_destroy_qp(g_client_ctx->cm_id);
        g_client_ctx->qp = NULL;
    }
    if (g_client_ctx->cq) {
        ibv_destroy_cq(g_client_ctx->cq);
        g_client_ctx->cq = NULL;
    }
    if (g_client_ctx->comp_channel) {
        ibv_destroy_comp_channel(g_client_ctx->comp_channel);
        g_client_ctx->comp_channel = NULL;
    }
    if (g_client_ctx->pd) {
        ibv_dealloc_pd(g_client_ctx->pd);
        g_client_ctx->pd = NULL;
    }
    if (g_client_ctx->cm_id) {
        rdma_destroy_id(g_client_ctx->cm_id);
        g_client_ctx->cm_id = NULL;
    }
    /* 【修复】销毁事件通道 */
    if (g_client_ctx->cm_channel) {
        rdma_destroy_event_channel(g_client_ctx->cm_channel);
        g_client_ctx->cm_channel = NULL;
    }

    /* 只有当 recv_buf 不为 NULL 时，才表示互斥锁已被初始化
     * 这防止在初始化未完成时销毁未初始化的互斥锁 */
    if (g_client_ctx->recv_buf) {
        pthread_mutex_destroy(&g_client_ctx->cmd_queue_lock);
    }

    kvs_free(g_client_ctx);
    g_client_ctx = NULL;

    kvs_logInfo("RDMA 连接已断开\n");
}

/**
 * @brief 检查是否正在进行存量同步
 */
int rdma_sync_in_progress(void) {
    if (!g_client_ctx) {
        return 0;
    }
    return !g_client_ctx->full_sync_done;
}

/**
 * @brief 从内存缓冲区解码VLQ编码的整数
 * 与ksf.c中的decode_vlq保持一致
 */
static int decode_vlq_rdma(const uint8_t* input, uint64_t* value) {
    int count = 0;
    *value = 0;
    int shift = 0;

    do {
        *value |= ((uint64_t)(input[count] & 0x7F)) << shift;
        shift += 7;
    } while (input[count++] & 0x80);

    return count;
}

/**
 * @brief 从内存缓冲区加载引擎数据
 *
 * 解析KSF格式的缓冲区数据，加载到指定引擎
 * 实现参考自ksf.c中的ksfLoadToEngine
 */
int ksf_load_engine_from_buffer(int engine_type,
                                 void *buffer,
                                 size_t length) {
    if (!buffer || length == 0) {
        kvs_logInfo("缓冲区为空，无需加载\n");
        return 0;
    }

    kvs_logInfo("开始从缓冲区加载引擎 %s，大小 %zu bytes\n",
                rdma_sync_engine_name(engine_type), length);

    char *buf = (char *)buffer;
    size_t pos = 0;
    int count = 0;

    /* 解析KSF内容并恢复数据 */
    while (pos < length) {
        /* 解码键长度（VLQ格式） */
        if (pos >= length) break;
        uint64_t key_len;
        int key_len_bytes = decode_vlq_rdma((const uint8_t *)(buf + pos), &key_len);
        pos += key_len_bytes;

        /* 解码值长度（VLQ格式） */
        if (pos >= length) break;
        uint64_t val_len;
        int val_len_bytes = decode_vlq_rdma((const uint8_t *)(buf + pos), &val_len);
        pos += val_len_bytes;

        /* 读取键内容 */
        if (pos + key_len > length) break;
        robj key = {0};
        key.len = (key_len > 0) ? (key_len - 1) : 0;
        if (key_len > 0) {
            key.ptr = (char *)kvs_malloc(key_len);
            if (!key.ptr) {
                kvs_logError("无法分配内存来存储键\n");
                return -1;
            }
            memcpy(key.ptr, buf + pos, key_len);
            key.ptr[key_len - 1] = '\0';
            pos += key_len;
        }

        /* 读取值内容 */
        robj value = {0};
        value.len = (val_len > 0) ? (val_len - 1) : 0;
        if (val_len > 0) {
            if (pos + val_len > length) {
                if (key.ptr) kvs_free(key.ptr);
                return -1;
            }
            value.ptr = (char *)kvs_malloc(val_len);
            if (!value.ptr) {
                kvs_logError("无法分配内存来存储值\n");
                if (key.ptr) kvs_free(key.ptr);
                return -1;
            }
            memcpy(value.ptr, buf + pos, val_len);
            value.ptr[val_len - 1] = '\0';
            pos += val_len;
        }

        /* 根据引擎类型执行SET操作 */
#if ENABLE_MULTI_ENGINE
        switch (engine_type) {
        case ENGINE_ARRAY:
#if ENABLE_ARRAY
            kvs_array_set(&array_engine, &key, &value);
#endif
            break;
        case ENGINE_RBTREE:
#if ENABLE_RBTREE
            kvs_rbtree_set(&rbtree_engine, &key, &value);
#endif
            break;
        case ENGINE_HASH:
#if ENABLE_HASH
            kvs_hash_set(&hash_engine, &key, &value);
#endif
            break;
        case ENGINE_SKIPLIST:
#if ENABLE_SKIPLIST
            kvs_skiplist_set(&skiplist_engine, &key, &value);
#endif
            break;
        default:
            break;
        }
#else
        kvs_main_set(&global_main_engine, &key, &value);
#endif

        /* 释放分配的内存 */
        if (key.ptr) kvs_free(key.ptr);
        if (value.ptr) kvs_free(value.ptr);
        count++;
    }

    kvs_logInfo("引擎 %s 加载完成，共 %d 条记录\n",
                rdma_sync_engine_name(engine_type), count);
    return 0;
}
