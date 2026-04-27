/*
 * SYNC/REPLICAOF 命令实现
 * 集成 RDMA 存量同步到主命令处理流程
 */

#include "../../include/kvstore.h"
#include "../../include/kvs_rdma_sync.h"
#include "../../include/kvs_log.h"

#include <pthread.h>
#include <string.h>
#include <errno.h>

/* 外部全局变量 */
extern struct rdma_client_context *g_client_ctx;
extern kv_config g_config;

/*
 * 【方案C废弃】从节点存量同步工作线程
 *
 * 原功能: 在后台执行 RDMA 存量同步
 * 废弃原因: 方案C使用同步阻塞的 rdma_sync_client_start_via_tcp()，
 *          无需后台线程。同步流程直接在调用线程中执行。
 *
 * 保留目的: 代码参考，如需异步执行可重新启用
 */
#if 0
static void* slave_sync_worker(void *arg) {
    kvs_logInfo("[SYNC] 从节点同步线程启动\n");

    /* 等待引擎初始化完成 */
    usleep(100000);  /* 100ms */

    /* 执行存量同步 */
    int ret = rdma_sync_perform_full_sync();
    if (ret < 0) {
        kvs_logError("[SYNC] 存量同步失败\n");
    } else {
        kvs_logInfo("[SYNC] 存量同步成功完成\n");
    }

    return NULL;
}
#endif

/*
 * 启动从节点的存量同步流程
 * 在配置加载后或收到 REPLICAOF 命令时调用
 *
 * 【新架构】多线程 RDMA 同步
 * - 主线程继续运行 io_uring，处理网络 I/O
 * - 创建 RDMA 线程执行存量同步（阻塞）
 * - 存量同步期间，mirror 命令入积压队列
 * - RDMA 完成后通过 eventfd 通知主线程处理积压
 */
int start_slave_sync(void) {
    if (g_config.master_host[0] == '\0') {
        kvs_logWarn("[SYNC] 未配置主节点\n");
        return -1;
    }

    /* 【新架构】初始化 RDMA 客户端 */
    if (rdma_sync_client_init() < 0) {
        kvs_logError("[SYNC] RDMA 客户端初始化失败\n");
        return -1;
    }

    /*
     * 【新架构】启动 RDMA 同步线程
     *
     * 主线程返回，继续处理 io_uring 事件
     * RDMA 线程在后台执行存量同步
     */
    extern int slave_sync_start(const char *master_host, uint16_t master_port);
    if (slave_sync_start(g_config.master_host, g_config.master_port) < 0) {
        kvs_logError("[SYNC] RDMA 同步线程启动失败\n");
        return -1;
    }

    kvs_logInfo("[SYNC] RDMA 同步线程已启动，主节点: %s:%d\n",
                g_config.master_host, g_config.master_port);
    kvs_logInfo("[SYNC] 存量同步期间收到的命令将入队，同步完成后自动执行\n");

    /*
     * 【方案C说明】同步流程已完成
     *
     * 注意: 原方案使用后台线程是因为RDMA连接是异步的。
     * 方案C中rdma_sync_client_start_via_tcp()是同步阻塞的，
     * 如果需要在后台执行，调用方应自行创建线程。
     *
     * 当前简化: 直接返回成功，同步已完成。
     * 如需后台执行，可将此调用包装在pthread中。
     */

    return 0;
}

/*
 * SYNC 命令处理函数
 *
 * 从节点：触发向主节点的存量同步
 * 主节点：返回错误（主节点不接受 SYNC 命令）
 */
int kvs_cmd_sync(reply_builder_t *rb) {
    /* 检查当前节点角色 */
    if (g_config.replica_mode == REPLICA_MODE_MASTER) {
        rb_add_reply_error(rb, "SYNC command not accepted on master");
        return 0;
    }

    /* 检查是否已配置主节点 */
    if (g_config.master_host[0] == '\0') {
        rb_add_reply_error(rb, "No master configured, use REPLICAOF first");
        return 0;
    }

    /* 检查是否已在同步中 */
    if (rdma_sync_in_progress()) {
        rb_add_reply_error(rb, "Sync already in progress");
        return 0;
    }

    /* 正常: 启动同步 */
    if (start_slave_sync() < 0) {
        rb_add_reply_error(rb, "Failed to start sync");
        return 0;
    }

    rb_add_reply_status(rb, "Background sync started");
    return 0;
}

/*
 * REPLICAOF 命令处理函数
 *
 * 语法: REPLICAOF <host> <port>
 *       REPLICAOF NO ONE  (取消主从关系)
 */
int kvs_cmd_replicaof(reply_builder_t *rb, int argc, robj *argv) {
    if (argc < 3) {
        rb_add_reply_error(rb, "Wrong number of arguments for 'replicaof' command");
        return 0;
    }

    /* 解析参数 */
    const char *host = argv[1].ptr;
    int port = atoi(argv[2].ptr);

    /* 检查是否为 "NO ONE"（取消主从） */
    if (strcasecmp(host, "NO") == 0 && strcasecmp(argv[2].ptr, "ONE") == 0) {
        /* 取消主从关系 */
        g_config.master_host[0] = '\0';
        g_config.master_port = 0;
        g_config.replica_mode = REPLICA_MODE_NONE;

        /* 断开 RDMA 连接 */
        rdma_sync_client_disconnect();

        rb_add_reply_status(rb, "OK");
        kvs_logInfo("[SYNC] 已取消主从关系\n");
        return 0;
    }

    /* 验证参数 */
    if (port <= 0 || port > 65535) {
        rb_add_reply_error(rb, "Invalid port");
        return 0;
    }

    /* 检查是否已经是该主节点的从节点 */
    if (strcmp(g_config.master_host, host) == 0 &&
        g_config.master_port == port) {
        rb_add_reply_error(rb, "Already connected to specified master");
        return 0;
    }

    /* 更新配置 */
    strncpy(g_config.master_host, host, sizeof(g_config.master_host) - 1);
    g_config.master_port = port;
    g_config.replica_mode = REPLICA_MODE_SLAVE;

    /* 立即启动同步 */
    if (start_slave_sync() < 0) {
        /* 恢复配置 */
        g_config.master_host[0] = '\0';
        g_config.master_port = 0;
        g_config.replica_mode = REPLICA_MODE_NONE;

        rb_add_reply_error(rb, "Failed to start replication");
        return 0;
    }

    rb_add_reply_status(rb, "OK");
    kvs_logInfo("[SYNC] 已设置主节点: %s:%d\n", host, port);

    return 0;
}

/*
 * 初始化同步模块
 *
 * 【方案C修改】主节点不再启动常驻RDMA服务器
 * 原行为: 主节点启动常驻RDMA监听线程
 * 新行为: 主节点仅做基础准备，RDMA服务器由TCP触发fork时创建
 */
int sync_module_init(void) {
    /*
     * 【方案C】主节点初始化
     *
     * 原代码:
     *   if (g_config.replica_mode == REPLICA_MODE_MASTER) {
     *       uint16_t rdma_port = g_config.port + RDMA_PORT_OFFSET;
     *       if (rdma_sync_server_init(rdma_port) < 0) ...
     *   }
     *
     * 新行为:
     *   rdma_sync_server_init()现在是一个空函数（直接返回0），
     *   但为了代码清晰，主节点在方案C下无需调用任何初始化。
     *
     *   真正的RDMA服务器由rdma_sync_child_server()在fork出的子进程中创建。
     */
    if (g_config.replica_mode == REPLICA_MODE_MASTER) {
        /*
         * 方案C: 主节点无需常驻RDMA服务器
         * 同步请求通过TCP命令触发，动态fork子进程处理
         */
        kvs_logInfo("[SYNC] 主节点同步模块初始化（方案C: TCP触发fork）\n");

        /* 初始化fork支持：必须在任何RDMA操作前调用，否则子进程无法使用RDMA */
        if (ibv_fork_init()) {
            kvs_logError("[SYNC] ibv_fork_init() 失败: %s\n", strerror(errno));
            return -1;
        }
        kvs_logInfo("[SYNC] ibv_fork_init() 成功\n");

        /* 可以在这里创建临时目录等准备工作 */
        /* mkdir(RDMA_SYNC_TEMP_DIR, 0755); */
    }

    /* 从节点：延迟到REPLICAOF/SYNC命令时再初始化连接 */

    return 0;
}

/*
 * 清理同步模块
 *
 * 【方案C修改】主节点无需显式清理RDMA服务器
 * 原行为: 主节点调用rdma_sync_server_stop()停止常驻服务器
 * 新行为: 子进程自动清理，主节点只需处理客户端断开
 */
void sync_module_cleanup(void) {
    if (g_config.replica_mode == REPLICA_MODE_MASTER) {
        /*
         * 【方案C】主节点无需显式停止RDMA服务器
         *
         * 原因:
         *   方案C中RDMA服务器运行在fork出的子进程中，
         *   子进程完成同步后自动exit()，资源由操作系统回收。
         *
         *   主进程无需（也无法）管理子进程的RDMA资源。
         *
         * 可选增强:
         *   如果需要优雅等待子进程，可以在这里发送信号或
         *   使用waitpid()收割已结束的子进程。
         */
        kvs_logInfo("[SYNC] 主节点清理（子进程自动退出，无需显式停止）\n");

        /* 可选: 收割所有已结束的子进程，避免僵尸进程 */
        /* while (waitpid(-1, NULL, WNOHANG) > 0); */
    } else {
        /* 从节点: 断开RDMA连接 */
        rdma_sync_client_disconnect();
    }
}

/*
 * 检查当前节点是否处于 LOADING 状态
 * 用于拒绝客户端查询命令
 */
int check_node_loading(void) {
    /* 如果是从节点且存量同步未完成，返回 LOADING */
    if (g_config.replica_mode == REPLICA_MODE_SLAVE) {
        return rdma_sync_in_progress();
    }
    return 0;
}
