/*
 * SYNC/REPLICAOF 命令实现
 * 集成 RDMA 存量同步到主命令处理流程
 */

#include "../../include/kvstore.h"
#include "../../include/kvs_rdma_sync.h"
#include "../../include/kvs_log.h"

#include <pthread.h>
#include <string.h>

/* 外部全局变量 */
extern struct rdma_client_context *g_client_ctx;
extern kv_config g_config;

/*
 * 从节点存量同步工作线程
 * 在后台执行 RDMA 存量同步
 */
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

/*
 * 启动从节点的存量同步流程
 * 在配置加载后或收到 REPLICAOF 命令时调用
 */
int start_slave_sync(void) {
    if (g_config.master_host[0] == '\0') {
        kvs_logWarn("[SYNC] 未配置主节点\n");
        return -1;
    }

    /* 初始化 RDMA 客户端 */
    if (rdma_sync_client_init() < 0) {
        kvs_logError("[SYNC] RDMA 客户端初始化失败\n");
        return -1;
    }

    /* 建立 RDMA 连接 */
    uint16_t rdma_port = g_config.master_port + RDMA_PORT_OFFSET;
    if (rdma_sync_client_connect(g_config.master_host, rdma_port) < 0) {
        kvs_logError("[SYNC] RDMA 连接失败\n");
        return -1;
    }

    /* 启动后台同步线程 */
    pthread_t sync_thread;
    if (pthread_create(&sync_thread, NULL, slave_sync_worker, NULL) != 0) {
        kvs_logError("[SYNC] 创建同步线程失败\n");
        rdma_sync_client_disconnect();
        return -1;
    }

    /* 分离线程，让其独立运行 */
    pthread_detach(sync_thread);

    kvs_logInfo("[SYNC] 存量同步已启动，主节点: %s:%d (RDMA: %d)\n",
                g_config.master_host, g_config.master_port, rdma_port);

    return 0;
}

/*
 * SYNC 命令处理函数
 *
 * 从节点：触发向主节点的存量同步
 * 主节点：返回错误（主节点不接受 SYNC 命令）
 */
int kvs_cmd_sync(struct conn *c) {
    /* 检查当前节点角色 */
    if (g_config.replica_mode == REPLICA_MODE_MASTER) {
        add_reply_error(c, "SYNC command not accepted on master");
        return 0;
    }

    /* 检查是否已配置主节点 */
    if (g_config.master_host[0] == '\0') {
        add_reply_error(c, "No master configured, use REPLICAOF first");
        return 0;
    }

    /* 检查是否已在同步中 */
    if (rdma_sync_in_progress()) {
        add_reply_error(c, "Sync already in progress");
        return 0;
    }

    /* 启动同步 */
    if (start_slave_sync() < 0) {
        add_reply_error(c, "Failed to start sync");
        return 0;
    }

    add_reply_status(c, "Background sync started");
    return 0;
}

/*
 * REPLICAOF 命令处理函数
 *
 * 语法: REPLICAOF <host> <port>
 *       REPLICAOF NO ONE  (取消主从关系)
 */
int kvs_cmd_replicaof(struct conn *c, int argc, struct robj *argv) {
    if (argc < 3) {
        add_reply_error(c, "Wrong number of arguments for 'replicaof' command");
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

        add_reply_status(c, "OK");
        kvs_logInfo("[SYNC] 已取消主从关系\n");
        return 0;
    }

    /* 验证参数 */
    if (port <= 0 || port > 65535) {
        add_reply_error(c, "Invalid port");
        return 0;
    }

    /* 检查是否已经是该主节点的从节点 */
    if (strcmp(g_config.master_host, host) == 0 &&
        g_config.master_port == port) {
        add_reply_error(c, "Already connected to specified master");
        return 0;
    }

    /* 更新配置 */
    strncpy(g_config.master_host, host, sizeof(g_config.master_host) - 1);
    g_config.master_port = port;
    g_config.replica_mode = REPLICA_MODE_SLAVE;

    /* 启动同步 */
    if (start_slave_sync() < 0) {
        /* 恢复配置 */
        g_config.master_host[0] = '\0';
        g_config.master_port = 0;
        g_config.replica_mode = REPLICA_MODE_NONE;

        add_reply_error(c, "Failed to start replication");
        return 0;
    }

    add_reply_status(c, "OK");
    kvs_logInfo("[SYNC] 已设置主节点: %s:%d\n", host, port);

    return 0;
}

/*
 * 初始化同步模块
 * 在主节点启动 RDMA 服务器，从节点准备客户端
 */
int sync_module_init(void) {
    /* 主节点：启动 RDMA 服务器 */
    if (g_config.replica_mode == REPLICA_MODE_MASTER) {
        uint16_t rdma_port = g_config.port + RDMA_PORT_OFFSET;
        if (rdma_sync_server_init(rdma_port) < 0) {
            kvs_logError("[SYNC] RDMA 服务器启动失败\n");
            return -1;
        }
        kvs_logInfo("[SYNC] 主节点 RDMA 服务器监听端口 %d\n", rdma_port);
    }
    /* 从节点：延迟到启动后或 REPLICAOF 命令时再连接 */

    return 0;
}

/*
 * 清理同步模块
 */
void sync_module_cleanup(void) {
    if (g_config.replica_mode == REPLICA_MODE_MASTER) {
        rdma_sync_server_stop();
    } else {
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
