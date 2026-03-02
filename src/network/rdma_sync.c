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
 * 全局变量
 * ============================================================================ */

/* 从节点全局客户端上下文 */
static struct rdma_client_context *g_client_ctx = NULL;

/* 主节点全局服务器状态 */
static struct rdma_event_channel *g_cm_channel = NULL;
static struct rdma_cm_id *g_listener = NULL;
static pthread_t g_server_poll_thread;
static volatile int g_server_running = 0;

/* 已连接的从节点链表（主节点端） */
static struct rdma_sync_context *g_slaves_head = NULL;
static pthread_mutex_t g_slaves_lock = PTHREAD_MUTEX_INITIALIZER;

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

    /* 9. 预投递接收请求（Receive WR）
     * 这样从节点一连接就可以发送控制消息 */
    struct ibv_sge sge = {
        .addr = (uint64_t)ctx->ctrl_buf,
        .length = sizeof(struct rdma_ctrl_msg),
        .lkey = ctx->mr_ctrl->lkey
    };

    struct ibv_recv_wr wr = {
        .wr_id = 0,
        .num_sge = 1,
        .sg_list = &sge
    };
    struct ibv_recv_wr *bad_wr;

    if (ibv_post_recv(ctx->qp, &wr, &bad_wr)) {
        kvs_logError("ibv_post_recv 失败\n");
        goto err_mr_ctrl;
    }

    kvs_logInfo("RDMA 连接资源创建成功\n");
    return 0;

err_mr_ctrl:
    ibv_dereg_mr(ctx->mr_ctrl);
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
        /* 空引擎也是合法的 */
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

/**
 * @brief 初始化 RDMA 同步服务器
 */
int rdma_sync_server_init(uint16_t listen_port) {
    /* 创建 CM 事件通道 */
    g_cm_channel = rdma_create_event_channel();
    if (!g_cm_channel) {
        kvs_logError("rdma_create_event_channel 失败\n");
        return -1;
    }

    /* 创建监听 ID */
    if (rdma_create_id(g_cm_channel, &g_listener, NULL, RDMA_PS_TCP)) {
        kvs_logError("rdma_create_id 失败\n");
        goto err_channel;
    }

    /* 绑定地址 */
    struct sockaddr_in addr = {
        .sin_family = AF_INET,
        .sin_port = htons(listen_port),
        .sin_addr.s_addr = INADDR_ANY
    };

    if (rdma_bind_addr(g_listener, (struct sockaddr *)&addr)) {
        kvs_logError("rdma_bind_addr 失败: %s\n", strerror(errno));
        goto err_id;
    }

    /* 开始监听 */
    if (rdma_listen(g_listener, 4)) {
        kvs_logError("rdma_listen 失败\n");
        goto err_id;
    }

    kvs_logInfo("RDMA 同步服务器监听端口 %d\n", listen_port);

    /* 启动轮询线程 */
    g_server_running = 1;
    if (pthread_create(&g_server_poll_thread, NULL, server_poll_loop, NULL)) {
        kvs_logError("创建轮询线程失败\n");
        goto err_id;
    }

    return 0;

err_id:
    rdma_destroy_id(g_listener);
    g_listener = NULL;
err_channel:
    rdma_destroy_event_channel(g_cm_channel);
    g_cm_channel = NULL;
    return -1;
}

/**
 * @brief 停止 RDMA 同步服务器
 */
void rdma_sync_server_stop(void) {
    g_server_running = 0;

    /* 等待轮询线程结束 */
    pthread_join(g_server_poll_thread, NULL);

    /* 断开所有从节点 */
    pthread_mutex_lock(&g_slaves_lock);
    while (g_slaves_head) {
        struct rdma_sync_context *ctx = g_slaves_head;
        g_slaves_head = ctx->next;

        rdma_disconnect(ctx->cm_id);
        cleanup_connection_resources(ctx);
        rdma_destroy_id(ctx->cm_id);
        free(ctx);
    }
    pthread_mutex_unlock(&g_slaves_lock);

    /* 清理监听资源 */
    if (g_listener) {
        rdma_destroy_id(g_listener);
        g_listener = NULL;
    }
    if (g_cm_channel) {
        rdma_destroy_event_channel(g_cm_channel);
        g_cm_channel = NULL;
    }

    kvs_logInfo("RDMA 同步服务器已停止\n");
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

    pthread_mutex_init(&g_client_ctx->cmd_queue_lock, NULL);
    g_client_ctx->full_sync_done = 0;
    g_client_ctx->state = SYNC_STATE_IDLE;

    /* 预分配接收缓冲区（256MB） */
    g_client_ctx->recv_buf_size = RDMA_BUFFER_SIZE;
    g_client_ctx->recv_buf = aligned_alloc(4096, RDMA_BUFFER_SIZE);
    if (!g_client_ctx->recv_buf) {
        kvs_logError("分配接收缓冲区失败\n");
        free(g_client_ctx);
        g_client_ctx = NULL;
        return -1;
    }

    kvs_logInfo("RDMA 同步客户端初始化完成\n");
    return 0;
}

/**
 * @brief 连接到主节点的 RDMA 服务
 */
int rdma_sync_client_connect(const char *master_host, uint16_t master_port) {
    if (!g_client_ctx) {
        kvs_logError("客户端未初始化\n");
        return -1;
    }

    strncpy(g_client_ctx->master_host, master_host,
            sizeof(g_client_ctx->master_host) - 1);
    g_client_ctx->master_rdma_port = master_port;

    /* 创建 CM 事件通道 */
    struct rdma_event_channel *cm_channel = rdma_create_event_channel();
    if (!cm_channel) {
        kvs_logError("rdma_create_event_channel 失败\n");
        return -1;
    }

    /* 创建 CM ID */
    struct rdma_cm_id *cm_id;
    if (rdma_create_id(cm_channel, &cm_id, NULL, RDMA_PS_TCP)) {
        kvs_logError("rdma_create_id 失败\n");
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

    /* 发起连接 */
    if (rdma_resolve_addr(cm_id, NULL, (struct sockaddr *)&addr, 2000)) {
        kvs_logError("rdma_resolve_addr 失败\n");
        goto err_id;
    }

    /* 等待地址解析完成 */
    struct rdma_cm_event *event;
    if (rdma_get_cm_event(cm_channel, &event)) {
        goto err_id;
    }

    if (event->event != RDMA_CM_EVENT_ADDR_RESOLVED) {
        kvs_logError("地址解析失败\n");
        rdma_ack_cm_event(event);
        goto err_id;
    }
    rdma_ack_cm_event(event);

    /* 解析路由 */
    if (rdma_resolve_route(cm_id, 2000)) {
        kvs_logError("rdma_resolve_route 失败\n");
        goto err_id;
    }

    if (rdma_get_cm_event(cm_channel, &event)) {
        goto err_id;
    }

    if (event->event != RDMA_CM_EVENT_ROUTE_RESOLVED) {
        kvs_logError("路由解析失败\n");
        rdma_ack_cm_event(event);
        goto err_id;
    }
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

    /* 8. 注册 MR */
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

    /* 10. 发送连接请求 */
    struct rdma_conn_param conn_param = {
        .responder_resources = 1,
        .initiator_depth = 1,
        .retry_count = 5
    };

    if (rdma_connect(cm_id, &conn_param)) {
        kvs_logError("rdma_connect 失败\n");
        goto err_mr_ctrl;
    }

    /* 11. 等待连接建立 */
    if (rdma_get_cm_event(cm_channel, &event)) {
        goto err_mr_ctrl;
    }

    if (event->event != RDMA_CM_EVENT_ESTABLISHED) {
        kvs_logError("连接建立失败\n");
        rdma_ack_cm_event(event);
        goto err_mr_ctrl;
    }
    rdma_ack_cm_event(event);

    g_client_ctx->cm_id = cm_id;
    g_client_ctx->state = SYNC_STATE_CONNECTING;

    kvs_logInfo("RDMA 连接已建立到 %s:%d\n", master_host, master_port);
    return 0;

err_mr_ctrl:
    if (g_client_ctx->mr_ctrl_send) ibv_dereg_mr(g_client_ctx->mr_ctrl_send);
    if (g_client_ctx->mr_ctrl_recv) ibv_dereg_mr(g_client_ctx->mr_ctrl_recv);
err_ctrl_buf:
    if (g_client_ctx->ctrl_send_buf) free(g_client_ctx->ctrl_send_buf);
    if (g_client_ctx->ctrl_recv_buf) free(g_client_ctx->ctrl_recv_buf);
err_qp:
    rdma_destroy_qp(cm_id);
err_cq:
    ibv_destroy_cq(g_client_ctx->cq);
err_comp_channel:
    ibv_destroy_comp_channel(g_client_ctx->comp_channel);
err_pd:
    ibv_dealloc_pd(g_client_ctx->pd);
err_id:
    rdma_destroy_id(cm_id);
err_channel:
    rdma_destroy_event_channel(cm_channel);
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
    if (ibv_post_send(ctx->qp, &wr, &bad_wr)) {
        kvs_logError("ibv_post_send 失败\n");
        return -1;
    }

    /* 等待发送完成 */
    struct ibv_wc wc;
    int ret;
    do {
        ret = ibv_poll_cq(ctx->cq, 1, &wc);
    } while (ret == 0);

    if (ret < 0 || wc.status != IBV_WC_SUCCESS) {
        kvs_logError("发送完成失败, status=%d\n", wc.status);
        return -1;
    }

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
 */
int rdma_sync_post_read(struct rdma_client_context *ctx,
                        uint64_t remote_addr,
                        uint32_t remote_rkey,
                        void *local_buf,
                        size_t length) {
    /* 注册本地接收缓冲区 */
    struct ibv_mr *mr = ibv_reg_mr(ctx->pd, local_buf, length,
                                   IBV_ACCESS_LOCAL_WRITE);
    if (!mr) {
        kvs_logError("ibv_reg_mr (recv) 失败\n");
        return -1;
    }

    /* 构建 SGE */
    struct ibv_sge sge = {
        .addr = (uint64_t)local_buf,
        .length = length,
        .lkey = mr->lkey
    };

    /* 构建 RDMA Read WR */
    struct ibv_send_wr wr = {
        .wr_id = (uint64_t)mr,      /* 保存 MR 指针以便后续释放 */
        .opcode = IBV_WR_RDMA_READ,  /* RDMA Read 操作 */
        .send_flags = IBV_SEND_SIGNALED,
        .num_sge = 1,
        .sg_list = &sge,
        .wr.rdma.remote_addr = remote_addr,
        .wr.rdma.rkey = remote_rkey
    };

    struct ibv_send_wr *bad_wr;
    if (ibv_post_send(ctx->qp, &wr, &bad_wr)) {
        kvs_logError("ibv_post_send (RDMA Read) 失败\n");
        ibv_dereg_mr(mr);
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

    /* 如果是 Send 操作，释放 MR */
    if (wc->opcode == IBV_WC_SEND) {
        struct ibv_mr *mr = (struct ibv_mr *)wc->wr_id;
        if (mr) {
            void *buf = mr->addr;
            ibv_dereg_mr(mr);
            free(buf);
        }
    }

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
    int ret = -1;

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

void rdma_sync_drain_tcp_queue(void) {
    if (!g_client_ctx) {
        return;
    }

    kvs_logInfo("开始处理积压的 TCP 命令...\n");

    int count = 0;

    while (1) {
        pthread_mutex_lock(&g_client_ctx->cmd_queue_lock);

        struct cmd_buffer *cmd = g_client_ctx->cmd_queue_head;
        if (!cmd) {
            pthread_mutex_unlock(&g_client_ctx->cmd_queue_lock);
            break;
        }

        g_client_ctx->cmd_queue_head = cmd->next;
        if (!g_client_ctx->cmd_queue_head) {
            g_client_ctx->cmd_queue_tail = NULL;
        }

        pthread_mutex_unlock(&g_client_ctx->cmd_queue_lock);

        /* 解析并执行命令 */
        /* 调用专用解析器执行积压命令，不处理 TCP 粘包，直接执行 */
        int ret = rdma_sync_execute_cmd(cmd->data, cmd->len);
        if (ret < 0) {
            kvs_logWarn("[TCP Queue] 命令执行失败 (%zu bytes)\n", cmd->len);
        } else {
            kvs_logDebug("[TCP Queue] 命令执行成功 (%zu bytes)\n", cmd->len);
        }

        kvs_free(cmd->data);
        kvs_free(cmd);
        count++;
    }

    kvs_logInfo("共处理 %d 条积压命令\n", count);
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
            /* 3. 确保接收缓冲区足够 */
            if (data_len > g_client_ctx->recv_buf_size) {
                kvs_logError("数据太大 (%u > %zu)\n",
                             data_len, g_client_ctx->recv_buf_size);
                return -1;
            }

            /* 4. 执行 RDMA Read */
            if (rdma_sync_post_read(g_client_ctx, remote_addr, remote_rkey,
                                    g_client_ctx->recv_buf, data_len) < 0) {
                kvs_logError("RDMA Read 失败\n");
                return -1;
            }

            /* 5. 等待 Read 完成 */
            struct ibv_wc wc;
            if (rdma_sync_poll_completion(g_client_ctx, &wc, 1) < 0) {
                kvs_logError("等待 RDMA Read 完成失败\n");
                return -1;
            }

            kvs_logInfo("RDMA Read 完成，读取 %u bytes\n", data_len);

            /* 6. 解析并加载 KSF 数据 */
            g_client_ctx->state = SYNC_STATE_LOADING;

            if (ksf_load_engine_from_buffer(engine,
                                             g_client_ctx->recv_buf,
                                             data_len) < 0) {
                kvs_logError("加载引擎 %s 失败\n", engine_name);
                return -1;
            }

            g_client_ctx->state = SYNC_STATE_TRANSFERRING;
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

    /* 处理积压的 TCP 命令 */
    rdma_sync_drain_tcp_queue();

    return 0;
}

/**
 * @brief 断开 RDMA 连接
 */
void rdma_sync_client_disconnect(void) {
    if (!g_client_ctx) {
        return;
    }

    if (g_client_ctx->cm_id) {
        rdma_disconnect(g_client_ctx->cm_id);
    }

    /* 清理资源 */
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

    pthread_mutex_destroy(&g_client_ctx->cmd_queue_lock);

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
