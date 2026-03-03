/*
 * RDMA 存量同步模块头文件
 * 实现基于 RDMA Read 的主从全量数据同步
 */

#ifndef KVS_RDMA_SYNC_H
#define KVS_RDMA_SYNC_H

#include <stdint.h>
#include <pthread.h>
#include <rdma/rdma_cma.h>
#include <infiniband/verbs.h>

#include "kvstore.h"

/* ============================================================================
 * 常量定义
 * ============================================================================ */

/* RDMA 端口偏移量 */
#define RDMA_PORT_OFFSET    10000

/* 默认 RDMA 缓冲区大小：32MB（减小以避免内存分配失败）
 * 如果数据大于此大小，使用分段传输 */
#define RDMA_BUFFER_SIZE    (32 * 1024 * 1024)

/* 最大的 scatter-gather 元素数量 */
#define RDMA_MAX_SGE        1

/* 完成队列容量 */
#define RDMA_CQ_CAPACITY    16

/* 队列对最大未处理请求数 */
#define RDMA_MAX_WR         8

/* 临时文件路径模板 */
#define RDMA_SYNC_TEMP_DIR  "./data/sync_temp"
#define RDMA_SYNC_TEMP_FILE "sync_%s.ksf"  /* %s 替换为引擎名称 */

/* 控制消息最大长度 */
#define RDMA_CTRL_MSG_MAX   4096

/* ============================================================================
 * 枚举定义
 * ============================================================================ */

/* 同步状态 */
typedef enum {
    SYNC_STATE_IDLE = 0,        /* 空闲状态 */
    SYNC_STATE_CONNECTING,      /* 正在建立 RDMA 连接 */
    SYNC_STATE_PREPARE,         /* 准备阶段：等待主节点生成快照 */
    SYNC_STATE_TRANSFERRING,    /* 正在传输数据 */
    SYNC_STATE_LOADING,         /* 正在加载数据到引擎 */
    SYNC_STATE_COMPLETE,        /* 同步完成 */
    SYNC_STATE_ERROR            /* 发生错误 */
} rdma_sync_state_t;

/* 控制命令类型（主从节点之间通过 RDMA Send/Recv 交换控制信息） */
typedef enum {
    /* 从节点 -> 主节点 */
    CTRL_CMD_PREPARE = 1,       /* 请求准备指定引擎的快照 */
    CTRL_CMD_COMPLETE,          /* 通知当前引擎同步完成 */
    CTRL_CMD_DONE,              /* 通知所有引擎同步完成 */

    /* 主节点 -> 从节点 */
    CTRL_RESP_READY,            /* 快照准备就绪，携带元数据 */
    CTRL_RESP_ERROR             /* 发生错误 */
} rdma_ctrl_cmd_t;

/* 引擎类型（与控制命令配合使用） */
typedef enum {
    ENGINE_ARRAY = 0,
    ENGINE_RBTREE,
    ENGINE_HASH,
    ENGINE_SKIPLIST,
    ENGINE_COUNT
} rdma_engine_type_t;

/* ============================================================================
 * 数据结构定义
 * ============================================================================ */

/*
 * RDMA 内存缓冲区属性
 * 用于在控制通道上交换内存区域信息
 * 必须保证 packed，防止编译器填充影响网络传输
 */
struct __attribute__((packed)) rdma_buffer_attr {
    uint64_t    addr;           /* 内存区域起始地址（远程虚拟地址） */
    uint32_t    length;         /* 内存区域长度（字节） */
    uint32_t    rkey;           /* 远程访问密钥（Remote Key） */
};

/*
 * 控制消息结构
 * 通过 RDMA Send/Recv 传输
 */
struct __attribute__((packed)) rdma_ctrl_msg {
    uint8_t     cmd;            /* 命令类型：rdma_ctrl_cmd_t */
    uint8_t     engine_type;    /* 引擎类型：rdma_engine_type_t */
    uint16_t    error_code;     /* 错误码（仅在 CMD_ERROR 时有效） */
    uint32_t    padding;        /* 填充对齐 */

    union {
        /* CTRL_RESP_READY 时携带的元数据 */
        struct rdma_buffer_attr buf_attr;

        /* 错误信息 */
        char error_msg[128];
    } payload;
};

/*
 * 存量同步上下文
 * 每个从节点连接对应一个此结构（主节点端）
 */
struct rdma_sync_context {
    /* RDMA 连接资源 */
    struct rdma_cm_id       *cm_id;         /* RDMA 连接标识符 */
    struct ibv_pd           *pd;            /* 保护域 */
    struct ibv_cq           *cq;            /* 完成队列 */
    struct ibv_comp_channel *comp_channel;  /* 完成事件通道 */
    struct ibv_qp           *qp;            /* 队列对 */

    /* 内存区域 */
    struct ibv_mr           *mr_ctrl;       /* 控制消息 MR */
    struct ibv_mr           *mr_data;       /* 数据缓冲区 MR */

    /* 缓冲区 */
    void                    *ctrl_buf;      /* 控制消息缓冲区 */
    void                    *data_buf;      /* 数据缓冲区（mmap 区域） */
    size_t                   data_buf_size; /* 数据缓冲区大小 */

    /* 状态 */
    rdma_sync_state_t        state;         /* 当前同步状态 */
    rdma_engine_type_t       current_engine;/* 当前正在同步的引擎 */

    /* 临时文件 */
    int                      temp_fd;       /* 临时文件描述符 */
    char                     temp_path[256];/* 临时文件路径 */

    /* 统计信息 */
    uint64_t                 bytes_sent;    /* 已发送字节数 */
    uint64_t                 keys_count;    /* 已同步键数 */

    /* 链表（主节点管理多个从节点） */
    struct rdma_sync_context *next;
};

/*
 * 从节点端的 RDMA 客户端上下文
 */
struct rdma_client_context {
    /* RDMA 连接资源 */
    struct rdma_event_channel *cm_channel;  /* CM事件通道 */
    struct rdma_cm_id       *cm_id;
    struct ibv_pd           *pd;
    struct ibv_cq           *cq;
    struct ibv_comp_channel *comp_channel;
    struct ibv_qp           *qp;

    /* 内存区域 */
    struct ibv_mr           *mr_ctrl_send;  /* 发送控制消息的 MR */
    struct ibv_mr           *mr_ctrl_recv;  /* 接收控制消息的 MR */
    struct ibv_mr           *mr_recv;       /* 接收数据的 MR */

    /* 缓冲区 */
    struct rdma_ctrl_msg    *ctrl_send_buf; /* 发送缓冲区 */
    struct rdma_ctrl_msg    *ctrl_recv_buf; /* 接收缓冲区 */
    void                    *recv_buf;      /* 数据接收缓冲区 */
    size_t                   recv_buf_size;

    /* 连接信息 */
    char                     master_host[64];
    uint16_t                 master_rdma_port;

    /* 状态 */
    rdma_sync_state_t        state;
    pthread_t                sync_thread;   /* 同步工作线程 */

    /* 与 TCP 增量同步的协作 */
    pthread_mutex_t          cmd_queue_lock;/* 命令队列锁 */
    struct cmd_buffer       *cmd_queue_head;/* 命令队列头（mirror 数据暂存） */
    struct cmd_buffer       *cmd_queue_tail;/* 命令队列尾 */
    volatile int             full_sync_done;/* 存量同步完成标志 */
};

/*
 * 命令缓冲区（用于暂存 mirror 的 TCP 数据）
 */
struct cmd_buffer {
    char                    *data;          /* 命令数据 */
    size_t                   len;           /* 数据长度 */
    struct cmd_buffer       *next;          /* 下一个节点 */
};

/* ============================================================================
 * 主节点（Server）接口 - 方案C重构 (TCP-Triggered Fork)
 * ============================================================================
 *
 * 架构变更说明:
 *   原方案(方案A/B): 主节点启动时创建常驻RDMA监听线程，持续占用资源
 *   新方案(方案C): 收到TCP的RDMASYNC命令时fork子进程，子进程临时创建RDMA服务器
 *
 * 优势:
 *   1. 主进程完全无阻塞，无需处理RDMA事件
 *   2. 无常驻资源占用，仅在同步时分配
 *   3. 简化架构，避免io_uring与RDMA的并发冲突
 */

/*
 * 【方案C新增】子进程RDMA服务器主函数
 *
 * 功能: fork出的子进程调用此函数，完成单个子节点的RDMA存量同步
 *
 * 设计原理:
 *   - 子进程拥有父进程内存快照(COW)，数据一致性由fork保证
 *   - 子进程通过原TCP连接发送控制信号(+RDMA_READY, +RDMA_DONE)
 *   - 子进程独立管理RDMA连接生命周期，父进程零参与
 *
 * 参数:
 *   @tcp_fd: 从节点TCP连接fd，子进程通过此fd发送控制信号
 *            注意: 子进程接管此fd，父进程应立即放弃该连接
 *   @engine_type: 需要同步的引擎类型
 *                 当前实现: ENGINE_COUNT表示所有引擎循环同步
 *                 未来扩展: 可指定单个引擎进行部分同步
 *
 * 返回值:
 *   0  - 同步成功完成
 *   -1 - 同步过程中发生错误
 *
 * 子进程生命周期:
 *   1. 【fd清理】关闭除tcp_fd外的所有fd，避免与父进程冲突
 *   2. 【RDMA初始化】创建event_channel, cm_id, 绑定动态端口
 *   3. 【就绪通知】通过TCP发送"+RDMA_READY <rdma_port>\r\n"
 *   4. 【等待连接】接受从节点的RDMA连接请求
 *   5. 【数据同步】循环处理PREPARE/READY/READ/COMPLETE流程
 *   6. 【完成通知】发送"+RDMA_DONE\r\n"，关闭TCP连接
 *   7. 【资源清理】断开RDMA，释放MR，退出进程
 *
 * 父进程注意事项:
 *   - fork()返回后应立即回复"+FORKED"给客户端
 *   - 将连接fd标记为已移交，避免父进程关闭
 *   - 子进程退出时会发送SIGCHLD，可选择忽略或收割
 */
int rdma_sync_child_server(int tcp_fd, rdma_engine_type_t engine_type);

/*
 * 【废弃-方案A/B遗留】初始化 RDMA 同步服务器
 * 原因: 方案C使用TCP触发fork，无需常驻监听
 * 状态: 保留声明避免编译错误，但实现为空函数直接返回0
 */
int rdma_sync_server_init(uint16_t listen_port) __attribute__((deprecated("使用方案C: TCP触发fork")));

/*
 * 【废弃-方案A/B遗留】停止 RDMA 同步服务器
 * 原因: 子进程退出即自动清理，无需显式停止
 * 状态: 保留声明，实现为空函数
 */
void rdma_sync_server_stop(void) __attribute__((deprecated("使用方案C: 子进程自动清理")));

/*
 * 【废弃-方案A/B遗留】处理 RDMA 事件
 * 原因: 方案C子进程中使用阻塞式处理
 * 状态: 保留声明，实现为空函数
 */
void rdma_sync_server_poll(void) __attribute__((deprecated("使用方案C: 子进程阻塞处理")));

/*
 * 为主节点的存量同步准备指定引擎的数据
 * 将引擎数据序列化为 KSF 格式到内存缓冲区
 *
 * @param ctx: 同步上下文
 * @param engine_type: 引擎类型
 * @return: 0 成功，-1 失败
 *
 * 实现逻辑：
 * 1. 创建临时文件
 * 2. 调用 ksfWrite<Engine> 将引擎数据写入临时文件
 * 3. mmap 临时文件到内存
 * 4. 注册 mmap 区域为 RDMA MR
 * 5. 返回元数据给客户端
 */
int rdma_sync_master_prepare_engine(struct rdma_sync_context *ctx,
                                    rdma_engine_type_t engine_type);

/*
 * 清理主节点的同步资源
 * 卸载 MR，删除临时文件
 */
void rdma_sync_master_cleanup(struct rdma_sync_context *ctx);

/* ============================================================================
 * 从节点（Client）接口 - 方案C更新 (TCP-First连接)
 * ============================================================================
 *
 * 架构变更说明:
 *   原方案: 直接连接主节点的RDMA端口，主节点需常驻监听
 *   新方案: 先TCP连接，发送RDMASYNC命令触发fork，再连接动态分配的RDMA端口
 *
 * 连接流程对比:
 *   方案A/B: socket(RDMA) -> connect() -> 直接RDMA通信
 *   方案C:   socket(TCP) -> connect() -> send RDMASYNC
 *            recv(+RDMA_READY <port>) -> socket(RDMA) -> connect(<port>)
 */

/*
 * 【保留】初始化 RDMA 同步客户端
 *
 * 功能: 分配和初始化客户端上下文g_client_ctx
 * 时机: 从节点启动时调用，或REPLICAOF设置后调用
 * 注意: 此函数不建立任何连接，仅准备资源
 */
int rdma_sync_client_init(void);

/*
 * 【方案C新增】通过TCP连接触发并执行RDMA存量同步
 *
 * 功能: 从节点主入口函数，完成TCP触发 -> RDMA同步的全流程
 *
 * 设计原理:
 *   - 封装方案C的完整连接流程，对外提供统一接口
 *   - 内部处理TCP控制信道和RDMA数据信道的协调
 *   - 保持与原有rdma_sync_perform_full_sync()的调用时机一致
 *
 * 参数:
 *   @master_host: 主节点IP地址或主机名
 *   @master_port: 主节点TCP服务端口(非RDMA端口)
 *   @engine_type: 需要同步的引擎类型
 *                 ENGINE_COUNT表示同步所有引擎
 *
 * 返回值:
 *   0  - 同步成功，数据已加载到本地引擎
 *   -1 - 同步失败，可能原因:
 *        * TCP连接失败
 *        * 主节点fork失败
 *        * RDMA连接失败
 *        * 数据传输错误
 *
 * 执行流程:
 *   ┌─────────────────────────────────────────────────────┐
 *   │  1. 建立TCP连接到主节点master_port                   │
 *   │     socket() -> connect()                           │
 *   ├─────────────────────────────────────────────────────┤
 *   │  2. 发送RDMASYNC命令                                 │
 *   │     write("*2\r\n$8\r\nRDMASYNC\r\n...")            │
 *   ├─────────────────────────────────────────────────────┤
 *   │  3. 等待并解析+RDMA_READY响应                        │
 *   │     read() -> parse "+RDMA_READY <rdma_port>"       │
 *   │     超时: 10秒                                      │
 *   ├─────────────────────────────────────────────────────┤
 *   │  4. 建立RDMA连接到动态端口                           │
 *   │     rdma_create_id() -> rdma_resolve_addr()         │
 *   │     -> rdma_connect()                               │
 *   ├─────────────────────────────────────────────────────┤
 *   │  5. 执行存量同步                                     │
 *   │     循环: send PREPARE -> wait READY -> RDMA Read   │
 *   │     -> send COMPLETE                                │
 *   ├─────────────────────────────────────────────────────┤
 *   │  6. 等待+RDMA_DONE响应，清理TCP连接                  │
 *   │     read("+RDMA_DONE") -> close(tcp_fd)             │
 *   └─────────────────────────────────────────────────────┘
 *
 * 与原有流程对比:
 *   - 原rdma_sync_client_connect()只处理上述流程的步骤4
 *   - 此函数封装完整流程，替代start_slave_sync()中的连接部分
 */
int rdma_sync_client_start_via_tcp(const char *master_host,
                                    uint16_t master_port,
                                    rdma_engine_type_t engine_type);

/*
 * 【修改-添加参数】连接到主节点的 RDMA 服务
 *
 * 变更说明:
 *   原签名: rdma_sync_client_connect(const char *host, uint16_t port)
 *   新签名: rdma_sync_client_connect(const char *host, uint16_t port, rdma_engine_type_t type)
 *
 * 变更原因:
 *   - 方案C中子进程需要知道要同步哪个引擎，以决定生成哪个引擎的快照
 *   - 虽然当前通过控制消息传递引擎类型，但连接时记录便于调试
 *
 * 参数:
 *   @master_host:  主节点地址
 *   @master_port:  主节点RDMA端口(动态分配，非固定偏移)
 *   @engine_type: 【新增】目标引擎类型，存入g_client_ctx供后续使用
 *
 * 注意:
 *   此函数现在由rdma_sync_client_start_via_tcp()内部调用
 *   不建议外部直接调用，应使用tcp-first接口
 */
int rdma_sync_client_connect(const char *master_host,
                              uint16_t master_port,
                              rdma_engine_type_t engine_type);

/*
 * 执行完整的存量同步
 * 这是从节点的主要入口函数
 *
 * 执行流程：
 * 1. 建立 RDMA 连接
 * 2. 循环每个引擎：
 *    a. 发送 PREPARE 命令
 *    b. 接收 READY 响应，获取元数据
 *    c. 执行 RDMA Read 读取数据
 *    d. 解析 KSF 数据并加载到本地引擎
 *    e. 发送 COMPLETE 命令
 * 3. 发送 DONE 命令
 * 4. 处理 TCP 队列中的积压命令
 *
 * @return: 0 成功，-1 失败
 */
int rdma_sync_perform_full_sync(void);

/*
 * 断开与主节点的 RDMA 连接
 */
void rdma_sync_client_disconnect(void);

/*
 * 暂存 mirror 的 TCP 命令到队列
 * 由 mirror 的 TCP 接收线程调用
 *
 * @param data: 命令数据
 * @param len: 数据长度
 */
void rdma_sync_enqueue_tcp_cmd(const char *data, size_t len);

/*
 * 处理积压的 TCP 命令
 * 存量同步完成后调用，将队列中的命令应用到引擎
 */
void rdma_sync_drain_tcp_queue(void);

/* ============================================================================
 * 底层工具函数
 * ============================================================================ */

/*
 * 执行单次 RDMA Read 操作
 * 从远程节点的内存读取数据到本地缓冲区
 *
 * @param ctx: 客户端上下文
 * @param remote_addr: 远程内存地址
 * @param remote_rkey: 远程内存密钥
 * @param local_buf: 本地缓冲区
 * @param length: 读取长度
 * @return: 0 成功，-1 失败
 */
int rdma_sync_post_read(struct rdma_client_context *ctx,
                        uint64_t remote_addr,
                        uint32_t remote_rkey,
                        void *local_buf,
                        size_t length);

/*
 * 发送控制消息（非阻塞）
 */
int rdma_sync_send_ctrl_msg(struct rdma_client_context *ctx,
                            const struct rdma_ctrl_msg *msg);

/*
 * 接收控制消息（阻塞，带超时）
 */
int rdma_sync_recv_ctrl_msg(struct rdma_client_context *ctx,
                            struct rdma_ctrl_msg *msg,
                            int timeout_ms);

/*
 * 等待工作完成（Work Completion）
 */
int rdma_sync_poll_completion(struct rdma_client_context *ctx,
                              struct ibv_wc *wc,
                              int max_wc);

/*
 * 引擎类型转字符串
 */
const char* rdma_sync_engine_name(rdma_engine_type_t type);

/* ============================================================================
 * 与现有系统集成
 * ============================================================================ */

/*
 * 同步模块初始化和清理
 * 在 main() 中调用，用于启动和停止 RDMA 同步服务
 */
int sync_module_init(void);       // 初始化同步模块：主节点启动 RDMA 服务器，从节点准备客户端
void sync_module_cleanup(void);   // 清理同步模块：关闭连接，释放资源

/*
 * SYNC 命令处理函数
 * 在 proactor.c 中调用，处理客户端发来的 SYNC 命令
 *
 * 如果是从节点收到 SYNC 命令：
 *   触发向主节点的存量同步
 * 如果是主节点收到 SYNC 命令：
 *   返回错误（主节点不接受 SYNC 命令）
 */
int kvs_cmd_sync(struct conn *c);

/*
 * REPLICAOF 命令处理函数
 * 用于动态设置/取消主从关系
 * 语法: REPLICAOF <host> <port>  或  REPLICAOF NO ONE
 */
int kvs_cmd_replicaof(struct conn *c, int argc, robj *argv);  // robj 定义在 kvs_network.h 中

/*
 * 检查当前节点是否正在进行存量同步
 * 用于控制是否接收客户端写命令
 */
int rdma_sync_in_progress(void);

#endif /* KVS_RDMA_SYNC_H */
