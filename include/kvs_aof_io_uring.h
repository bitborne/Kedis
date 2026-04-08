#ifndef __KVS_AOF_IO_URING_H__
#define __KVS_AOF_IO_URING_H__

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include <liburing.h>
#include "kvs_aof.h"

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================================
 * 配置常量
 * ============================================================================ */

/* 页大小 - 通常4096字节 */
#define AOF_PAGE_SIZE           4096

/* 大值写入阈值 */
#define AOF_LARGE_WRITE_THRESHOLD   (256 * 1024)        /* 256KB */
#define AOF_HUGE_WRITE_THRESHOLD    (4 * 1024 * 1024)   /* 4MB */

/* io_uring注册缓冲区配置 */
#define AOF_REG_BUF_COUNT       64                      /* 注册缓冲区数量 */
#define AOF_REG_BUF_SIZE        (64 * 1024)             /* 每个缓冲区64KB */

/* 内存池配置 */
#define AOF_MEMPOOL_DEFAULT_BLOCKS  1024                /* 默认块数 */

/* 双缓冲配置 */
#define AOF_BUFFER_SIZE             (4 * 1024 * 1024)   /* 每个缓冲区4MB */
#define AOF_BUFFER_FLUSH_THRESHOLD  (3 * 1024 * 1024)   /* 刷新阈值3MB */

/* 批量提交配置 */
#define AOF_BATCH_SIZE              64                  /* 每批最大SQE数量 */
#define AOF_BATCH_TIMEOUT_US        100                 /* 批量超时100微秒 */
#define AOF_RING_SQ_SIZE            256                 /* 提交队列大小 */
#define AOF_RING_CQ_SIZE            512                 /* 完成队列大小 */

/* 组提交配置 */
#define AOF_GROUP_COMMIT_MAX_WAITERS    1024            /* 最大等待者数量 */
#define AOF_EVERYSEC_INTERVAL_MS        1000            /* EVERYSEC模式间隔1秒 */

/* ============================================================================
 * 数据类型定义
 * ============================================================================ */

/* 写入类型枚举 */
typedef enum {
    AOF_WRITE_NORMAL = 0,       /* 普通写入，使用双缓冲 */
    AOF_WRITE_LARGE,            /* 大值写入，使用注册缓冲区 */
    AOF_WRITE_HUGE              /* 超大值写入，使用splice */
} aof_write_type_t;

/* 缓冲区状态枚举（CAS状态机） */
typedef enum {
    AOF_BUF_STATE_IDLE = 0,     /* 空闲状态，可接受新写入 */
    AOF_BUF_STATE_FILLING,      /* 正在填充数据 */
    AOF_BUF_STATE_READY,        /* 准备写入磁盘 */
    AOF_BUF_STATE_WRITING,      /* 正在写入磁盘 */
    AOF_BUF_STATE_SYNCING       /* 正在同步到磁盘 */
} aof_buf_state_t;

/* 同步策略枚举 */
typedef enum {
    AOF_SYNC_ALWAYS = 0,        /* 每个命令都fsync */
    AOF_SYNC_EVERYSEC,          /* 每秒fsync一次 */
    AOF_SYNC_NO                 /* 由操作系统决定 */
} aof_sync_policy_t;

/* 内存池空闲块节点 */
typedef struct aof_mempool_node {
    struct aof_mempool_node *next;
} aof_mempool_node_t;

/* 页对齐内存池结构 */
typedef struct aof_mempool {
    void *base_addr;                /* 基地址 */
    size_t block_size;              /* 块大小（页对齐） */
    size_t num_blocks;              /* 总块数 */
    size_t free_count;              /* 空闲块数 */
    aof_mempool_node_t *free_list;  /* 空闲链表 */
    pthread_mutex_t lock;           /* 线程安全锁 */
    bool initialized;               /* 初始化标志 */
} aof_mempool_t;

/* io_uring注册缓冲区管理结构 */
typedef struct aof_registered_buffers {
    struct iovec *iovecs;           /* 缓冲区向量数组 */
    void **buffers;                 /* 缓冲区指针数组 */
    uint64_t *bitmap;               /* 使用状态位图 */
    int buf_count;                  /* 缓冲区数量 */
    size_t buf_size;                /* 每个缓冲区大小 */
    int free_count;                 /* 空闲缓冲区数 */
    pthread_mutex_t lock;           /* 线程安全锁 */
    bool registered;                /* 是否已注册 */
} aof_registered_buffers_t;

/* 大值写入上下文结构 */
typedef struct aof_large_write {
    int cmd_type;                   /* 命令类型 */
    const robj *key;                /* 键对象 */
    const robj *value;              /* 值对象 */
    void *buffer;                   /* 使用的缓冲区 */
    size_t buf_len;                 /* 缓冲区中数据长度 */
    int buf_idx;                    /* 注册缓冲区索引（-1表示未使用） */
    bool use_fixed;                 /* 是否使用fixed write */
} aof_large_write_t;

/* 单个缓冲区结构 */
typedef struct aof_buf {
    _Atomic aof_buf_state_t state;  /* 缓冲区状态（CAS操作） */
    void *data;                     /* 数据指针（页对齐） */
    size_t size;                    /* 缓冲区总大小 */
    size_t used;                    /* 已使用字节数 */
    uint64_t sequence;              /* 序列号，用于排序 */
    pthread_mutex_t lock;           /* 保护used字段的锁 */
} aof_buf_t;

/* 双缓冲结构 */
typedef struct aof_double_buf {
    aof_buf_t buffers[2];           /* 两个缓冲区 */
    _Atomic int active_idx;         /* 当前活跃缓冲区索引 */
    int fd;                         /* AOF文件描述符 */
    uint64_t global_seq;            /* 全局序列号生成器 */
    pthread_cond_t flush_cond;      /* 刷新条件变量 */
    pthread_mutex_t flush_lock;     /* 刷新锁 */
    bool shutdown;                  /* 关闭标志 */
} aof_double_buf_t;

/* 批量提交上下文 */
typedef struct aof_batch_ctx {
    struct io_uring ring;           /* io_uring实例 */
    pthread_t worker_thread;        /* 工作线程 */
    pthread_mutex_t submit_lock;    /* 提交锁 */
    pthread_cond_t submit_cond;     /* 提交条件变量 */

    /* 批处理状态 */
    struct io_uring_sqe *sqe_batch[AOF_BATCH_SIZE];  /* SQE批次数组 */
    void *user_data[AOF_BATCH_SIZE];                 /* 用户数据数组 */
    int batch_count;                                 /* 当前批次数量 */
    uint64_t batch_timeout_us;                       /* 批量超时（微秒） */

    /* 统计信息 */
    uint64_t total_submitted;       /* 总提交数 */
    uint64_t total_completed;       /* 总完成数 */
    uint64_t batch_count_total;     /* 批次数 */

    bool running;                   /* 运行标志 */
} aof_batch_ctx_t;

/* 组提交等待者 */
typedef struct aof_commit_waiter {
    pthread_cond_t cond;            /* 条件变量 */
    pthread_mutex_t lock;           /* 锁 */
    bool completed;                 /* 是否完成 */
    int error;                      /* 错误码 */
} aof_commit_waiter_t;

/* 组提交管理结构 */
typedef struct aof_group_commit {
    aof_sync_policy_t sync_policy;  /* 同步策略 */

    /* EVERYSEC模式 */
    timer_t everysec_timer;         /* POSIX定时器 */
    struct sigevent sev;            /* 定时器事件 */
    struct itimerspec its;          /* 定时器规格 */

    /* 等待者管理 */
    aof_commit_waiter_t *waiters[AOF_GROUP_COMMIT_MAX_WAITERS];
    int waiter_count;
    pthread_mutex_t waiter_lock;

    /* 统计信息 */
    uint64_t group_commits;         /* 组提交次数 */
    uint64_t grouped_waiters;       /* 被分组的等待者数 */
    uint64_t fsync_count;           /* fsync调用次数 */

    /* 状态 */
    _Atomic bool fsync_pending;     /* 是否有待处理的fsync */
    struct timespec last_fsync;     /* 上次fsync时间 */
} aof_group_commit_t;

/* 零拷贝统计信息 */
typedef struct aof_zerocopy_stats {
    uint64_t page_allocs;           /* 页对齐分配次数 */
    uint64_t page_frees;            /* 页对齐释放次数 */
    uint64_t mempool_allocs;        /* 内存池分配次数 */
    uint64_t mempool_frees;         /* 内存池释放次数 */
    uint64_t reg_buf_acquired;      /* 注册缓冲区获取次数 */
    uint64_t reg_buf_released;      /* 注册缓冲区释放次数 */
    uint64_t large_writes;          /* 大值写入次数 */
    uint64_t huge_writes;           /* 超大值写入次数 */
    uint64_t splice_bytes;          /* splice传输字节数 */
} aof_zerocopy_stats_t;

/* ============================================================================
 * 页对齐内存分配函数
 * ============================================================================ */

/**
 * 分配页对齐内存
 * @param size 请求大小
 * @return 页对齐的内存指针，失败返回NULL
 */
void *aof_page_aligned_alloc(size_t size);

/**
 * 释放页对齐内存
 * @param ptr 内存指针
 */
void aof_page_aligned_free(void *ptr);

/**
 * 检查指针是否页对齐
 * @param ptr 内存指针
 * @return 1是页对齐，0不是
 */
int aof_is_page_aligned(const void *ptr);

/**
 * 将大小对齐到页大小倍数
 * @param size 原始大小
 * @return 对齐后的大小
 */
size_t aof_align_to_page(size_t size);

/* ============================================================================
 * 内存池管理函数
 * ============================================================================ */

/**
 * 初始化页对齐内存池
 * @param pool 内存池结构
 * @param block_size 块大小（自动对齐到页大小）
 * @param num_blocks 块数量
 * @return 0成功，-1失败
 */
int aof_mempool_init(aof_mempool_t *pool, size_t block_size, size_t num_blocks);

/**
 * 销毁内存池
 * @param pool 内存池结构
 */
void aof_mempool_destroy(aof_mempool_t *pool);

/**
 * 从内存池分配一块内存
 * @param pool 内存池结构
 * @return 内存指针，NULL表示失败
 */
void *aof_mempool_alloc(aof_mempool_t *pool);

/**
 * 释放内存回内存池
 * @param pool 内存池结构
 * @param ptr 内存指针
 */
void aof_mempool_free(aof_mempool_t *pool, void *ptr);

/* ============================================================================
 * io_uring注册缓冲区管理函数
 * ============================================================================ */

/**
 * 注册缓冲区到io_uring
 * @param ring io_uring实例
 * @param reg 注册缓冲区管理结构
 * @param buf_size 每个缓冲区大小
 * @param buf_count 缓冲区数量
 * @return 0成功，-1失败
 */
int aof_register_buffers(struct io_uring *ring, aof_registered_buffers_t *reg,
                         size_t buf_size, int buf_count);

/**
 * 注销io_uring注册缓冲区
 * @param reg 注册缓冲区管理结构
 */
void aof_unregister_buffers(aof_registered_buffers_t *reg);

/**
 * 获取一个可用的注册缓冲区
 * @param reg 注册缓冲区管理结构
 * @param buf_out 输出缓冲区指针
 * @return 缓冲区索引（>=0），失败返回-1
 */
int aof_acquire_reg_buffer(aof_registered_buffers_t *reg, void **buf_out);

/**
 * 释放注册缓冲区
 * @param reg 注册缓冲区管理结构
 * @param buf_idx 缓冲区索引
 */
void aof_release_reg_buffer(aof_registered_buffers_t *reg, int buf_idx);

/* ============================================================================
 * 大值写入函数
 * ============================================================================ */

/**
 * 提交大值写入请求（256KB - 4MB）
 * 使用io_uring注册缓冲区 + io_uring_prep_write_fixed
 * @param ring io_uring实例
 * @param fd 文件描述符
 * @param cmd_type 命令类型
 * @param key 键对象
 * @param value 值对象
 * @return 0成功，-1失败
 */
int aof_submit_large_write(struct io_uring *ring, int fd,
                           int cmd_type, const robj *key, const robj *value);

/**
 * 提交超大值写入请求（>= 4MB）
 * 使用splice实现零拷贝
 * @param ring io_uring实例
 * @param fd 文件描述符
 * @param cmd_type 命令类型
 * @param key 键对象
 * @param value 值对象
 * @return 0成功，-1失败
 */
int aof_submit_huge_write(struct io_uring *ring, int fd,
                          int cmd_type, const robj *key, const robj *value);

/**
 * 根据数据大小选择写入类型
 * @param total_len 总数据长度
 * @return 写入类型
 */
aof_write_type_t aof_select_write_type(size_t total_len);

/* ============================================================================
 * 双缓冲系统函数（CAS状态机）
 * ============================================================================ */

/**
 * 初始化双缓冲系统
 * @param dbuf 双缓冲结构
 * @param fd AOF文件描述符
 * @return 0成功，-1失败
 */
int aof_double_buf_init(aof_double_buf_t *dbuf, int fd);

/**
 * 销毁双缓冲系统
 * @param dbuf 双缓冲结构
 */
void aof_double_buf_destroy(aof_double_buf_t *dbuf);

/**
 * 获取用于写入的缓冲区（CAS操作）
 * @param dbuf 双缓冲结构
 * @param size 需要的大小
 * @param buf_out 输出缓冲区指针
 * @param offset_out 输出偏移量
 * @return 0成功，-1失败（需要等待）
 */
int aof_buf_acquire_for_write(aof_double_buf_t *dbuf, size_t size,
                              void **buf_out, size_t *offset_out);

/**
 * 标记缓冲区为就绪状态（CAS: FILLING -> READY）
 * @param dbuf 双缓冲结构
 * @param used 实际使用的字节数
 * @return 0成功，-1失败
 */
int aof_buf_mark_ready(aof_double_buf_t *dbuf, size_t used);

/**
 * 等待可刷新的缓冲区（CAS: READY -> WRITING）
 * @param dbuf 双缓冲结构
 * @param buf_idx 输出缓冲区索引
 * @return 0成功，-1失败
 */
int aof_buf_wait_for_flushable(aof_double_buf_t *dbuf, int *buf_idx);

/**
 * 释放缓冲区回空闲状态（CAS: SYNCING -> IDLE）
 * @param dbuf 双缓冲结构
 * @param buf_idx 缓冲区索引
 * @return 0成功，-1失败
 */
int aof_buf_release(aof_double_buf_t *dbuf, int buf_idx);

/**
 * 触发缓冲区切换（当活跃缓冲区满时）
 * @param dbuf 双缓冲结构
 * @return 0成功，-1失败
 */
int aof_buf_trigger_switch(aof_double_buf_t *dbuf);

/* ============================================================================
 * io_uring批量提交函数（独立ring实例）
 * ============================================================================ */

/**
 * 初始化批量提交上下文
 * @param ctx 批量提交上下文
 * @return 0成功，-1失败
 */
int aof_batch_init(aof_batch_ctx_t *ctx);

/**
 * 销毁批量提交上下文
 * @param ctx 批量提交上下文
 */
void aof_batch_destroy(aof_batch_ctx_t *ctx);

/**
 * 添加写入请求到批次
 * @param ctx 批量提交上下文
 * @param fd 文件描述符
 * @param buf 数据缓冲区
 * @param len 数据长度
 * @param offset 文件偏移量
 * @param user_data 用户数据（回调用）
 * @return 0成功，-1失败
 */
int aof_batch_add(aof_batch_ctx_t *ctx, int fd, const void *buf, size_t len,
                  off_t offset, void *user_data);

/**
 * 强制提交当前批次
 * @param ctx 批量提交上下文
 * @return 提交的SQE数量，-1失败
 */
int aof_batch_submit(aof_batch_ctx_t *ctx);

/**
 * 处理完成事件（在工作线程中调用）
 * @param ctx 批量提交上下文
 * @param timeout_us 超时时间（微秒）
 * @return 处理的完成事件数量
 */
int aof_process_completions(aof_batch_ctx_t *ctx, int timeout_us);

/**
 * 启动批量提交工作线程
 * @param ctx 批量提交上下文
 * @return 0成功，-1失败
 */
int aof_batch_start_worker(aof_batch_ctx_t *ctx);

/**
 * 停止批量提交工作线程
 * @param ctx 批量提交上下文
 */
void aof_batch_stop_worker(aof_batch_ctx_t *ctx);

/* ============================================================================
 * 组提交函数（三种同步策略）
 * ============================================================================ */

/**
 * 初始化组提交系统
 * @param gc 组提交结构
 * @param policy 同步策略
 * @param batch_ctx 批量提交上下文（用于实际IO）
 * @return 0成功，-1失败
 */
int aof_group_commit_init(aof_group_commit_t *gc, aof_sync_policy_t policy,
                          aof_batch_ctx_t *batch_ctx);

/**
 * 销毁组提交系统
 * @param gc 组提交结构
 */
void aof_group_commit_destroy(aof_group_commit_t *gc);

/**
 * 触发组提交
 * 根据策略决定是立即fsync、加入等待队列还是直接返回
 * @param gc 组提交结构
 * @param fd AOF文件描述符
 * @return 0成功，-1失败
 */
int aof_group_commit_trigger(aof_group_commit_t *gc, int fd);

/**
 * EVERYSEC模式的定时器处理器
 * @param sig 信号编号
 * @param si 信号信息
 * @param uc 用户上下文
 */
void aof_everysec_timer_handler(int sig, siginfo_t *si, void *uc);

/**
 * 提交链接的write+fsync操作
 * 使用IOSQE_IO_LINK将write和fsync链接为原子操作
 * @param ctx 批量提交上下文
 * @param fd 文件描述符
 * @param buf 数据缓冲区
 * @param len 数据长度
 * @param offset 文件偏移量
 * @return 0成功，-1失败
 */
int aof_submit_linked_write_fsync(aof_batch_ctx_t *ctx, int fd,
                                   const void *buf, size_t len, off_t offset);

/**
 * 等待组提交完成
 * @param gc 组提交结构
 * @param timeout_ms 超时时间（毫秒）
 * @return 0成功，-1超时，-2错误
 */
int aof_group_commit_wait(aof_group_commit_t *gc, int timeout_ms);

/**
 * 唤醒所有等待者
 * @param gc 组提交结构
 * @param error 错误码（0表示成功）
 */
void aof_group_commit_wake_all(aof_group_commit_t *gc, int error);

/**
 * 执行fsync操作（根据策略）
 * @param gc 组提交结构
 * @param fd 文件描述符
 * @return 0成功，-1失败
 */
int aof_perform_fsync(aof_group_commit_t *gc, int fd);

/* ============================================================================
 * 辅助函数
 * ============================================================================ */

/**
 * 获取零拷贝统计信息
 * @param stats 统计结构体输出
 */
void aof_zerocopy_get_stats(aof_zerocopy_stats_t *stats);

/**
 * 重置零拷贝统计信息
 */
void aof_zerocopy_reset_stats(void);

/**
 * 打印零拷贝统计信息
 */
void aof_zerocopy_print_stats(void);

/**
 * 计算AOF命令编码后的大小
 * @param cmd_type 命令类型
 * @param key 键对象
 * @param value 值对象
 * @return 编码后的总大小
 */
size_t aof_calc_encoded_size(int cmd_type, const robj *key, const robj *value);

/**
 * 编码AOF命令到缓冲区
 * @param buf 目标缓冲区
 * @param buf_size 缓冲区大小
 * @param cmd_type 命令类型
 * @param key 键对象
 * @param value 值对象
 * @return 实际编码的字节数，-1表示失败
 */
int aof_encode_command(void *buf, size_t buf_size, int cmd_type,
                       const robj *key, const robj *value);

#ifdef __cplusplus
}
#endif

#endif /* __KVS_AOF_IO_URING_H__ */
