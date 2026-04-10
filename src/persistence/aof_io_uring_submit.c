/**
 * AOF io_uring 批量提交系统实现
 * 独立的io_uring实例，批量SQE提交
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/time.h>
#include <linux/io_uring.h>
#include <liburing.h>

#include "kvs_aof_io_uring.h"

/* 用户数据结构 */
typedef struct aof_batch_user_data {
    void *original_data;
    void (*callback)(void *, int);
} aof_batch_user_data_t;

static __thread aof_batch_user_data_t user_data_pool[AOF_BATCH_SIZE];
static __thread int user_data_idx = 0;

/**
 * 获取用户数据结构
 */
static aof_batch_user_data_t *get_user_data_slot(void)
{
    aof_batch_user_data_t *slot = &user_data_pool[user_data_idx];
    user_data_idx = (user_data_idx + 1) % AOF_BATCH_SIZE;
    return slot;
}

/**
 * 获取当前时间（微秒）
 */
static uint64_t get_time_us(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000 + ts.tv_nsec / 1000;
}

/**
 * 初始化批量提交上下文
 */
int aof_batch_init(aof_batch_ctx_t *ctx)
{
    if (!ctx) {
        return -1;
    }

    memset(ctx, 0, sizeof(*ctx));

    /* 初始化io_uring */
    struct io_uring_params params;
    memset(&params, 0, sizeof(params));

    /* 配置io_uring参数 - 独立ring */
    params.sq_entries = AOF_RING_SQ_SIZE;
    params.cq_entries = AOF_RING_CQ_SIZE;
    params.flags = IORING_SETUP_SUBMIT_ALL |      /* 批量提交 */
                   IORING_SETUP_COOP_TASKRUN;      /* 协作任务运行 */

    /* 初始化ring */
    if (io_uring_queue_init_params(AOF_RING_SQ_SIZE, &ctx->ring, &params) < 0) {
        perror("io_uring_queue_init_params");
        return -1;
    }

    /* 检查支持的功能 */
    struct io_uring_probe *probe = io_uring_get_probe_ring(&ctx->ring);
    if (!probe) {
        io_uring_queue_exit(&ctx->ring);
        return -1;
    }

    /* 检查是否支持write和fsync */
    if (!io_uring_opcode_supported(probe, IORING_OP_WRITE) ||
        !io_uring_opcode_supported(probe, IORING_OP_FSYNC)) {
        fprintf(stderr, "io_uring does not support required opcodes\n");
        io_uring_free_probe(probe);
        io_uring_queue_exit(&ctx->ring);
        return -1;
    }

    io_uring_free_probe(probe);

    /* 初始化锁和条件变量 */
    pthread_mutex_init(&ctx->submit_lock, NULL);
    pthread_cond_init(&ctx->submit_cond, NULL);

    ctx->batch_count = 0;
    ctx->batch_timeout_us = AOF_BATCH_TIMEOUT_US;
    ctx->running = false;

    return 0;
}

/**
 * 销毁批量提交上下文
 */
void aof_batch_destroy(aof_batch_ctx_t *ctx)
{
    if (!ctx) {
        return;
    }

    /* 确保工作线程已停止 */
    if (ctx->running) {
        aof_batch_stop_worker(ctx);
    }

    /* 提交任何剩余的SQE */
    if (ctx->batch_count > 0) {
        aof_batch_submit(ctx);
    }

    /* 处理剩余的完成事件 */
    aof_process_completions(ctx, 1000);

    /* 销毁ring */
    io_uring_queue_exit(&ctx->ring);

    pthread_mutex_destroy(&ctx->submit_lock);
    pthread_cond_destroy(&ctx->submit_cond);
}

/**
 * 添加写入请求到批次
 */
int aof_batch_add(aof_batch_ctx_t *ctx, int fd, const void *buf, size_t len,
                  off_t offset, void *user_data)
{
    if (!ctx || fd < 0 || !buf || len == 0) {
        return -1;
    }

    pthread_mutex_lock(&ctx->submit_lock);

    /* 如果批次已满，先提交 */
    if (ctx->batch_count >= AOF_BATCH_SIZE) {
        pthread_mutex_unlock(&ctx->submit_lock);
        aof_batch_submit(ctx);
        pthread_mutex_lock(&ctx->submit_lock);
    }

    /* 获取SQE */
    struct io_uring_sqe *sqe = io_uring_get_sqe(&ctx->ring);
    if (!sqe) {
        pthread_mutex_unlock(&ctx->submit_lock);
        /* Ring已满，提交并重试 */
        aof_batch_submit(ctx);
        return aof_batch_add(ctx, fd, buf, len, offset, user_data);
    }

    /* 准备write操作 */
    io_uring_prep_write(sqe, fd, buf, len, offset);

    /* 设置用户数据 */
    aof_batch_user_data_t *ud = get_user_data_slot();
    ud->original_data = user_data;
    ud->callback = NULL;
    io_uring_sqe_set_data(sqe, ud);

    /* 存储SQE引用 */
    ctx->sqe_batch[ctx->batch_count] = sqe;
    ctx->user_data[ctx->batch_count] = user_data;
    ctx->batch_count++;

    pthread_mutex_unlock(&ctx->submit_lock);

    return 0;
}

/**
 * 添加带链接的write+fsync请求
 * 使用IOSQE_IO_LINK将write和fsync链接
 */
int aof_batch_add_linked_write_fsync(aof_batch_ctx_t *ctx, int fd,
                                      const void *buf, size_t len,
                                      off_t offset, void *user_data)
{
    if (!ctx || fd < 0 || !buf || len == 0) {
        return -1;
    }

    pthread_mutex_lock(&ctx->submit_lock);

    /* 需要2个SQE，检查空间 */
    if (ctx->batch_count + 2 > AOF_BATCH_SIZE) {
        pthread_mutex_unlock(&ctx->submit_lock);
        aof_batch_submit(ctx);
        pthread_mutex_lock(&ctx->submit_lock);
    }

    /* 获取write SQE */
    struct io_uring_sqe *write_sqe = io_uring_get_sqe(&ctx->ring);
    if (!write_sqe) {
        pthread_mutex_unlock(&ctx->submit_lock);
        aof_batch_submit(ctx);
        return aof_batch_add_linked_write_fsync(ctx, fd, buf, len, offset, user_data);
    }

    /* 准备write操作 */
    io_uring_prep_write(write_sqe, fd, buf, len, offset);
    write_sqe->flags |= IOSQE_IO_LINK;  /* 链接下一个操作 */

    aof_batch_user_data_t *write_ud = get_user_data_slot();
    write_ud->original_data = user_data;
    write_ud->callback = NULL;
    io_uring_sqe_set_data(write_sqe, write_ud);

    ctx->sqe_batch[ctx->batch_count] = write_sqe;
    ctx->user_data[ctx->batch_count] = user_data;
    ctx->batch_count++;

    /* 获取fsync SQE */
    struct io_uring_sqe *fsync_sqe = io_uring_get_sqe(&ctx->ring);
    if (!fsync_sqe) {
        /* 这不应该发生，因为我们已经检查了空间 */
        pthread_mutex_unlock(&ctx->submit_lock);
        return -1;
    }

    /* 准备fsync操作 */
    io_uring_prep_fsync(fsync_sqe, fd, 0);

    aof_batch_user_data_t *fsync_ud = get_user_data_slot();
    fsync_ud->original_data = user_data;
    fsync_ud->callback = NULL;
    io_uring_sqe_set_data(fsync_sqe, fsync_ud);

    ctx->sqe_batch[ctx->batch_count] = fsync_sqe;
    ctx->user_data[ctx->batch_count] = user_data;
    ctx->batch_count++;

    pthread_mutex_unlock(&ctx->submit_lock);

    return 0;
}

/**
 * 强制提交当前批次
 */
int aof_batch_submit(aof_batch_ctx_t *ctx)
{
    if (!ctx) {
        return -1;
    }

    pthread_mutex_lock(&ctx->submit_lock);

    int count = ctx->batch_count;
    if (count == 0) {
        pthread_mutex_unlock(&ctx->submit_lock);
        return 0;
    }

    /* 提交所有SQE */
    int submitted = io_uring_submit(&ctx->ring);
    if (submitted < 0) {
        pthread_mutex_unlock(&ctx->submit_lock);
        return -1;
    }

    /* 重置批次 */
    ctx->batch_count = 0;
    ctx->total_submitted += submitted;
    ctx->batch_count_total++;

    pthread_mutex_unlock(&ctx->submit_lock);

    return submitted;
}

/**
 * 处理完成事件
 */
int aof_process_completions(aof_batch_ctx_t *ctx, int timeout_us)
{
    if (!ctx) {
        return -1;
    }

    struct io_uring_cqe *cqe;
    int processed = 0;
    unsigned head;

    if (timeout_us > 0) {
        /* 等待完成事件 */
        struct __kernel_timespec ts;
        ts.tv_sec = timeout_us / 1000000;
        ts.tv_nsec = (timeout_us % 1000000) * 1000;

        int ret = io_uring_wait_cqe_timeout(&ctx->ring, &cqe, &ts);
        if (ret < 0) {
            if (ret == -ETIME) {
                return 0; /* 超时 */
            }
            return -1;
        }
    }

    /* 批量处理完成事件 */
    io_uring_for_each_cqe(&ctx->ring, head, cqe) {
        aof_batch_user_data_t *ud = io_uring_cqe_get_data(cqe);
        int res = cqe->res;

        if (res < 0) {
            /* 处理错误 */
            fprintf(stderr, "io_uring completion error: %s\n", strerror(-res));
        }

        /* 如果有回调，调用它 */
        if (ud && ud->callback) {
            ud->callback(ud->original_data, res);
        }

        processed++;
    }

    /* 更新CQ头 */
    if (processed > 0) {
        io_uring_cq_advance(&ctx->ring, processed);
        ctx->total_completed += processed;
    }

    return processed;
}

/**
 * 工作线程函数
 */
static void *aof_batch_worker_thread(void *arg)
{
    aof_batch_ctx_t *ctx = (aof_batch_ctx_t *)arg;
    uint64_t last_submit_time = get_time_us();

    while (ctx->running) {
        /* 检查是否需要超时提交 */
        uint64_t now = get_time_us();
        if (ctx->batch_count > 0 &&
            (now - last_submit_time) >= ctx->batch_timeout_us) {
            aof_batch_submit(ctx);
            last_submit_time = now;
        }

        /* 处理完成事件（非阻塞） */
        aof_process_completions(ctx, 0);

        /* 短暂休眠，避免忙等待 */
        usleep(10);
    }

    return NULL;
}

/**
 * 启动批量提交工作线程
 */
int aof_batch_start_worker(aof_batch_ctx_t *ctx)
{
    if (!ctx || ctx->running) {
        return -1;
    }

    ctx->running = true;

    if (pthread_create(&ctx->worker_thread, NULL, aof_batch_worker_thread, ctx) != 0) {
        ctx->running = false;
        return -1;
    }

    return 0;
}

/**
 * 停止批量提交工作线程
 */
void aof_batch_stop_worker(aof_batch_ctx_t *ctx)
{
    if (!ctx || !ctx->running) {
        return;
    }

    ctx->running = false;
    pthread_join(ctx->worker_thread, NULL);

    /* 提交任何剩余的SQE */
    if (ctx->batch_count > 0) {
        aof_batch_submit(ctx);
    }

    /* 等待所有完成 */
    while (aof_process_completions(ctx, 1000) > 0) {
        /* 继续处理 */
    }
}

/**
 * 获取批量提交统计信息
 */
void aof_batch_get_stats(aof_batch_ctx_t *ctx, uint64_t *submitted,
                          uint64_t *completed, uint64_t *batches)
{
    if (!ctx) {
        return;
    }

    pthread_mutex_lock(&ctx->submit_lock);
    if (submitted) *submitted = ctx->total_submitted;
    if (completed) *completed = ctx->total_completed;
    if (batches) *batches = ctx->batch_count_total;
    pthread_mutex_unlock(&ctx->submit_lock);
}