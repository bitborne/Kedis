/**
 * AOF io_uring 组提交策略实现
 * 三种同步策略: ALWAYS, EVERYSEC, NO
 */

#include <stdio.h>
#include <string.h>
#include <stdatomic.h>
#include <pthread.h>
#include <signal.h>
#include <time.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>

#include "kvs_aof_io_uring.h"
#include "kmem.h"

/* 全局组提交上下文（用于信号处理程序） */
static aof_group_commit_t *g_group_commit = NULL;

/**
 * 创建新的等待者
 */
static aof_commit_waiter_t *create_waiter(void)
{
    aof_commit_waiter_t *waiter = kmem_alloc(sizeof(*waiter));
    if (!waiter) {
        return NULL;
    }
    memset(waiter, 0, sizeof(*waiter));

    pthread_cond_init(&waiter->cond, NULL);
    pthread_mutex_init(&waiter->lock, NULL);
    waiter->completed = false;
    waiter->error = 0;

    return waiter;
}

/**
 * 销毁等待者
 */
static void destroy_waiter(aof_commit_waiter_t *waiter)
{
    if (!waiter) {
        return;
    }

    pthread_cond_destroy(&waiter->cond);
    pthread_mutex_destroy(&waiter->lock);
    kmem_free(waiter);
}

/**
 * 等待完成
 */
static int waiter_wait(aof_commit_waiter_t *waiter, int timeout_ms)
{
    pthread_mutex_lock(&waiter->lock);

    if (waiter->completed) {
        pthread_mutex_unlock(&waiter->lock);
        return waiter->error;
    }

    if (timeout_ms > 0) {
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += timeout_ms / 1000;
        ts.tv_nsec += (timeout_ms % 1000) * 1000000;
        if (ts.tv_nsec >= 1000000000) {
            ts.tv_sec++;
            ts.tv_nsec -= 1000000000;
        }

        int ret = pthread_cond_timedwait(&waiter->cond, &waiter->lock, &ts);
        pthread_mutex_unlock(&waiter->lock);

        if (ret == ETIMEDOUT) {
            return -1; /* 超时 */
        }
    } else {
        pthread_cond_wait(&waiter->cond, &waiter->lock);
    }

    pthread_mutex_unlock(&waiter->lock);
    return waiter->error;
}

/**
 * 唤醒等待者
 */
static void waiter_wake(aof_commit_waiter_t *waiter, int error)
{
    pthread_mutex_lock(&waiter->lock);
    waiter->completed = true;
    waiter->error = error;
    pthread_cond_signal(&waiter->cond);
    pthread_mutex_unlock(&waiter->lock);
}

/**
 * EVERYSEC模式的定时器处理函数
 */
void aof_everysec_timer_handler(union sigval sv)
{
    (void)sv;

    aof_group_commit_t *gc = g_group_commit;
    if (!gc) {
        return;
    }

    /* 检查是否有待处理的fsync */
    if (atomic_load(&gc->fsync_pending)) {
        /* 唤醒所有等待者 */
        pthread_mutex_lock(&gc->waiter_lock);

        for (int i = 0; i < gc->waiter_count; i++) {
            if (gc->waiters[i]) {
                waiter_wake(gc->waiters[i], 0);
            }
        }

        gc->waiter_count = 0;
        atomic_store(&gc->fsync_pending, false);

        pthread_mutex_unlock(&gc->waiter_lock);

        /* 更新统计 */
        gc->fsync_count++;
    }
}

/**
 * 初始化组提交系统
 */
int aof_group_commit_init(aof_group_commit_t *gc, aof_sync_policy_t policy,
                          aof_batch_ctx_t *batch_ctx)
{
    if (!gc) {
        return -1;
    }

    memset(gc, 0, sizeof(*gc));
    gc->sync_policy = policy;

    pthread_mutex_init(&gc->waiter_lock, NULL);
    atomic_store(&gc->fsync_pending, false);
    clock_gettime(CLOCK_MONOTONIC, &gc->last_fsync);

    /* 设置为全局上下文 */
    g_group_commit = gc;

    /* EVERYSEC模式: 设置POSIX定时器 */
    if (policy == AOF_SYNC_EVERYSEC) {
        gc->sev.sigev_notify = SIGEV_THREAD;
        gc->sev.sigev_value.sival_ptr = gc;
        gc->sev.sigev_notify_function = aof_everysec_timer_handler;
        gc->sev.sigev_notify_attributes = NULL;

        if (timer_create(CLOCK_MONOTONIC, &gc->sev, &gc->everysec_timer) < 0) {
            perror("timer_create");
            return -1;
        }

        /* 设置1秒间隔 */
        gc->its.it_value.tv_sec = 1;
        gc->its.it_value.tv_nsec = 0;
        gc->its.it_interval.tv_sec = 1;
        gc->its.it_interval.tv_nsec = 0;

        if (timer_settime(gc->everysec_timer, 0, &gc->its, NULL) < 0) {
            perror("timer_settime");
            timer_delete(gc->everysec_timer);
            return -1;
        }
    }

    (void)batch_ctx; /* 保留供将来使用 */

    return 0;
}

/**
 * 销毁组提交系统
 */
void aof_group_commit_destroy(aof_group_commit_t *gc)
{
    if (!gc) {
        return;
    }

    /* 停止定时器 */
    if (gc->sync_policy == AOF_SYNC_EVERYSEC) {
        timer_delete(gc->everysec_timer);
    }

    /* 唤醒所有等待者 */
    pthread_mutex_lock(&gc->waiter_lock);
    for (int i = 0; i < gc->waiter_count; i++) {
        if (gc->waiters[i]) {
            waiter_wake(gc->waiters[i], -1);
        }
    }
    pthread_mutex_unlock(&gc->waiter_lock);

    pthread_mutex_destroy(&gc->waiter_lock);

    if (g_group_commit == gc) {
        g_group_commit = NULL;
    }
}

/**
 * 执行fsync操作
 */
int aof_perform_fsync(aof_group_commit_t *gc, int fd)
{
    if (!gc || fd < 0) {
        return -1;
    }

    /* 根据策略决定如何fsync */
    switch (gc->sync_policy) {
        case AOF_SYNC_ALWAYS:
            /* 立即fsync */
            if (fsync(fd) < 0) {
                return -1;
            }
            gc->fsync_count++;
            return 0;

        case AOF_SYNC_EVERYSEC:
            /* 由定时器处理fsync，这里只设置标志 */
            atomic_store(&gc->fsync_pending, true);
            clock_gettime(CLOCK_MONOTONIC, &gc->last_fsync);
            return 0;

        case AOF_SYNC_NO:
            /* 不执行fsync，依赖操作系统 */
            return 0;

        default:
            return -1;
    }
}

/**
 * 触发组提交
 */
int aof_group_commit_trigger(aof_group_commit_t *gc, int fd)
{
    if (!gc || fd < 0) {
        return -1;
    }

    /* 根据策略处理 */
    switch (gc->sync_policy) {
        case AOF_SYNC_ALWAYS: {
            /* 每个写入都fsync */
            if (fsync(fd) < 0) {
                return -1;
            }
            gc->fsync_count++;
            return 0;
        }

        case AOF_SYNC_EVERYSEC: {
            /* 创建等待者并加入队列 */
            aof_commit_waiter_t *waiter = create_waiter();
            if (!waiter) {
                return -1;
            }

            pthread_mutex_lock(&gc->waiter_lock);

            if (gc->waiter_count >= AOF_GROUP_COMMIT_MAX_WAITERS) {
                pthread_mutex_unlock(&gc->waiter_lock);
                destroy_waiter(waiter);
                return -1;
            }

            gc->waiters[gc->waiter_count++] = waiter;
            gc->grouped_waiters++;

            /* 设置fsync待处理标志 */
            atomic_store(&gc->fsync_pending, true);

            pthread_mutex_unlock(&gc->waiter_lock);

            /* 等待完成 */
            int ret = waiter_wait(waiter, 2000); /* 2秒超时 */

            /* 清理等待者 */
            destroy_waiter(waiter);

            return ret;
        }

        case AOF_SYNC_NO:
            /* 不等待，直接返回 */
            return 0;

        default:
            return -1;
    }
}

/**
 * 唤醒所有等待者
 */
void aof_group_commit_wake_all(aof_group_commit_t *gc, int error)
{
    if (!gc) {
        return;
    }

    pthread_mutex_lock(&gc->waiter_lock);

    for (int i = 0; i < gc->waiter_count; i++) {
        if (gc->waiters[i]) {
            waiter_wake(gc->waiters[i], error);
        }
    }

    gc->waiter_count = 0;
    atomic_store(&gc->fsync_pending, false);

    pthread_mutex_unlock(&gc->waiter_lock);
}

/**
 * 提交链接的write+fsync操作（使用io_uring的IOSQE_IO_LINK）
 */
int aof_submit_linked_write_fsync(aof_batch_ctx_t *ctx, int fd,
                                   const void *buf, size_t len, off_t offset)
{
    if (!ctx || fd < 0 || !buf || len == 0) {
        return -1;
    }

    /* 使用batch_ctx的链接写入函数 */
    extern int aof_batch_add_linked_write_fsync(aof_batch_ctx_t *ctx, int fd,
                                                 const void *buf, size_t len,
                                                 off_t offset, void *user_data);

    return aof_batch_add_linked_write_fsync(ctx, fd, buf, len, offset, NULL);
}

/**
 * 等待组提交完成（带超时）
 */
int aof_group_commit_wait(aof_group_commit_t *gc, int timeout_ms)
{
    if (!gc) {
        return -1;
    }

    aof_commit_waiter_t *waiter = create_waiter();
    if (!waiter) {
        return -1;
    }

    pthread_mutex_lock(&gc->waiter_lock);

    if (gc->waiter_count >= AOF_GROUP_COMMIT_MAX_WAITERS) {
        pthread_mutex_unlock(&gc->waiter_lock);
        destroy_waiter(waiter);
        return -1;
    }

    gc->waiters[gc->waiter_count++] = waiter;

    pthread_mutex_unlock(&gc->waiter_lock);

    /* 等待 */
    int ret = waiter_wait(waiter, timeout_ms);

    destroy_waiter(waiter);

    return ret;
}

/**
 * 获取组提交统计信息
 */
void aof_group_commit_get_stats(aof_group_commit_t *gc,
                                 uint64_t *group_commits,
                                 uint64_t *grouped_waiters,
                                 uint64_t *fsync_count)
{
    if (!gc) {
        return;
    }

    pthread_mutex_lock(&gc->waiter_lock);
    if (group_commits) *group_commits = gc->group_commits;
    if (grouped_waiters) *grouped_waiters = gc->grouped_waiters;
    if (fsync_count) *fsync_count = gc->fsync_count;
    pthread_mutex_unlock(&gc->waiter_lock);
}

/**
 * 强制执行fsync（用于关闭或checkpoint）
 */
int aof_group_commit_force_fsync(aof_group_commit_t *gc, int fd)
{
    if (!gc || fd < 0) {
        return -1;
    }

    if (fsync(fd) < 0) {
        return -1;
    }

    gc->fsync_count++;

    /* 唤醒所有等待者 */
    aof_group_commit_wake_all(gc, 0);

    return 0;
}