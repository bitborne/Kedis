/**
 * AOF io_uring 双缓冲系统实现
 * 使用CAS状态机实现零阻塞写入
 */

#include <stdio.h>
#include <string.h>
#include <stdatomic.h>
#include <pthread.h>
#include <errno.h>
#include <unistd.h>
#include <sys/mman.h>

#include "kvs_aof_io_uring.h"
#include "kmem.h"

/* 缓冲区状态转换宏 */
#define CAS_TRANSITION(buf, expected, desired) \
    atomic_compare_exchange_strong(&(buf)->state, &(expected), desired)

/**
 * 初始化单个缓冲区
 */
static int aof_buf_init(aof_uring_buf_t *buf, size_t size)
{
    buf->state = AOF_BUF_STATE_IDLE;
    buf->size = size;
    buf->used = 0;
    buf->sequence = 0;

    /* 页对齐内存分配（使用kmem） */
    buf->data = kmem_aligned_alloc(size, 4096);
    if (!buf->data) {
        return -1;
    }

    /* 使用mlock锁定内存，防止被交换到磁盘 */
    if (mlock(buf->data, size) != 0) {
        /* 非致命错误，继续 */
    }

    pthread_mutex_init(&buf->lock, NULL);
    return 0;
}

/**
 * 销毁单个缓冲区
 */
static void aof_buf_destroy(aof_uring_buf_t *buf)
{
    if (buf->data) {
        munlock(buf->data, buf->size);
        kmem_aligned_free(buf->data);
        buf->data = NULL;
    }
    pthread_mutex_destroy(&buf->lock);
}

/**
 * 初始化双缓冲系统
 */
int aof_double_buf_init(aof_uring_double_buf_t *dbuf, int fd)
{
    if (!dbuf || fd < 0) {
        return -1;
    }

    memset(dbuf, 0, sizeof(*dbuf));
    dbuf->fd = fd;
    dbuf->global_seq = 0;
    dbuf->shutdown = false;

    /* 初始化两个缓冲区 */
    for (int i = 0; i < 2; i++) {
        if (aof_buf_init(&dbuf->buffers[i], AOF_BUFFER_SIZE) != 0) {
            /* 清理已初始化的缓冲区 */
            for (int j = 0; j < i; j++) {
                aof_buf_destroy(&dbuf->buffers[j]);
            }
            return -1;
        }
    }

    pthread_cond_init(&dbuf->flush_cond, NULL);
    pthread_mutex_init(&dbuf->flush_lock, NULL);

    return 0;
}

/**
 * 销毁双缓冲系统
 */
void aof_double_buf_destroy(aof_uring_double_buf_t *dbuf)
{
    if (!dbuf) {
        return;
    }

    dbuf->shutdown = true;

    /* 唤醒所有等待的线程 */
    pthread_cond_broadcast(&dbuf->flush_cond);

    /* 销毁两个缓冲区 */
    for (int i = 0; i < 2; i++) {
        aof_buf_destroy(&dbuf->buffers[i]);
    }

    pthread_cond_destroy(&dbuf->flush_cond);
    pthread_mutex_destroy(&dbuf->flush_lock);
}

/**
 * 获取用于写入的缓冲区（CAS操作）
 * 状态转换: IDLE -> FILLING 或保持 FILLING
 */
int aof_buf_acquire_for_write(aof_uring_double_buf_t *dbuf, size_t size,
                              void **buf_out, size_t *offset_out)
{
    if (!dbuf || !buf_out || !offset_out || size > AOF_BUFFER_SIZE) {
        return -1;
    }

    int active_idx = atomic_load(&dbuf->active_idx);
    aof_uring_buf_t *buf = &dbuf->buffers[active_idx];

    /* 尝试将状态从IDLE转换为FILLING */
    aof_buf_state_t expected = AOF_BUF_STATE_IDLE;
    if (!CAS_TRANSITION(buf, expected, AOF_BUF_STATE_FILLING)) {
        /* 如果不是IDLE，检查是否已经是FILLING */
        aof_buf_state_t current = atomic_load(&buf->state);
        if (current != AOF_BUF_STATE_FILLING) {
            /* 缓冲区不可用，需要触发切换 */
            return -1;
        }
    }

    /* 获取锁并分配空间 */
    pthread_mutex_lock(&buf->lock);

    /* 检查是否有足够空间 */
    if (buf->used + size > AOF_BUFFER_FLUSH_THRESHOLD) {
        pthread_mutex_unlock(&buf->lock);

        /* 标记为READY，触发刷新 */
        aof_buf_state_t ready_expected = AOF_BUF_STATE_FILLING;
        CAS_TRANSITION(buf, ready_expected, AOF_BUF_STATE_READY);

        /* 触发缓冲区切换 */
        aof_buf_trigger_switch(dbuf);

        /* 通知刷新线程 */
        pthread_cond_signal(&dbuf->flush_cond);

        return -1; /* 调用者需要重试 */
    }

    *buf_out = (char *)buf->data + buf->used;
    *offset_out = buf->used;
    buf->used += size;

    pthread_mutex_unlock(&buf->lock);

    return 0;
}

/**
 * 标记缓冲区为就绪状态（CAS: FILLING -> READY）
 */
int aof_buf_mark_ready(aof_uring_double_buf_t *dbuf, size_t used)
{
    if (!dbuf) {
        return -1;
    }

    int active_idx = atomic_load(&dbuf->active_idx);
    aof_uring_buf_t *buf = &dbuf->buffers[active_idx];

    pthread_mutex_lock(&buf->lock);
    buf->used = used;
    pthread_mutex_unlock(&buf->lock);

    /* CAS: FILLING -> READY */
    aof_buf_state_t expected = AOF_BUF_STATE_FILLING;
    if (!CAS_TRANSITION(buf, expected, AOF_BUF_STATE_READY)) {
        /* 状态不是FILLING，可能是其他线程已经处理 */
        return -1;
    }

    /* 通知刷新线程 */
    pthread_cond_signal(&dbuf->flush_cond);

    return 0;
}

/**
 * 等待可刷新的缓冲区（CAS: READY -> WRITING）
 */
int aof_buf_wait_for_flushable(aof_uring_double_buf_t *dbuf, int *buf_idx)
{
    if (!dbuf || !buf_idx) {
        return -1;
    }

    pthread_mutex_lock(&dbuf->flush_lock);

    while (!dbuf->shutdown) {
        /* 检查两个缓冲区 */
        for (int i = 0; i < 2; i++) {
            aof_uring_buf_t *buf = &dbuf->buffers[i];
            aof_buf_state_t state = atomic_load(&buf->state);

            if (state == AOF_BUF_STATE_READY) {
                /* CAS: READY -> WRITING */
                aof_buf_state_t expected = AOF_BUF_STATE_READY;
                if (CAS_TRANSITION(buf, expected, AOF_BUF_STATE_WRITING)) {
                    *buf_idx = i;
                    pthread_mutex_unlock(&dbuf->flush_lock);
                    return 0;
                }
            }
        }

        /* 没有就绪的缓冲区，等待 */
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_nsec += 10000000; /* 10ms */
        if (ts.tv_nsec >= 1000000000) {
            ts.tv_sec++;
            ts.tv_nsec -= 1000000000;
        }

        pthread_cond_timedwait(&dbuf->flush_cond, &dbuf->flush_lock, &ts);
    }

    pthread_mutex_unlock(&dbuf->flush_lock);
    return -1; /* 关闭 */
}

/**
 * 释放缓冲区回空闲状态（CAS: SYNCING -> IDLE）
 */
int aof_buf_release(aof_uring_double_buf_t *dbuf, int buf_idx)
{
    if (!dbuf || buf_idx < 0 || buf_idx >= 2) {
        return -1;
    }

    aof_uring_buf_t *buf = &dbuf->buffers[buf_idx];

    /* CAS: SYNCING -> IDLE */
    aof_buf_state_t expected = AOF_BUF_STATE_SYNCING;
    if (!CAS_TRANSITION(buf, expected, AOF_BUF_STATE_IDLE)) {
        return -1;
    }

    /* 重置缓冲区 */
    pthread_mutex_lock(&buf->lock);
    buf->used = 0;
    pthread_mutex_unlock(&buf->lock);

    return 0;
}

/**
 * 触发缓冲区切换
 * 状态机: FILLING -> READY, 切换active_idx, 新缓冲区IDLE -> FILLING
 */
int aof_buf_trigger_switch(aof_uring_double_buf_t *dbuf)
{
    if (!dbuf) {
        return -1;
    }

    int current_idx = atomic_load(&dbuf->active_idx);
    aof_uring_buf_t *current_buf = &dbuf->buffers[current_idx];

    /* 将当前缓冲区标记为READY */
    aof_buf_state_t expected = AOF_BUF_STATE_FILLING;
    if (!CAS_TRANSITION(current_buf, expected, AOF_BUF_STATE_READY)) {
        /* 可能已经是READY或正在被处理 */
        aof_buf_state_t state = atomic_load(&current_buf->state);
        if (state != AOF_BUF_STATE_READY &&
            state != AOF_BUF_STATE_WRITING &&
            state != AOF_BUF_STATE_SYNCING) {
            return -1;
        }
    }

    /* 切换到另一个缓冲区 */
    int new_idx = (current_idx + 1) % 2;
    aof_uring_buf_t *new_buf = &dbuf->buffers[new_idx];

    /* 确保新缓冲区是IDLE状态 */
    aof_buf_state_t idle_expected = AOF_BUF_STATE_IDLE;
    if (!CAS_TRANSITION(new_buf, idle_expected, AOF_BUF_STATE_FILLING)) {
        /* 新缓冲区不是IDLE，可能还在处理中 */
        return -1;
    }

    /* 原子更新active索引 */
    atomic_store(&dbuf->active_idx, new_idx);

    /* 分配新的序列号 */
    uint64_t seq = atomic_fetch_add(&dbuf->global_seq, 1);
    pthread_mutex_lock(&new_buf->lock);
    new_buf->sequence = seq;
    pthread_mutex_unlock(&new_buf->lock);

    /* 通知刷新线程 */
    pthread_cond_signal(&dbuf->flush_cond);

    return 0;
}

/**
 * 获取当前缓冲区已使用空间（用于调试/监控）
 */
size_t aof_double_buf_used(aof_uring_double_buf_t *dbuf)
{
    if (!dbuf) {
        return 0;
    }

    int active_idx = atomic_load(&dbuf->active_idx);
    aof_uring_buf_t *buf = &dbuf->buffers[active_idx];

    pthread_mutex_lock(&buf->lock);
    size_t used = buf->used;
    pthread_mutex_unlock(&buf->lock);

    return used;
}

/**
 * 强制刷新当前缓冲区
 */
int aof_double_buf_flush(aof_uring_double_buf_t *dbuf)
{
    if (!dbuf) {
        return -1;
    }

    int active_idx = atomic_load(&dbuf->active_idx);
    aof_uring_buf_t *buf = &dbuf->buffers[active_idx];

    pthread_mutex_lock(&buf->lock);
    size_t used = buf->used;
    pthread_mutex_unlock(&buf->lock);

    if (used > 0) {
        return aof_buf_mark_ready(dbuf, used);
    }

    return 0;
}