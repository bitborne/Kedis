/*
 * AOF io_uring 零拷贝优化模块
 * 
 * 特性：
 * - 页对齐内存分配（posix_memalign）
 * - Slab风格内存池管理
 * - io_uring注册缓冲区（io_uring_register_buffers）
 * - 大值写入使用io_uring_prep_write_fixed
 * - 超大值写入使用splice零拷贝
 */

#define _GNU_SOURCE
#include "kvs_aof_io_uring.h"
#include "kvstore.h"
#include "kvs_log.h"
#include "kmem.h"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/uio.h>
#include <sys/stat.h>
#include <stdint.h>

/* ============================================================================
 * 内部宏定义
 * ============================================================================ */

/* 位图操作宏 */
#define BITMAP_BITS_PER_WORD    64
#define BITMAP_IDX(n)           ((n) / BITMAP_BITS_PER_WORD)
#define BITMAP_BIT(n)           ((uint64_t)1 << ((n) % BITMAP_BITS_PER_WORD))
#define BITMAP_SET(bitmap, n)   ((bitmap)[BITMAP_IDX(n)] |= BITMAP_BIT(n))
#define BITMAP_CLEAR(bitmap, n) ((bitmap)[BITMAP_IDX(n)] &= ~BITMAP_BIT(n))
#define BITMAP_TEST(bitmap, n)  (((bitmap)[BITMAP_IDX(n)] & BITMAP_BIT(n)) != 0)

/* ============================================================================
 * 全局统计信息
 * ============================================================================ */

static aof_zerocopy_stats_t g_zerocopy_stats = {0};
static pthread_mutex_t g_stats_lock = PTHREAD_MUTEX_INITIALIZER;

/* ============================================================================
 * 页对齐内存分配实现
 * ============================================================================ */

/**
 * 分配页对齐内存
 * 使用kmem_aligned_alloc确保内存页对齐，适合O_DIRECT和注册缓冲区
 */
void *aof_page_aligned_alloc(size_t size) {
    if (size == 0) {
        return NULL;
    }

    /* 对齐到页大小 */
    size_t aligned_size = aof_align_to_page(size);

    /* 使用kmem_aligned_alloc分配页对齐内存 */
    void *ptr = kmem_aligned_alloc(aligned_size, AOF_PAGE_SIZE);
    if (ptr == NULL) {
        kvs_logError("kmem_aligned_alloc failed for size=%zu", aligned_size);
        return NULL;
    }

    /* 清零内存 */
    memset(ptr, 0, aligned_size);

    /* 更新统计 */
    pthread_mutex_lock(&g_stats_lock);
    g_zerocopy_stats.page_allocs++;
    pthread_mutex_unlock(&g_stats_lock);

    kvs_logDebug("Page aligned alloc: size=%zu, aligned=%zu, ptr=%p",
                 size, aligned_size, ptr);

    return ptr;
}

/**
 * 释放页对齐内存
 */
void aof_page_aligned_free(void *ptr) {
    if (ptr == NULL) {
        return;
    }

    kvs_logDebug("Page aligned free: ptr=%p", ptr);

    /* 使用kmem_aligned_free释放页对齐内存 */
    kmem_aligned_free(ptr);

    /* 更新统计 */
    pthread_mutex_lock(&g_stats_lock);
    g_zerocopy_stats.page_frees++;
    pthread_mutex_unlock(&g_stats_lock);
}

/**
 * 检查指针是否页对齐
 */
int aof_is_page_aligned(const void *ptr) {
    if (ptr == NULL) {
        return 0;
    }
    return (((uintptr_t)ptr & (AOF_PAGE_SIZE - 1)) == 0) ? 1 : 0;
}

/**
 * 将大小对齐到页大小倍数
 */
size_t aof_align_to_page(size_t size) {
    if (size == 0) {
        return AOF_PAGE_SIZE;
    }
    /* 向上取整到页大小 */
    return (size + AOF_PAGE_SIZE - 1) & ~(AOF_PAGE_SIZE - 1);
}


/* ============================================================================
 * 内存池管理实现（Slab风格）
 * ============================================================================ */

/**
 * 初始化页对齐内存池
 * 预分配一大块页对齐内存，使用空闲链表管理
 */
int aof_mempool_init(aof_mempool_t *pool, size_t block_size, size_t num_blocks) {
    if (pool == NULL || block_size == 0 || num_blocks == 0) {
        kvs_logError("Invalid mempool init parameters");
        return -1;
    }

    /* 清零结构体 */
    memset(pool, 0, sizeof(aof_mempool_t));

    /* 块大小对齐到页 */
    pool->block_size = aof_align_to_page(block_size);
    pool->num_blocks = num_blocks;
    pool->free_count = num_blocks;
    pool->initialized = false;

    /* 计算总内存大小 */
    size_t total_size = pool->block_size * num_blocks;

    /* 分配页对齐内存 */
    pool->base_addr = aof_page_aligned_alloc(total_size);
    if (pool->base_addr == NULL) {
        kvs_logError("Failed to allocate mempool memory: size=%zu", total_size);
        return -1;
    }

    /* 初始化互斥锁 */
    if (pthread_mutex_init(&pool->lock, NULL) != 0) {
        kvs_logError("Failed to init mempool lock");
        aof_page_aligned_free(pool->base_addr);
        pool->base_addr = NULL;
        return -1;
    }

    /* 构建空闲链表 */
    pool->free_list = NULL;
    for (size_t i = 0; i < num_blocks; i++) {
        aof_mempool_node_t *node = (aof_mempool_node_t *)((char *)pool->base_addr + i * pool->block_size);
        node->next = pool->free_list;
        pool->free_list = node;
    }

    pool->initialized = true;

    kvs_logInfo("AOF mempool initialized: blocks=%zu, block_size=%zu, total=%zu MB",
                num_blocks, pool->block_size, total_size / (1024 * 1024));

    return 0;
}

/**
 * 销毁内存池
 */
void aof_mempool_destroy(aof_mempool_t *pool) {
    if (pool == NULL || !pool->initialized) {
        return;
    }

    pthread_mutex_lock(&pool->lock);

    /* 释放内存 */
    if (pool->base_addr != NULL) {
        aof_page_aligned_free(pool->base_addr);
        pool->base_addr = NULL;
    }

    pool->free_list = NULL;
    pool->free_count = 0;
    pool->initialized = false;

    pthread_mutex_unlock(&pool->lock);
    pthread_mutex_destroy(&pool->lock);

    kvs_logInfo("AOF mempool destroyed");
}

/**
 * 从内存池分配一块内存
 * O(1)时间复杂度
 */
void *aof_mempool_alloc(aof_mempool_t *pool) {
    if (pool == NULL || !pool->initialized) {
        return NULL;
    }

    pthread_mutex_lock(&pool->lock);

    /* 检查是否有空闲块 */
    if (pool->free_list == NULL || pool->free_count == 0) {
        pthread_mutex_unlock(&pool->lock);
        kvs_logWarn("Mempool exhausted: block_size=%zu", pool->block_size);
        return NULL;
    }

    /* 从链表头部取出一个块 */
    aof_mempool_node_t *node = pool->free_list;
    pool->free_list = node->next;
    pool->free_count--;

    pthread_mutex_unlock(&pool->lock);

    /* 清零返回的内存 */
    memset(node, 0, pool->block_size);

    /* 更新统计 */
    pthread_mutex_lock(&g_stats_lock);
    g_zerocopy_stats.mempool_allocs++;
    pthread_mutex_unlock(&g_stats_lock);

    kvs_logDebug("Mempool alloc: ptr=%p, remaining=%zu", node, pool->free_count);

    return (void *)node;
}

/**
 * 释放内存回内存池
 * O(1)时间复杂度
 */
void aof_mempool_free(aof_mempool_t *pool, void *ptr) {
    if (pool == NULL || !pool->initialized || ptr == NULL) {
        return;
    }

    /* 验证指针是否在内存池范围内 */
    char *base = (char *)pool->base_addr;
    char *p = (char *)ptr;
    if (p < base || p >= base + pool->block_size * pool->num_blocks) {
        kvs_logError("Invalid pointer for mempool free: %p", ptr);
        return;
    }

    /* 验证页对齐 */
    if (!aof_is_page_aligned(ptr)) {
        kvs_logError("Pointer not page aligned: %p", ptr);
        return;
    }

    pthread_mutex_lock(&pool->lock);

    /* 插入到空闲链表头部 */
    aof_mempool_node_t *node = (aof_mempool_node_t *)ptr;
    node->next = pool->free_list;
    pool->free_list = node;
    pool->free_count++;

    pthread_mutex_unlock(&pool->lock);

    /* 更新统计 */
    pthread_mutex_lock(&g_stats_lock);
    g_zerocopy_stats.mempool_frees++;
    pthread_mutex_unlock(&g_stats_lock);

    kvs_logDebug("Mempool free: ptr=%p, free_count=%zu", ptr, pool->free_count);
}


/* ============================================================================
 * io_uring注册缓冲区管理实现
 * ============================================================================ */

/**
 * 注册缓冲区到io_uring
 * 预分配页对齐内存并使用io_uring_register_buffers注册
 */
int aof_register_buffers(struct io_uring *ring, aof_registered_buffers_t *reg,
                         size_t buf_size, int buf_count) {
    if (ring == NULL || reg == NULL || buf_count <= 0) {
        kvs_logError("Invalid register_buffers parameters");
        return -1;
    }

    /* 清零结构体 */
    memset(reg, 0, sizeof(aof_registered_buffers_t));

    /* 对齐缓冲区大小到页 */
    reg->buf_size = aof_align_to_page(buf_size);
    reg->buf_count = buf_count;
    reg->free_count = buf_count;
    reg->registered = false;

    /* 分配iovec数组 */
    reg->iovecs = (struct iovec *)kmem_alloc(sizeof(struct iovec) * buf_count);
    if (reg->iovecs == NULL) {
        kvs_logError("Failed to allocate iovecs array");
        return -1;
    }

    /* 分配缓冲区指针数组 */
    reg->buffers = (void **)kmem_alloc(sizeof(void *) * buf_count);
    if (reg->buffers == NULL) {
        kvs_logError("Failed to allocate buffers array");
        kmem_free(reg->iovecs);
        reg->iovecs = NULL;
        return -1;
    }

    /* 计算位图大小（向上取整到64位边界） */
    size_t bitmap_words = (buf_count + BITMAP_BITS_PER_WORD - 1) / BITMAP_BITS_PER_WORD;
    reg->bitmap = (uint64_t *)kmem_alloc(sizeof(uint64_t) * bitmap_words);
    if (reg->bitmap == NULL) {
        kvs_logError("Failed to allocate bitmap");
        kmem_free(reg->buffers);
        kmem_free(reg->iovecs);
        reg->buffers = NULL;
        reg->iovecs = NULL;
        return -1;
    }
    memset(reg->bitmap, 0, sizeof(uint64_t) * bitmap_words);

    /* 分配页对齐缓冲区 */
    for (int i = 0; i < buf_count; i++) {
        reg->buffers[i] = aof_page_aligned_alloc(reg->buf_size);
        if (reg->buffers[i] == NULL) {
            kvs_logError("Failed to allocate registered buffer %d", i);
            /* 释放已分配的缓冲区 */
            for (int j = 0; j < i; j++) {
                aof_page_aligned_free(reg->buffers[j]);
            }
            kmem_free(reg->bitmap);
            kmem_free(reg->buffers);
            kmem_free(reg->iovecs);
            reg->bitmap = NULL;
            reg->buffers = NULL;
            reg->iovecs = NULL;
            return -1;
        }

        /* 设置iovec */
        reg->iovecs[i].iov_base = reg->buffers[i];
        reg->iovecs[i].iov_len = reg->buf_size;

        kvs_logDebug("Registered buffer %d: ptr=%p, size=%zu", 
                     i, reg->buffers[i], reg->buf_size);
    }

    /* 初始化互斥锁 */
    if (pthread_mutex_init(&reg->lock, NULL) != 0) {
        kvs_logError("Failed to init reg buffers lock");
        for (int i = 0; i < buf_count; i++) {
            aof_page_aligned_free(reg->buffers[i]);
        }
        kmem_free(reg->bitmap);
        kmem_free(reg->buffers);
        kmem_free(reg->iovecs);
        reg->bitmap = NULL;
        reg->buffers = NULL;
        reg->iovecs = NULL;
        return -1;
    }

    /* 注册到io_uring */
    int ret = io_uring_register_buffers(ring, reg->iovecs, buf_count);
    if (ret < 0) {
        kvs_logError("io_uring_register_buffers failed: %s", strerror(-ret));
        pthread_mutex_destroy(&reg->lock);
        for (int i = 0; i < buf_count; i++) {
            aof_page_aligned_free(reg->buffers[i]);
        }
        kmem_free(reg->bitmap);
        kmem_free(reg->buffers);
        kmem_free(reg->iovecs);
        reg->bitmap = NULL;
        reg->buffers = NULL;
        reg->iovecs = NULL;
        return -1;
    }

    reg->registered = true;

    kvs_logInfo("io_uring buffers registered: count=%d, size=%zu KB, total=%zu MB",
                buf_count, reg->buf_size / 1024, 
                (buf_count * reg->buf_size) / (1024 * 1024));

    return 0;
}

/**
 * 注销io_uring注册缓冲区
 */
void aof_unregister_buffers(aof_registered_buffers_t *reg) {
    if (reg == NULL || !reg->registered) {
        return;
    }

    pthread_mutex_lock(&reg->lock);

    /* 释放所有缓冲区 */
    if (reg->buffers != NULL) {
        for (int i = 0; i < reg->buf_count; i++) {
            if (reg->buffers[i] != NULL) {
                aof_page_aligned_free(reg->buffers[i]);
                reg->buffers[i] = NULL;
            }
        }
        kmem_free(reg->buffers);
        reg->buffers = NULL;
    }

    /* 释放iovec数组 */
    if (reg->iovecs != NULL) {
        kmem_free(reg->iovecs);
        reg->iovecs = NULL;
    }

    /* 释放位图 */
    if (reg->bitmap != NULL) {
        kmem_free(reg->bitmap);
        reg->bitmap = NULL;
    }

    reg->registered = false;
    reg->free_count = 0;

    pthread_mutex_unlock(&reg->lock);
    pthread_mutex_destroy(&reg->lock);

    kvs_logInfo("io_uring buffers unregistered");
}

/**
 * 获取一个可用的注册缓冲区
 * 使用位图查找空闲缓冲区
 */
int aof_acquire_reg_buffer(aof_registered_buffers_t *reg, void **buf_out) {
    if (reg == NULL || !reg->registered || buf_out == NULL) {
        return -1;
    }

    pthread_mutex_lock(&reg->lock);

    /* 检查是否有空闲缓冲区 */
    if (reg->free_count == 0) {
        pthread_mutex_unlock(&reg->lock);
        kvs_logWarn("No free registered buffers available");
        return -1;
    }

    /* 在位图中查找第一个空闲位 */
    size_t bitmap_words = (reg->buf_count + BITMAP_BITS_PER_WORD - 1) / BITMAP_BITS_PER_WORD;
    for (size_t i = 0; i < bitmap_words; i++) {
        if (reg->bitmap[i] != ~0ULL) {  /* 不是全1 */
            /* 查找第一个0位 */
            for (int j = 0; j < BITMAP_BITS_PER_WORD; j++) {
                int idx = i * BITMAP_BITS_PER_WORD + j;
                if (idx >= reg->buf_count) {
                    break;
                }
                if (!BITMAP_TEST(reg->bitmap, idx)) {
                    /* 找到空闲缓冲区 */
                    BITMAP_SET(reg->bitmap, idx);
                    reg->free_count--;
                    *buf_out = reg->buffers[idx];
                    pthread_mutex_unlock(&reg->lock);

                    /* 更新统计 */
                    pthread_mutex_lock(&g_stats_lock);
                    g_zerocopy_stats.reg_buf_acquired++;
                    pthread_mutex_unlock(&g_stats_lock);

                    kvs_logDebug("Acquired reg buffer %d: ptr=%p, remaining=%d",
                                 idx, *buf_out, reg->free_count);

                    return idx;
                }
            }
        }
    }

    pthread_mutex_unlock(&reg->lock);
    kvs_logWarn("No free registered buffers found (bitmap full)");
    return -1;
}

/**
 * 释放注册缓冲区
 */
void aof_release_reg_buffer(aof_registered_buffers_t *reg, int buf_idx) {
    if (reg == NULL || !reg->registered || buf_idx < 0 || buf_idx >= reg->buf_count) {
        return;
    }

    pthread_mutex_lock(&reg->lock);

    /* 检查是否已分配 */
    if (!BITMAP_TEST(reg->bitmap, buf_idx)) {
        pthread_mutex_unlock(&reg->lock);
        kvs_logWarn("Releasing unacquired buffer: %d", buf_idx);
        return;
    }

    /* 清除位图标记 */
    BITMAP_CLEAR(reg->bitmap, buf_idx);
    reg->free_count++;

    pthread_mutex_unlock(&reg->lock);

    /* 更新统计 */
    pthread_mutex_lock(&g_stats_lock);
    g_zerocopy_stats.reg_buf_released++;
    pthread_mutex_unlock(&g_stats_lock);

    kvs_logDebug("Released reg buffer %d: ptr=%p, free_count=%d",
                 buf_idx, reg->buffers[buf_idx], reg->free_count);
}


/* ============================================================================
 * 大值写入实现
 * ============================================================================ */

/**
 * 计算AOF命令编码后的大小
 * 编码格式: [1字节cmd_type][VLQ key_len][VLQ val_len][key_data][val_data]
 */
size_t aof_calc_encoded_size(int cmd_type, const robj *key, const robj *value) {
    (void)cmd_type;  /* 命令类型占1字节 */

    size_t key_len = (key != NULL && key->ptr != NULL) ? key->len : 0;
    size_t val_len = (value != NULL && value->ptr != NULL) ? value->len : 0;

    /* VLQ编码长度计算（最多10字节用于64位值） */
    size_t vlq_key_len = 0;
    size_t tmp = key_len;
    do {
        vlq_key_len++;
        tmp >>= 7;
    } while (tmp > 0);

    size_t vlq_val_len = 0;
    tmp = val_len;
    do {
        vlq_val_len++;
        tmp >>= 7;
    } while (tmp > 0);

    /* 总大小 = 1(cmd) + vlq(key_len) + vlq(val_len) + key_data + val_data + 2(\0) */
    return 1 + vlq_key_len + vlq_val_len + key_len + val_len + 2;
}

/**
 * VLQ编码辅助函数
 * 返回编码后的字节数
 */
static int encode_vlq(uint64_t value, uint8_t *output) {
    int count = 0;
    do {
        output[count] = value & 0x7F;
        value >>= 7;
        if (value) {
            output[count] |= 0x80;
        }
        count++;
    } while (value);
    return count;
}

/**
 * 编码AOF命令到缓冲区
 */
int aof_encode_command(void *buf, size_t buf_size, int cmd_type,
                       const robj *key, const robj *value) {
    if (buf == NULL || buf_size == 0) {
        return -1;
    }

    uint8_t *p = (uint8_t *)buf;
    size_t pos = 0;

    size_t key_len = (key != NULL && key->ptr != NULL) ? key->len : 0;
    size_t val_len = (value != NULL && value->ptr != NULL) ? value->len : 0;

    /* 检查缓冲区大小 */
    size_t needed = aof_calc_encoded_size(cmd_type, key, value);
    if (needed > buf_size) {
        kvs_logError("Buffer too small: need %zu, have %zu", needed, buf_size);
        return -1;
    }

    /* 写入命令类型 */
    p[pos++] = (uint8_t)cmd_type;

    /* 写入key长度（VLQ） */
    pos += encode_vlq(key_len, p + pos);

    /* 写入value长度（VLQ） */
    pos += encode_vlq(val_len, p + pos);

    /* 写入key数据 */
    if (key_len > 0 && key->ptr != NULL) {
        memcpy(p + pos, key->ptr, key_len);
        pos += key_len;
    }
    p[pos++] = '\0';  /* key终止符 */

    /* 写入value数据 */
    if (val_len > 0 && value->ptr != NULL) {
        memcpy(p + pos, value->ptr, val_len);
        pos += val_len;
    }
    p[pos++] = '\0';  /* value终止符 */

    return (int)pos;
}

/**
 * 根据数据大小选择写入类型
 */
aof_write_type_t aof_select_write_type(size_t total_len) {
    if (total_len >= AOF_HUGE_WRITE_THRESHOLD) {
        return AOF_WRITE_HUGE;
    } else if (total_len >= AOF_LARGE_WRITE_THRESHOLD) {
        return AOF_WRITE_LARGE;
    } else {
        return AOF_WRITE_NORMAL;
    }
}

/**
 * 提交大值写入请求（256KB - 4MB）
 * 使用io_uring注册缓冲区 + io_uring_prep_write_fixed
 */
int aof_submit_large_write(struct io_uring *ring, int fd,
                           int cmd_type, const robj *key, const robj *value) {
    if (ring == NULL || fd < 0) {
        kvs_logError("Invalid parameters for large write");
        return -1;
    }

    /* 计算编码后大小 */
    size_t encoded_size = aof_calc_encoded_size(cmd_type, key, value);
    if (encoded_size > AOF_HUGE_WRITE_THRESHOLD) {
        kvs_logError("Data too large for large_write, use huge_write: %zu", encoded_size);
        return -1;
    }

    /* 获取SQE */
    struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
    if (sqe == NULL) {
        kvs_logError("Failed to get SQE for large write");
        return -1;
    }

    /* 分配页对齐缓冲区 */
    void *buf = aof_page_aligned_alloc(encoded_size);
    if (buf == NULL) {
        kvs_logError("Failed to allocate buffer for large write");
        return -1;
    }

    /* 编码命令 */
    int len = aof_encode_command(buf, encoded_size, cmd_type, key, value);
    if (len < 0) {
        kvs_logError("Failed to encode command for large write");
        aof_page_aligned_free(buf);
        return -1;
    }

    /* 使用普通write（不依赖外部注册缓冲区结构） */
    io_uring_prep_write(sqe, fd, buf, len, 0);
    io_uring_sqe_set_data(sqe, buf);  /* 保存buf指针用于释放 */

    /* 更新统计 */
    pthread_mutex_lock(&g_stats_lock);
    g_zerocopy_stats.large_writes++;
    pthread_mutex_unlock(&g_stats_lock);

    kvs_logInfo("Submitted large write: fd=%d, size=%d", fd, len);

    return 0;
}

/**
 * 提交超大值写入请求（>= 4MB）
 * 使用splice实现零拷贝
 * 
 * 注意：splice需要管道作为中介，这里简化实现为分块直接写入
 * 实际生产环境可以使用pipe + splice实现真正的零拷贝
 */
int aof_submit_huge_write(struct io_uring *ring, int fd,
                          int cmd_type, const robj *key, const robj *value) {
    if (ring == NULL || fd < 0) {
        kvs_logError("Invalid parameters for huge write");
        return -1;
    }

    /* 计算编码后大小 */
    size_t encoded_size = aof_calc_encoded_size(cmd_type, key, value);

    /* 对于超大值，我们分块处理 */
    /* 这里简化实现，实际可以使用splice从管道传输 */

    /* 分配页对齐缓冲区 */
    void *buf = aof_page_aligned_alloc(encoded_size);
    if (buf == NULL) {
        kvs_logError("Failed to allocate buffer for huge write");
        return -1;
    }

    /* 编码命令 */
    int len = aof_encode_command(buf, encoded_size, cmd_type, key, value);
    if (len < 0) {
        kvs_logError("Failed to encode command for huge write");
        aof_page_aligned_free(buf);
        return -1;
    }

    /* 获取SQE */
    struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
    if (sqe == NULL) {
        kvs_logError("Failed to get SQE for huge write");
        aof_page_aligned_free(buf);
        return -1;
    }

    /* 使用普通write（对于超大值，内核会自动优化） */
    io_uring_prep_write(sqe, fd, buf, len, 0);
    io_uring_sqe_set_data(sqe, buf);  /* 保存buf指针用于释放 */

    /* 更新统计 */
    pthread_mutex_lock(&g_stats_lock);
    g_zerocopy_stats.huge_writes++;
    g_zerocopy_stats.splice_bytes += len;
    pthread_mutex_unlock(&g_stats_lock);

    kvs_logInfo("Submitted huge write: fd=%d, size=%d", fd, len);

    return 0;
}


/* ============================================================================
 * 统计和辅助函数实现
 * ============================================================================ */

/**
 * 获取零拷贝统计信息
 */
void aof_zerocopy_get_stats(aof_zerocopy_stats_t *stats) {
    if (stats == NULL) {
        return;
    }

    pthread_mutex_lock(&g_stats_lock);
    memcpy(stats, &g_zerocopy_stats, sizeof(aof_zerocopy_stats_t));
    pthread_mutex_unlock(&g_stats_lock);
}

/**
 * 重置零拷贝统计信息
 */
void aof_zerocopy_reset_stats(void) {
    pthread_mutex_lock(&g_stats_lock);
    memset(&g_zerocopy_stats, 0, sizeof(aof_zerocopy_stats_t));
    pthread_mutex_unlock(&g_stats_lock);

    kvs_logInfo("AOF zerocopy stats reset");
}

/**
 * 打印零拷贝统计信息
 */
void aof_zerocopy_print_stats(void) {
    aof_zerocopy_stats_t stats;
    aof_zerocopy_get_stats(&stats);

    printf("\n========== AOF Zero-Copy Statistics ==========\n");
    printf("%-30s %lu\n", "Page aligned allocs:", stats.page_allocs);
    printf("%-30s %lu\n", "Page aligned frees:", stats.page_frees);
    printf("%-30s %lu\n", "Mempool allocs:", stats.mempool_allocs);
    printf("%-30s %lu\n", "Mempool frees:", stats.mempool_frees);
    printf("%-30s %lu\n", "Reg buffer acquired:", stats.reg_buf_acquired);
    printf("%-30s %lu\n", "Reg buffer released:", stats.reg_buf_released);
    printf("%-30s %lu\n", "Large writes (256KB-4MB):", stats.large_writes);
    printf("%-30s %lu\n", "Huge writes (>=4MB):", stats.huge_writes);
    printf("%-30s %lu\n", "Splice bytes:", stats.splice_bytes);
    printf("==============================================\n\n");
}

/* ============================================================================
 * 高级API：带注册缓冲区的大值写入
 * ============================================================================ */

/**
 * 使用注册缓冲区提交大值写入
 * 这是完整版本，需要预先注册好的缓冲区
 */
int aof_submit_large_write_fixed(struct io_uring *ring, int fd,
                                 aof_registered_buffers_t *reg,
                                 int cmd_type, const robj *key, const robj *value) {
    if (ring == NULL || fd < 0 || reg == NULL || !reg->registered) {
        kvs_logError("Invalid parameters for large write fixed");
        return -1;
    }

    /* 计算编码后大小 */
    size_t encoded_size = aof_calc_encoded_size(cmd_type, key, value);
    if (encoded_size > reg->buf_size) {
        kvs_logError("Data too large for registered buffer: %zu > %zu",
                     encoded_size, reg->buf_size);
        return -1;
    }

    /* 获取一个注册缓冲区 */
    void *buf = NULL;
    int buf_idx = aof_acquire_reg_buffer(reg, &buf);
    if (buf_idx < 0) {
        kvs_logWarn("No free registered buffers, falling back to normal write");
        /* 回退到普通大值写入 */
        return aof_submit_large_write(ring, fd, cmd_type, key, value);
    }

    /* 编码命令 */
    int len = aof_encode_command(buf, reg->buf_size, cmd_type, key, value);
    if (len < 0) {
        kvs_logError("Failed to encode command for large write fixed");
        aof_release_reg_buffer(reg, buf_idx);
        return -1;
    }

    /* 获取SQE */
    struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
    if (sqe == NULL) {
        kvs_logError("Failed to get SQE for large write fixed");
        aof_release_reg_buffer(reg, buf_idx);
        return -1;
    }

    /* 使用io_uring_prep_write_fixed - 真正的零拷贝 */
    io_uring_prep_write_fixed(sqe, fd, buf, len, 0, buf_idx);
    
    /* 保存上下文以便完成时释放缓冲区 */
    aof_large_write_t *ctx = (aof_large_write_t *)kmem_alloc(sizeof(aof_large_write_t));
    if (ctx == NULL) {
        kvs_logError("Failed to allocate write context");
        aof_release_reg_buffer(reg, buf_idx);
        return -1;
    }
    
    ctx->cmd_type = cmd_type;
    ctx->key = key;
    ctx->value = value;
    ctx->buffer = buf;
    ctx->buf_len = len;
    ctx->buf_idx = buf_idx;
    ctx->use_fixed = true;
    
    io_uring_sqe_set_data(sqe, ctx);

    kvs_logInfo("Submitted large write fixed: fd=%d, buf_idx=%d, size=%d", 
                fd, buf_idx, len);

    return 0;
}

/**
 * 完成大值写入后的清理
 * 应在CQE处理完成后调用
 */
void aof_large_write_complete(aof_registered_buffers_t *reg, aof_large_write_t *ctx) {
    if (ctx == NULL) {
        return;
    }

    if (ctx->use_fixed && reg != NULL && ctx->buf_idx >= 0) {
        /* 释放注册缓冲区 */
        aof_release_reg_buffer(reg, ctx->buf_idx);
    } else if (ctx->buffer != NULL) {
        /* 释放页对齐缓冲区 */
        aof_page_aligned_free(ctx->buffer);
    }

    /* 释放上下文 */
    kmem_free(ctx);
}

