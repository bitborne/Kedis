/*
 * Kedis Memory Allocator - kmem
 * 
 * 高性能多尺寸类内存池，专为Kedis KV存储优化
 * 
 * 特性：
 * - 6种固定尺寸类：64B/128B/256B/512B/1KB/2KB
 * - 动态Chunk扩展，无固定容量限制
 * - 线程本地缓存(TLS)减少锁竞争
 * - 智能大小路由，自动选择最优尺寸类
 * - 大块内存(>2KB)独立管理
 */

#ifndef __KMEM_H__
#define __KMEM_H__

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================================
 * 配置常量
 * ============================================================================ */

#define KMEM_SIZE_CLASS_COUNT    6

// 尺寸类索引
#define KMEM_CLASS_64B           0
#define KMEM_CLASS_128B          1
#define KMEM_CLASS_256B          2
#define KMEM_CLASS_512B          3
#define KMEM_CLASS_1KB           4
#define KMEM_CLASS_2KB           5

// 各尺寸类的块大小
static const size_t kmem_class_sizes[KMEM_SIZE_CLASS_COUNT] = {
    64, 128, 256, 512, 1024, 2048
};

// 各尺寸类的默认Chunk大小（MB）
static const size_t kmem_chunk_sizes[KMEM_SIZE_CLASS_COUNT] = {
    4 * 1024 * 1024,   // 64B  -> 4MB chunk (65536 blocks)
    4 * 1024 * 1024,   // 128B -> 4MB chunk (32768 blocks)
    8 * 1024 * 1024,   // 256B -> 8MB chunk (32768 blocks)
    8 * 1024 * 1024,   // 512B -> 8MB chunk (16384 blocks)
    4 * 1024 * 1024,   // 1KB  -> 4MB chunk (4096 blocks)
    4 * 1024 * 1024    // 2KB  -> 4MB chunk (2048 blocks)
};

// 大块分配阈值（超过此大小使用malloc/mmap）
#define KMEM_LARGE_THRESHOLD     2048

// 线程本地缓存配置
#define KMEM_TLS_CACHE_SIZE      64   // 每尺寸类缓存块数
#define KMEM_TLS_BATCH_SIZE      32   // 批量从全局池获取/归还

/* ============================================================================
 * 数据结构
 * ============================================================================ */

// 内存块头（嵌入在用户数据前）
typedef struct kmem_block_hdr {
    uint16_t size_class;    // 尺寸类索引 (0-5)
    uint16_t flags;         // 标志位
    uint32_t magic;         // 魔数校验
} kmem_block_hdr_t;

#define KMEM_MAGIC_ALLOCATED   0x4B4D4441  // "KMDA"
#define KMEM_MAGIC_FREED       0x4B4D4420  // "KMD "

// 空闲块链表节点
typedef struct kmem_free_node {
    struct kmem_free_node *next;
} kmem_free_node_t;

// Chunk结构 - 支持链表
typedef struct kmem_chunk {
    void *memory;               // 实际内存区域
    size_t size;                // Chunk大小
    size_t used_count;          // 已分配块数
    struct kmem_chunk *next;    // 下一个Chunk
} kmem_chunk_t;

// 尺寸类内存池
typedef struct kmem_slab {
    size_t block_size;          // 块大小
    size_t chunk_size;          // Chunk大小
    size_t blocks_per_chunk;    // 每chunk的块数
    
    kmem_chunk_t *chunks;       // Chunk链表
    kmem_free_node_t *free_list; // 全局空闲链表
    
    pthread_mutex_t lock;       // 保护本slab
    
    // 统计
    size_t total_blocks;
    size_t free_blocks;
    size_t alloc_count;
    size_t free_count;
    size_t chunk_count;
} kmem_slab_t;

// 线程本地缓存
typedef struct kmem_tls_cache {
    kmem_free_node_t *cache[KMEM_SIZE_CLASS_COUNT];
    int cache_count[KMEM_SIZE_CLASS_COUNT];
    int init_magic;
} kmem_tls_cache_t;

#define KMEM_TLS_MAGIC  0x4B4D5453  // "KMTS"

// 全局内存池
typedef struct kmem_pool {
    kmem_slab_t slabs[KMEM_SIZE_CLASS_COUNT];
    pthread_mutex_t init_lock;
    int initialized;
    
    // 大块分配统计
    size_t large_allocs;
    size_t large_bytes;
    pthread_mutex_t large_lock;
} kmem_pool_t;

/* ============================================================================
 * 核心API
 * ============================================================================ */

/**
 * 初始化全局内存池
 * @return 0成功，-1失败
 */
int kmem_init(void);

/**
 * 销毁全局内存池，释放所有内存
 */
void kmem_destroy(void);

/**
 * 智能分配 - 根据大小自动选择最佳尺寸类
 * 内存块头会自动记录尺寸类信息
 * @param size 请求的内存大小
 * @return 内存指针，NULL表示失败
 */
void* kmem_alloc(size_t size);

/**
 * 释放内存
 * @param ptr kmem_alloc返回的指针
 */
void kmem_free(void *ptr);

/**
 * 重新分配内存
 * @param ptr 原指针（可为NULL）
 * @param new_size 新大小
 * @return 新内存指针
 */
void* kmem_realloc(void *ptr, size_t new_size);

/**
 * 获取尺寸类索引
 * @param size 请求大小
 * @return 尺寸类索引(0-5)，超过阈值返回-1
 */
int kmem_size_class(size_t size);

/**
 * 获取实际分配的块大小
 * @param ptr kmem_alloc返回的指针
 * @return 实际块大小，无效指针返回0
 */
size_t kmem_block_size(void *ptr);

/* ============================================================================
 * 高级API（指定尺寸类，用于性能敏感场景）
 * ============================================================================ */

/**
 * 从指定尺寸类分配
 * @param class_idx 尺寸类索引(0-5)
 * @return 内存指针
 */
void* kmem_alloc_class(int class_idx);

/**
 * 释放到指定尺寸类
 * @param ptr 内存指针
 * @param class_idx 尺寸类索引
 */
void kmem_free_class(void *ptr, int class_idx);

/* ============================================================================
 * 线程本地缓存API（极致性能）
 * ============================================================================ */

/**
 * 初始化线程本地缓存
 * 每个使用kmem的线程应在启动时调用
 */
void kmem_tls_init(void);

/**
 * 销毁线程本地缓存，归还缓存块到全局池
 * 线程退出前调用
 */
void kmem_tls_destroy(void);

/**
 * 使用TLS缓存分配（比kmem_alloc更快）
 */
void* kmem_alloc_fast(size_t size);

/**
 * 使用TLS缓存释放
 */
void kmem_free_fast(void *ptr);

/* ============================================================================
 * 页对齐分配（用于O_DIRECT和io_uring注册缓冲区）
 * ============================================================================ */

/**
 * 分配页对齐内存
 * @param size 请求大小
 * @param align 对齐边界（必须是2的幂，通常为4096）
 * @return 页对齐内存指针，NULL表示失败
 */
void* kmem_aligned_alloc(size_t size, size_t align);

/**
 * 释放页对齐内存
 * @param ptr kmem_aligned_alloc返回的指针
 */
void kmem_aligned_free(void *ptr);

/* ============================================================================
 * 统计与调试
 * ============================================================================ */

/**
 * 打印内存池统计信息
 */
void kmem_stats_print(void);

/**
 * 获取内存池统计
 * @param total_used 总已用字节
 * @param total_free 总空闲字节
 * @param slab_stats 各slab统计数组（长度KMEM_SIZE_CLASS_COUNT）
 */
void kmem_stats_get(size_t *total_used, size_t *total_free, 
                    size_t slab_stats[KMEM_SIZE_CLASS_COUNT][4]);

/**
 * 检查指针是否由kmem管理
 */
bool kmem_contains(void *ptr);

/**
 * 内存泄漏检测（调试用）
 * 打印所有未释放的块
 */
void kmem_leak_check(void);

#ifdef __cplusplus
}
#endif

#endif /* __KMEM_H__ */
