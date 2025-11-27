#ifndef __MEMORY_POOL_H__
#define __MEMORY_POOL_H__

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <pthread.h>

// 内存块大小定义，针对KV存储常见数据大小优化
#define BLOCK_SIZE 256    // 默认块大小
#define CHUNK_SIZE (1024 * 1024)  // 1MB的内存块，包含多个分配单元

// 内存池块结构
typedef struct mem_block {
    void *data;
    struct mem_block *next;  // 指向下一个可用块的指针
} mem_block_t;

// 内存池结构
typedef struct memory_pool {
    void *chunk;             // 内存池大块
    size_t chunk_size;       // 块大小
    size_t block_size;       // 每个分配单元大小
    size_t block_count;      // 总块数
    mem_block_t *free_list;  // 空闲块链表
    pthread_mutex_t lock;    // 线程安全锁
    size_t allocated_count;  // 已分配的块数
    size_t max_blocks;       // 最大块数
} memory_pool_t;

// 初始化内存池
memory_pool_t* mem_pool_init(size_t block_size);

// 从内存池分配内存
void* mem_pool_alloc(memory_pool_t *pool);

// 释放内存回内存池
void mem_pool_free(memory_pool_t *pool, void *ptr);

// 销毁内存池
void mem_pool_destroy(memory_pool_t *pool);

#endif