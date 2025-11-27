#include "memory_pool.h"
#include <string.h>
#include <unistd.h>

memory_pool_t* mem_pool_init(size_t block_size) {
    memory_pool_t *pool = (memory_pool_t*)malloc(sizeof(memory_pool_t));
    if (!pool) return NULL;

    // 调整block_size以确保对齐并包含指针
    block_size = (block_size < sizeof(void*)) ? sizeof(void*) : block_size;
    block_size = (block_size + sizeof(void*) - 1) & ~(sizeof(void*) - 1); // 对齐

    pool->block_size = block_size;
    pool->chunk_size = CHUNK_SIZE;
    pool->block_count = 0;
    pool->allocated_count = 0;
    pool->free_list = NULL;
    pthread_mutex_init(&pool->lock, NULL);

    // 计算每个chunk中可以容纳的块数
    size_t header_size = sizeof(mem_block_t);
    size_t usable_size = pool->chunk_size - sizeof(mem_block_t); // 减去头部信息
    pool->max_blocks = usable_size / (pool->block_size + header_size);

    // 预分配一个chunk
    void *chunk = malloc(pool->chunk_size);
    if (!chunk) {
        free(pool);
        return NULL;
    }
    pool->chunk = chunk;

    // 初始化空闲列表
    char *block_ptr = (char*)chunk + sizeof(mem_block_t);
    for (int i = 0; i < pool->max_blocks; i++) {
        mem_block_t *block = (mem_block_t*)block_ptr;
        block->data = block_ptr + sizeof(mem_block_t);
        block->next = pool->free_list;
        pool->free_list = block;
        pool->block_count++;
        
        block_ptr += (sizeof(mem_block_t) + pool->block_size);
        if (block_ptr + sizeof(mem_block_t) + pool->block_size > (char*)chunk + pool->chunk_size) {
            break; // 超出chunk范围
        }
    }

    return pool;
}

void* mem_pool_alloc(memory_pool_t *pool) {
    if (!pool) return NULL;

    pthread_mutex_lock(&pool->lock);
    
    // 如果空闲列表为空，尝试扩展内存池
    if (!pool->free_list) {
        pthread_mutex_unlock(&pool->lock);
        return malloc(pool->block_size); // 如果内存池耗尽，使用标准malloc
    }

    // 从空闲列表获取一个块
    mem_block_t *block = pool->free_list;
    pool->free_list = block->next;
    pool->allocated_count++;

    pthread_mutex_unlock(&pool->lock);

    // 返回数据指针
    return block->data;
}

void mem_pool_free(memory_pool_t *pool, void *ptr) {
    if (!pool || !ptr) return;

    // 验证指针是否在内存池范围内
    char *start = (char*)pool->chunk + sizeof(mem_block_t);
    char *end = start + pool->max_blocks * (sizeof(mem_block_t) + pool->block_size);
    char *ptr_char = (char*)ptr;

    if (ptr_char >= start && ptr_char < end) {
        // 指针在内存池管理范围内，添加回空闲列表
        pthread_mutex_lock(&pool->lock);

        mem_block_t *block = (mem_block_t*)(ptr_char - sizeof(mem_block_t));
        block->next = pool->free_list;
        pool->free_list = block;
        pool->allocated_count--;

        pthread_mutex_unlock(&pool->lock);
    } else {
        // 不在内存池管理范围内，使用标准free
        free(ptr);
    }
}

void mem_pool_destroy(memory_pool_t *pool) {
    if (!pool) return;

    pthread_mutex_lock(&pool->lock);

    // 释放整个chunk
    if (pool->chunk) {
        free(pool->chunk);
    }

    pthread_mutex_unlock(&pool->lock);
    pthread_mutex_destroy(&pool->lock);
    free(pool);
}