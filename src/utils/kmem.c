/*
 * Kedis Memory Allocator - kmem
 * 核心实现
 */

#include "../../include/kmem.h"
#include <string.h>
#include <unistd.h>
#include <sys/mman.h>

/* ============================================================================
 * 全局内存池实例
 * ============================================================================ */

static kmem_pool_t g_kmem_pool = {0};
static __thread kmem_tls_cache_t g_tls_cache = {0};

/* ============================================================================
 * 内部辅助函数
 * ============================================================================ */

// 初始化单个slab
static int kmem_slab_init(kmem_slab_t *slab, size_t block_size, size_t chunk_size, int class_idx) {
    slab->block_size = block_size;
    slab->chunk_size = chunk_size;
    slab->blocks_per_chunk = (chunk_size - sizeof(kmem_chunk_t)) / 
                             (sizeof(kmem_block_hdr_t) + block_size);
    if (slab->blocks_per_chunk == 0) slab->blocks_per_chunk = 1;
    slab->chunks = NULL;
    slab->free_list = NULL;
    slab->total_blocks = 0;
    slab->free_blocks = 0;
    slab->alloc_count = 0;
    slab->free_count = 0;
    slab->chunk_count = 0;
    
    if (pthread_mutex_init(&slab->lock, NULL) != 0) {
        return -1;
    }
    
    return 0;
}

// 创建新chunk并切分为空闲块
static int kmem_slab_grow(kmem_slab_t *slab) {
    // 分配chunk内存
    size_t alloc_size = slab->chunk_size;
    void *memory = mmap(NULL, alloc_size, PROT_READ | PROT_WRITE,
                        MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (memory == MAP_FAILED) {
        // mmap失败，尝试malloc
        memory = malloc(alloc_size);
        if (!memory) {
            return -1;
        }
    }
    
    // 创建chunk头（放在内存末尾）
    kmem_chunk_t *chunk = (kmem_chunk_t *)((char *)memory + alloc_size - sizeof(kmem_chunk_t));
    chunk->memory = memory;
    chunk->size = alloc_size;
    chunk->used_count = 0;
    chunk->next = slab->chunks;
    slab->chunks = chunk;
    slab->chunk_count++;
    
    // 切分chunk为空闲块
    char *block_start = (char *)memory;
    // 根据block_size确定class_idx
    int class_idx = -1;
    for (int i = 0; i < KMEM_SIZE_CLASS_COUNT; i++) {
        if (kmem_class_sizes[i] == slab->block_size) {
            class_idx = i;
            break;
        }
    }
    
    for (size_t i = 0; i < slab->blocks_per_chunk; i++) {
        kmem_block_hdr_t *hdr = (kmem_block_hdr_t *)block_start;
        hdr->size_class = (class_idx >= 0) ? class_idx : 0;
        hdr->flags = 0;
        hdr->magic = KMEM_MAGIC_FREED;
        
        kmem_free_node_t *node = (kmem_free_node_t *)(block_start + sizeof(kmem_block_hdr_t));
        node->next = slab->free_list;
        slab->free_list = node;
        
        block_start += sizeof(kmem_block_hdr_t) + slab->block_size;
        
        // 安全检查
        if (block_start + sizeof(kmem_block_hdr_t) + slab->block_size > 
            (char *)memory + alloc_size - sizeof(kmem_chunk_t)) {
            break;
        }
    }
    
    slab->total_blocks += slab->blocks_per_chunk;
    slab->free_blocks += slab->blocks_per_chunk;
    
    return 0;
}

// 从slab分配一块
static void* kmem_slab_alloc(kmem_slab_t *slab) {
    pthread_mutex_lock(&slab->lock);
    
    // 如果没有空闲块，扩展slab
    if (!slab->free_list) {
        if (kmem_slab_grow(slab) != 0) {
            pthread_mutex_unlock(&slab->lock);
            return NULL;
        }
    }
    
    // 从空闲链表取一块
    kmem_free_node_t *node = slab->free_list;
    slab->free_list = node->next;
    slab->free_blocks--;
    slab->alloc_count++;
    
    pthread_mutex_unlock(&slab->lock);
    
    // 设置块头
    kmem_block_hdr_t *hdr = (kmem_block_hdr_t *)((char *)node - sizeof(kmem_block_hdr_t));
    hdr->magic = KMEM_MAGIC_ALLOCATED;
    
    return (void *)node;
}

// 释放块回slab
static void kmem_slab_free(kmem_slab_t *slab, void *ptr) {
    kmem_block_hdr_t *hdr = (kmem_block_hdr_t *)((char *)ptr - sizeof(kmem_block_hdr_t));
    hdr->magic = KMEM_MAGIC_FREED;
    
    kmem_free_node_t *node = (kmem_free_node_t *)ptr;
    
    pthread_mutex_lock(&slab->lock);
    node->next = slab->free_list;
    slab->free_list = node;
    slab->free_blocks++;
    slab->free_count++;
    pthread_mutex_unlock(&slab->lock);
}

// 从slab批量获取块
static int kmem_slab_alloc_batch(kmem_slab_t *slab, kmem_free_node_t **out_nodes, int count) {
    pthread_mutex_lock(&slab->lock);
    
    int got = 0;
    while (got < count) {
        if (!slab->free_list) {
            if (kmem_slab_grow(slab) != 0) {
                break;
            }
        }
        
        kmem_free_node_t *node = slab->free_list;
        slab->free_list = node->next;
        slab->free_blocks--;
        slab->alloc_count++;
        
        // 设置块头
        kmem_block_hdr_t *hdr = (kmem_block_hdr_t *)((char *)node - sizeof(kmem_block_hdr_t));
        hdr->magic = KMEM_MAGIC_ALLOCATED;
        
        node->next = *out_nodes;
        *out_nodes = node;
        got++;
    }
    
    pthread_mutex_unlock(&slab->lock);
    return got;
}

// 批量归还块到slab
static void kmem_slab_free_batch(kmem_slab_t *slab, kmem_free_node_t *nodes, int count) {
    pthread_mutex_lock(&slab->lock);
    
    for (int i = 0; i < count && nodes; i++) {
        kmem_free_node_t *node = nodes;
        nodes = nodes->next;
        
        kmem_block_hdr_t *hdr = (kmem_block_hdr_t *)((char *)node - sizeof(kmem_block_hdr_t));
        hdr->magic = KMEM_MAGIC_FREED;
        
        node->next = slab->free_list;
        slab->free_list = node;
        slab->free_blocks++;
        slab->free_count++;
    }
    
    pthread_mutex_unlock(&slab->lock);
}

/* ============================================================================
 * 公开API实现
 * ============================================================================ */

int kmem_init(void) {
    if (g_kmem_pool.initialized) {
        return 0;
    }
    
    pthread_mutex_lock(&g_kmem_pool.init_lock);
    
    if (g_kmem_pool.initialized) {
        pthread_mutex_unlock(&g_kmem_pool.init_lock);
        return 0;
    }
    
    // 初始化各slab
    for (int i = 0; i < KMEM_SIZE_CLASS_COUNT; i++) {
        if (kmem_slab_init(&g_kmem_pool.slabs[i], 
                          kmem_class_sizes[i],
                          kmem_chunk_sizes[i], i) != 0) {
            pthread_mutex_unlock(&g_kmem_pool.init_lock);
            return -1;
        }
    }
    
    pthread_mutex_init(&g_kmem_pool.large_lock, NULL);
    g_kmem_pool.initialized = 1;
    
    pthread_mutex_unlock(&g_kmem_pool.init_lock);
    return 0;
}

void kmem_destroy(void) {
    if (!g_kmem_pool.initialized) {
        return;
    }
    
    pthread_mutex_lock(&g_kmem_pool.init_lock);
    
    // 销毁各slab
    for (int i = 0; i < KMEM_SIZE_CLASS_COUNT; i++) {
        kmem_slab_t *slab = &g_kmem_pool.slabs[i];
        
        // 释放所有chunk
        kmem_chunk_t *chunk = slab->chunks;
        while (chunk) {
            kmem_chunk_t *next = chunk->next;
            munmap(chunk->memory, chunk->size);
            chunk = next;
        }
        
        pthread_mutex_destroy(&slab->lock);
    }
    
    pthread_mutex_destroy(&g_kmem_pool.large_lock);
    g_kmem_pool.initialized = 0;
    
    pthread_mutex_unlock(&g_kmem_pool.init_lock);
}

int kmem_size_class(size_t size) {
    if (size == 0) return -1;
    
    // 快速路径：常见大小
    if (size <= 64) return KMEM_CLASS_64B;
    if (size <= 128) return KMEM_CLASS_128B;
    if (size <= 256) return KMEM_CLASS_256B;
    if (size <= 512) return KMEM_CLASS_512B;
    if (size <= 1024) return KMEM_CLASS_1KB;
    if (size <= 2048) return KMEM_CLASS_2KB;
    
    return -1; // 大块
}

void* kmem_alloc(size_t size) {
    if (!g_kmem_pool.initialized) {
        if (kmem_init() != 0) {
            return NULL;
        }
    }
    
    int class_idx = kmem_size_class(size);
    if (class_idx < 0) {
        // 大块分配
        size_t alloc_size = sizeof(kmem_block_hdr_t) + size;
        kmem_block_hdr_t *hdr = malloc(alloc_size);
        if (!hdr) return NULL;
        
        hdr->size_class = 0xFFFF; // 标记为大块
        hdr->flags = size;        // 存储实际大小供释放时使用
        hdr->magic = KMEM_MAGIC_ALLOCATED;
        
        pthread_mutex_lock(&g_kmem_pool.large_lock);
        g_kmem_pool.large_allocs++;
        g_kmem_pool.large_bytes += alloc_size;
        pthread_mutex_unlock(&g_kmem_pool.large_lock);
        
        return (void *)(hdr + 1);
    }
    
    return kmem_slab_alloc(&g_kmem_pool.slabs[class_idx]);
}

void kmem_free(void *ptr) {
    if (!ptr) return;
    
    kmem_block_hdr_t *hdr = (kmem_block_hdr_t *)ptr - 1;
    
    // 验证魔数
    if (hdr->magic != KMEM_MAGIC_ALLOCATED) {
        // 可能是重复释放或无效指针
        // 生产环境应该记录日志
        return;
    }
    
    if (hdr->size_class == 0xFFFF) {
        // 大块释放
        pthread_mutex_lock(&g_kmem_pool.large_lock);
        g_kmem_pool.large_bytes -= sizeof(kmem_block_hdr_t) + hdr->flags;
        pthread_mutex_unlock(&g_kmem_pool.large_lock);
        
        free(hdr);
        return;
    }
    
    if (hdr->size_class >= KMEM_SIZE_CLASS_COUNT) {
        return; // 无效的size class
    }
    
    kmem_slab_free(&g_kmem_pool.slabs[hdr->size_class], ptr);
}

void* kmem_realloc(void *ptr, size_t new_size) {
    if (!ptr) {
        return kmem_alloc(new_size);
    }
    
    if (new_size == 0) {
        kmem_free(ptr);
        return NULL;
    }
    
    // 获取当前块大小
    size_t old_size = kmem_block_size(ptr);
    if (old_size == 0) {
        return NULL; // 无效指针
    }
    
    // 检查是否能在原尺寸类满足
    int old_class = kmem_size_class(old_size);
    int new_class = kmem_size_class(new_size);
    
    if (old_class == new_class && old_class >= 0) {
        // 同尺寸类，无需重新分配
        return ptr;
    }
    
    // 需要重新分配
    void *new_ptr = kmem_alloc(new_size);
    if (!new_ptr) return NULL;
    
    size_t copy_size = (old_size < new_size) ? old_size : new_size;
    memcpy(new_ptr, ptr, copy_size);
    
    kmem_free(ptr);
    return new_ptr;
}

size_t kmem_block_size(void *ptr) {
    if (!ptr) return 0;
    
    kmem_block_hdr_t *hdr = (kmem_block_hdr_t *)ptr - 1;
    if (hdr->magic != KMEM_MAGIC_ALLOCATED) {
        return 0;
    }
    
    if (hdr->size_class == 0xFFFF) {
        return hdr->flags; // 大块用flags存储实际大小
    }
    
    if (hdr->size_class >= KMEM_SIZE_CLASS_COUNT) {
        return 0;
    }
    
    return kmem_class_sizes[hdr->size_class];
}

void* kmem_alloc_class(int class_idx) {
    if (!g_kmem_pool.initialized) {
        if (kmem_init() != 0) {
            return NULL;
        }
    }
    
    if (class_idx < 0 || class_idx >= KMEM_SIZE_CLASS_COUNT) {
        return NULL;
    }
    
    return kmem_slab_alloc(&g_kmem_pool.slabs[class_idx]);
}

void kmem_free_class(void *ptr, int class_idx) {
    if (!ptr || class_idx < 0 || class_idx >= KMEM_SIZE_CLASS_COUNT) {
        return;
    }
    
    kmem_slab_free(&g_kmem_pool.slabs[class_idx], ptr);
}

/* ============================================================================
 * TLS实现
 * ============================================================================ */

void kmem_tls_init(void) {
    memset(&g_tls_cache, 0, sizeof(g_tls_cache));
    g_tls_cache.init_magic = KMEM_TLS_MAGIC;
}

void kmem_tls_destroy(void) {
    if (g_tls_cache.init_magic != KMEM_TLS_MAGIC) {
        return;
    }
    
    // 归还所有缓存块到全局池
    for (int i = 0; i < KMEM_SIZE_CLASS_COUNT; i++) {
        if (g_tls_cache.cache_count[i] > 0) {
            kmem_slab_free_batch(&g_kmem_pool.slabs[i], 
                                g_tls_cache.cache[i],
                                g_tls_cache.cache_count[i]);
        }
    }
    
    g_tls_cache.init_magic = 0;
}

void* kmem_alloc_fast(size_t size) {
    if (g_tls_cache.init_magic != KMEM_TLS_MAGIC) {
        kmem_tls_init();
    }
    
    int class_idx = kmem_size_class(size);
    if (class_idx < 0) {
        return kmem_alloc(size);
    }
    
    // 从TLS缓存取
    if (g_tls_cache.cache_count[class_idx] > 0) {
        kmem_free_node_t *node = g_tls_cache.cache[class_idx];
        g_tls_cache.cache[class_idx] = node->next;
        g_tls_cache.cache_count[class_idx]--;
        return (void *)node;
    }
    
    // 缓存空，批量从全局池获取
    kmem_free_node_t *nodes = NULL;
    int got = kmem_slab_alloc_batch(&g_kmem_pool.slabs[class_idx], &nodes, KMEM_TLS_BATCH_SIZE);
    
    if (got == 0) {
        return NULL;
    }
    
    // 返回第一个，其余放入缓存
    void *result = (void *)nodes;
    nodes = nodes->next;
    got--;
    
    g_tls_cache.cache[class_idx] = nodes;
    g_tls_cache.cache_count[class_idx] = got;
    
    return result;
}

void kmem_free_fast(void *ptr) {
    if (!ptr) return;
    
    if (g_tls_cache.init_magic != KMEM_TLS_MAGIC) {
        kmem_free(ptr);
        return;
    }
    
    kmem_block_hdr_t *hdr = (kmem_block_hdr_t *)ptr - 1;
    if (hdr->magic != KMEM_MAGIC_ALLOCATED || hdr->size_class >= KMEM_SIZE_CLASS_COUNT) {
        kmem_free(ptr);
        return;
    }
    
    int class_idx = hdr->size_class;
    
    // 如果缓存未满，放入缓存
    if (g_tls_cache.cache_count[class_idx] < KMEM_TLS_CACHE_SIZE) {
        kmem_free_node_t *node = (kmem_free_node_t *)ptr;
        hdr->magic = KMEM_MAGIC_FREED;
        node->next = g_tls_cache.cache[class_idx];
        g_tls_cache.cache[class_idx] = node;
        g_tls_cache.cache_count[class_idx]++;
        return;
    }
    
    // 缓存满，批量归还一半
    kmem_free_node_t *nodes = g_tls_cache.cache[class_idx];
    int count = g_tls_cache.cache_count[class_idx];
    int to_free = count / 2;
    
    // 分割链表
    kmem_free_node_t *keep = nodes;
    for (int i = 0; i < to_free - 1 && keep; i++) {
        keep = keep->next;
    }
    
    if (keep) {
        kmem_free_node_t *to_return = keep->next;
        keep->next = NULL;
        
        g_tls_cache.cache[class_idx] = to_return;
        g_tls_cache.cache_count[class_idx] = count - to_free;
        
        kmem_slab_free_batch(&g_kmem_pool.slabs[class_idx], nodes, to_free);
    }
    
    // 当前块放入缓存
    kmem_free_node_t *node = (kmem_free_node_t *)ptr;
    hdr->magic = KMEM_MAGIC_FREED;
    node->next = g_tls_cache.cache[class_idx];
    g_tls_cache.cache[class_idx] = node;
    g_tls_cache.cache_count[class_idx]++;
}

/* ============================================================================
 * 统计实现
 * ============================================================================ */

void kmem_stats_print(void) {
    if (!g_kmem_pool.initialized) {
        printf("kmem not initialized\n");
        return;
    }
    
    printf("\n========== KEMEM Statistics ==========\n");
    printf("%-8s %-10s %-10s %-10s %-10s %-10s\n",
           "Class", "BlockSize", "Total", "Free", "Used", "Chunks");
    printf("----------------------------------------\n");
    
    size_t total_used = 0;
    size_t total_free = 0;
    
    for (int i = 0; i < KMEM_SIZE_CLASS_COUNT; i++) {
        kmem_slab_t *slab = &g_kmem_pool.slabs[i];
        
        pthread_mutex_lock(&slab->lock);
        size_t used = slab->total_blocks - slab->free_blocks;
        total_used += used * slab->block_size;
        total_free += slab->free_blocks * slab->block_size;
        
        printf("%-8d %-10zu %-10zu %-10zu %-10zu %-10zu\n",
               i,
               slab->block_size,
               slab->total_blocks,
               slab->free_blocks,
               used,
               slab->chunk_count);
        pthread_mutex_unlock(&slab->lock);
    }
    
    printf("----------------------------------------\n");
    pthread_mutex_lock(&g_kmem_pool.large_lock);
    printf("Large allocs: %zu, Large bytes: %zu\n",
           g_kmem_pool.large_allocs, g_kmem_pool.large_bytes);
    pthread_mutex_unlock(&g_kmem_pool.large_lock);
    
    printf("Total used: %zu bytes (%.2f MB)\n", 
           total_used, total_used / (1024.0 * 1024.0));
    printf("Total free: %zu bytes (%.2f MB)\n",
           total_free, total_free / (1024.0 * 1024.0));
    printf("=======================================\n\n");
}

void kmem_stats_get(size_t *total_used, size_t *total_free,
                    size_t slab_stats[KMEM_SIZE_CLASS_COUNT][4]) {
    if (!g_kmem_pool.initialized) {
        if (total_used) *total_used = 0;
        if (total_free) *total_free = 0;
        return;
    }
    
    size_t used = 0, free = 0;
    
    for (int i = 0; i < KMEM_SIZE_CLASS_COUNT; i++) {
        kmem_slab_t *slab = &g_kmem_pool.slabs[i];
        pthread_mutex_lock(&slab->lock);
        
        if (slab_stats) {
            slab_stats[i][0] = slab->block_size;
            slab_stats[i][1] = slab->total_blocks;
            slab_stats[i][2] = slab->free_blocks;
            slab_stats[i][3] = slab->chunk_count;
        }
        
        used += (slab->total_blocks - slab->free_blocks) * slab->block_size;
        free += slab->free_blocks * slab->block_size;
        
        pthread_mutex_unlock(&slab->lock);
    }
    
    if (total_used) *total_used = used;
    if (total_free) *total_free = free;
}

bool kmem_contains(void *ptr) {
    if (!ptr || !g_kmem_pool.initialized) {
        return false;
    }
    
    // 检查是否在slab管理的内存中
    for (int i = 0; i < KMEM_SIZE_CLASS_COUNT; i++) {
        kmem_slab_t *slab = &g_kmem_pool.slabs[i];
        kmem_chunk_t *chunk = slab->chunks;
        
        while (chunk) {
            char *chunk_start = (char *)chunk->memory;
            char *chunk_end = chunk_start + chunk->size - sizeof(kmem_chunk_t);
            if ((char *)ptr >= chunk_start && (char *)ptr < chunk_end) {
                return true;
            }
            chunk = chunk->next;
        }
    }
    
    return false;
}

void kmem_leak_check(void) {
    // TODO: 实现泄漏检测，需要遍历所有已分配块
    // 这需要额外的跟踪结构，暂不实现
    printf("kmem_leak_check: not implemented yet\n");
}
