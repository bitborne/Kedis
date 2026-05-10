#include "../../include/kvstore.h"
#include "../../include/kvs_rdma_sync.h"  // RDMA 同步功能头文件，提供 kvs_cmd_sync() 和 kvs_cmd_replicaof()

#include <assert.h>
#include <fcntl.h>
#include <pthread.h>
#include <semaphore.h>
#include <signal.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>


// jemalloc头文件
#ifdef HAVE_JEMALLOC
#include <jemalloc/jemalloc.h>
#endif

#if (NETWORK_SELECT == NETWORK_REACTOR)
#include "src/network/reactor_server.h"    // only for reactor.c
#endif

// 多引擎模式下的引擎实例定义
#if ENABLE_MULTI_ENGINE
#if ENABLE_RBTREE
kvs_rbtree_t rbtree_engine;
pthread_rwlock_t rbtree_engine_lock;  // 保护rbtree引擎的读写锁
#endif
#if ENABLE_HASH
kvs_hash_t hash_engine;
pthread_rwlock_t hash_engine_lock;    // 保护hash引擎的读写锁
#endif
#if ENABLE_ARRAY
kvs_array_t array_engine;
pthread_rwlock_t array_engine_lock;   // 保护array引擎的读写锁
#endif
#if ENABLE_SKIPLIST
kvs_skiplist_t skiplist_engine;
pthread_rwlock_t skiplist_engine_lock; // 保护skiplist引擎的读写锁
#endif
#else
// 单引擎模式：根据优先级选择使用的数据结构：红黑树 > 哈希 > 跳表 > 数组
#if ENABLE_RBTREE
kvs_rbtree_t global_main_engine;
pthread_rwlock_t main_engine_lock;
#elif ENABLE_HASH
kvs_hash_t global_main_engine;
pthread_rwlock_t main_engine_lock;
#elif ENABLE_SKIPLIST
kvs_skiplist_t global_main_engine;
pthread_rwlock_t main_engine_lock;
#elif ENABLE_ARRAY
kvs_array_t global_main_engine;
pthread_rwlock_t main_engine_lock;
#else
#error "至少需要启用一种数据结构"
#endif
#endif

// AOF缓冲区和长度
#if ENABLE_MULTI_ENGINE

aof_buf_t aofBuffer[4] = {0};

#else
aof_buf_t aofBuffer = {0};
#endif
extern const char* aof_filename;

const char* snap_filename = "./data/dump.ksf";

// 不直接使用系统调用(第三方接口)
// 跨平台的时候，只需要修改这个函数即可--> 可迭代
// 现在使用kmem作为底层内存分配器

void* kvs_calloc(size_t num, size_t size) {
#ifdef HAVE_JEMALLOC
    return calloc(num, size);
#else
    size_t total_size = num * size;
    void* ptr = kvs_malloc(total_size);
    if (ptr) {
        memset(ptr, 0, total_size);
    }
    return ptr;
#endif
}

void* kvs_malloc(size_t size) {
#ifdef HAVE_JEMALLOC
    return malloc(size);
#else
    // 使用kmem智能分配
    return kmem_alloc(size);
#endif
}

void kvs_free(void* ptr) {
#ifdef HAVE_JEMALLOC
    free(ptr);
#else
    // 使用kmem释放
    kmem_free(ptr);
#endif
}
// 定义了头文件中 command 变量的声明
const char*
        command[] =
                {"SET", "GET", "DEL", "MOD", "EXIST", "ASET",
                 "AGET", "ADEL", "AMOD", "AEXIST", "HSET", "HGET",
                 "HDEL", "HMOD", "HEXIST", "RSET", "RGET", "RDEL",
                 "RMOD", "REXIST", "SSET", "SGET", "SDEL", "SMOD",
                 "SEXIST", "SAVE", "BGSAVE", "SYNC", "REPLICAOF",
                 "RDMASYNC", "REPLCONF"};    // 添加SAVE、BGSAVE、SYNC、REPLICAOF和RDMASYNC命令


// 命令查找 hash 表（开放寻址）
#define CMD_HASH_SIZE 64
static int cmd_hash_table[CMD_HASH_SIZE];

static inline unsigned int cmd_hash_djb2(const char *str) {
    unsigned int hash = 5381;
    int c;
    while ((c = (unsigned char)*str++))
        hash = ((hash << 5) + hash) + c; /* hash * 33 + c */
    return hash;
}

static void init_cmd_hash(void) {
    for (int i = 0; i < CMD_HASH_SIZE; i++)
        cmd_hash_table[i] = -1;
    for (int cmd = KVS_CMD_START; cmd < KVS_CMD_COUNT; cmd++) {
        const char *name = command[cmd];
        unsigned int h = cmd_hash_djb2(name) & (CMD_HASH_SIZE - 1);
        while (cmd_hash_table[h] != -1)
            h = (h + 1) & (CMD_HASH_SIZE - 1);
        cmd_hash_table[h] = cmd;
    }
}

static int cmd_hash_lookup(const char *name) {
    unsigned int h = cmd_hash_djb2(name) & (CMD_HASH_SIZE - 1);
    while (cmd_hash_table[h] != -1) {
        int cmd = cmd_hash_table[h];
        if (strcasecmp(name, command[cmd]) == 0)
            return cmd;
        h = (h + 1) & (CMD_HASH_SIZE - 1);
    }
    return KVS_CMD_COUNT;
}
// 自动保存参数：save seconds changes
// static int save_params_seconds = 300;        // 5分钟
// static int save_params_changes = 100;        // 100次变化
static time_t last_save_time = 0;                // 上次保存时间
static int changes_since_last_save = 0;    // 自上次保存以来的变化次数

/*
 * 检查命令是否为写操作
 * @param command 命令名称
 * @return 1表示写操作，0表示非写操作
 */
/*
 * 检查命令是否为写操作（数据修改命令）
 * @param command 命令名称
 * @return 1表示写操作，0表示非写操作
 *
 * 【v3.0 架构用途】
 * 在从节点存量同步期间（SYNCING状态），写命令需要入积压队列，
 * 而不是直接执行，以避免与 RDMA 线程并发写入引擎。
 */
int is_write_command(const char* command) {
    if (command == NULL) {
        kvs_logInfo("[is_write_command] command is NULL\n");
        return 0;
    }

    kvs_logInfo("[is_write_command] checking command: '%s'\n", command);

    /* 【v3.0 修复】使用 strcasecmp 替代 strcmp，支持大小写不敏感匹配
     * 原因：mirror 可能转发小写命令（如 "set"），而原代码只匹配大写（"SET"）
     * 这导致小写写命令被误判为读命令，穿透执行而不入积压队列 */
    if (strcasecmp(command, "SET") == 0 || strcasecmp(command, "RSET") == 0 ||
            strcasecmp(command, "HSET") == 0 || strcasecmp(command, "MOD") == 0 ||
            strcasecmp(command, "RMOD") == 0 || strcasecmp(command, "HMOD") == 0 ||
            strcasecmp(command, "DEL") == 0 || strcasecmp(command, "RDEL") == 0 ||
            strcasecmp(command, "HDEL") == 0 || strcasecmp(command, "ASET") == 0 ||
            strcasecmp(command, "AMOD") == 0 || strcasecmp(command, "ADEL") == 0 ||
            strcasecmp(command, "SSET") == 0 || strcasecmp(command, "SMOD") == 0 ||
            strcasecmp(command, "SDEL") == 0) {
        kvs_logInfo("[is_write_command] '%s' is a WRITE command\n", command);
        return 1;
    }
    kvs_logInfo("[is_write_command] '%s' is NOT a write command\n", command);
    return 0;
}

/*
 * 检查命令是否为读操作（查询命令，不修改数据）
 * @param command 命令名称
 * @return 1表示读操作，0表示非读操作
 *
 * 【v3.0 架构用途】
 * 在从节点存量同步期间（SYNCING状态），读命令可以安全地穿透执行，
 * 因为引擎读取是线程安全的（RDMA 线程写入不影响读取）。
 *
 * 注意：此函数目前主要用于代码清晰性，实际判断使用 is_write_command 的反向逻辑，
 * 确保任何新增命令默认被视为写命令（保守策略）。
 */
int is_read_command(const char* command) {
    if (command == NULL) return 0;

    /* 【v3.0 修复】使用 strcasecmp 替代 strcmp，保持与 is_write_command 一致 */
    /* 查询类命令：只读不写的命令 */
    if (strcasecmp(command, "GET") == 0 || strcasecmp(command, "RGET") == 0 ||
            strcasecmp(command, "HGET") == 0 || strcasecmp(command, "AGET") == 0 ||
            strcasecmp(command, "SGET") == 0 ||
            /* 存在性检查：只读 */
            strcasecmp(command, "EXIST") == 0 || strcasecmp(command, "REXIST") == 0 ||
            strcasecmp(command, "HEXIST") == 0 || strcasecmp(command, "AEXIST") == 0 ||
            strcasecmp(command, "SEXIST") == 0 ||
            /* 持久化命令：不修改 KV 数据 */
            strcasecmp(command, "SAVE") == 0 || strcasecmp(command, "BGSAVE") == 0 ||
            /* 同步触发命令：控制命令，非数据修改 */
            strcasecmp(command, "SYNC") == 0) {
        return 1;
    }
    return 0;
}

/* ============================================================================
 * 引擎锁管理 - 延迟加锁方案
 * 只在RDMA同步期间（SYNCING状态）使用锁，平时无锁运行
 * ============================================================================ */

/**
 * @brief 初始化所有引擎的读写锁
 * 在从节点进入SYNCING状态前调用
 */
void engine_locks_init(void) {
#if ENABLE_MULTI_ENGINE
#if ENABLE_RBTREE
    pthread_rwlock_init(&rbtree_engine_lock, NULL);
#endif
#if ENABLE_HASH
    pthread_rwlock_init(&hash_engine_lock, NULL);
#endif
#if ENABLE_ARRAY
    pthread_rwlock_init(&array_engine_lock, NULL);
#endif
#if ENABLE_SKIPLIST
    pthread_rwlock_init(&skiplist_engine_lock, NULL);
#endif
#else
    /* 单引擎模式 */
    pthread_rwlock_init(&main_engine_lock, NULL);
#endif
    kvs_logInfo("[engine_locks_init] 引擎锁初始化完成");
}

/**
 * @brief 销毁所有引擎的读写锁
 * 在从节点回到IDLE状态时调用
 */
void engine_locks_destroy(void) {
#if ENABLE_MULTI_ENGINE
#if ENABLE_RBTREE
    pthread_rwlock_destroy(&rbtree_engine_lock);
#endif
#if ENABLE_HASH
    pthread_rwlock_destroy(&hash_engine_lock);
#endif
#if ENABLE_ARRAY
    pthread_rwlock_destroy(&array_engine_lock);
#endif
#if ENABLE_SKIPLIST
    pthread_rwlock_destroy(&skiplist_engine_lock);
#endif
#else
    /* 单引擎模式 */
    pthread_rwlock_destroy(&main_engine_lock);
#endif
    kvs_logInfo("[engine_locks_destroy] 引擎锁已销毁");
}

/* ============================================================================
 * 引擎操作的加锁包装函数
 * RDMA线程和SYNCING期间的主线程使用这些函数
 * ============================================================================ */

#if ENABLE_MULTI_ENGINE

/* ---------------- Array引擎加锁版本 ---------------- */
#if ENABLE_ARRAY
int kvs_array_set_safe(kvs_array_t *inst, robj* key, robj* value) {
    pthread_rwlock_wrlock(&array_engine_lock);
    int ret = kvs_array_set(inst, key, value);
    pthread_rwlock_unlock(&array_engine_lock);
    return ret;
}
char* kvs_array_get_safe(kvs_array_t *inst, robj* key) {
    pthread_rwlock_rdlock(&array_engine_lock);
    char *ret = kvs_array_get(inst, key);
    pthread_rwlock_unlock(&array_engine_lock);
    return ret;
}
int kvs_array_del_safe(kvs_array_t *inst, robj* key) {
    pthread_rwlock_wrlock(&array_engine_lock);
    int ret = kvs_array_del(inst, key);
    pthread_rwlock_unlock(&array_engine_lock);
    return ret;
}
int kvs_array_mod_safe(kvs_array_t *inst, robj* key, robj* value) {
    pthread_rwlock_wrlock(&array_engine_lock);
    int ret = kvs_array_mod(inst, key, value);
    pthread_rwlock_unlock(&array_engine_lock);
    return ret;
}
int kvs_array_exist_safe(kvs_array_t *inst, robj* key) {
    pthread_rwlock_rdlock(&array_engine_lock);
    int ret = kvs_array_exist(inst, key);
    pthread_rwlock_unlock(&array_engine_lock);
    return ret;
}
#endif

/* ---------------- Hash引擎加锁版本 ---------------- */
#if ENABLE_HASH
int kvs_hash_set_safe(kvs_hash_t *inst, robj* key, robj* value) {
    pthread_rwlock_wrlock(&hash_engine_lock);
    int ret = kvs_hash_set(inst, key, value);
    pthread_rwlock_unlock(&hash_engine_lock);
    return ret;
}
char* kvs_hash_get_safe(kvs_hash_t *inst, robj* key) {
    pthread_rwlock_rdlock(&hash_engine_lock);
    char *ret = kvs_hash_get(inst, key);
    pthread_rwlock_unlock(&hash_engine_lock);
    return ret;
}
int kvs_hash_del_safe(kvs_hash_t *inst, robj* key) {
    pthread_rwlock_wrlock(&hash_engine_lock);
    int ret = kvs_hash_del(inst, key);
    pthread_rwlock_unlock(&hash_engine_lock);
    return ret;
}
int kvs_hash_mod_safe(kvs_hash_t *inst, robj* key, robj* value) {
    pthread_rwlock_wrlock(&hash_engine_lock);
    int ret = kvs_hash_mod(inst, key, value);
    pthread_rwlock_unlock(&hash_engine_lock);
    return ret;
}
int kvs_hash_exist_safe(kvs_hash_t *inst, robj* key) {
    pthread_rwlock_rdlock(&hash_engine_lock);
    int ret = kvs_hash_exist(inst, key);
    pthread_rwlock_unlock(&hash_engine_lock);
    return ret;
}
#endif

/* ---------------- RBTREE引擎加锁版本 ---------------- */
#if ENABLE_RBTREE
int kvs_rbtree_set_safe(kvs_rbtree_t *inst, robj* key, robj* value) {
    pthread_rwlock_wrlock(&rbtree_engine_lock);
    int ret = kvs_rbtree_set(inst, key, value);
    pthread_rwlock_unlock(&rbtree_engine_lock);
    return ret;
}
char* kvs_rbtree_get_safe(kvs_rbtree_t *inst, robj* key) {
    pthread_rwlock_rdlock(&rbtree_engine_lock);
    char *ret = kvs_rbtree_get(inst, key);
    pthread_rwlock_unlock(&rbtree_engine_lock);
    return ret;
}
int kvs_rbtree_del_safe(kvs_rbtree_t *inst, robj* key) {
    pthread_rwlock_wrlock(&rbtree_engine_lock);
    int ret = kvs_rbtree_del(inst, key);
    pthread_rwlock_unlock(&rbtree_engine_lock);
    return ret;
}
int kvs_rbtree_mod_safe(kvs_rbtree_t *inst, robj* key, robj* value) {
    pthread_rwlock_wrlock(&rbtree_engine_lock);
    int ret = kvs_rbtree_mod(inst, key, value);
    pthread_rwlock_unlock(&rbtree_engine_lock);
    return ret;
}
int kvs_rbtree_exist_safe(kvs_rbtree_t *inst, robj* key) {
    pthread_rwlock_rdlock(&rbtree_engine_lock);
    int ret = kvs_rbtree_exist(inst, key);
    pthread_rwlock_unlock(&rbtree_engine_lock);
    return ret;
}
#endif

/* ---------------- SkipList引擎加锁版本 ---------------- */
#if ENABLE_SKIPLIST
int kvs_skiplist_set_safe(kvs_skiplist_t *inst, robj* key, robj* value) {
    pthread_rwlock_wrlock(&skiplist_engine_lock);
    int ret = kvs_skiplist_set(inst, key, value);
    pthread_rwlock_unlock(&skiplist_engine_lock);
    return ret;
}
char* kvs_skiplist_get_safe(kvs_skiplist_t *inst, robj* key) {
    pthread_rwlock_rdlock(&skiplist_engine_lock);
    char *ret = kvs_skiplist_get(inst, key);
    pthread_rwlock_unlock(&skiplist_engine_lock);
    return ret;
}
int kvs_skiplist_del_safe(kvs_skiplist_t *inst, robj* key) {
    pthread_rwlock_wrlock(&skiplist_engine_lock);
    int ret = kvs_skiplist_del(inst, key);
    pthread_rwlock_unlock(&skiplist_engine_lock);
    return ret;
}
int kvs_skiplist_mod_safe(kvs_skiplist_t *inst, robj* key, robj* value) {
    pthread_rwlock_wrlock(&skiplist_engine_lock);
    int ret = kvs_skiplist_mod(inst, key, value);
    pthread_rwlock_unlock(&skiplist_engine_lock);
    return ret;
}
int kvs_skiplist_exist_safe(kvs_skiplist_t *inst, robj* key) {
    pthread_rwlock_rdlock(&skiplist_engine_lock);
    int ret = kvs_skiplist_exist(inst, key);
    pthread_rwlock_unlock(&skiplist_engine_lock);
    return ret;
}
#endif

#else /* !ENABLE_MULTI_ENGINE */

/* ---------------- 单引擎模式加锁版本 ---------------- */
#if ENABLE_RBTREE
int kvs_main_set_safe(void *inst, robj* key, robj* value) {
    pthread_rwlock_wrlock(&main_engine_lock);
    int ret = kvs_rbtree_set((kvs_rbtree_t*)inst, key, value);
    pthread_rwlock_unlock(&main_engine_lock);
    return ret;
}
char* kvs_main_get_safe(void *inst, robj* key) {
    pthread_rwlock_rdlock(&main_engine_lock);
    char *ret = kvs_rbtree_get((kvs_rbtree_t*)inst, key);
    pthread_rwlock_unlock(&main_engine_lock);
    return ret;
}
int kvs_main_del_safe(void *inst, robj* key) {
    pthread_rwlock_wrlock(&main_engine_lock);
    int ret = kvs_rbtree_del((kvs_rbtree_t*)inst, key);
    pthread_rwlock_unlock(&main_engine_lock);
    return ret;
}
int kvs_main_mod_safe(void *inst, robj* key, robj* value) {
    pthread_rwlock_wrlock(&main_engine_lock);
    int ret = kvs_rbtree_mod((kvs_rbtree_t*)inst, key, value);
    pthread_rwlock_unlock(&main_engine_lock);
    return ret;
}
int kvs_main_exist_safe(void *inst, robj* key) {
    pthread_rwlock_rdlock(&main_engine_lock);
    int ret = kvs_rbtree_exist((kvs_rbtree_t*)inst, key);
    pthread_rwlock_unlock(&main_engine_lock);
    return ret;
}
#elif ENABLE_HASH
int kvs_main_set_safe(void *inst, robj* key, robj* value) {
    pthread_rwlock_wrlock(&main_engine_lock);
    int ret = kvs_hash_set((kvs_hash_t*)inst, key, value);
    pthread_rwlock_unlock(&main_engine_lock);
    return ret;
}
char* kvs_main_get_safe(void *inst, robj* key) {
    pthread_rwlock_rdlock(&main_engine_lock);
    char *ret = kvs_hash_get((kvs_hash_t*)inst, key);
    pthread_rwlock_unlock(&main_engine_lock);
    return ret;
}
int kvs_main_del_safe(void *inst, robj* key) {
    pthread_rwlock_wrlock(&main_engine_lock);
    int ret = kvs_hash_del((kvs_hash_t*)inst, key);
    pthread_rwlock_unlock(&main_engine_lock);
    return ret;
}
int kvs_main_mod_safe(void *inst, robj* key, robj* value) {
    pthread_rwlock_wrlock(&main_engine_lock);
    int ret = kvs_hash_mod((kvs_hash_t*)inst, key, value);
    pthread_rwlock_unlock(&main_engine_lock);
    return ret;
}
int kvs_main_exist_safe(void *inst, robj* key) {
    pthread_rwlock_rdlock(&main_engine_lock);
    int ret = kvs_hash_exist((kvs_hash_t*)inst, key);
    pthread_rwlock_unlock(&main_engine_lock);
    return ret;
}
#elif ENABLE_SKIPLIST
int kvs_main_set_safe(void *inst, robj* key, robj* value) {
    pthread_rwlock_wrlock(&main_engine_lock);
    int ret = kvs_skiplist_set((kvs_skiplist_t*)inst, key, value);
    pthread_rwlock_unlock(&main_engine_lock);
    return ret;
}
char* kvs_main_get_safe(void *inst, robj* key) {
    pthread_rwlock_rdlock(&main_engine_lock);
    char *ret = kvs_skiplist_get((kvs_skiplist_t*)inst, key);
    pthread_rwlock_unlock(&main_engine_lock);
    return ret;
}
int kvs_main_del_safe(void *inst, robj* key) {
    pthread_rwlock_wrlock(&main_engine_lock);
    int ret = kvs_skiplist_del((kvs_skiplist_t*)inst, key);
    pthread_rwlock_unlock(&main_engine_lock);
    return ret;
}
int kvs_main_mod_safe(void *inst, robj* key, robj* value) {
    pthread_rwlock_wrlock(&main_engine_lock);
    int ret = kvs_skiplist_mod((kvs_skiplist_t*)inst, key, value);
    pthread_rwlock_unlock(&main_engine_lock);
    return ret;
}
int kvs_main_exist_safe(void *inst, robj* key) {
    pthread_rwlock_rdlock(&main_engine_lock);
    int ret = kvs_skiplist_exist((kvs_skiplist_t*)inst, key);
    pthread_rwlock_unlock(&main_engine_lock);
    return ret;
}
#elif ENABLE_ARRAY
int kvs_main_set_safe(void *inst, robj* key, robj* value) {
    pthread_rwlock_wrlock(&main_engine_lock);
    int ret = kvs_array_set((kvs_array_t*)inst, key, value);
    pthread_rwlock_unlock(&main_engine_lock);
    return ret;
}
char* kvs_main_get_safe(void *inst, robj* key) {
    pthread_rwlock_rdlock(&main_engine_lock);
    char *ret = kvs_array_get((kvs_array_t*)inst, key);
    pthread_rwlock_unlock(&main_engine_lock);
    return ret;
}
int kvs_main_del_safe(void *inst, robj* key) {
    pthread_rwlock_wrlock(&main_engine_lock);
    int ret = kvs_array_del((kvs_array_t*)inst, key);
    pthread_rwlock_unlock(&main_engine_lock);
    return ret;
}
int kvs_main_mod_safe(void *inst, robj* key, robj* value) {
    pthread_rwlock_wrlock(&main_engine_lock);
    int ret = kvs_array_mod((kvs_array_t*)inst, key, value);
    pthread_rwlock_unlock(&main_engine_lock);
    return ret;
}
int kvs_main_exist_safe(void *inst, robj* key) {
    pthread_rwlock_rdlock(&main_engine_lock);
    int ret = kvs_array_exist((kvs_array_t*)inst, key);
    pthread_rwlock_unlock(&main_engine_lock);
    return ret;
}
#endif

#endif /* ENABLE_MULTI_ENGINE */

// 检查是否需要执行自动快照保存（根据save参数）
void check_and_perform_autosave() {
    time_t current_time = time(0); 

    // 检查是否满足自动保存条件：时间间隔达到且写入次数达到阈值
    if (current_time - last_save_time >= g_config.auto_save_seconds && changes_since_last_save >= g_config.auto_save_changes) {

        kvs_logWarn("触发自动快照保存：已超过 %d 秒且发生 %d 次变化", g_config.auto_save_seconds, g_config.auto_save_changes);

        // 更新最后保存时间
        last_save_time = current_time;
        changes_since_last_save = 0;    // 重置变化计数

        // 执行后台保存
        ksfSaveBackground();
    }
}

static void rb_add_reply_exist(reply_builder_t *rb, int exists) {
    char buf[32];
    sprintf(buf, ":%d\r\n", exists ? 1 : 0);
    rb_add_reply_str(rb, buf);
}

/* ---------------- 核心命令执行逻辑 ---------------- */
int kvs_protocol(reply_builder_t *rb, int argc, robj *argv) {

    char* cmd_name = argv[0].ptr;
    robj* key = &argv[1];
    robj* value = &argv[2];
    int use_engine_lock = 0;  /* 在SYNCING状态时设置为1，使用加锁版本的引擎操作 */

    /* =========================================================================
     * 【v3.0 双 Channel 架构】从节点存量同步期间的特殊处理
     *
     * 架构原则：
     *   - Main Channel (io_uring): 负责客户端交互和读命令穿透
     *   - RDMA Channel (独立线程): 负责存量数据同步，独占写引擎
     *
     * 状态机：
     *   IDLE    -> 未开始同步，返回 LOADING 错误
     *   SYNCING -> 读命令穿透执行，写命令入积压队列
     *   READY   -> 正常执行所有命令（积压已回放完成）
     * ========================================================================= */

    /* 【调试】每次进入 kvs_protocol 都打印关键信息 */
    /* kvs_logInfo("[kvs_protocol ENTRY] replica_mode=%d, cmd='%s', argc=%d, fd=%d\n", */
                /* g_config.replica_mode, cmd_name ? cmd_name : "NULL", argc, rb->nc->fd); */

    if (g_config.replica_mode == REPLICA_MODE_SLAVE) {
        extern int slave_sync_get_state(void);
        extern int slave_sync_enqueue(int argc, robj *argv);

        int sync_state = slave_sync_get_state();

        /* 【调试】打印状态机信息 */
        /* kvs_logInfo("[kvs_protocol SLAVE] sync_state=%d (IDLE=%d, SYNCING=%d, READY=%d)\n", */
                    /* sync_state, SLAVE_STATE_IDLE, SLAVE_STATE_SYNCING, SLAVE_STATE_READY); */

        /* 状态1: IDLE - 尚未开始存量同步
         * 此时引擎为空，任何查询都应返回 LOADING 错误
         * 这防止客户端在数据就绪前读到空结果 */
        if (sync_state == SLAVE_STATE_IDLE) {
            /* kvs_logInfo("[kvs_protocol SLAVE] 状态为 IDLE，返回 LOADING\n"); */
            rb_add_reply_error(rb, "LOADING data from master, please wait");
            return 0;  /* 提前返回，不执行命令 */
        }

        /* 状态2: SYNCING - 存量同步进行中
         * RDMA 线程正在写入引擎，主线程不能并发写入
         * 读命令必须加锁执行（防止与RDMA线程并发访问）
         * 写命令必须入队，等待存量完成后再回放 */
        if (sync_state == SLAVE_STATE_SYNCING) {
            /* 写命令：入积压队列，保证最终一致性 */
            int is_write = is_write_command(cmd_name);
            /* kvs_logInfo("[kvs_protocol SYNCING] 命令: '%s', is_write=%d, argc=%d\n", */
                        /* cmd_name, is_write, argc); */

            if (is_write) {
                /* kvs_logInfo("[kvs_protocol SYNCING] 检测到写命令 '%s'，准备入队\n", cmd_name); */
                int ret = slave_sync_enqueue(argc, argv);
                if (ret == 0) {
                    /* 入队成功，返回 QUEUED 让客户端知道命令被暂存 */
                    /* kvs_logInfo("[kvs_protocol SYNCING] 写命令 '%s' 已入积压队列，返回 QUEUED\n", cmd_name); */
                    rb_add_reply_status(rb, "QUEUED");
                } else {
                    /* 入队失败（内存不足），返回错误 */
                    kvs_logError("[kvs_protocol SYNCING] 写命令 '%s' 入队失败: ret=%d\n",
                                 cmd_name, ret);
                    rb_add_reply_error(rb, "Sync queue full");
                }
                /* kvs_logInfo("[kvs_protocol SYNCING] 写命令处理完成，return 0\n"); */
                return 0;  /* 已处理，不继续执行 */
            }

            /* 读命令：需要加锁执行，防止与RDMA线程并发访问引擎 */
            /* kvs_logInfo("[kvs_protocol SYNCING] 读命令 '%s' 加锁执行\n", cmd_name); */
            use_engine_lock = 1;  /* 标记需要在引擎操作时使用锁 */
            /* 不 return，继续执行后续命令处理逻辑 */
        } else {
            /* kvs_logInfo("[kvs_protocol SLAVE] 状态不是 SYNCING，继续正常执行\n"); */
        }

        /* 状态3: READY - 存量同步完成
         * 积压队列已回放，引擎进入正常读写状态
         * 所有命令正常执行，不干涉 */
        /* sync_state == SLAVE_STATE_READY，不做任何处理 */
    } else {
        /* kvs_logInfo("[kvs_protocol] 不是从节点模式，正常执行命令\n"); */
    }

    // 查找命令 ID（O(1) hash 查找）
    int cmd = cmd_hash_lookup(cmd_name);

    int ret = 0;
    char* gotValue = NULL;

    /* 【调试】确认执行到了 switch-case */
    /* kvs_logInfo("[kvs_protocol] 执行到 switch-case，cmd=%d, cmd_name='%s'\n", cmd, cmd_name ? cmd_name : "NULL"); */

    switch (cmd) {
#if ENABLE_MULTI_ENGINE
        case KVS_CMD_ASET:
            /* kvs_logInfo("[kvs_protocol] 执行 ASET 命令！key(%zu bytes) value(%zu bytes)\n", key->len, value->len); */
            ret = kvs_array_set(&array_engine, key, value);
            if (ret < 0) {
                rb_add_reply_error(rb, "ERROR");
            } else if (ret == 0) {
                if (g_config.aof_enabled) {
                    appendToAofBufferToEngine(AOF_ENGINE_TYPE_ARRAY, AOF_CMD_SET, key, value);
                }
                rb_add_reply_status(rb, "OK");
            } else {
                 rb_add_reply_error(rb, "Key has existed");
            }
            break;
        case KVS_CMD_AGET:
            /* kvs_logInfo("AGET key(%zu bytes) value(%zu bytes)", key->len, value->len); */
            gotValue = use_engine_lock ? kvs_array_get_safe(&array_engine, key) : kvs_array_get(&array_engine, key);
            // fprintf(stderr, "--> gotValue:\n%s", gotValue);
            if (gotValue == NULL) {
                rb_add_reply_error(rb, "ERROR / Not Exist"); // Redis style: return nil
            } else {
                rb_add_reply_bulk_len(rb, gotValue, strlen(gotValue));
            }
            break;
        case KVS_CMD_ADEL:
            /* kvs_logInfo("ADEL key(%zu bytes) value(%zu bytes)", key->len, value->len); */
            ret = kvs_array_del(&array_engine, key);
            if (ret < 0) {
                rb_add_reply_error(rb, "ERROR");
            } else if (ret == 0) {
                if (g_config.aof_enabled) {
                    appendToAofBufferToEngine(AOF_ENGINE_TYPE_ARRAY, AOF_CMD_DEL, key, NULL);
                }
                rb_add_reply_status(rb, "OK");
            } else {
                rb_add_reply_error(rb, "ERROR / Not Exist");
            }
            break;
        case KVS_CMD_AMOD:
            /* kvs_logInfo("AMOD key(%zu bytes) value(%zu bytes)", key->len, value->len); */
            ret = kvs_array_mod(&array_engine, key, value);
            if (ret < 0) {
                 rb_add_reply_error(rb, "ERROR");
            } else if (ret == 0) {
                if (g_config.aof_enabled) {
                    appendToAofBufferToEngine(AOF_ENGINE_TYPE_ARRAY, AOF_CMD_MOD, key, value);
                }
                rb_add_reply_status(rb, "OK");
            } else {
                rb_add_reply_error(rb, "Not Exist");
            }
            break;
        case KVS_CMD_AEXIST:
            /* kvs_logInfo("AEXIST key(%zu bytes) value(%zu bytes)", key->len, value->len); */
            ret = use_engine_lock ? kvs_array_exist_safe(&array_engine, key) : kvs_array_exist(&array_engine, key);
            if (ret > 0) {
                rb_add_reply_exist(rb, 1);
            } else if (ret == 0) {
                rb_add_reply_exist(rb, 0);
            } else {
                rb_add_reply_error(rb, "ERROR");
            }
            break;

        // 多引擎模式 - Hash 引擎命令
        case KVS_CMD_HSET:
            /* kvs_logInfo("HSET key(%zu bytes) value(%zu bytes)", key->len, value->len); */
            ret = kvs_hash_set(&hash_engine, key, value);
            if (ret < 0) {
                 rb_add_reply_error(rb, "ERROR");
            } else if (ret == 0) {
                if (g_config.aof_enabled) {
                    appendToAofBufferToEngine(AOF_ENGINE_TYPE_HASH, AOF_CMD_SET, key, value);
                }
                rb_add_reply_status(rb, "OK");
            } else {
                rb_add_reply_error(rb, "Key has existed");
            }
            break;
        case KVS_CMD_HGET:
            /* kvs_logInfo("HGET key(%zu bytes) value(%zu bytes)", key->len, value->len); */
            gotValue = use_engine_lock ? kvs_hash_get_safe(&hash_engine, key) : kvs_hash_get(&hash_engine, key);
            if (gotValue == NULL) {
                rb_add_reply_error(rb, "ERROR / Not Exist");
            } else {
                rb_add_reply_bulk_len(rb, gotValue, strlen(gotValue));
            }
            break;
        case KVS_CMD_HDEL:
            /* kvs_logInfo("HDEL key(%zu bytes) value(%zu bytes)", key->len, value->len); */
            ret = kvs_hash_del(&hash_engine, key);
            if (ret < 0) {
                rb_add_reply_error(rb, "ERROR");
            } else if (ret == 0) {
                if (g_config.aof_enabled) {
                    appendToAofBufferToEngine(AOF_ENGINE_TYPE_HASH, AOF_CMD_DEL, key, NULL);
                }
                rb_add_reply_status(rb, "OK");
            } else {
                rb_add_reply_error(rb, "ERROR / Not Exist");
            }
            break;
        case KVS_CMD_HMOD:
            /* kvs_logInfo("HMOD key(%zu bytes) value(%zu bytes)", key->len, value->len); */
            ret = kvs_hash_mod(&hash_engine, key, value);
            if (ret < 0) {
                rb_add_reply_error(rb, "ERROR");
            } else if (ret == 0) {
                if (g_config.aof_enabled) {
                    appendToAofBufferToEngine(AOF_ENGINE_TYPE_HASH, AOF_CMD_MOD, key, value);
                }
                rb_add_reply_status(rb, "OK");
            } else {
                rb_add_reply_error(rb, "Not Exist");
            }
            break;
        case KVS_CMD_HEXIST:
            /* kvs_logInfo("HEXIST key(%zu bytes) value(%zu bytes)", key->len, value->len); */
            ret = use_engine_lock ? kvs_hash_exist_safe(&hash_engine, key) : kvs_hash_exist(&hash_engine, key);
            if (ret > 0) {
                rb_add_reply_exist(rb, 1);
            } else if (ret == 0) {
                rb_add_reply_exist(rb, 0);
            } else {
                rb_add_reply_error(rb, "ERROR");
            }
            break;

        // 多引擎模式 - RBTREE 引擎命令
        case KVS_CMD_RSET:
            /* kvs_logInfo("RSET key(%zu bytes) value(%zu bytes)", key->len, value->len); */
            ret = kvs_rbtree_set(&rbtree_engine, key, value);
            if (ret < 0) {
                rb_add_reply_error(rb, "ERROR");
            } else if (ret == 0) {
                if (g_config.aof_enabled) {
                    appendToAofBufferToEngine(AOF_ENGINE_TYPE_RBTREE, AOF_CMD_SET, key, value);
                }
                rb_add_reply_status(rb, "OK");
            } else {
                rb_add_reply_error(rb, "Key has existed");
            }
            break;
        case KVS_CMD_RGET:
            /* kvs_logInfo("RGET key(%zu bytes) value(%zu bytes)", key->len, value->len); */
            gotValue = use_engine_lock ? kvs_rbtree_get_safe(&rbtree_engine, key) : kvs_rbtree_get(&rbtree_engine, key);
            if (gotValue == NULL) {
                rb_add_reply_error(rb, "ERROR / Not Exist");
            } else {
                rb_add_reply_bulk_len(rb, gotValue, strlen(gotValue));
            }
            break;
        case KVS_CMD_RDEL:
            /* kvs_logInfo("RDEL key(%zu bytes) value(%zu bytes)", key->len, value->len); */
            ret = kvs_rbtree_del(&rbtree_engine, key);
            if (ret < 0) {
                rb_add_reply_error(rb, "ERROR");
            } else if (ret == 0) {
                if (g_config.aof_enabled) {
                    appendToAofBufferToEngine(AOF_ENGINE_TYPE_RBTREE, AOF_CMD_DEL, key, NULL);
                }
                rb_add_reply_status(rb, "OK");
            } else {
                rb_add_reply_error(rb, "ERROR / Not Exist");
            }
            break;
        case KVS_CMD_RMOD:
            /* kvs_logInfo("RMOD key(%zu bytes) value(%zu bytes)", key->len, value->len); */
            ret = kvs_rbtree_mod(&rbtree_engine, key, value);
            if (ret < 0) {
                rb_add_reply_error(rb, "ERROR");
            } else if (ret == 0) {
                if (g_config.aof_enabled) {
                    appendToAofBufferToEngine(AOF_ENGINE_TYPE_RBTREE, AOF_CMD_MOD, key, value);
                }
                rb_add_reply_status(rb, "OK");
            } else {
                rb_add_reply_error(rb, "Not Exist");
            }
            break;
        case KVS_CMD_REXIST:
            /* kvs_logInfo("REXIST key(%zu bytes) value(%zu bytes)", key->len, value->len); */
            ret = use_engine_lock ? kvs_rbtree_exist_safe(&rbtree_engine, key) : kvs_rbtree_exist(&rbtree_engine, key);
            if (ret > 0) {
                rb_add_reply_exist(rb, 1);
            } else if (ret == 0) {
                rb_add_reply_exist(rb, 0);
            } else {
                rb_add_reply_error(rb, "ERROR");
            }
            break;
        case KVS_CMD_SSET:
            /* kvs_logInfo("SSET key(%zu bytes) value(%zu bytes)", key->len, value->len); */
            ret = kvs_skiplist_set(&skiplist_engine, key, value);
            if (ret < 0) {
                rb_add_reply_error(rb, "ERROR");
            } else if (ret == 0) {
                if (g_config.aof_enabled) {
                    appendToAofBufferToEngine(AOF_ENGINE_TYPE_SKIPLIST, AOF_CMD_SET, key, value);
                }
                rb_add_reply_status(rb, "OK");
            } else {
                rb_add_reply_error(rb, "Key has existed");
            }
            break;
        case KVS_CMD_SGET:
            /* kvs_logInfo("SGET key(%zu bytes) value(%zu bytes)", key->len, value->len); */
            gotValue = use_engine_lock ? kvs_skiplist_get_safe(&skiplist_engine, key) : kvs_skiplist_get(&skiplist_engine, key);
            if (gotValue == NULL) {
                rb_add_reply_error(rb, "ERROR / Not Exist");
            } else {
                rb_add_reply_bulk_len(rb, gotValue, strlen(gotValue));
            }
            break;
        case KVS_CMD_SDEL:
            /* kvs_logInfo("SDEL key(%zu bytes) value(%zu bytes)", key->len, value->len); */
            ret = kvs_skiplist_del(&skiplist_engine, key);
            if (ret < 0) {
                rb_add_reply_error(rb, "ERROR");
            } else if (ret == 0) {
                if (g_config.aof_enabled) {
                    appendToAofBufferToEngine(AOF_ENGINE_TYPE_SKIPLIST, AOF_CMD_DEL, key, NULL);
                }
                rb_add_reply_status(rb, "OK");
            } else {
                rb_add_reply_error(rb, "ERROR / Not Exist");
            }
            break;
        case KVS_CMD_SMOD:
            /* kvs_logInfo("SMOD key(%zu bytes) value(%zu bytes)", key->len, value->len); */
            ret = kvs_skiplist_mod(&skiplist_engine, key, value);
            if (ret < 0) {
                rb_add_reply_error(rb, "ERROR");
            } else if (ret == 0) {
                if (g_config.aof_enabled) {
                    appendToAofBufferToEngine(AOF_ENGINE_TYPE_SKIPLIST, AOF_CMD_MOD, key, value);
                }
                rb_add_reply_status(rb, "OK");
            } else {
                rb_add_reply_error(rb, "Not Exist");
            }
            break;
        case KVS_CMD_SEXIST:
            /* kvs_logInfo("SEXIST key(%zu bytes) value(%zu bytes)", key->len, value->len); */
            ret = use_engine_lock ? kvs_skiplist_exist_safe(&skiplist_engine, key) : kvs_skiplist_exist(&skiplist_engine, key);
            if (ret > 0) {
                rb_add_reply_exist(rb, 1);
            } else if (ret == 0) {
                rb_add_reply_exist(rb, 0);
            } else {
                rb_add_reply_error(rb, "ERROR");
            }
            break;
#else
        case KVS_CMD_SET:
            /* kvs_logInfo("SET key(%zu bytes) value(%zu bytes)", key->len, value->len); */
            ret = kvs_main_set(&global_main_engine, key, value);
            if (ret < 0) {
                rb_add_reply_error(rb, "ERROR");
            } else if (ret == 0) {
                if (g_config.aof_enabled) {
                    appendToAofBuffer(AOF_CMD_SET, key, value);
                }
                rb_add_reply_status(rb, "OK");
            } else {
                rb_add_reply_error(rb, "Key has existed");
            }
            break;
        case KVS_CMD_GET:
            /* kvs_logInfo("GET key(%zu bytes) value(%zu bytes)", key->len, value->len); */
            gotValue = use_engine_lock ? kvs_main_get_safe(&global_main_engine, key) : kvs_main_get(&global_main_engine, key);
            if (gotValue == NULL) {
                rb_add_reply_error(rb, "ERROR / Not Exist");
            } else {
                rb_add_reply_bulk_len(rb, gotValue, strlen(gotValue));
            }
            break;
        case KVS_CMD_DEL:
            /* kvs_logInfo("DEL key(%zu bytes) value(%zu bytes)", key->len, value->len); */
            ret = kvs_main_del(&global_main_engine, key);
            if (ret < 0) {
                rb_add_reply_error(rb, "ERROR");
            } else if (ret == 0) {
                if (g_config.aof_enabled) {
                    appendToAofBuffer(AOF_CMD_DEL, key, NULL);
                }
                rb_add_reply_status(rb, "OK");
            } else {
                rb_add_reply_error(rb, "Not Exist");
            }
            break;
        case KVS_CMD_MOD:
            /* kvs_logInfo("MOD key(%zu bytes) value(%zu bytes)", key->len, value->len); */
            ret = kvs_main_mod(&global_main_engine, key, value);
            if (ret < 0) {
                rb_add_reply_error(rb, "ERROR");
            } else if (ret == 0) {
                if (g_config.aof_enabled) {
                    appendToAofBuffer(AOF_CMD_MOD, key, value);
                }
                rb_add_reply_status(rb, "OK");
            } else {
                rb_add_reply_error(rb, "Not Exist");
            }
            break;
        case KVS_CMD_EXIST:
            /* kvs_logInfo("EXIST key(%zu bytes) value(%zu bytes)", key->len, value->len); */
            ret = use_engine_lock ? kvs_main_exist_safe(&global_main_engine, key) : kvs_main_exist(&global_main_engine, key);
            if (ret > 0) {
                rb_add_reply_exist(rb, 1);
            } else if (ret == 0) {
                rb_add_reply_exist(rb, 0);
            } else {
                rb_add_reply_error(rb, "ERROR");
            }
            break;
#endif

        case KVS_CMD_SAVE:
            // 同步保存快照
        #if ENABLE_MULTI_ENGINE
            ksfSaveAll();
        #else
            ksfSave(snap_filename);
        #endif 
            rb_add_reply_status(rb, "OK");
            break;
        case KVS_CMD_BGSAVE:
            // 异步保存快照
            ksfSaveBackground();
            rb_add_reply_status(rb, "Background saving started");
            break;
        case KVS_CMD_SYNC:
            // SYNC 命令：触发从节点向主节点执行 RDMA 存量同步
            // 仅可在从节点上执行，主节点会返回错误
            return kvs_cmd_sync(rb);  // 调用 sync_command.c 中实现的命令处理函数
        case KVS_CMD_REPLICAOF:
            // REPLICAOF 命令：设置或取消主从复制关系
            // 用法: REPLICAOF <host> <port>  或  REPLICAOF NO ONE
            if (argc < 3) {
                rb_add_reply_error(rb, "wrong number of arguments for 'replicaof' command");
                return 0;
            }
            return kvs_cmd_replicaof(rb, argc, argv);  // 调用 sync_command.c 中的处理函数

        case KVS_CMD_REPLCONF:
            slave_register(rb->nc);
            rb_add_reply_status(rb, "OK");
            break;

        case KVS_CMD_RDMASYNC:
            /*
             * 【方案C核心】RDMASYNC 命令处理
             *
             * 功能: 从节点通过此命令触发主节点fork子进程进行RDMA同步
             *
             * 执行流程:
             *   1. 检查是否为主节点（从节点不应收到此命令）
             *   2. 解析引擎类型参数
             *   3. fork子进程
             *   4. 子进程调用rdma_sync_child_server()处理RDMA同步
             *   5. 父进程立即返回+FORKED，继续处理其他连接
             *
             * 注意事项:
             *   - 此命令会移交TCP连接fd给子进程，父进程不再处理此连接
             *   - 子进程完成同步后自动exit()，通过原TCP连接发送+RDMA_DONE
             */
            {
                /* 1. 检查是否为主节点 */
                if (g_config.replica_mode != REPLICA_MODE_MASTER) {
                    rb_add_reply_error(rb, "RDMASYNC only available on master");
                    return 0;
                }

                /* 2. 检查参数 */
                if (argc < 2) {
                    rb_add_reply_error(rb, "wrong number of arguments for 'rdmasync' command");
                    return 0;
                }

                /* 3. 解析引擎类型 */
                int engine_type = atoi(argv[1].ptr);
                if (engine_type < 0 || engine_type > ENGINE_COUNT) {
                    rb_add_reply_error(rb, "invalid engine type");
                    return 0;
                }

                /* 4. fork子进程 */
                /* 【安全修复】在fork前设置所有fd的CLOEXEC标志，防止子进程继承不必要的fd */
                set_cloexec_on_all_fds();

                pid_t pid = fork();
                if (pid < 0) {
                    kvs_logError("[RDMASYNC] fork失败: %s\n", strerror(errno));
                    rb_add_reply_error(rb, "fork failed");
                    return 0;
                }

                if (pid == 0) {
                    /* ========== 子进程 ========== */
                    /* 子进程接管TCP fd，处理RDMA同步 */
                    int tcp_fd = rb->nc->fd;

                    /*
                     * 注意: 子进程继承父进程的内存空间（COW），
                     * 但父进程的io_uring/epoll等资源不应使用。
                     * rdma_sync_child_server()会关闭除tcp_fd外的所有fd。
                     */
                    int ret = rdma_sync_child_server(tcp_fd, (rdma_engine_type_t)engine_type);

                    /* 子进程直接exit，不返回 */
                    exit(ret == 0 ? 0 : 1);
                }

                /* ========== 父进程 ========== */
                /* kvs_logInfo("[RDMASYNC] 已fork子进程(pid=%d)处理RDMA同步\n", pid); */

                /*
                 * 发送+FORKED响应给客户端
                 * 子进程随后会发送+RDMA_READY和+RDMA_DONE
                 */
                rb_add_reply_status(rb, "FORKED");

                /*
                 * 重要: 标记连接fd为已移交
                 * 防止父进程在连接清理时关闭该fd（子进程正在使用）
                 */
                rb->nc->fd = -1;  /* fd已移交给子进程 */
                rb->nc->state = ST_CLOSE;  /* 标记连接需要清理（但不关闭fd） */

                return 0;
            }

        default:
            rb_add_reply_error(rb, "UNKNOWN COMMAND");
    }
    

    if (g_config.auto_save_enabled) {

        // 检查是否需要计数（自动保存）
        changes_since_last_save += is_write_command(cmd_name);
        check_and_perform_autosave();
    }

    /* 主节点：写命令执行后广播给所有已注册从节点 */
    if (g_config.replica_mode == REPLICA_MODE_MASTER && is_write_command(cmd_name)) {
        repl_propagate(&g_ring, argc, argv);
    }

    return 0;
}

int init_kvengine(void) {
    // 初始化命令 hash 表
    init_cmd_hash();

    // 初始化kmem内存池系统
    if (kmem_init() != 0) {
        kvs_logError("Failed to initialize kmem\n");
        return -1;
    }
    // fprintf(stderr, "-1-->\n");
    #if ENABLE_MULTI_ENGINE
    // 多引擎模式：初始化所有启用的引擎
    #if ENABLE_RBTREE
    // fprintf(stderr, "rbt-->\n");
    memset(&rbtree_engine, 0, sizeof(rbtree_engine));
    kvs_rbtree_create(&rbtree_engine);
    #endif
    #if ENABLE_HASH
    // fprintf(stderr, "hash-->\n");
    memset(&hash_engine, 0, sizeof(hash_engine));
    kvs_hash_create(&hash_engine);
    #endif
    #if ENABLE_ARRAY
    // fprintf(stderr, "arr-->\n");
    memset(&array_engine, 0, sizeof(array_engine));
    kvs_array_create(&array_engine);
    #endif
    #if ENABLE_SKIPLIST
    // fprintf(stderr, "skip-->\n");
    memset(&skiplist_engine, 0, sizeof(skiplist_engine));
    kvs_skiplist_create(&skiplist_engine);
    #endif
    #else
    // 单引擎模式：只初始化一个引擎
    memset(&global_main_engine, 0, sizeof(global_main_engine));
    kvs_main_create(&global_main_engine);
    #endif
    // fprintf(stderr, "-2-->\n");
    return 0;
}

/* 防止重复调用的静态标志 */
static int g_dest_already_called = 0;

void dest_kvengine(void) {
    if (g_dest_already_called) {
        kvs_logWarn("dest_kvengine 已经被调用过，跳过重复执行");
        return;
    }
    g_dest_already_called = 1;

    // 【重要】停止AOF后台线程，确保数据完整写入
    if (g_config.aof_enabled) {
        stop_aof_fsync_process();
    }
    
#if ENABLE_MULTI_ENGINE
    ksfSaveAll();
// 多引擎模式：销毁所有引擎
#if ENABLE_RBTREE
    kvs_rbtree_destroy(&rbtree_engine);
#endif
#if ENABLE_HASH
    kvs_hash_destroy(&hash_engine);
#endif
#if ENABLE_ARRAY
    kvs_array_destroy(&array_engine);
#endif
#if ENABLE_SKIPLIST
    kvs_skiplist_destroy(&skiplist_engine);
#endif
#else
    ksfSave(snap_filename);
    // 单引擎模式：只销毁一个引擎
    kvs_main_destroy(&global_main_engine);
#endif

    // 清理同步模块（关闭 RDMA 连接、释放资源）
    // 【必须在 kmem_destroy() 之前调用】因为 sync_module_cleanup() 使用 kvs_free()
    // 释放内存，而 kvs_free() 在没有 jemalloc 时会调用 kmem_free()。如果 kmem_destroy()
    // 先执行，内存池会被销毁，导致访问已释放资源触发 SIGSEGV。
    sync_module_cleanup();

    // 销毁kmem内存池系统
    kmem_destroy();
}

// 信号处理函数
// 【安全修复】使用异步信号安全的 write() 替代 printf()
void signal_handler(int sig) {
    static volatile sig_atomic_t g_signal_received = 0;

    // 防止重复进入
    if (g_signal_received) {
        return;
    }
    g_signal_received = 1;

    // 使用 write() 输出信号信息（异步信号安全）
    char msg[64];
    int len = snprintf(msg, sizeof(msg), "\n接收到信号 %d，准备关闭服务...\n", sig);
    if (len > 0 && len < (int)sizeof(msg)) {
        write(STDOUT_FILENO, msg, (size_t)len);
    }
    
    // 【重要】设置全局退出标志，通知网络层优雅退出
    // proactor 使用 io_uring_wait_cqe_timeout，100ms 内会检查此标志
#if (NETWORK_SELECT == NETWORK_PROACTOR)
    extern void proactor_stop(void);
    proactor_stop();
#elif (NETWORK_SELECT == NETWORK_REACTOR)
    extern void reactor_stop(void);
    reactor_stop();
#elif (NETWORK_SELECT == NETWORK_NTYCO)
    extern void ntyco_stop(void);
    ntyco_stop();
#endif
    
    // 不调用 exit()！让 proactor 自然退出，main 函数继续执行 dest_kvengine()
    // 优点：
    // 1. 控制流清晰：signal → stop → proactor 退出 → dest_kvengine
    // 2. 避免 exit() 的强制终止可能导致的资源不一致
    // 3. 便于调试，堆栈跟踪清晰
}

int main(int argc, char* argv[]) {
    /* 1. 初始化配置(目前配置为写死的默认配置) */
    kv_config_init();
    // printf("=== 当前默认配置 ===\n");
    // kv_config_print_all();

    /* 2. 加载配置文件（根目录下的 kvstore.conf） */    
    if (argc > 1) {
        // 命令行指定配置
        kvs_logInfo("指定配置文件: %s\n", argv[1]);
        if (kv_config_load(argv[1]) < 0) {
            kvs_logError("Using default configuration\n");
        }
    } else {
        // 加载默认路径
        if (kv_config_load_default() < 0) {
            kvs_logError("No config file found, using defaults\n");
        }
    }
    kvs_logDebug("\n=== 配置文件加载完毕 ===\n");

    /* 3. 打印最终配置 */
    kv_config_print_all();
    unsigned short port = g_config.port;

    
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    // 注意：不使用 atexit，改为显式调用 dest_kvengine
    // 优点：
    // 1. 控制流清晰，便于调试
    // 2. 避免 atexit 的执行顺序问题
    // 3. 异常终止时不会执行不完整的清理

    init_kvengine();

    /* 4. 初始化同步模块（RDMA 主从复制）
     * 如果是主节点，启动 RDMA 服务器等待从节点连接
     * 如果是从节点，准备 RDMA 客户端资源
     */
    if (sync_module_init() < 0) {
        kvs_logError("[main] 同步模块初始化失败\n");
        return -1;
    }

    /* 5. 如果是从节点且配置了主节点，自动启动存量同步
     *
     * 【v3.0 架构调整】先初始化 slave_sync，再启动同步
     *
     * 原因：slave_sync_init() 创建 eventfd，用于 RDMA 线程通知主线程。
     * 如果先启动同步，RDMA 线程可能在 eventfd 创建前完成，导致通知丢失，
     * 积压队列无法回放。
     *
     * 正确顺序：
     *   1. slave_sync_init() - 创建 eventfd
     *   2. start_slave_sync() - 启动 RDMA 线程
     *   3. RDMA 完成时通过已创建的 eventfd 通知
     */
    if (g_config.replica_mode == REPLICA_MODE_SLAVE &&
        g_config.master_host[0] != '\0') {
        kvs_logInfo("[main] 从节点配置检测到主节点 %s:%d\n",
                    g_config.master_host, g_config.master_port);

        /* 先初始化从节点同步系统（创建 eventfd） */
        extern int slave_sync_init(void);
        int event_fd = slave_sync_init();
        if (event_fd < 0) {
            kvs_logError("[main] 从节点同步系统初始化失败\n");
            return -1;
        }
        kvs_logInfo("[main] 从节点同步系统初始化完成，event_fd=%d\n", event_fd);

        /* 再启动存量同步（创建 RDMA 线程） */
        extern int start_slave_sync(void);  // 来自 sync_command.c
        if (start_slave_sync() < 0) {
            kvs_logError("[main] 自动同步启动失败\n");
            /* 不返回错误，允许用户手动重试 */
        }
    }

        if (g_config.init_mode == INIT_MODE_AOF) {
#if ENABLE_MULTI_ENGINE
        #if ENABLE_MMAP
            aofLoadAll_mmap();
        #else
            aofLoadAll();
        #endif
#else
            aofLoad(aof_filename);
        #endif 
        } else if (g_config.init_mode == INIT_MODE_SNAPSHOT) {
#if ENABLE_MULTI_ENGINE
            #if ENABLE_MMAP
            // fprintf(stderr, "3-->\n");
            ksfLoadAll_mmap();
            // fprintf(stderr, "4-->\n");
            #else
            ksfLoadAll();
            #endif
#else
            ksfLoad(snap_filename);
#endif
        }
if (g_config.aof_enabled) {
    start_aof_fsync_process();
}

#if (NETWORK_SELECT == NETWORK_REACTOR)
    reactor_start(port, kvs_protocol);
#elif (NETWORK_SELECT == NETWORK_PROACTOR)
    proactor_start(port, kvs_protocol);
#elif (NETWORK_SELECT == NETWORK_NTYCO)
    ntyco_start(port, kvs_protocol);
#endif

    // 【优雅退出】proactor 正常退出后，显式执行清理
    // 信号处理流程：SIGTERM → signal_handler → proactor_stop → proactor 超时退出 → dest_kvengine
    // 正常流程：proactor 完成工作 → 退出循环 → dest_kvengine
    dest_kvengine();
    
    kvs_logInfo("服务已完全关闭");
}
