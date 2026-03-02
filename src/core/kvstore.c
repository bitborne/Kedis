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

// 全局内存池实例
static memory_pool_t* g_mem_pool = NULL;

#if (NETWORK_SELECT == NETWORK_REACTOR)
#include "src/network/reactor_server.h"    // only for reactor.c
#endif

// enum {
//     KVS_CMD_START = 0,
//     // 统一的KV操作命令（单引擎模式）
//     KVS_CMD_SET = KVS_CMD_START,
//     KVS_CMD_GET,
//     KVS_CMD_DEL,
//     KVS_CMD_MOD,
//     KVS_CMD_EXIST,

//     // 多引擎模式 - Array 引擎命令
//     KVS_CMD_ASET,
//     KVS_CMD_AGET,
//     KVS_CMD_ADEL,
//     KVS_CMD_AMOD,
//     KVS_CMD_AEXIST,

//     // 多引擎模式 - Hash 引擎命令
//     KVS_CMD_HSET,
//     KVS_CMD_HGET,
//     KVS_CMD_HDEL,
//     KVS_CMD_HMOD,
//     KVS_CMD_HEXIST,

//     // 多引擎模式 - RBTREE 引擎命令
//     KVS_CMD_RSET,
//     KVS_CMD_RGET,
//     KVS_CMD_RDEL,
//     KVS_CMD_RMOD,
//     KVS_CMD_REXIST,

//     // 多引擎模式 - Skiplist 引擎命令
//     KVS_CMD_SSET,
//     KVS_CMD_SGET,
//     KVS_CMD_SDEL,
//     KVS_CMD_SMOD,
//     KVS_CMD_SEXIST,

//     // 通用命令（两种模式都支持）
//     KVS_CMD_SAVE,
//     KVS_CMD_BGSAVE,
//     KVS_CMD_SYNC,

//     KVS_CMD_COUNT
// };

// // Global Lock and Context
// pthread_mutex_t global_kvs_lock = PTHREAD_MUTEX_INITIALIZER;
// __thread int current_processing_fd = -1;

// 多引擎模式下的引擎实例定义
#if ENABLE_MULTI_ENGINE
#if ENABLE_RBTREE
kvs_rbtree_t rbtree_engine;
#endif
#if ENABLE_HASH
kvs_hash_t hash_engine;
#endif
#if ENABLE_ARRAY
kvs_array_t array_engine;
#endif
#if ENABLE_SKIPLIST
kvs_skiplist_t skiplist_engine;
#endif
#else
// 单引擎模式：根据优先级选择使用的数据结构：红黑树 > 哈希 > 数组
#if ENABLE_RBTREE
kvs_rbtree_t global_main_engine;
#elif ENABLE_HASH
kvs_hash_t global_main_engine;
#elif ENABLE_SKIPLIST
kvs_skiplist_t global_main_engine;
#elif ENABLE_ARRAY
kvs_array_t global_main_engine;
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
    // 如果内存池已初始化且请求大小适合内存池，则使用内存池
    if (g_mem_pool && size <= g_mem_pool->block_size) {
        return mem_pool_alloc(g_mem_pool);
    }
    // 否则使用标准malloc
    return malloc(size);
#endif
}

void kvs_free(void* ptr) {
    // 检查指针是否属于内存池管理范围
#ifdef HAVE_JEMALLOC
    free(ptr);
#else
    if (g_mem_pool && ptr) {
        char* start = (char*)g_mem_pool->chunk + sizeof(mem_block_t);
        char* end = start + g_mem_pool->max_blocks *
                                                        (sizeof(mem_block_t) + g_mem_pool->block_size);
        char* ptr_char = (char*)ptr;

        if (ptr_char >= start && ptr_char < end) {
            mem_pool_free(g_mem_pool, ptr);
            return;
        }
    }
    // 不在内存池范围内的指针使用标准free
    free(ptr);
#endif
}
// 定义了头文件中 command 变量的声明
const char*
        command[] =
                {"SET", "GET", "DEL", "MOD", "EXIST", "ASET",
                 "AGET", "ADEL", "AMOD", "AEXIST", "HSET", "HGET",
                 "HDEL", "HMOD", "HEXIST", "RSET", "RGET", "RDEL",
                 "RMOD", "REXIST", "SSET", "SGET", "SDEL", "SMOD",
                 "SEXIST", "SAVE", "BGSAVE", "SYNC", "REPLICAOF"};    // 添加SAVE、BGSAVE、SYNC和REPLICAOF命令

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
int is_write_command(const char* command) {
    if (command == NULL) return 0;

    if (strcmp(command, "SET") == 0 || strcmp(command, "RSET") == 0 ||
            strcmp(command, "HSET") == 0 || strcmp(command, "MOD") == 0 ||
            strcmp(command, "RMOD") == 0 || strcmp(command, "HMOD") == 0 ||
            strcmp(command, "DEL") == 0 || strcmp(command, "RDEL") == 0 ||
            strcmp(command, "HDEL") == 0 || strcmp(command, "ASET") == 0 ||
            strcmp(command, "AMOD") == 0 || strcmp(command, "ADEL") == 0 || 
            strcmp(command, "SSET") == 0 || strcmp(command, "SMOD") == 0 || 
            strcmp(command, "SDEL") == 0) {
        return 1;
    }
    return 0;
}

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

/* ---------------- RESP 响应辅助函数 ---------------- */
// 发送原始字符串到回复缓冲区，供命令处理使用
void add_reply_str(struct conn* c, const char* str) {
        if (!str) return;
        size_t len = strlen(str);
        if (c->wlen + len > RESP_BUF_SIZE) return; // 简单保护
        memcpy(c->wbuf + c->wlen, str, len);
        c->wlen += len;
}

// 发送错误回复 (-ERR ...)，供命令处理使用
void add_reply_error(struct conn* c, const char* err) {
        add_reply_str(c, "-");
        add_reply_str(c, err);
        add_reply_str(c, "\r\n");
        c->send_st = ST_SEND_SMALL;
}

// 发送状态回复 (+OK ...)，供命令处理使用
void add_reply_status(struct conn* c, const char* status) {
        add_reply_str(c, "+");
        add_reply_str(c, status);
        add_reply_str(c, "\r\n");
        c->send_st = ST_SEND_SMALL;
}

// 发送批量字符串回复 ($len...)，供命令处理使用
void add_reply_bulk(struct conn* c, char* str) {
        // 如果 str 为 NULL，返回 Null Bulk String
        if (str == NULL) {
                add_reply_str(c, "$-1\r\n"); // Null Bulk String
                return;
        }
        
        // 计算数据的长度
        c->bulk_tt = strlen(str) + 2; // 为了 \r\n
        c->bulk_data = str;
        // 计算响应数据的总长度
        // 格式：$<len>\r\n<data>\r\n    (第二个 \r\n 已包含在数据中)
        // 长度：1 ($) + 数字位数 + 2 (\r\n) + len + 2 (\r\n)
        char hdr_buf[32]; // 临时缓冲区，用于存储 RESP 头部
        c->hdr_len = sprintf(hdr_buf, "$%zu\r\n", c->bulk_tt - 2); // 生成 RESP 头部

        memcpy(c->wbuf + c->wlen, hdr_buf, c->hdr_len);
        c->wlen += c->hdr_len;

        size_t avail    = RESP_BUF_SIZE - c->wlen;
        size_t remain = c->bulk_tt - c->bulk_sent;
        size_t cp = avail < remain ? avail : remain;
        memcpy(c->wbuf + c->wlen, str, cp);

        c->wlen += cp;
        if (remain <= avail) {
            c->wbuf[c->wlen - 2] = '\r';
            c->wbuf[c->wlen - 1] = '\n';
        } else if (remain == avail + 1) {
            c->wbuf[c->wlen - 1] = '\r';
        } else if (remain == 1) {
            c->wbuf[c->wlen] = '\n';
        }

        c->send_st = ST_SEND_HDR_SENT;
}

// 为了兼容旧的 "YES, Exist" 返回格式，这里做个简单映射，也可以直接返回 RESP Integer
static void add_reply_exist(struct conn* c, int exists) {
        // 按照 proactor.c 之前的逻辑，返回 :1 或 :0
        char buf[32];
        sprintf(buf, ":%d\r\n", exists ? 1 : 0);
        add_reply_str(c, buf);
        c->send_st = ST_SEND_SMALL;
}


/* ---------------- 核心命令执行逻辑 ---------------- */
int kvs_protocol(struct conn* c) {
    
    char* cmd_name = c->argv[0].ptr;
    robj* key = &c->argv[1];
    robj* value = &c->argv[2];

    // 查找命令 ID
    int cmd = KVS_CMD_START;
    for (cmd = KVS_CMD_START; cmd < KVS_CMD_COUNT; cmd++) {
        if (strcasecmp(cmd_name, command[cmd]) == 0) { // strcasecmp 忽略大小写
            break;
        }
    }

    int ret = 0;
    char* gotValue = NULL;

    switch (cmd) {
#if ENABLE_MULTI_ENGINE
        case KVS_CMD_ASET:
            kvs_logInfo("ASET key(%zu bytes) value(%zu bytes)", key->len, value->len);
            ret = kvs_array_set(&array_engine, key, value);
            if (ret < 0) {
                add_reply_error(c, "ERROR");
            } else if (ret == 0) {
                if (g_config.aof_enabled) {
                    appendToAofBufferToEngine(AOF_ENGINE_TYPE_ARRAY, AOF_CMD_SET, key, value);
                }
                add_reply_status(c, "OK");
            } else {
                 add_reply_error(c, "Key has existed");
            }
            break;
        case KVS_CMD_AGET:
            kvs_logInfo("AGET key(%zu bytes) value(%zu bytes)", key->len, value->len);
            gotValue = kvs_array_get(&array_engine, key);
            // fprintf(stderr, "--> gotValue:\n%s", gotValue);
            if (gotValue == NULL) {
                add_reply_error(c, "ERROR / Not Exist"); // Redis style: return nil
            } else {
                add_reply_bulk(c, gotValue);
            }
            break;
        case KVS_CMD_ADEL:
            kvs_logInfo("ADEL key(%zu bytes) value(%zu bytes)", key->len, value->len);
            ret = kvs_array_del(&array_engine, key);
            if (ret < 0) {
                add_reply_error(c, "ERROR");
            } else if (ret == 0) {
                if (g_config.aof_enabled) {
                    appendToAofBufferToEngine(AOF_ENGINE_TYPE_ARRAY, AOF_CMD_DEL, key, NULL);
                }
                add_reply_status(c, "OK");
            } else {
                add_reply_error(c, "ERROR / Not Exist");
            }
            break;
        case KVS_CMD_AMOD:
            kvs_logInfo("AMOD key(%zu bytes) value(%zu bytes)", key->len, value->len);
            ret = kvs_array_mod(&array_engine, key, value);
            if (ret < 0) {
                 add_reply_error(c, "ERROR");
            } else if (ret == 0) {
                if (g_config.aof_enabled) {
                    appendToAofBufferToEngine(AOF_ENGINE_TYPE_ARRAY, AOF_CMD_MOD, key, value);
                }
                add_reply_status(c, "OK");
            } else {
                add_reply_error(c, "Not Exist");
            }
            break;
        case KVS_CMD_AEXIST:
            kvs_logInfo("AEXIST key(%zu bytes) value(%zu bytes)", key->len, value->len);
            ret = kvs_array_exist(&array_engine, key);
            if (ret > 0) {
                add_reply_exist(c, 1);
            } else if (ret == 0) {
                add_reply_exist(c, 0);
            } else {
                add_reply_error(c, "ERROR");
            }
            break;

        // 多引擎模式 - Hash 引擎命令
        case KVS_CMD_HSET:
            kvs_logInfo("HSET key(%zu bytes) value(%zu bytes)", key->len, value->len);
            ret = kvs_hash_set(&hash_engine, key, value);
            if (ret < 0) {
                 add_reply_error(c, "ERROR");
            } else if (ret == 0) {
                if (g_config.aof_enabled) {
                    appendToAofBufferToEngine(AOF_ENGINE_TYPE_HASH, AOF_CMD_SET, key, value);
                }
                add_reply_status(c, "OK");
            } else {
                add_reply_error(c, "Key has existed");
            }
            break;
        case KVS_CMD_HGET:
            kvs_logInfo("HGET key(%zu bytes) value(%zu bytes)", key->len, value->len);
            gotValue = kvs_hash_get(&hash_engine, key);
            if (gotValue == NULL) {
                add_reply_error(c, "ERROR / Not Exist");
            } else {
                add_reply_bulk(c, gotValue);
            }
            break;
        case KVS_CMD_HDEL:
            kvs_logInfo("HDEL key(%zu bytes) value(%zu bytes)", key->len, value->len);
            ret = kvs_hash_del(&hash_engine, key);
            if (ret < 0) {
                add_reply_error(c, "ERROR");
            } else if (ret == 0) {
                if (g_config.aof_enabled) {
                    appendToAofBufferToEngine(AOF_ENGINE_TYPE_HASH, AOF_CMD_DEL, key, NULL);
                }
                add_reply_status(c, "OK");
            } else {
                add_reply_error(c, "ERROR / Not Exist");
            }
            break;
        case KVS_CMD_HMOD:
            kvs_logInfo("HMOD key(%zu bytes) value(%zu bytes)", key->len, value->len);
            ret = kvs_hash_mod(&hash_engine, key, value);
            if (ret < 0) {
                add_reply_error(c, "ERROR");
            } else if (ret == 0) {
                if (g_config.aof_enabled) {
                    appendToAofBufferToEngine(AOF_ENGINE_TYPE_HASH, AOF_CMD_MOD, key, value);
                }
                add_reply_status(c, "OK");
            } else {
                add_reply_error(c, "Not Exist");
            }
            break;
        case KVS_CMD_HEXIST:
            kvs_logInfo("HEXIST key(%zu bytes) value(%zu bytes)", key->len, value->len);
            ret = kvs_hash_exist(&hash_engine, key);
            if (ret > 0) {
                add_reply_exist(c, 1);
            } else if (ret == 0) {
                add_reply_exist(c, 0);
            } else {
                add_reply_error(c, "ERROR");
            }
            break;

        // 多引擎模式 - RBTREE 引擎命令
        case KVS_CMD_RSET:
            kvs_logInfo("RSET key(%zu bytes) value(%zu bytes)", key->len, value->len);
            ret = kvs_rbtree_set(&rbtree_engine, key, value);
            if (ret < 0) {
                add_reply_error(c, "ERROR");
            } else if (ret == 0) {
                if (g_config.aof_enabled) {
                    appendToAofBufferToEngine(AOF_ENGINE_TYPE_RBTREE, AOF_CMD_SET, key, value);
                }
                add_reply_status(c, "OK");
            } else {
                add_reply_error(c, "Key has existed");
            }
            break;
        case KVS_CMD_RGET:
            kvs_logInfo("RGET key(%zu bytes) value(%zu bytes)", key->len, value->len);
            gotValue = kvs_rbtree_get(&rbtree_engine, key);
            if (gotValue == NULL) {
                add_reply_error(c, "ERROR / Not Exist");
            } else {
                add_reply_bulk(c, gotValue);
            }
            break;
        case KVS_CMD_RDEL:
            kvs_logInfo("RDEL key(%zu bytes) value(%zu bytes)", key->len, value->len);
            ret = kvs_rbtree_del(&rbtree_engine, key);
            if (ret < 0) {
                add_reply_error(c, "ERROR");
            } else if (ret == 0) {
                if (g_config.aof_enabled) {
                    appendToAofBufferToEngine(AOF_ENGINE_TYPE_RBTREE, AOF_CMD_DEL, key, NULL);
                }
                add_reply_status(c, "OK");
            } else {
                add_reply_error(c, "ERROR / Not Exist");
            }
            break;
        case KVS_CMD_RMOD:
            kvs_logInfo("RMOD key(%zu bytes) value(%zu bytes)", key->len, value->len);
            ret = kvs_rbtree_mod(&rbtree_engine, key, value);
            if (ret < 0) {
                add_reply_error(c, "ERROR");
            } else if (ret == 0) {
                if (g_config.aof_enabled) {
                    appendToAofBufferToEngine(AOF_ENGINE_TYPE_RBTREE, AOF_CMD_MOD, key, value);
                }
                add_reply_status(c, "OK");
            } else {
                add_reply_error(c, "Not Exist");
            }
            break;
        case KVS_CMD_REXIST:
            kvs_logInfo("REXIST key(%zu bytes) value(%zu bytes)", key->len, value->len);
            ret = kvs_rbtree_exist(&rbtree_engine, key);
            if (ret > 0) {
                add_reply_exist(c, 1);
            } else if (ret == 0) {
                add_reply_exist(c, 0);
            } else {
                add_reply_error(c, "ERROR");
            }
            break;
        case KVS_CMD_SSET:
            kvs_logInfo("SSET key(%zu bytes) value(%zu bytes)", key->len, value->len);
            ret = kvs_skiplist_set(&skiplist_engine, key, value);
            if (ret < 0) {
                add_reply_error(c, "ERROR");
            } else if (ret == 0) {
                if (g_config.aof_enabled) {
                    appendToAofBufferToEngine(AOF_ENGINE_TYPE_SKIPLIST, AOF_CMD_SET, key, value);
                }
                add_reply_status(c, "OK");
            } else {
                add_reply_error(c, "Key has existed");
            }
            break;
        case KVS_CMD_SGET:
            kvs_logInfo("SGET key(%zu bytes) value(%zu bytes)", key->len, value->len);
            gotValue = kvs_skiplist_get(&skiplist_engine, key);
            if (gotValue == NULL) {
                add_reply_error(c, "ERROR / Not Exist");
            } else {
                add_reply_bulk(c, gotValue);
            }
            break;
        case KVS_CMD_SDEL:
            kvs_logInfo("SDEL key(%zu bytes) value(%zu bytes)", key->len, value->len);
            ret = kvs_skiplist_del(&skiplist_engine, key);
            if (ret < 0) {
                add_reply_error(c, "ERROR");
            } else if (ret == 0) {
                if (g_config.aof_enabled) {
                    appendToAofBufferToEngine(AOF_ENGINE_TYPE_SKIPLIST, AOF_CMD_DEL, key, NULL);
                }
                add_reply_status(c, "OK");
            } else {
                add_reply_error(c, "ERROR / Not Exist");
            }
            break;
        case KVS_CMD_SMOD:
            kvs_logInfo("SMOD key(%zu bytes) value(%zu bytes)", key->len, value->len);
            ret = kvs_skiplist_mod(&skiplist_engine, key, value);
            if (ret < 0) {
                add_reply_error(c, "ERROR");
            } else if (ret == 0) {
                if (g_config.aof_enabled) {
                    appendToAofBufferToEngine(AOF_ENGINE_TYPE_SKIPLIST, AOF_CMD_MOD, key, value);
                }
                add_reply_status(c, "OK");
            } else {
                add_reply_error(c, "Not Exist");
            }
            break;
        case KVS_CMD_SEXIST:
            kvs_logInfo("SEXIST key(%zu bytes) value(%zu bytes)", key->len, value->len);
            ret = kvs_skiplist_exist(&skiplist_engine, key);
            if (ret > 0) {
                add_reply_exist(c, 1);
            } else if (ret == 0) {
                add_reply_exist(c, 0);
            } else {
                add_reply_error(c, "ERROR");
            }
            break;
#else
        case KVS_CMD_SET:
            kvs_logInfo("SET key(%zu bytes) value(%zu bytes)", key->len, value->len);
            ret = kvs_main_set(&global_main_engine, key, value);
            if (ret < 0) {
                add_reply_error(c, "ERROR");
            } else if (ret == 0) {
                appendToAofBuffer(AOF_CMD_SET, key, value);
                add_reply_status(c, "OK");
            } else {
                add_reply_error(c, "Key has existed");
            }
            break;
        case KVS_CMD_GET:
            kvs_logInfo("GET key(%zu bytes) value(%zu bytes)", key->len, value->len);
            gotValue = kvs_main_get(&global_main_engine, key);
            if (gotValue == NULL) {
                add_reply_error(c, "ERROR / Not Exist");
            } else {
                add_reply_bulk(c, gotValue);
            }
            break;
        case KVS_CMD_DEL:
            kvs_logInfo("DEL key(%zu bytes) value(%zu bytes)", key->len, value->len);
            ret = kvs_main_del(&global_main_engine, key);
            if (ret < 0) {
                add_reply_error(c, "ERROR");
            } else if (ret == 0) {
                appendToAofBuffer(AOF_CMD_DEL, key, NULL);
                add_reply_status(c, "OK");
            } else {
                add_reply_error(c, "Not Exist");
            }
            break;
        case KVS_CMD_MOD:
            kvs_logInfo("MOD key(%zu bytes) value(%zu bytes)", key->len, value->len);
            ret = kvs_main_mod(&global_main_engine, key, value);
            if (ret < 0) {
                add_reply_error(c, "ERROR");
            } else if (ret == 0) {
                appendToAofBuffer(AOF_CMD_MOD, key, value);
                add_reply_status(c, "OK");
            } else {
                add_reply_error(c, "Not Exist");
            }
            break;
        case KVS_CMD_EXIST:
            kvs_logInfo("EXIST key(%zu bytes) value(%zu bytes)", key->len, value->len);
            ret = kvs_main_exist(&global_main_engine, key);
            if (ret > 0) {
                add_reply_exist(c, 1);
            } else if (ret == 0) {
                add_reply_exist(c, 0);
            } else {
                add_reply_error(c, "ERROR");
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
            add_reply_status(c, "OK");
            break;
        case KVS_CMD_BGSAVE:
            // 异步保存快照
            ksfSaveBackground();
            add_reply_status(c, "Background saving started");
            break;
        case KVS_CMD_SYNC:
            // SYNC 命令：触发从节点向主节点执行 RDMA 存量同步
            // 仅可在从节点上执行，主节点会返回错误
            return kvs_cmd_sync(c);  // 调用 sync_command.c 中实现的命令处理函数
        case KVS_CMD_REPLICAOF:
            // REPLICAOF 命令：设置或取消主从复制关系
            // 用法: REPLICAOF <host> <port>  或  REPLICAOF NO ONE
            if (c->argc < 3) {
                add_reply_error(c, "wrong number of arguments for 'replicaof' command");
                return 0;
            }
            return kvs_cmd_replicaof(c, c->argc, c->argv);  // 调用 sync_command.c 中的处理函数
        default:
            add_reply_error(c, "UNKNOWN COMMAND");
    }
    

    if (g_config.auto_save_enabled) {

        // 检查是否需要计数（自动保存）
        changes_since_last_save += is_write_command(cmd_name);    
        check_and_perform_autosave();
    }

    return c->wlen;
}

int init_kvengine(void) {
    // 初始化内存池，针对KV存储的典型数据大小进行优化
    g_mem_pool = mem_pool_init(MEM_BLOCK_SIZE);
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

void dest_kvengine(void) {
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

    // 释放内存池
    if (g_mem_pool) {
        mem_pool_destroy(g_mem_pool);
        g_mem_pool = NULL;
    }

    // 清理同步模块（关闭 RDMA 连接、释放资源）
    sync_module_cleanup();
}

// 信号处理函数
void signal_handler(int sig) {
    printf("\n接收到信号 %d，准备关闭服务...\n", sig);
    // 不直接调用dest_kvengine，而是正常退出，让atexit处理清理
    exit(0);    // 正常退出，会调用atexit注册的函数
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

    // fprintf(stderr, "0-->\n");
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    if (atexit(dest_kvengine) != 0) {
        kvs_logWarn("无法注册退出函数\n");
    }

    init_kvengine();

    /* 4. 初始化同步模块（RDMA 主从复制）
     * 如果是主节点，启动 RDMA 服务器等待从节点连接
     * 如果是从节点，准备 RDMA 客户端资源
     */
    if (sync_module_init() < 0) {
        kvs_logError("[main] 同步模块初始化失败\n");
        return -1;
    }

    /* 5. 如果是从节点且配置了主节点，自动启动存量同步 */
    if (g_config.replica_mode == REPLICA_MODE_SLAVE &&
        g_config.master_host[0] != '\0') {
        kvs_logInfo("[main] 从节点配置检测到主节点 %s:%d，启动自动同步\n",
                    g_config.master_host, g_config.master_port);
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

    dest_kvengine();

}
