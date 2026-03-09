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
                 "RDMASYNC"};    // 添加SAVE、BGSAVE、SYNC、REPLICAOF和RDMASYNC命令

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
    kvs_logInfo("[kvs_protocol ENTRY] replica_mode=%d, cmd='%s', argc=%d, fd=%d\n",
                g_config.replica_mode, cmd_name ? cmd_name : "NULL", c->argc, c->fd);

    if (g_config.replica_mode == REPLICA_MODE_SLAVE) {
        extern int slave_sync_get_state(void);
        extern int slave_sync_enqueue(int argc, robj *argv);

        int sync_state = slave_sync_get_state();

        /* 【调试】打印状态机信息 */
        kvs_logInfo("[kvs_protocol SLAVE] sync_state=%d (IDLE=%d, SYNCING=%d, READY=%d)\n",
                    sync_state, SLAVE_STATE_IDLE, SLAVE_STATE_SYNCING, SLAVE_STATE_READY);

        /* 状态1: IDLE - 尚未开始存量同步
         * 此时引擎为空，任何查询都应返回 LOADING 错误
         * 这防止客户端在数据就绪前读到空结果 */
        if (sync_state == SLAVE_STATE_IDLE) {
            kvs_logInfo("[kvs_protocol SLAVE] 状态为 IDLE，返回 LOADING\n");
            add_reply_error(c, "LOADING data from master, please wait");
            return 0;  /* 提前返回，不执行命令 */
        }

        /* 状态2: SYNCING - 存量同步进行中
         * RDMA 线程正在写入引擎，主线程不能并发写入
         * 读命令可以安全执行（引擎读取是线程安全的）
         * 写命令必须入队，等待存量完成后再回放 */
        if (sync_state == SLAVE_STATE_SYNCING) {
            /* 写命令：入积压队列，保证最终一致性 */
            int is_write = is_write_command(cmd_name);
            kvs_logInfo("[kvs_protocol SYNCING] 命令: '%s', is_write=%d, argc=%d\n",
                        cmd_name, is_write, c->argc);

            if (is_write) {
                kvs_logInfo("[kvs_protocol SYNCING] 检测到写命令 '%s'，准备入队\n", cmd_name);
                int ret = slave_sync_enqueue(c->argc, c->argv);
                if (ret == 0) {
                    /* 入队成功，返回 QUEUED 让客户端知道命令被暂存 */
                    kvs_logInfo("[kvs_protocol SYNCING] 写命令 '%s' 已入积压队列，返回 QUEUED\n", cmd_name);
                    add_reply_status(c, "QUEUED");
                } else {
                    /* 入队失败（内存不足），返回错误 */
                    kvs_logError("[kvs_protocol SYNCING] 写命令 '%s' 入队失败: ret=%d\n",
                                 cmd_name, ret);
                    add_reply_error(c, "Sync queue full");
                }
                kvs_logInfo("[kvs_protocol SYNCING] 写命令处理完成，return 0\n");
                return 0;  /* 已处理，不继续执行 */
            }

            /* 读命令：穿透执行，不阻塞查询
             * 注意：可能读到不完整数据（RDMA 同步中的中间状态）
             * 这是 CAP 权衡：接受短暂的不一致，保证可用性 */
            kvs_logInfo("[kvs_protocol SYNCING] 读命令 '%s' 穿透执行\n", cmd_name);
            /* 不 return，继续执行后续命令处理逻辑 */
        } else {
            kvs_logInfo("[kvs_protocol SLAVE] 状态不是 SYNCING，继续正常执行\n");
        }

        /* 状态3: READY - 存量同步完成
         * 积压队列已回放，引擎进入正常读写状态
         * 所有命令正常执行，不干涉 */
        /* sync_state == SLAVE_STATE_READY，不做任何处理 */
    } else {
        kvs_logInfo("[kvs_protocol] 不是从节点模式，正常执行命令\n");
    }

    // 查找命令 ID
    int cmd = KVS_CMD_START;
    for (cmd = KVS_CMD_START; cmd < KVS_CMD_COUNT; cmd++) {
        if (strcasecmp(cmd_name, command[cmd]) == 0) { // strcasecmp 忽略大小写
            break;
        }
    }

    int ret = 0;
    char* gotValue = NULL;

    /* 【调试】确认执行到了 switch-case */
    kvs_logInfo("[kvs_protocol] 执行到 switch-case，cmd=%d, cmd_name='%s'\n", cmd, cmd_name ? cmd_name : "NULL");

    switch (cmd) {
#if ENABLE_MULTI_ENGINE
        case KVS_CMD_ASET:
            kvs_logInfo("[kvs_protocol] 执行 ASET 命令！key(%zu bytes) value(%zu bytes)\n", key->len, value->len);
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
                    add_reply_error(c, "RDMASYNC only available on master");
                    return 0;
                }

                /* 2. 检查参数 */
                if (c->argc < 2) {
                    add_reply_error(c, "wrong number of arguments for 'rdmasync' command");
                    return 0;
                }

                /* 3. 解析引擎类型 */
                int engine_type = atoi(c->argv[1].ptr);
                if (engine_type < 0 || engine_type > ENGINE_COUNT) {
                    add_reply_error(c, "invalid engine type");
                    return 0;
                }

                /* 4. fork子进程 */
                pid_t pid = fork();
                if (pid < 0) {
                    kvs_logError("[RDMASYNC] fork失败: %s\n", strerror(errno));
                    add_reply_error(c, "fork failed");
                    return 0;
                }

                if (pid == 0) {
                    /* ========== 子进程 ========== */
                    /* 子进程接管TCP fd，处理RDMA同步 */
                    int tcp_fd = c->fd;

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
                kvs_logInfo("[RDMASYNC] 已fork子进程(pid=%d)处理RDMA同步\n", pid);

                /*
                 * 发送+FORKED响应给客户端
                 * 子进程随后会发送+RDMA_READY和+RDMA_DONE
                 */
                add_reply_status(c, "FORKED");

                /*
                 * 重要: 标记连接fd为已移交
                 * 防止父进程在连接清理时关闭该fd（子进程正在使用）
                 */
                c->fd = -1;  /* fd已移交给子进程 */
                c->state = ST_CLOSE;  /* 标记连接需要清理（但不关闭fd） */

                return 0;
            }

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

    // 销毁kmem内存池系统
    kmem_destroy();

    // 清理同步模块（关闭 RDMA 连接、释放资源）
    sync_module_cleanup();
}

// 信号处理函数
void signal_handler(int sig) {
    static volatile int g_signal_received = 0;
    
    // 防止重复进入
    if (g_signal_received) {
        return;
    }
    g_signal_received = 1;
    
    printf("\n接收到信号 %d，准备关闭服务...\n", sig);
    
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
