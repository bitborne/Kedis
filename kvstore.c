#include "kvstore.h"

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

#include "include/memory_pool.h"
#include "include/kvs_protocol.h"
#include "include/kvs_network.h"

// jemalloc头文件
#ifdef HAVE_JEMALLOC
#include <jemalloc/jemalloc.h>
#endif

// 全局内存池实例
static memory_pool_t* g_mem_pool = NULL;

#if (NETWORK_SELECT == NETWORK_REACTOR)
#include "src/network/reactor_server.h"  // only for reactor.c
#endif

enum {
  KVS_CMD_START = 0,
  // 统一的KV操作命令（单引擎模式）
  KVS_CMD_SET = KVS_CMD_START,
  KVS_CMD_GET,
  KVS_CMD_DEL,
  KVS_CMD_MOD,
  KVS_CMD_EXIST,

  // 多引擎模式 - Array 引擎命令
  KVS_CMD_ASET,
  KVS_CMD_AGET,
  KVS_CMD_ADEL,
  KVS_CMD_AMOD,
  KVS_CMD_AEXIST,

  // 多引擎模式 - Hash 引擎命令
  KVS_CMD_HSET,
  KVS_CMD_HGET,
  KVS_CMD_HDEL,
  KVS_CMD_HMOD,
  KVS_CMD_HEXIST,

  // 多引擎模式 - RBTREE 引擎命令
  KVS_CMD_RSET,
  KVS_CMD_RGET,
  KVS_CMD_RDEL,
  KVS_CMD_RMOD,
  KVS_CMD_REXIST,

  // 多引擎模式 - Skiplist 引擎命令
  KVS_CMD_SSET,
  KVS_CMD_SGET,
  KVS_CMD_SDEL,
  KVS_CMD_SMOD,
  KVS_CMD_SEXIST,

  // 通用命令（两种模式都支持）
  KVS_CMD_SAVE,
  KVS_CMD_BGSAVE,
  KVS_CMD_SYNC,

  KVS_CMD_COUNT
};

// Global Lock and Context
pthread_mutex_t global_kvs_lock = PTHREAD_MUTEX_INITIALIZER;
__thread int current_processing_fd = -1;

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
        {"SET",  "GET",    "DEL",    "MOD",    "EXIST", "ASET",
         "AGET", "ADEL",   "AMOD",   "AEXIST", "HSET",  "HGET",
         "HDEL", "HMOD",   "HEXIST", "RSET",   "RGET",  "RDEL",
         "RMOD", "REXIST", "SSET",   "SGET",   "SDEL",  "SMOD",
         "SEXIST", "SAVE", "BGSAVE", "SYNC"};  // 添加SAVE和BGSAVE命令

// 全局变量存储持久化模式和自动保存参数
static int load_mode = INIT_LOAD_SNAP;

// 自动保存参数：save seconds changes
static int save_params_seconds = 300;    // 5分钟
static int save_params_changes = 100;    // 100次变化
static time_t last_save_time = 0;        // 上次保存时间
static int changes_since_last_save = 0;  // 自上次保存以来的变化次数

extern replication_state_t replication_info;

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
  if (current_time - last_save_time >= save_params_seconds &&
      changes_since_last_save >= save_params_changes) {
    printf("触发自动快照保存：已超过 %d 秒且发生 %d 次变化\n",
           save_params_seconds, save_params_changes);

    // 更新最后保存时间
    last_save_time = current_time;
    changes_since_last_save = 0;  // 重置变化计数

    // 执行后台保存
    ksfSaveBackground();
  }
}

/* ---------------- RESP 响应辅助函数 ---------------- */
static void add_reply_str(struct conn* c, const char* str) {
    if (!str) return;
    size_t len = strlen(str);
    if (c->wlen + len > RESP_BUF_SIZE) return; // 简单保护
    memcpy(c->wbuf + c->wlen, str, len);
    c->wlen += len;
}

static void add_reply_error(struct conn* c, const char* err) {
    add_reply_str(c, "-");
    add_reply_str(c, err);
    add_reply_str(c, "\r\n");
}

static void add_reply_status(struct conn* c, const char* status) {
    add_reply_str(c, "+");
    add_reply_str(c, status);
    add_reply_str(c, "\r\n");
}

static void add_reply_bulk(struct conn* c, const char* str) {
    if (str == NULL) {
        add_reply_str(c, "$-1\r\n"); // Null Bulk String
        return;
    }
    char buf[32];
    size_t len = strlen(str);
    sprintf(buf, "$%lu\r\n", len);
    add_reply_str(c, buf);
    add_reply_str(c, str);
    add_reply_str(c, "\r\n");
}

// 为了兼容旧的 "YES, Exist" 返回格式，这里做个简单映射，也可以直接返回 RESP Integer
static void add_reply_exist(struct conn* c, int exists) {
    // 按照 proactor.c 之前的逻辑，返回 :1 或 :0
    char buf[32];
    sprintf(buf, ":%d\r\n", exists ? 1 : 0);
    add_reply_str(c, buf);
}

/* ---------------- 流式发送相关函数 ---------------- */

// 初始化流式发送状态
static int init_streaming_send(struct conn* c, const char* data, size_t len) {
    if (c == NULL || data == NULL || len == 0) {
        return -1;
    }

    // 设置流式发送标志
    c->is_streaming = 1;
    c->stream_state = STREAM_STATE_HEADER;
    c->stream_data = data;
    c->stream_total_len = len;
    c->stream_sent_len = 0;
    c->stream_chunk_size = IOP_SIZE;  // 128KB

    // 生成头部："$len\r\n"
    c->header_len = snprintf(c->stream_header, sizeof(c->stream_header), "$%zu\r\n", len);
    c->header_sent = 0;

    // 设置尾部："\r\n"
    strcpy(c->stream_tail, "\r\n");
    c->tail_sent = 0;

    return 0;
}

// 重置流式发送状态
extern void reset_streaming_send(struct conn* c) {
    if (c == NULL) return;

    c->is_streaming = 0;
    c->stream_state = STREAM_STATE_IDLE;
    c->stream_data = NULL;
    c->stream_total_len = 0;
    c->stream_sent_len = 0;
    c->stream_chunk_size = 0;

    memset(c->stream_header, 0, sizeof(c->stream_header));
    c->header_len = 0;
    c->header_sent = 0;

    memset(c->stream_tail, 0, sizeof(c->stream_tail));
    c->tail_sent = 0;
}

// 高层接口：启用流式发送模式发送 bulk string
static int add_reply_bulk_streaming(struct conn* c, const char* str) {
    if (c == NULL) {
        return -1;
    }

    // 处理 NULL bulk string
    if (str == NULL) {
        add_reply_str(c, "$-1\r\n");
        return 0;
    }

    // 计算数据长度
    size_t len = strlen(str);

    // 根据数据大小选择发送方式
    if (len < STREAMING_THRESHOLD) {
        // 小数据，使用传统方式
        add_reply_bulk(c, str);
    } else {
        // 大数据，使用流式发送
        printf("[DEBUG] Using streaming send for %zu bytes\n", len);
        if (init_streaming_send(c, str, len) < 0) {
            return -1;
        }
    }

    return 0;
}

/* ---------------- 核心命令执行逻辑 ---------------- */
int kvs_protocol(struct conn* c) {
  pthread_mutex_lock(&global_kvs_lock);  // LOCK

  if (c->argc == 0) {
    pthread_mutex_unlock(&global_kvs_lock);
    return 0;
  }

  // 提取命令参数
  // 注意：c->argv[i].ptr 已经是 null-terminated 的字符串 (我们在 kvs_resp_feed 里保证了)
  char* cmd_name = c->argv[0].ptr;
  char* key = (c->argc > 1) ? c->argv[1].ptr : NULL;
  char* value = (c->argc > 2) ? c->argv[2].ptr : NULL;

  // 查找命令 ID
  int cmd = KVS_CMD_START;
  for (cmd = KVS_CMD_START; cmd < KVS_CMD_COUNT; cmd++) {
    if (strcasecmp(cmd_name, command[cmd]) == 0) { // strcasecmp 忽略大小写
      break;
    }
  }

  // SLAVE READ-ONLY CHECK
  if (!replication_info.is_master) {
    if (is_write_command(cmd_name)) {
      if (current_processing_fd != slave_info.master_fd) {
        add_reply_error(c, "READONLY You can't write against a read only replica");
        pthread_mutex_unlock(&global_kvs_lock);
        return c->wlen;
      }
    }
  }

  int ret = 0;
  char* gotValue = NULL;

  switch (cmd) {
    // 多引擎模式 - Array 引擎命令
#if ENABLE_MULTI_ENGINE
    case KVS_CMD_ASET:
      ret = kvs_array_set(&array_engine, key, value);
      if (ret < 0) {
        add_reply_error(c, "ERROR");
      } else if (ret == 0) {
        if (replication_info.is_master) {
          // appendToAofBufferToEngine(0, AOF_CMD_SET, key, value);
        }
        add_reply_status(c, "OK");
      } else {
         add_reply_error(c, "Key has existed");
      }
      break;
    case KVS_CMD_AGET:
      gotValue = kvs_array_get(&array_engine, key);
      if (gotValue == NULL) {
        add_reply_error(c, "ERROR / Not Exist"); // Redis style: return nil
      } else {
        add_reply_bulk_streaming(c, gotValue);
      }
      break;
    case KVS_CMD_ADEL:
      ret = kvs_array_del(&array_engine, key);
      if (ret < 0) {
        add_reply_error(c, "ERROR");
      } else if (ret == 0) {
        if (replication_info.is_master) {
          // appendToAofBufferToEngine(0, AOF_CMD_DEL, key, NULL);
        }
        add_reply_status(c, "OK");
      } else {
        add_reply_error(c, "Not Exist");
      }
      break;
    case KVS_CMD_AMOD:
      ret = kvs_array_mod(&array_engine, key, value);
      if (ret < 0) {
         add_reply_error(c, "ERROR");
      } else if (ret == 0) {
        if (replication_info.is_master) {
          // appendToAofBufferToEngine(0, AOF_CMD_MOD, key, value);
        }
        add_reply_status(c, "OK");
      } else {
        add_reply_error(c, "Not Exist");
      }
      break;
    case KVS_CMD_AEXIST:
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
      ret = kvs_hash_set(&hash_engine, key, value);
      if (ret < 0) {
         add_reply_error(c, "ERROR");
      } else if (ret == 0) {
        if (replication_info.is_master) {
          // appendToAofBufferToEngine(1, AOF_CMD_SET, key, value);
        }
        add_reply_status(c, "OK");
      } else {
        add_reply_error(c, "Key has existed");
      }
      break;
    case KVS_CMD_HGET:
      gotValue = kvs_hash_get(&hash_engine, key);
      if (gotValue == NULL) {
        add_reply_error(c, "ERROR / Not Exist");
      } else {
        add_reply_bulk_streaming(c, gotValue);
      }
      break;
    case KVS_CMD_HDEL:
      ret = kvs_hash_del(&hash_engine, key);
      if (ret < 0) {
        add_reply_error(c, "ERROR");
      } else if (ret == 0) {
        if (replication_info.is_master) {
          // appendToAofBufferToEngine(1, AOF_CMD_DEL, key, NULL);
        }
        add_reply_status(c, "OK");
      } else {
        add_reply_error(c, "Not Exist");
      }
      break;
    case KVS_CMD_HMOD:
      ret = kvs_hash_mod(&hash_engine, key, value);
      if (ret < 0) {
        add_reply_error(c, "ERROR");
      } else if (ret == 0) {
        if (replication_info.is_master) {
          // appendToAofBufferToEngine(1, AOF_CMD_MOD, key, value);
        }
        add_reply_status(c, "OK");
      } else {
        add_reply_error(c, "Not Exist");
      }
      break;
    case KVS_CMD_HEXIST:
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
      ret = kvs_rbtree_set(&rbtree_engine, key, value);
      if (ret < 0) {
        add_reply_error(c, "ERROR");
      } else if (ret == 0) {
        if (replication_info.is_master) {
          // appendToAofBufferToEngine(2, AOF_CMD_SET, key, value);
        }
        add_reply_status(c, "OK");
      } else {
        add_reply_error(c, "Key has existed");
      }
      break;
    case KVS_CMD_RGET:
      gotValue = kvs_rbtree_get(&rbtree_engine, key);
      if (gotValue == NULL) {
        add_reply_error(c, "ERROR / Not Exist");
      } else {
        add_reply_bulk_streaming(c, gotValue);
      }
      break;
    case KVS_CMD_RDEL:
      ret = kvs_rbtree_del(&rbtree_engine, key);
      if (ret < 0) {
        add_reply_error(c, "ERROR");
      } else if (ret == 0) {
        if (replication_info.is_master) {
          // appendToAofBufferToEngine(2, AOF_CMD_DEL, key, NULL);
        }
        add_reply_status(c, "OK");
      } else {
        add_reply_error(c, "Not Exist");
      }
      break;
    case KVS_CMD_RMOD:
      ret = kvs_rbtree_mod(&rbtree_engine, key, value);
      if (ret < 0) {
        add_reply_error(c, "ERROR");
      } else if (ret == 0) {
        if (replication_info.is_master) {
          // appendToAofBufferToEngine(2, AOF_CMD_MOD, key, value);
        }
        add_reply_status(c, "OK");
      } else {
        add_reply_error(c, "Not Exist");
      }
      break;
    case KVS_CMD_REXIST:
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
      ret = kvs_skiplist_set(&skiplist_engine, key, value);
      if (ret < 0) {
        add_reply_error(c, "ERROR");
      } else if (ret == 0) {
        if (replication_info.is_master) {
          // appendToAofBufferToEngine(3, AOF_CMD_SET, key, value);
        }
        add_reply_status(c, "OK");
      } else {
        add_reply_error(c, "Key has existed");
      }
      break;
    case KVS_CMD_SGET:
      gotValue = kvs_skiplist_get(&skiplist_engine, key);
      if (gotValue == NULL) {
        add_reply_error(c, "ERROR / Not Exist");
      } else {
        add_reply_bulk_streaming(c, gotValue);
      }
      break;
    case KVS_CMD_SDEL:
      ret = kvs_skiplist_del(&skiplist_engine, key);
      if (ret < 0) {
        add_reply_error(c, "ERROR");
      } else if (ret == 0) {
        if (replication_info.is_master) {
          // appendToAofBufferToEngine(3, AOF_CMD_DEL, key, NULL);
        }
        add_reply_status(c, "OK");
      } else {
        add_reply_error(c, "Not Exist");
      }
      break;
    case KVS_CMD_SMOD:
      ret = kvs_skiplist_mod(&skiplist_engine, key, value);
      if (ret < 0) {
        add_reply_error(c, "ERROR");
      } else if (ret == 0) {
        if (replication_info.is_master) {
          // appendToAofBufferToEngine(3, AOF_CMD_MOD, key, value);
        }
        add_reply_status(c, "OK");
      } else {
        add_reply_error(c, "Not Exist");
      }
      break;
    case KVS_CMD_SEXIST:
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
      ret = kvs_main_set(&global_main_engine, key, value);
      if (ret < 0) {
        add_reply_error(c, "ERROR");
      } else if (ret == 0) {
        if (replication_info.is_master) {
          appendToAofBuffer(AOF_CMD_SET, key, value);
        }
        add_reply_status(c, "OK");
      } else {
        add_reply_error(c, "Key has existed");
      }
      break;
    case KVS_CMD_GET:
      gotValue = kvs_main_get(&global_main_engine, key);
      if (gotValue == NULL) {
        add_reply_error(c, "ERROR / Not Exist");
      } else {
        add_reply_bulk_streaming(c, gotValue);
      }
      break;
    case KVS_CMD_DEL:
      ret = kvs_main_del(&global_main_engine, key);
      if (ret < 0) {
        add_reply_error(c, "ERROR");
      } else if (ret == 0) {
        if (replication_info.is_master) {
          appendToAofBuffer(AOF_CMD_DEL, key, NULL);
        }
        add_reply_status(c, "OK");
      } else {
        add_reply_error(c, "Not Exist");
      }
      break;
    case KVS_CMD_MOD:
      ret = kvs_main_mod(&global_main_engine, key, value);
      if (ret < 0) {
        add_reply_error(c, "ERROR");
      } else if (ret == 0) {
        if (replication_info.is_master) {
          appendToAofBuffer(AOF_CMD_MOD, key, value);
        }
        add_reply_status(c, "OK");
      } else {
        add_reply_error(c, "Not Exist");
      }
      break;
    case KVS_CMD_EXIST:
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
      if (handle_sync_command(current_processing_fd) == 0) {
        // SYNC 处理可能特殊，暂不做修改，或者假设 handle_sync_command 自己写了 fd？
        // 原始代码: return 0; // Response sent by handler
        // 这里保持原样
        pthread_mutex_unlock(&global_kvs_lock);
        return 0;
      } else {
        add_reply_error(c, "ERROR SYNC");
      }
      break;
    default:
      add_reply_error(c, "UNKNOWN COMMAND");
  }

  // 检查是否需要计数（自动保存）
  if (is_write_command(cmd_name)) {
    changes_since_last_save++;
  }
  
  check_and_perform_autosave();
  pthread_mutex_unlock(&global_kvs_lock);  // UNLOCK
  
  return c->wlen;
}

int init_kvengine(void) {
  // 初始化内存池，针对KV存储的典型数据大小进行优化
  g_mem_pool = mem_pool_init(BLOCK_SIZE);

#if ENABLE_MULTI_ENGINE
// 多引擎模式：初始化所有启用的引擎
#if ENABLE_RBTREE
  memset(&rbtree_engine, 0, sizeof(rbtree_engine));
  kvs_rbtree_create(&rbtree_engine);
#endif
#if ENABLE_HASH
  memset(&hash_engine, 0, sizeof(hash_engine));
  kvs_hash_create(&hash_engine);
#endif
#if ENABLE_ARRAY
  memset(&array_engine, 0, sizeof(array_engine));
  kvs_array_create(&array_engine);
#endif
#if ENABLE_SKIPLIST
  memset(&skiplist_engine, 0, sizeof(skiplist_engine));
  kvs_skiplist_create(&skiplist_engine);
#endif
#else
  // 单引擎模式：只初始化一个引擎
  memset(&global_main_engine, 0, sizeof(global_main_engine));
  kvs_main_create(&global_main_engine);
#endif
  return 0;
}

void dest_kvengine(void) {
#if ENABLE_MULTI_ENGINE
  if (replication_info.is_master) ksfSaveAll();
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
  if (replication_info.is_master) ksfSave(snap_filename);
  // 单引擎模式：只销毁一个引擎
  kvs_main_destroy(&global_main_engine);
#endif

  // 释放内存池
  if (g_mem_pool) {
    mem_pool_destroy(g_mem_pool);
    g_mem_pool = NULL;
  }
}

// 信号处理函数
void signal_handler(int sig) {
  printf("\n接收到信号 %d，准备关闭服务...\n", sig);
  // 不直接调用dest_kvengine，而是正常退出，让atexit处理清理
  exit(0);  // 正常退出，会调用atexit注册的函数
}

int main(int argc, char* argv[]) {
  if (argc < 2) {
    printf("Usage: %s <port> [aof|snap] [--slaveof ip port]\n", argv[0]);
    return -1;
  }

  int port = atoi(argv[1]);
  char* master_ip = NULL;
  int master_port = 0;

  // Arg parsing
  for (int i = 2; i < argc; i++) {
    if (strcmp(argv[i], "--slaveof") == 0) {
      if (i + 2 < argc) {
        master_ip = argv[i + 1];
        master_port = atoi(argv[i + 2]);
        i += 2;
      }
    } else if (strcmp(argv[i], "aof") == 0) {
      load_mode = INIT_LOAD_AOF;
    } else if (strcmp(argv[i], "snap") == 0) {
      load_mode = INIT_LOAD_SNAP;
    }
  }

  signal(SIGINT, signal_handler);
  signal(SIGTERM, signal_handler);

  if (atexit(dest_kvengine) != 0) {
    printf("警告：无法注册退出函数\n");
  }

  init_kvengine();

  if (master_ip != NULL) {
    // SLAVE MODE
    printf("Starting as SLAVE. Syncing with Master %s:%d...\n", master_ip,
           master_port);
    if (init_slave_replication(master_ip, master_port) != 0) {
      fprintf(stderr, "Failed to sync with master. Exiting.\n");
      return -1;
    }
  } else {
    // MASTER MODE
    if (load_mode == INIT_LOAD_AOF) {
#if ENABLE_MULTI_ENGINE
      aofLoadAll();
#else
      aofLoad(aof_filename);
#endif
    } else if (load_mode == INIT_LOAD_SNAP) {
#if ENABLE_MULTI_ENGINE
      ksfLoadAll();
#else
      ksfLoad(snap_filename);
#endif
    }
  }

  start_aof_fsync_process();

#if (NETWORK_SELECT == NETWORK_REACTOR)
  reactor_start(port, kvs_protocol);  //
#elif (NETWORK_SELECT == NETWORK_PROACTOR)
  proactor_start(port, kvs_protocol);
#elif (NETWORK_SELECT == NETWORK_NTYCO)
  ntyco_start(port, kvs_protocol);
#endif

  dest_kvengine();
}
