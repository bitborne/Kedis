#include "kvstore.h"
#include "memory_pool.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <stdbool.h>
#include <unistd.h>
#include <sys/wait.h>
#include <time.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <pthread.h>
#include <semaphore.h>

// jemalloc头文件
#ifdef HAVE_JEMALLOC
#include <jemalloc/jemalloc.h>
#endif

// 全局内存池实例
static memory_pool_t* g_mem_pool = NULL;

#if (NETWORK_SELECT == NETWORK_REACTOR)
#include "server.h"  // only for reactor.c
#endif

enum {
  KVS_CMD_START = 0,
  // 统一的KV操作命令
  KVS_CMD_SET = KVS_CMD_START,
  KVS_CMD_GET,
  KVS_CMD_DEL,
  KVS_CMD_MOD,
  KVS_CMD_EXIST,

  KVS_CMD_SAVE,
  KVS_CMD_BGSAVE,
  KVS_CMD_SYNC,

  KVS_CMD_COUNT
};

#define ECHO_LOGIC 0
#define KV_LOGIC 1

// Global Lock and Context
pthread_mutex_t global_kvs_lock = PTHREAD_MUTEX_INITIALIZER;
__thread int current_processing_fd = -1;

// 根据优先级选择使用的数据结构：红黑树 > 哈希 > 数组
// 以下是当前使用的数据结构的统一接口定义
#if ENABLE_RBTREE
  kvs_rbtree_t global_main_engine;
#elif ENABLE_HASH
  kvs_hash_t global_main_engine;
#elif ENABLE_ARRAY
  kvs_array_t global_main_engine;
#else
  #error "至少需要启用一种数据结构"
#endif

// AOF缓冲区和长度
char aof_buf[AOF_BUF_SIZE] = {0};
int aof_len = 0;
extern const char* aof_filename;
const char* snap_filename = "./data/dump.ksf";

// 不直接使用系统调用(第三方接口)
// 跨平台的时候，只需要修改这个函数即可--> 可迭代
void* kvs_calloc(size_t num, size_t size) {
#ifdef HAVE_JEMALLOC
  return je_calloc(num, size);
#else
    size_t total_size = num * size;
    void *ptr = kvs_malloc(total_size);
    if (ptr) {
        memset(ptr, 0, total_size);
    }
    return ptr;
#endif 
}

void* kvs_malloc(size_t size) {
  #ifdef HAVE_JEMALLOC
    return je_malloc(size);
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
    je_free(ptr);
  #else
    if (g_mem_pool && ptr) {
        char *start = (char*)g_mem_pool->chunk + sizeof(mem_block_t);
        char *end = start + g_mem_pool->max_blocks * (sizeof(mem_block_t) + g_mem_pool->block_size);
        char *ptr_char = (char*)ptr;

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
const char* command[] = {"SET",  "GET",  "DEL",  "MOD",  "EXIST",
                         "SAVE", "BGSAVE", "SYNC"};  // 添加SAVE和BGSAVE命令

// 全局变量存储持久化模式和自动保存参数
static int load_mode = INIT_LOAD_SNAP;

// 自动保存参数：save seconds changes
static int save_params_seconds = 300;      // 5分钟
static int save_params_changes = 100;      // 100次变化
static time_t last_save_time = 0;          // 上次保存时间
static int changes_since_last_save = 0;    // 自上次保存以来的变化次数

extern replication_state_t replication_info;

/*
 * 解析单条命令的token
 */
int kvs_split_token(char* msg, char* tokens[]) {
  if (msg == NULL || tokens == NULL) return -1;

  int len = strlen(msg);
  if (len > 0 && msg[len - 1] == '\n') {
    if (len > 1 && msg[len - 2] == '\r') {
      msg[len - 2] = '\0';  // Handle CRLF
    } else {
      msg[len - 1] = '\0';  // Handle LF only
    }
  }

  char* token = strtok(msg, " ");  // 拆出来一个token 返回首地址
  int idx = 0;
  while (token != NULL) {
    tokens[idx++] = token;
    token = strtok(NULL, " ");
  }
  return idx;
}

/*
 * 分割多条命令
 */
int kvs_split_multicmd(char* msg, char* commands[], int max_commands) {
  if (msg == NULL || commands == NULL || max_commands <= 0) return -1;

  int cmd_count = 0;
  char* start = msg;
  char* pos = msg;
  int len = strlen(msg);

  while (pos < msg + len && cmd_count < max_commands) {
    // 查找命令分隔符 & 或 &&
    if (*pos == '&') {
      // 检查是单个&还是&&
      int is_andand = 0;
      if (pos + 1 < msg + len && *(pos + 1) == '&') {
        is_andand = 1;
      }

      // 临时结束字符串以分割命令
      *pos = '\0';

      // 复制命令到数组
      commands[cmd_count] = kvs_malloc(strlen(start) + 1);
      if (commands[cmd_count] == NULL) {
        // 释放已分配的内存
        for (int i = 0; i < cmd_count; i++) {
          kvs_free(commands[i]);
        }
        return -1;
      }
      strcpy(commands[cmd_count], start);

      // 恢复字符
      *pos = '&';

      // 跳过&符
      pos++;
      if (is_andand) {
        pos++; // 跳过第二个&
      }

      // 跳过可能的空格
      while (*pos == ' ' || *pos == '\t') pos++;
      start = pos;
      cmd_count++;
    } else {
      pos++;
    }
  }

  // 添加最后一个命令（或唯一命令）
  if (start < msg + len && cmd_count < max_commands) {
    commands[cmd_count] = kvs_malloc(strlen(start) + 1);
    if (commands[cmd_count] == NULL) {
      for (int i = 0; i < cmd_count; i++) {
        kvs_free(commands[i]);
      }
      return -1;
    }
    strcpy(commands[cmd_count], start);
    cmd_count++;
  }

  return cmd_count;
}

/*
 * 检查命令是否为写操作
 * @param command 命令名称
 * @return 1表示写操作，0表示非写操作
 */
int is_write_command(const char* command) {
    if (command == NULL) return 0;

    if (strcmp(command, "SET") == 0 ||
        strcmp(command, "RSET") == 0 ||
        strcmp(command, "HSET") == 0 ||
        strcmp(command, "MOD") == 0 ||
        strcmp(command, "RMOD") == 0 ||
        strcmp(command, "HMOD") == 0 ||
        strcmp(command, "DEL") == 0 ||
        strcmp(command, "RDEL") == 0 ||
        strcmp(command, "HDEL") == 0) {
        return 1;
    }
    return 0;
}

int kvs_filter_protocol(char** tokens, int count, char* response) {
  if (tokens[0] == NULL || count == 0 || response == NULL) return -1;
  int cmd = KVS_CMD_START;
  for (cmd = KVS_CMD_START; cmd < KVS_CMD_COUNT; cmd++) {
    if (strcmp(tokens[0], command[cmd]) == 0) {
      break;
    }
  }

  // SLAVE READ-ONLY CHECK
  if (!replication_info.is_master) {
      if (is_write_command(tokens[0])) {
          if (current_processing_fd != slave_info.master_fd) {
               sprintf(response, "READONLY You can't write against a read only replica\r\n");
               return strlen(response);
          }
      }
  }

  int length = 0;
  int ret = 0;
  char* key = tokens[1];
  char* value = tokens[2];

  switch (cmd) {
    case KVS_CMD_SET:  // --> OK
      ret = kvs_main_set(&global_main_engine, key, value);

      if (ret < 0) {
        length = sprintf(response, "ERROR\r\n");
      } else if (ret == 0) {
        // Only Master persists and propagates
        if (replication_info.is_master) {
            appendToAofBuffer(AOF_CMD_SET, key, value); // CMD_SET = 1
        }
        length = sprintf(response, "OK\r\n");
      } else {
        length = sprintf(response, "Key has existed\r\n");
      }
      break;
    case KVS_CMD_GET:  // --> Value
      char* gotValue = kvs_main_get(&global_main_engine, key);
      if (gotValue == NULL) {
        length = sprintf(response, "ERROR / Not Exist\r\n");
      } else {
        length = sprintf(response, "%s\r\n", gotValue);
      }
      break;
    case KVS_CMD_DEL:  // -> OK
      ret = kvs_main_del(&global_main_engine, key);
      if (ret < 0) {
        length = sprintf(response, "ERROR\r\n");
      } else if (ret == 0) {
        if (replication_info.is_master) {
            appendToAofBuffer(AOF_CMD_DEL, key, NULL); // AOF_CMD_DEL = 3
        }
        length = sprintf(response, "OK\r\n");
      } else {
        length = sprintf(response, "Not Exist\r\n");
      }
      break;
    case KVS_CMD_MOD:  // -> OK
      ret = kvs_main_mod(&global_main_engine, key, value);
      if (ret < 0) {
        length = sprintf(response, "ERROR\r\n");
      } else if (ret == 0) {
        if (replication_info.is_master) {
            appendToAofBuffer(AOF_CMD_MOD, key, value); // AOF_CMD_MOD = 2
        }
        length = sprintf(response, "OK\r\n");
      } else {
        length = sprintf(response, "Not Exist\r\n");
      }
      break;
    case KVS_CMD_EXIST:  // -> Yes / No
      ret = kvs_main_exist(&global_main_engine, key);
      if (ret > 0) {
        length = sprintf(response, "YES, Exist\r\n");
      } else if (ret == 0) {
        length = sprintf(response, "NO, Not Exist\r\n");
      } else {
        length = sprintf(response, "ERROR\r\n");
      }
      break;

    case KVS_CMD_SAVE:
        // 同步保存快照
        ksfSave(snap_filename);
        length = sprintf(response, "OK\r\n");
        break;
    case KVS_CMD_BGSAVE:
        // 异步保存快照
        ksfSaveBackground();
        length = sprintf(response, "Background saving started\r\n");
        break;
    case KVS_CMD_SYNC:
        if (handle_sync_command(current_processing_fd) == 0) {
            return 0; // Response sent by handler
        } else {
            length = sprintf(response, "ERROR SYNC\r\n");
        }
        break;
    default:
      length = sprintf(response, "UNKNOWN COMMAND\r\n");
  }

  return length;
}

/*
 * 分析命令分隔符的类型
 * 返回 0 表示 & (并行执行)
 * 返回 1 表示 && (顺序执行，前一个成功才执行下一个)
 */
int get_command_separator_type(char* msg, int position, int msg_len) {
  if (position >= msg_len - 1) {
    return 0; // 默认为并行执行
  }

  if (msg[position] == '&' && msg[position + 1] == '&') {
    return 1; // 顺序执行
  } else {
    return 0; // 并行执行
  }
}

/*
 * 处理多命令
 * 模拟shell的&和&&操作符行为
 */
int kvs_process_multicmd(char* msg, int length, char* response) {
  if (msg == NULL || response == NULL) return -1;

  // 分割多命令
  char* commands[64]; // 最多支持64个子命令
  int cmd_count = kvs_split_multicmd(msg, commands, 64);

  if (cmd_count <= 0) {
    return sprintf(response, "ERROR: Invalid multi-command format\r\n");
  }

  int total_len = 0;
  int msg_len = strlen(msg);
  int current_pos = 0;

  // 找到第一个命令的位置
  while (current_pos < msg_len && msg[current_pos] != '\0' &&
         (msg[current_pos] == ' ' || msg[current_pos] == '\t')) {
    current_pos++;
  }

  // 遍历处理每个命令
  for (int i = 0; i < cmd_count; i++) {
    // 计算当前命令在原始消息中的位置
    current_pos += strlen(commands[i]);

    // 跳过可能的空格
    while (current_pos < msg_len &&
           (msg[current_pos] == ' ' || msg[current_pos] == '\t')) {
      current_pos++;
    }

    // 检查是否有分隔符
    int separator_type = (current_pos < msg_len - 1) ?
                         get_command_separator_type(msg, current_pos, msg_len) : 0;

    // 跳过分隔符
    if (current_pos < msg_len && msg[current_pos] == '&') {
      current_pos++;
      if (separator_type == 1) { // 如果是&&，还要跳过第二个&
        current_pos++;
      }
      // 跳过可能的空格
      while (current_pos < msg_len &&
             (msg[current_pos] == ' ' || msg[current_pos] == '\t')) {
        current_pos++;
      }
    }

    // 处理命令
    char temp_buffer[BUFFER_SIZE];
    strcpy(temp_buffer, commands[i]);

    // 分割token
    char* tokens[KVS_MAX_TOKENS] = {0};
    int count = kvs_split_token(temp_buffer, tokens);
    if (count == -1) {
      total_len += sprintf(response + total_len, "ERROR: Token parsing failed for command: %s\r\n", commands[i]);
      continue;
    }

    // 处理命令
    char single_response[BUFFER_SIZE];
    int ret = kvs_filter_protocol(tokens, count, single_response);
    if (ret < 0) {
      total_len += sprintf(response + total_len, "ERROR: Command execution failed: %s\r\n", commands[i]);
    } else {
      total_len += sprintf(response + total_len, "%s", single_response);
    }

    // 检查是否为写操作，如果是则增加计数
    if (count > 0 && tokens[0] != NULL) {
        if (is_write_command(tokens[0])) {
            changes_since_last_save++;
        }
    }

    // 如果是&&操作符，且上一个命令失败，则跳过后续命令
    if (separator_type == 1) { // 是&&操作符
      // 检查响应是否表示错误
      if (strncmp(single_response, "ERROR", 5) == 0 ||
          strcmp(single_response, "Not Exist\r\n") == 0 ||
          strcmp(single_response, "Key has existed\r\n") == 0) {
        // 如果前一个命令失败，跳过后续所有命令
        break;
      }
    }
  }

  // 释放分配的内存
  for (int i = 0; i < cmd_count; i++) {
    if (commands[i] != NULL) {
      kvs_free(commands[i]);
    }
  }

  return total_len;
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
        changes_since_last_save = 0; // 重置变化计数

        // 执行后台保存
        ksfSaveBackground();
    }
}

int kvs_protocol(char* rmsg, int length, char* response) {
  pthread_mutex_lock(&global_kvs_lock); // LOCK

  if (rmsg == NULL || length == 0 || response == NULL) {
      pthread_mutex_unlock(&global_kvs_lock);
      return -1;
  }

  if (length < MAX_MULTICMD_LENGTH) {
    rmsg[length] = '\0';
  } else {
    rmsg[MAX_MULTICMD_LENGTH - 1] = '\0';
  }

#if ECHO_LOGIC  // echo
  memcpy(response, rmsg, length);
  pthread_mutex_unlock(&global_kvs_lock);
  return strlen(response);
#elif KV_LOGIC  // 真正的 KV 存储业务逻辑
  if (strchr(rmsg, '&') != NULL) {
    int ret = kvs_process_multicmd(rmsg, length, response);
    check_and_perform_autosave();
    pthread_mutex_unlock(&global_kvs_lock);
    return ret;
  } else {
    char* tokens[KVS_MAX_TOKENS] = {0};
    int count = kvs_split_token(rmsg, tokens);
    if (count == -1) {
        pthread_mutex_unlock(&global_kvs_lock);
        return -2;
    }

    int ret = kvs_filter_protocol(tokens, count, response);

    if (count > 0 && tokens[0] != NULL) {
        if (is_write_command(tokens[0])) {
            changes_since_last_save++;
        }
    }

    check_and_perform_autosave();
    pthread_mutex_unlock(&global_kvs_lock); // UNLOCK
    return ret;
  }
#endif
}


int init_kvengine(void) {
  // 初始化内存池，针对KV存储的典型数据大小进行优化
  g_mem_pool = mem_pool_init(BLOCK_SIZE);

  memset(&global_main_engine, 0, sizeof(global_main_engine));
  kvs_main_create(&global_main_engine);
  return 0;
}

void dest_kvengine(void) {
  if (replication_info.is_master) ksfSave(snap_filename);
  kvs_main_destroy(&global_main_engine);

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
  char *master_ip = NULL;
  int master_port = 0;

  // Arg parsing
  for (int i=2; i<argc; i++) {
      if (strcmp(argv[i], "--slaveof") == 0) {
          if (i+2 < argc) {
              master_ip = argv[i+1];
              master_port = atoi(argv[i+2]);
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
      printf("Starting as SLAVE. Syncing with Master %s:%d...\n", master_ip, master_port);
      if (init_slave_replication(master_ip, master_port) != 0) {
          fprintf(stderr, "Failed to sync with master. Exiting.\n");
          return -1;
      }
  } else {
      // MASTER MODE
      if (load_mode == INIT_LOAD_AOF) {
        aofLoad(aof_filename);
      } else if (load_mode == INIT_LOAD_SNAP) {
        ksfLoad(snap_filename);
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