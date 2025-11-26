#include "kvstore.h"

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

#if (NETWORK_SELECT == NETWORK_REACTOR)
#include "server.h"  // only for reactor.c
#endif

enum {
  KVS_CMD_START = 0,
  // array
  KVS_CMD_SET = KVS_CMD_START,
  KVS_CMD_GET,
  KVS_CMD_DEL,
  KVS_CMD_MOD,
  KVS_CMD_EXIST,
  // rbtree
  KVS_CMD_RSET,
  KVS_CMD_RGET,
  KVS_CMD_RDEL,
  KVS_CMD_RMOD,
  KVS_CMD_REXIST,
  // hash
  KVS_CMD_HSET,
  KVS_CMD_HGET,
  KVS_CMD_HDEL,
  KVS_CMD_HMOD,
  KVS_CMD_HEXIST,

  KVS_CMD_SAVE,
  KVS_CMD_BGSAVE,

  KVS_CMD_COUNT
};

#define ECHO_LOGIC 0
#define KV_LOGIC 1

#include "kvstore.h"

#if ENABLE_ARRAY
extern kvs_array_t global_array;
#endif

#if ENABLE_RBTREE
extern kvs_rbtree_t global_rbtree;
#endif

#if ENABLE_HASH
extern kvs_hash_t global_hash;
#endif

// AOF缓冲区和长度
char aof_buf[AOF_BUF_SIZE] = {0};
int aof_len = 0;

// 不直接使用系统调用(第三方接口)
// 跨平台的时候，只需要修改这个函数即可--> 可迭代
void* kvs_calloc(size_t num, size_t size) { return calloc(num, size); }

void* kvs_malloc(size_t size) { return malloc(size); }

void kvs_free(void* ptr) { free(ptr); }
// 定义了头文件中 command 变量的声明
const char* command[] = {"SET",  "GET",  "DEL",  "MOD",  "EXIST",
                         "RSET", "RGET", "RDEL", "RMOD", "REXIST",
                         "HSET", "HGET", "HDEL", "HMOD", "HEXIST",
                         "SAVE", "BGSAVE"};  // 添加SAVE和BGSAVE命令

// 全局变量存储持久化模式和自动保存参数
// static int g_persist_mode = PERSIST_MODE_INCREMENTAL;

// 自动保存参数：save seconds changes
static int save_params_seconds = 300;      // 5分钟
static int save_params_changes = 100;      // 100次变化
static time_t last_save_time = 0;          // 上次保存时间
static int changes_since_last_save = 0;    // 自上次保存以来的变化次数

/*
 * 解析单条命令的token
 */
int kvs_split_token(char* msg, char* tokens[]) {
  if (msg == NULL || tokens == NULL) return -1;

  // 处理行尾换行符 --> 否则导致 strcmp 失效 --> kvs_array_get/del
  // 等`token只有两个`且`key尾带\n` 的操作失效
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
  // 将tokens[i]指针 赋值为上面的首地址
  while (token != NULL) {
    tokens[idx++] = token;
    // printf("idx: %d -> %s\n", idx - 1, token); // DEBUG
    token = strtok(NULL, " ");
  }
  return idx;  // 每拆出来一个token，idx++ ==> 返回值为token的个数
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
      // 释放已分配的内存
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

// SET Key Value
// tokens[0] : SET
// tokens[1] : Key
// tokens[2] : Value

int kvs_filter_protocol(char** tokens, int count, char* response) {
  if (tokens[0] == NULL || count == 0 || response == NULL) return -1;
  int cmd = KVS_CMD_START;
  for (cmd = KVS_CMD_START; cmd < KVS_CMD_COUNT; cmd++) {
    if (strcmp(tokens[0], command[cmd]) == 0) {
      break;
    }
  }

  int length = 0;
  int ret = 0;
  char* key = tokens[1];
  char* value = tokens[2];

  switch (cmd) {
#if ENABLE_ARRAY
    case KVS_CMD_SET:  // --> OK
      ret = kvs_array_set(&global_array, key, value);

      if (ret < 0) {
        length = sprintf(response, "ERROR\r\n");
      } else if (ret == 0) {
        // 记录SET操作到AOF缓冲区
        appendToAofBuffer(AOF_CMD_SET, key, value); // CMD_SET = 1
        length = sprintf(response, "OK\r\n");
      } else {
        length = sprintf(response, "Key has existed\r\n");
      }
      break;
    case KVS_CMD_GET:  // --> Value
      char* gotValue = kvs_array_get(&global_array, key);
      if (gotValue == NULL) {
        length = sprintf(response, "ERROR / Not Exist\r\n");
      } else {
        length = sprintf(response, "%s\r\n", gotValue);
      }
      break;
    case KVS_CMD_DEL:  // -> OK
      ret = kvs_array_del(&global_array, key);
      if (ret < 0) {
        length = sprintf(response, "ERROR\r\n");
      } else if (ret == 0) {
        // 记录DEL操作到AOF缓冲区
        appendToAofBuffer(AOF_CMD_DEL, key, NULL); // AOF_CMD_DEL = 3
        length = sprintf(response, "OK\r\n");
      } else {
        length = sprintf(response, "Not Exist\r\n");
      }
      break;
    case KVS_CMD_MOD:  // -> OK
      ret = kvs_array_mod(&global_array, key, value);
      if (ret < 0) {
        length = sprintf(response, "ERROR\r\n");
      } else if (ret == 0) {
        // 记录MOD操作到AOF缓冲区
        appendToAofBuffer(AOF_CMD_MOD, key, value); // AOF_CMD_MOD = 2
        length = sprintf(response, "OK\r\n");
      } else {
        length = sprintf(response, "Not Exist\r\n");
      }
      break;
    case KVS_CMD_EXIST:  // -> Yes / No
      ret = kvs_array_exist(&global_array, key);
      if (ret > 0) {
        length = sprintf(response, "YES, Exist\r\n");
      } else if (ret == 0) {
        length = sprintf(response, "NO, Not Exist\r\n");
      } else {
        length = sprintf(response, "ERROR\r\n");
      }
      break;
#endif
      // rbtree
      #if ENABLE_RBTREE
      case KVS_CMD_RSET:  // --> OK
        ret = kvs_rbtree_set(&global_rbtree , key, value);
        if (ret < 0) {
          length = sprintf(response, "ERROR\r\n");
        } else if (ret == 0) {
          appendToAofBuffer(AOF_CMD_SET, key, value); // CMD_SET = 1
          length = sprintf(response, "OK\r\n");
        } else {
          length = sprintf(response, "Key has existed\r\n");
        }
        break;
        case KVS_CMD_RGET:  // --> Value
        char* rgotValue = kvs_rbtree_get(&global_rbtree , key);
        if (rgotValue == NULL) {
          length = sprintf(response, "ERROR / Not Exist\r\n");
        } else {
          length = sprintf(response, "%s\r\n", rgotValue);
        }
        break;
        case KVS_CMD_RDEL:  // -> OK
        ret = kvs_rbtree_del(&global_rbtree , key);
        if (ret < 0) {
          length = sprintf(response, "ERROR\r\n");
        } else if (ret == 0) {
          appendToAofBuffer(AOF_CMD_DEL, key, NULL); // CMD_DEL = 3
          length = sprintf(response, "OK\r\n");
        } else {
          length = sprintf(response, "Not Exist\r\n");
        }
        break;
        case KVS_CMD_RMOD:  // -> OK
        ret = kvs_rbtree_mod(&global_rbtree , key, value);
        if (ret < 0) {
          length = sprintf(response, "ERROR\r\n");
        } else if (ret == 0) {
          appendToAofBuffer(AOF_CMD_MOD, key, value); // CMD_MOD = 2
          length = sprintf(response, "OK\r\n");
        } else {
          length = sprintf(response, "Not Exist\r\n");
        }
        break;
      case KVS_CMD_REXIST:  // -> Yes / No
        ret = kvs_rbtree_exist(&global_rbtree , key);
        if (ret > 0) {
          length = sprintf(response, "YES, Exist\r\n");
        } else if (ret == 0) {
          length = sprintf(response, "NO, Not Exist\r\n");
        } else {
          length = sprintf(response, "ERROR\r\n");
        }
        break;
        #endif
    // hash
    #if ENABLE_HASH
    case KVS_CMD_HSET:  // --> OK
      ret = kvs_hash_set(&global_hash , key, value);
      if (ret < 0) {
        length = sprintf(response, "ERROR\r\n");
      } else if (ret == 0) {
        appendToAofBuffer(AOF_CMD_SET, key, value); // CMD_SET = 1
        length = sprintf(response, "OK\r\n");
      } else {
        length = sprintf(response, "Key has existed\r\n");
      }
      break;
      case KVS_CMD_HGET:  // --> Value
      char* hgotValue = kvs_hash_get(&global_hash , key);
      if (hgotValue == NULL) {
        length = sprintf(response, "ERROR / Not Exist\r\n");
      } else {
        length = sprintf(response, "%s\r\n", hgotValue);
      }
      break;
      case KVS_CMD_HDEL:  // -> OK
      ret = kvs_hash_del(&global_hash , key);
      if (ret < 0) {
        length = sprintf(response, "ERROR\r\n");
      } else if (ret == 0) {
        appendToAofBuffer(AOF_CMD_DEL, key, NULL); // CMD_DEL = 3
        length = sprintf(response, "OK\r\n");
      } else {
        length = sprintf(response, "Not Exist\r\n");
      }
      break;
      case KVS_CMD_HMOD:  // -> OK
      ret = kvs_hash_mod(&global_hash, key, value);
      if (ret < 0) {
        length = sprintf(response, "ERROR\r\n");
      } else if (ret == 0) {
        appendToAofBuffer(AOF_CMD_MOD, key, value); // CMD_MOD = 2
        length = sprintf(response, "OK\r\n");
      } else {
        length = sprintf(response, "Not Exist\r\n");
      }
      break;
    case KVS_CMD_HEXIST:  // -> Yes / No
      ret = kvs_hash_exist(&global_hash , key);
      if (ret > 0) {
        length = sprintf(response, "YES, Exist\r\n");
      } else if (ret == 0) {
        length = sprintf(response, "NO, Not Exist\r\n");
      } else {
        length = sprintf(response, "ERROR\r\n");
      }
      break;

#endif

    // 添加SAVE和BGSAVE命令处理
    case KVS_CMD_SAVE:
        // 同步保存快照
        ksfSave("dump.ksf");
        length = sprintf(response, "OK\r\n");
        break;
    case KVS_CMD_BGSAVE:
        // 异步保存快照
        ksfSaveBackground();
        length = sprintf(response, "Background saving started\r\n");
        break;

    default:
      assert(0);
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

/*
 * msg: request message
 * length: length of request message
 * response: need to send
 * @return : length of response
 */

int kvs_protocol(char* rmsg, int length, char* response) {
  if (rmsg == NULL || length == 0 || response == NULL) return -1;

  // 确保消息以null结尾以便于字符串操作
  if (length < MAX_MULTICMD_LENGTH) {
    rmsg[length] = '\0';
  } else {
    // 如果缓冲区满，确保最后字节是null终止符
    rmsg[MAX_MULTICMD_LENGTH - 1] = '\0';
  }

  // printf("recv: %dbytes:\n%s\n", strlen(rmsg), rmsg);

#if ECHO_LOGIC  // echo
  printf("copy from rmsg: %s\n", rmsg);
  memcpy(response, rmsg, length);
  return strlen(response);
#elif KV_LOGIC  // 真正的 KV 存储业务逻辑
  // 检查是否包含多命令操作符
  if (strchr(rmsg, '&') != NULL) {
    // 如果存在&操作符，处理多命令
    int ret = kvs_process_multicmd(rmsg, length, response);

    // 在多命令处理中，如果有写操作，也需要检查是否需要自动保存
    check_and_perform_autosave();
    return ret;
  } else {
    // 单命令处理
    char* tokens[KVS_MAX_TOKENS] = {0};

    // 分割token
    int count = kvs_split_token(rmsg, tokens);
    if (count == -1) return -2;

    //  *协议分类器* : 解析 rmsg
    int ret = kvs_filter_protocol(tokens, count, response);
    // 返回值相当于 strlen(response)

    // 检查是否为写操作（SET、MOD、DEL等），如果是则增加计数
    if (count > 0 && tokens[0] != NULL) {
        if (is_write_command(tokens[0])) {
            changes_since_last_save++;
        }
    }

    // 检查是否需要自动保存
    check_and_perform_autosave();

    return ret;
  }
#endif
}


int init_kvengine(void) {
  // // 首先初始化持久化功能
  // kvs_persist_init(g_persist_mode);

#if ENABLE_ARRAY
  memset(&global_array, 0, sizeof(kvs_array_t));
  kvs_array_create(&global_array);
#endif

#if ENABLE_RBTREE
  memset(&global_rbtree, 0, sizeof(kvs_rbtree_t));
  kvs_rbtree_create(&global_rbtree);
#endif

#if ENABLE_HASH
  memset(&global_hash, 0, sizeof(kvs_hash_t));
  kvs_hash_create(&global_hash);
#endif

  // 在数据结构初始化完成之后，根据模式加载数据
  // if (g_persist_mode == PERSIST_MODE_INCREMENTAL) {
  //   kvs_persist_replay_log();
  // } else if (g_persist_mode == PERSIST_MODE_SNAPSHOT) {
  //   kvs_snapshot_load();
  // }

  return 0;
}

void dest_kvengine(void) {
  // 根据模式保存数据
  // if (g_persist_mode == PERSIST_MODE_SNAPSHOT) {
  //   kvs_snapshot_save();
  // }

  // 在程序退出前保存快照
  ksfSave("dump.ksf");

#if ENABLE_ARRAY
  kvs_array_destroy(&global_array);
#endif
#if ENABLE_RBTREE
  kvs_rbtree_destroy(&global_rbtree);
#endif
#if ENABLE_HASH
  kvs_hash_destroy(&global_hash);
#endif

  // // 关闭持久化功能
  // kvs_persist_close();
}


// 信号处理函数
void signal_handler(int sig) {
    printf("\n接收到信号 %d，准备关闭服务...\n", sig);
    // 不直接调用dest_kvengine，而是正常退出，让atexit处理清理
    exit(0);  // 正常退出，会调用atexit注册的函数
}

int main(int argc, char* argv[]) {
  if (argc < 2 || argc > 3) return -1;

  int port = atoi(argv[1]);

  // // 解析持久化模式参数
  // if (argc == 3) {
  //   if (strcmp(argv[2], "log") == 0) {
  //     g_persist_mode = PERSIST_MODE_INCREMENTAL;
  //   } else if (strcmp(argv[2], "snap") == 0) {
  //     g_persist_mode = PERSIST_MODE_SNAPSHOT;
  //   } else {
  //     printf("错误：未知的持久化模式 '%s'\n", argv[2]);
  //     printf("用法: %s <port> [log|snap]\n", argv[0]);
  //     return -1;
  //   }
  // } else {
  //   // 默认使用增量模式
  //   g_persist_mode = PERSIST_MODE_INCREMENTAL;
  // }

  // 注册信号处理器，捕获SIGINT (Ctrl+C) 和 SIGTERM
  signal(SIGINT, signal_handler);
  signal(SIGTERM, signal_handler);

  // 注册退出函数，确保在程序正常退出时保存数据（仅在非信号退出时使用）
  if (atexit(dest_kvengine) != 0) {
      printf("警告：无法注册退出函数\n");
  }

  init_kvengine();

  // 启动AOF同步后台线程
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
