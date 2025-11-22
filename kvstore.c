#include "kvstore.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <stdbool.h>

#if (NETWORK_SELECT == NETWORK_REACTOR)
#include "server.h"  // only for reactor.c
#endif



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
// 不直接使用系统调用(第三方接口)
// 跨平台的时候，只需要修改这个函数即可--> 可迭代
void* kvs_calloc(size_t num, size_t size) { return calloc(num, size); }

void* kvs_malloc(size_t size) { return malloc(size); }

void kvs_free(void* ptr) { free(ptr); }
// 定义了头文件中 command 变量的声明
const char* command[] = {"SET",  "GET",  "DEL",  "MOD",  "EXIST",
                         "RSET", "RGET", "RDEL", "RMOD", "REXIST",
                         "HSET", "HGET", "HDEL", "HMOD", "HEXIST"};

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

  KVS_CMD_COUNT,
};

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
    printf("idx: %d -> %s\n", idx - 1, token); // DEBUG
    token = strtok(NULL, " ");
  }
  for (int i = 0; i < 3; i++) {
    printf("tokens[%d] =  %s", i , tokens[i]);
  }
  return idx;  // 每拆出来一个token，idx++ ==> 返回值为token的个数
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
  printf("keyOUT = %s\n", key);

  switch (cmd) {
#if ENABLE_ARRAY
    case KVS_CMD_SET:  // --> OK
      ret = kvs_array_set(&global_array, key, value);
      printf("inSET key = %s\n", key);

      if (ret < 0) {
        length = sprintf(response, "ERROR\r\n");
      } else if (ret == 0) {
        // 记录SET操作到日志
        kvs_persist_log_operation("SET", key, value);
        length = sprintf(response, "OK\r\n");
      } else {
        length = sprintf(response, "Key has existed\r\n");
      }
      break;
    case KVS_CMD_GET:  // --> Value
    printf("keyIN = %s\n", key);
      char* gotValue = kvs_array_get(&global_array, key);
      if (gotValue == NULL) {
        length = sprintf(response, "ERROR / Not Exist\r\n");
      } else {
        length = sprintf(response, "Value: %s\r\n", gotValue);
      }
      break;
    case KVS_CMD_DEL:  // -> OK
      ret = kvs_array_del(&global_array, key);
      if (ret < 0) {
        length = sprintf(response, "ERROR\r\n");
      } else if (ret == 0) {
        // 记录DEL操作到日志
        kvs_persist_log_operation("DEL", key, NULL);
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
        // 记录MOD操作到日志
        kvs_persist_log_operation("MOD", key, value);
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
          kvs_persist_log_operation("RSET", key, value);
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
          length = sprintf(response, "Value: %s\r\n", rgotValue);
        }
        break;
        case KVS_CMD_RDEL:  // -> OK
        ret = kvs_rbtree_del(&global_rbtree , key);
        if (ret < 0) {
          length = sprintf(response, "ERROR\r\n");
        } else if (ret == 0) {
          kvs_persist_log_operation("RDEL", key, NULL);
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
          kvs_persist_log_operation("RMOD", key, value);
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
        kvs_persist_log_operation("HSET", key, value);
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
        length = sprintf(response, "Value: %s\r\n", hgotValue);
      }
      break;
      case KVS_CMD_HDEL:  // -> OK
      ret = kvs_hash_del(&global_hash , key);
      if (ret < 0) {
        length = sprintf(response, "ERROR\r\n");
      } else if (ret == 0) {
        kvs_persist_log_operation("HDEL", key, NULL);
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
        kvs_persist_log_operation("HMOD", key, value);
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

    default:
      assert(0);
  }

  return length;
}

/*
 * msg: request message
 * length: length of request message
 * response: need to send
 * @return : length of response
 */

 int kvs_protocol(char* rmsg, int length, char* response) {
  if (rmsg == NULL || length == 0 || response == NULL) return -1;

  // Ensure the message is null-terminated for string operations
  if (length < BUFFER_SIZE) {
    rmsg[length] = '\0';
  } else {
    // If buffer is full, ensure last byte is null terminator
    rmsg[BUFFER_SIZE - 1] = '\0';
  }

  // printf("recv: %dbytes:\n%s\n", strlen(rmsg), rmsg);

#if ECHO_LOGIC  // echo
  printf("copy from rmsg: %s\n", rmsg);
  memcpy(response, rmsg, length);
  return strlen(response);
#elif KV_LOGIC  // 真正的 KV 存储业务逻辑
  char* tokens[KVS_MAX_TOKENS] = {0};

  // 分割token
  int count = kvs_split_token(rmsg, tokens);
  if (count == -1) return -2;

  //  *协议分类器* : 解析 rmsg
  int ret = kvs_filter_protocol(tokens, count, response);
  // 返回值相当于 strlen(response)
  return ret;
#endif
}


// 全局变量存储持久化模式
static int g_persist_mode = PERSIST_MODE_INCREMENTAL;

int init_kvengine(void) {
  // 首先初始化持久化功能
  kvs_persist_init(g_persist_mode);

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
  if (g_persist_mode == PERSIST_MODE_INCREMENTAL) {
    kvs_persist_replay_log();
  } else if (g_persist_mode == PERSIST_MODE_SNAPSHOT) {
    kvs_snapshot_load();
  }

  return 0;
}

void dest_kvengine(void) {
  // 根据模式保存数据
  if (g_persist_mode == PERSIST_MODE_SNAPSHOT) {
    kvs_snapshot_save();
  }

#if ENABLE_ARRAY
  kvs_array_destroy(&global_array);
#endif
#if ENABLE_RBTREE
  kvs_rbtree_destroy(&global_rbtree);
#endif
#if ENABLE_HASH
  kvs_hash_destroy(&global_hash);
#endif

  // 关闭持久化功能
  kvs_persist_close();
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

  // 解析持久化模式参数
  if (argc == 3) {
    if (strcmp(argv[2], "inc") == 0) {
      g_persist_mode = PERSIST_MODE_INCREMENTAL;
    } else if (strcmp(argv[2], "snap") == 0) {
      g_persist_mode = PERSIST_MODE_SNAPSHOT;
    } else {
      printf("错误：未知的持久化模式 '%s'\n", argv[2]);
      printf("用法: %s <port> [inc|snap]\n", argv[0]);
      return -1;
    }
  } else {
    // 默认使用增量模式
    g_persist_mode = PERSIST_MODE_INCREMENTAL;
  }

  // 注册信号处理器，捕获SIGINT (Ctrl+C) 和 SIGTERM
  signal(SIGINT, signal_handler);
  signal(SIGTERM, signal_handler);

  // 注册退出函数，确保在程序正常退出时保存数据（仅在非信号退出时使用）
  if (atexit(dest_kvengine) != 0) {
      printf("警告：无法注册退出函数\n");
  }

  init_kvengine();

#if (NETWORK_SELECT == NETWORK_REACTOR)
  reactor_start(port, kvs_protocol);  //
#elif (NETWORK_SELECT == NETWORK_PROACTOR)
  proactor_start(port, kvs_protocol);
#elif (NETWORK_SELECT == NETWORK_NTYCO)
  ntyco_start(port, kvs_protocol);
#endif

  dest_kvengine();
}
