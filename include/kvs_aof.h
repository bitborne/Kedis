#ifndef __KVS_AOF_H__
#define __KVS_AOF_H__

#include "kvstore.h"

#define ENABLE_AOF 0

#define AOF_ENGINE_TYPE_ARRAY 0
#define AOF_ENGINE_TYPE_HASH 1
#define AOF_ENGINE_TYPE_RBTREE 2
#define AOF_ENGINE_TYPE_SKIPLIST 3


// 持久化配置
#define INCREMENTAL_PERSISTENCE 1
#define AOF_BUF_SIZE (8*1024*1024)  // 8MB AOF缓冲区
#define AOF_SYNC_INTERVAL 1       // AOF同步间隔（秒）

// AOF命令码定义
#define AOF_CMD_SET 1
#define AOF_CMD_MOD 2
#define AOF_CMD_DEL 3

// 阈值：超过此大小的命令将绕过缓冲区直接写入
#define LARGE_CMD_THRESHOLD (AOF_BUF_SIZE / 2)

typedef struct {
  char buf[AOF_BUF_SIZE];
  int len;
} aof_buf_t;

// AOF相关函数声明
void appendToAofBuffer(int type, const robj* key, const robj* value);  // 添加到AOF缓冲区
int flushAofBuffer(void);  // 刷新AOF缓冲区
int start_aof_fsync_process(void);  // 启动AOF同步线程
void stop_aof_fsync_process(void);   // 停止AOF同步线程
void before_sleep(void);  // 事件循环前的处理函数
int aofLoad(const char* filename);  // 加载AOF文件

// 多引擎模式下的AOF函数
#if ENABLE_MULTI_ENGINE
int aofLoadAll(void);  // 加载所有引擎的AOF文件
void appendToAofBufferToEngine(int engine_type, int type, const robj* key, const robj* value);  // 向指定引擎的AOF文件写入命令
#endif

// mmap 优化版本
int aofLoadAll_mmap(void);  // 使用 mmap 加载所有 AOF 文件

#endif // __KVS_AOF_H__
