#ifndef __KVS_AOF_H__
#define __KVS_AOF_H__

#include "kvs_constants.h"

// AOF相关函数声明
void appendToAofBuffer(int type, const char* key, const char* value);  // 添加到AOF缓冲区
int flushAofBuffer(void);  // 刷新AOF缓冲区
int start_aof_fsync_process(void);  // 启动AOF同步线程
void before_sleep(void);  // 事件循环前的处理函数
int aofLoad(const char* filename);  // 加载AOF文件

// 多引擎模式下的AOF函数
#if ENABLE_MULTI_ENGINE
int aofLoadAll(void);  // 加载所有引擎的AOF文件
void appendToAofBufferToEngine(int engine_type, int type, const char* key, const char* value);  // 向指定引擎的AOF文件写入命令
#endif

// mmap 优化版本
int aofLoadAll_mmap(void);  // 使用 mmap 加载所有 AOF 文件

// mmap 持久化落盘的核心函数 (也在AOF中使用)
int mmap_append(int fd, const char* filename, char *data, size_t n);

#endif // __KVS_AOF_H__
