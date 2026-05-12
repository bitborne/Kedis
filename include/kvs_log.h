#ifndef KVS_LOG_H
#define KVS_LOG_H

#include <stdio.h>
#include <stdarg.h>
#include <unistd.h>

#include "config.h"

/* 日志级别 */
#define KVS_DEBUG    0
#define KVS_INFO     1
#define KVS_WARNING  2
#define KVS_ERROR    3

/* 终端打印信息颜色 */
#define C_DEBUG   "\033[36m"
#define C_INFO    "\033[32m"
#define C_WARN    "\033[33m"
#define C_ERROR   "\033[31m"
#define C_RESET   "\033[0m"

/* 全局配置（放在你的 server 结构体里） */
extern kv_config g_config;

/* 实际输出函数（类似 _serverLog） */
void kvs_log_raw(int level, const char *msg);

/* 可变参数处理函数 */
void kvs_log(int level, const char *fmt, ...);

/* 带级别控制的宏（核心） */
#define kvs_serverLog(level, ...) do { \
        if ((level) < g_config.log_level) break; \
        kvs_log(level, __VA_ARGS__); \
    } while(0)

/* 快速调试宏，自动带位置 */
#ifdef KVS_NODEBUG
#define debug(fmt, ...) ((void)0)
#else
#define debug(fmt, ...) \
    printf(C_DEBUG "[DEBUG] %s:%d (%s) " fmt C_RESET "\n", \
        __FILE__, __LINE__, __func__, ##__VA_ARGS__)
#endif

#define mark() \
    printf(C_DEBUG "[MARK] %s:%d (%s)" C_RESET "\n", \
        __FILE__, __LINE__, __func__)

/* 带级别的便捷宏 */
#define kvs_logDebug(...)  kvs_serverLog(KVS_DEBUG, __VA_ARGS__)
#define kvs_logInfo(...)   kvs_serverLog(KVS_INFO, __VA_ARGS__)
#define kvs_logWarn(...)   kvs_serverLog(KVS_WARNING, __VA_ARGS__)
#define kvs_logError(...)  kvs_serverLog(KVS_ERROR, __VA_ARGS__)

/* 双层宏字符串化 */
#define MACRO_NAME(x) #x
#define STRINGIFY(x) MACRO_NAME(x)

#endif