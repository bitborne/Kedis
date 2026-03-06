#ifndef MIRROR_LOG_H
#define MIRROR_LOG_H

#include <stdio.h>
#include <stdarg.h>
#include <unistd.h>

/* 日志级别 */
#define MIRROR_LOG_LEVEL 2  // [修改] 默认 WARNING 级别，减少日志阻塞

#define MIRROR_DEBUG    0
#define MIRROR_INFO     1
#define MIRROR_WARNING  2
#define MIRROR_ERROR    3

#define LOG_TO_STDOUT 0
#define LOG_FILE "mirror.log"

/* 终端打印信息颜色 */
#define C_DEBUG   "\033[36m"
#define C_INFO    "\033[32m"
#define C_WARN    "\033[33m"
#define C_ERROR   "\033[31m"
#define C_RESET   "\033[0m"

/* 实际输出函数（类似 _serverLog） */
void mirror_log_raw(int level, const char *msg);

/* 可变参数处理函数 */
void mirror_log(int level, const char *fmt, ...);

/* 带级别控制的宏（核心） */
#define mirror_serverLog(level, ...) do { \
        if ((level) < MIRROR_LOG_LEVEL) break; \
        mirror_log(level, __VA_ARGS__); \
    } while(0)

/* 快速调试宏，自动带位置 */
#define debug(fmt, ...) \
    printf(C_DEBUG "[DEBUG] %s:%d (%s) " fmt C_RESET "\n", \
        __FILE__, __LINE__, __func__, ##__VA_ARGS__)

#define mark() \
    printf(C_DEBUG "[MARK] %s:%d (%s)" C_RESET "\n", \
        __FILE__, __LINE__, __func__)

/* 带级别的便捷宏 */
#define mirror_logDebug(...)  mirror_serverLog(MIRROR_DEBUG, __VA_ARGS__)
#define mirror_logInfo(...)   mirror_serverLog(MIRROR_INFO, __VA_ARGS__)
#define mirror_logWarn(...)   mirror_serverLog(MIRROR_WARNING, __VA_ARGS__)
#define mirror_logError(...)  mirror_serverLog(MIRROR_ERROR, __VA_ARGS__)

/* 双层宏字符串化 */
#define MACRO_NAME(x) #x
#define STRINGIFY(x) MACRO_NAME(x)

#endif