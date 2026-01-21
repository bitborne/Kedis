#ifndef __KVS_CONSTANTS_H__
#define __KVS_CONSTANTS_H__

// 网络模型定义
#define NETWORK_REACTOR 	0
#define NETWORK_PROACTOR	1
#define NETWORK_NTYCO		2

// 当前选择的网络模型
#define NETWORK_SELECT		NETWORK_PROACTOR

// Token 解析配置
#define KVS_MAX_TOKENS		128

// 多引擎模式开关：0=单引擎模式（按优先级选择），1=多引擎模式（同时启用所有引擎）
#define ENABLE_MULTI_ENGINE	1

// 引擎启用开关
#define ENABLE_ARRAY		1
#define ENABLE_RBTREE		1
#define ENABLE_HASH			1
#define ENABLE_SKIPLIST		1

// 缓冲区配置
#define BUFFER_SIZE 1024
#define MAX_MULTICMD_LENGTH 8192

// 持久化配置
#define INCREMENTAL_PERSISTENCE 1
#define AOF_BUF_SIZE (1024*1024)  // 1MB AOF缓冲区
#define AOF_SYNC_INTERVAL 1       // AOF同步间隔（秒）

// AOF命令码定义
#define AOF_CMD_SET 1
#define AOF_CMD_MOD 2
#define AOF_CMD_DEL 3

// 加载模式定义
#define INIT_LOAD_AOF 0
#define INIT_LOAD_SNAP 1

#define TO_RESP 1

#endif // __KVS_CONSTANTS_H__
