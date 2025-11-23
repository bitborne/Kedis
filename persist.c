#include "kvstore.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

// 全局日志文件指针
static FILE *log_file = NULL;

// 持久化模式：0-增量，1-全量
static int persist_mode = PERSIST_MODE_INCREMENTAL;

// 日志文件和快照文件路径
#define LOG_FILE_PATH "./kvs_operation.log"
#define SNAPSHOT_FILE_PATH "./kvs_snapshot.dat"

// 重新声明全局变量
#if ENABLE_ARRAY
extern kvs_array_t global_array;
#endif

#if ENABLE_RBTREE
extern kvs_rbtree_t global_rbtree;
#endif

#if ENABLE_HASH
extern kvs_hash_t global_hash;
#endif

/**
 * 初始化持久化功能
 * @param mode 持久化模式：0-增量，1-全量
 */
int kvs_persist_init(int mode) {
    persist_mode = mode;

    if (mode == PERSIST_MODE_INCREMENTAL) {
        // 以追加模式打开日志文件
        log_file = fopen(LOG_FILE_PATH, "a+");
        if (log_file == NULL) {
            printf("错误：无法打开日志文件 %s\n", LOG_FILE_PATH);
            return -1;
        }

        // 设置文件缓冲模式为行缓冲，确保日志能及时写入
        setvbuf(log_file, NULL, _IOLBF, 0);

        printf("增量持久化功能初始化成功，日志文件：%s\n", LOG_FILE_PATH);
    } else if (mode == PERSIST_MODE_SNAPSHOT) {
        printf("全量持久化功能初始化成功，快照文件：%s\n", SNAPSHOT_FILE_PATH);
    } else {
        printf("错误：未知的持久化模式 %d\n", mode);
        return -1;
    }

    return 0;
}

/**
 * 记录操作日志
 * @param operation 操作类型 (SET/MOD/DEL等)
 * @param key 键
 * @param value 值
 * @return 成功返回0，失败返回-1
 */
int kvs_persist_log_operation(const char *operation, const char *key, const char *value) {
    // 只在增量模式下记录日志
    if (persist_mode != PERSIST_MODE_INCREMENTAL) {
        // 在快照模式下，不记录操作日志，因为数据会在关闭时一次性保存
        return 0;
    }

    if (log_file == NULL) {
        printf("错误：日志文件未初始化\n");
        return -1;
    }

    // 获取当前时间
    time_t now;
    time(&now);
    char *time_str = ctime(&now);
    // 移除换行符
    time_str[strlen(time_str) - 1] = '\0';

    // 根据操作类型记录日志
    if (value != NULL) {
        // 对于SET、MOD操作，记录键值对
        fprintf(log_file, "[%s] %s %s %s\n", time_str, operation, key, value);
    } else {
        // 对于DEL、EXISTS操作，只记录键
        fprintf(log_file, "[%s] %s %s\n", time_str, operation, key);
    }

    // 确保日志立即写入磁盘
    fflush(log_file);

    // printf("已记录操作日志: %s %s %s\n", operation, key, value ? value : "");
    return 0;
}

/**
 * 回放日志 - 在服务启动时重新执行日志中的操作
 */
int kvs_persist_replay_log(void) {
    FILE *read_file = fopen(LOG_FILE_PATH, "r");
    if (read_file == NULL) {
        printf("日志文件 %s 不存在，跳过回放\n", LOG_FILE_PATH);
        return 0; // 没有日志文件不是错误
    }

    printf("开始回放日志...\n");

    char line[1024];
    int replay_count = 0;

    // 逐行读取日志并执行操作
    while (fgets(line, sizeof(line), read_file) != NULL) {
        // 移除开头的时间戳部分
        char *log_start = strchr(line, ']');
        if (log_start == NULL) continue;
        log_start += 2; // 跳过 '] ' 

        // 解析操作和参数
        char operation[32], key[256], value[512];
        int args_count = sscanf(log_start, "%31s %255s %511[^\n]", operation, key, value);
        
        // 输出解析的详细信息，便于调试
        // printf("解析日志: '%s', 参数个数: %d, 操作: %s, 键: %s", log_start, args_count, operation, key);
        if (args_count >= 3) {
            // printf(", 值: %s", value);
        }
        // printf("\n");

        if (args_count >= 2) {
            // 检查是否是删除操作（只有操作和键）
            if (args_count == 2 && 
                (strcmp(operation, "DEL") == 0 || 
                 strcmp(operation, "RDEL") == 0 || 
                 strcmp(operation, "HDEL") == 0)) {
                // 对于删除操作，执行删除
                if (strcmp(operation, "DEL") == 0) {
#if ENABLE_ARRAY
                    int result = kvs_array_del(&global_array, key);
                    // printf("执行DEL操作，键: %s, 结果: %d\n", key, result);
#endif
                } else if (strcmp(operation, "RDEL") == 0) {
#if ENABLE_RBTREE
                    int result = kvs_rbtree_del(&global_rbtree, key);
                    // printf("执行RDEL操作，键: %s, 结果: %d\n", key, result);
#endif
                } else if (strcmp(operation, "HDEL") == 0) {
#if ENABLE_HASH
                    int result = kvs_hash_del(&global_hash, key);
                    // printf("执行HDEL操作，键: %s, 结果: %d\n", key, result);
#endif
                }
                replay_count++;
            }
            // 检查是否是设置或修改操作（操作、键和值）
            else if (args_count >= 3) {
                // 去除值前面可能的空格
                char *val_start = value;
                while (*val_start == ' ' && *val_start != '\0') val_start++;
                
                // 对于SET、MOD操作
                if (strcmp(operation, "SET") == 0) {
#if ENABLE_ARRAY
                    int result = kvs_array_set(&global_array, key, val_start);
                    // printf("执行SET操作，键: %s, 值: %s, 结果: %d\n", key, val_start, result);
#endif
                } else if (strcmp(operation, "RSET") == 0) {
#if ENABLE_RBTREE
                    int result = kvs_rbtree_set(&global_rbtree, key, val_start);
                    // printf("执行RSET操作，键: %s, 值: %s, 结果: %d\n", key, val_start, result);
#endif
                } else if (strcmp(operation, "HSET") == 0) {
#if ENABLE_HASH
                    int result = kvs_hash_set(&global_hash, key, val_start);
                    // printf("执行HSET操作，键: %s, 值: %s, 结果: %d\n", key, val_start, result);
#endif
                } else if (strcmp(operation, "MOD") == 0) {
#if ENABLE_ARRAY
                    int result = kvs_array_mod(&global_array, key, val_start);
                    // printf("执行MOD操作，键: %s, 值: %s, 结果: %d\n", key, val_start, result);
#endif
                } else if (strcmp(operation, "RMOD") == 0) {
#if ENABLE_RBTREE
                    int result = kvs_rbtree_mod(&global_rbtree, key, val_start);
                    // printf("执行RMOD操作，键: %s, 值: %s, 结果: %d\n", key, val_start, result);
#endif
                } else if (strcmp(operation, "HMOD") == 0) {
#if ENABLE_HASH
                    int result = kvs_hash_mod(&global_hash, key, val_start);
                    // printf("执行HMOD操作，键: %s, 值: %s, 结果: %d\n", key, val_start, result);
#endif
                }
                replay_count++;
            }
            else {
                printf("未知操作或参数不足，跳过: %s\n", log_start);
            }
        }
        else {
            printf("参数不足，跳过: %s\n", log_start);
        }
    }

    fclose(read_file);
    printf("日志回放完成，共处理 %d 条操作\n", replay_count);
    return 0;
}

/**
 * 关闭持久化功能，确保所有日志写入完成
 */
int kvs_persist_close(void) {
    if (persist_mode == PERSIST_MODE_INCREMENTAL && log_file != NULL) {
        fclose(log_file);
        log_file = NULL;
        printf("日志文件已关闭\n");
    }
    return 0;
}

/**
 * 保存数据快照
 */
int kvs_snapshot_save(void) {
    if (persist_mode != PERSIST_MODE_SNAPSHOT) {
        printf("错误：当前非快照模式\n");
        return -1;
    }

    FILE *snapshot_file = fopen(SNAPSHOT_FILE_PATH, "w");
    if (snapshot_file == NULL) {
        printf("错误：无法创建快照文件 %s\n", SNAPSHOT_FILE_PATH);
        return -1;
    }

    printf("开始保存快照...\n");

    // 保存数组后端数据 (使用SET命令)
#if ENABLE_ARRAY
    if (global_array.table != NULL) {
        for (int i = 0; i < KVS_ARRAY_SIZE; i++) {  // 遍历整个数组大小，而不是total
            // 额外检查数组元素是否有效
            // printf("--> out of if\n");
            if (global_array.table[i].key != NULL && global_array.table[i].value != NULL) {
              // printf("--> in if\n");
                fprintf(snapshot_file, "SET %s %s\n", global_array.table[i].key, global_array.table[i].value);
            }
        }
    }
    // printf("--> arr OK\n");
    #endif
    
    
    // 保存红黑树后端数据 (使用RSET命令)
    #if ENABLE_RBTREE
    kvs_rbtree_save_snapshot(&global_rbtree, snapshot_file);
    // printf("--> tree OK\n");
    #endif
    
    // 保存哈希表后端数据 (使用HSET命令)
    #if ENABLE_HASH
    kvs_hash_save_snapshot(&global_hash, snapshot_file);
    // printf("--> hash OK\n");
    #endif

    fclose(snapshot_file);
    printf("快照保存完成\n");
    return 0;
}

/**
 * 加载数据快照
 */
int kvs_snapshot_load(void) {
    if (persist_mode != PERSIST_MODE_SNAPSHOT) {
        printf("错误：当前非快照模式\n");
        return -1;
    }

    FILE *snapshot_file = fopen(SNAPSHOT_FILE_PATH, "r");
    if (snapshot_file == NULL) {
        printf("快照文件 %s 不存在，跳过加载\n", SNAPSHOT_FILE_PATH);
        return 0; // 没有快照文件不是错误
    }

    printf("开始加载快照...\n");

    char line[1024];
    char operation[32], key[256], value[512];

    // 逐行读取快照并执行操作
    while (fgets(line, sizeof(line), snapshot_file) != NULL) {
        int args_count = sscanf(line, "%31s %255s %511[^\n]", operation, key, value);

        if (args_count >= 3) {
            // 去除值前面可能的空格
            char *val_start = value;
            while (*val_start == ' ' && *val_start != '\0') val_start++;

                    // 根据操作类型执行对应操作
            if (strcmp(operation, "SET") == 0) {
#if ENABLE_ARRAY
                // 对于快照加载，尝试设置，如果键存在则修改
                int result = kvs_array_set(&global_array, key, val_start);
                if (result > 0) {  // 如果键已存在
                    kvs_array_mod(&global_array, key, val_start);
                }
#endif
            } else if (strcmp(operation, "RSET") == 0) {
#if ENABLE_RBTREE
                // 红黑树的set会自动处理已存在的键
                kvs_rbtree_set(&global_rbtree, key, val_start);
#endif
            } else if (strcmp(operation, "HSET") == 0) {
#if ENABLE_HASH
                // 哈希表的set会自动处理已存在的键
                kvs_hash_set(&global_hash, key, val_start);
#endif
            }
        }
    }

    fclose(snapshot_file);
    printf("快照加载完成\n");
    return 0;
}