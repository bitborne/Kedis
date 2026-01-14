#include "kvstore.h"

#include <sys/mman.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/stat.h>
#include <signal.h>
#include <pthread.h>

// 重新声明全局变量
#if ENABLE_MULTI_ENGINE
  #if ENABLE_RBTREE
  extern kvs_rbtree_t rbtree_engine;
  #endif
  #if ENABLE_HASH
  extern kvs_hash_t hash_engine;
  #endif
  #if ENABLE_ARRAY
  extern kvs_array_t array_engine;
  #endif
  #if ENABLE_SKIPLIST
  extern kvs_skiplist_t skiplist_engine;
  #endif
#else
  // 根据优先级选择使用的数据结构：红黑树 > 哈希 > 数组
  // 以下是当前使用的数据结构的统一接口定义
  #if ENABLE_RBTREE
  extern kvs_rbtree_t global_main_engine;
  #elif ENABLE_HASH
  extern kvs_hash_t global_main_engine;
  #elif ENABLE_ARRAY
  extern kvs_array_t global_main_engine;
  #else
  #error "至少需要启用一种数据结构"
  #endif
#endif

// 多引擎模式下的AOF文件名定义
#if ENABLE_MULTI_ENGINE
  #if ENABLE_ARRAY
  const char* aof_filename_array = "./data/appendonly_array.ksf";
  #endif
  #if ENABLE_HASH
  const char* aof_filename_hash = "./data/appendonly_hash.ksf";
  #endif
  #if ENABLE_RBTREE
  const char* aof_filename_rbtree = "./data/appendonly_rbtree.ksf";
  #endif
  #if ENABLE_SKIPLIST
  const char* aof_filename_skiplist = "./data/appendonly_skiplist.ksf";
  #endif
#else
  const char* aof_filename = "./data/appendonly.ksf";
#endif

// AOF缓冲区和长度（在kvstore.c中定义）
// AOF缓冲区和长度
#if ENABLE_MULTI_ENGINE

extern aof_buf_t aofBuffer[4];

#else
extern aof_buf_t aofBuffer;
#endif

// AOF文件描述符 - 多引擎模式下每个引擎有独立的文件描述符
#if ENABLE_MULTI_ENGINE
  #if ENABLE_ARRAY
  static int aof_fd_array = -1;
  #endif
  #if ENABLE_HASH
  static int aof_fd_hash = -1;
  #endif
  #if ENABLE_RBTREE
  static int aof_fd_rbtree = -1;
  #endif
  #if ENABLE_SKIPLIST
  static int aof_fd_skiplist = -1;
  #endif
#else
  static int aof_fd = -1;
#endif

// 后台fsync线程相关
static pthread_t fsync_thread;
static int fsync_running = 0;
static time_t last_fsync_time = 0;
static time_t last_write_time = 0;

// 阈值：超过此大小的命令将绕过缓冲区直接写入
#define LARGE_CMD_THRESHOLD (AOF_BUF_SIZE / 2)

/**
 * 安全写入函数 - 确保所有数据都被写入
 * @param fd 文件描述符
 * @param buf 要写入的数据缓冲区
 * @param count 要写入的字节数
 * @return 成功返回写入的字节数，失败返回-1
 */
static ssize_t write_all(int fd, const void *buf, size_t count) {
    const char *p = buf;
    size_t written = 0;
    while (written < count) {
        ssize_t n = write(fd, p + written, count - written);
        if (n <= 0) {
            if (n == -1 && errno == EINTR) continue;  // 被中断，继续写入
            return -1;  // 错误
        }
        written += n;
    }
    last_write_time = time(NULL);
    return (ssize_t)written;
}


/**
 * 将变长整数编码为VLQ（Variable Length Quantity）格式
 * @param value 要编码的值
 * @param output 存储编码结果的缓冲区
 * @return 编码后占用的字节数
 */
static int encode_vlq(uint64_t value, uint8_t *output) {
    int count = 0;
    do {
        output[count] = value & 0x7F;
        value >>= 7;
        if (value) {
            output[count] |= 0x80;
        }
        count++;
    } while (value);
    return count;
}

/**
 * 将VLQ格式解码为整数
 * @param input 包含VLQ编码数据的缓冲区
 * @param value 存储解码结果的变量
 * @return 解码后占用的字节数
 */
static int decode_vlq(const uint8_t *input, uint64_t *value) {
    int count = 0;
    *value = 0;
    int shift = 0;

    do {
        *value |= ((uint64_t)(input[count] & 0x7F)) << shift;
        shift += 7;
    } while (input[count++] & 0x80);

    return count;
}

/**
 * 将命令追加到AOF缓冲区（使用新的二进制格式）
 * 更新：实现混合写入策略（小命令缓冲+大命令直写）
 * 注意：此函数仅在单引擎模式下使用
 * @param type 命令类型: CMD_SET, CMD_MOD, CMD_DEL
 * @param key 键
 * @param value 值
 */
void appendToAofBuffer(int type, const char* key, const char* value) {
#if !ENABLE_MULTI_ENGINE
    if (type != AOF_CMD_DEL && (key == NULL || value == NULL)) return;
    if (type == AOF_CMD_DEL && key == NULL) return;

    // REPLICATION BROADCAST
    char cmd_text[4096];
    if (type == AOF_CMD_SET) {
        snprintf(cmd_text, sizeof(cmd_text), "SET %s %s", key, value);
        replication_feed_slaves(cmd_text);
    } else if (type == AOF_CMD_MOD) {
        snprintf(cmd_text, sizeof(cmd_text), "MOD %s %s", key, value);
        replication_feed_slaves(cmd_text);
    } else if (type == AOF_CMD_DEL) {
        snprintf(cmd_text, sizeof(cmd_text), "DEL %s", key);
        replication_feed_slaves(cmd_text);
    }

    char* aof_buf = aofBuffer.buf;

    int klen = key ? strlen(key) : 0;
    int vlen = (value && type != AOF_CMD_DEL) ? strlen(value) : 0;

    uint8_t vlq[16];
    int key_len_bytes = encode_vlq(klen, vlq);
    int val_len_bytes = encode_vlq(vlen, vlq + key_len_bytes);

    int total_needed = 1 + key_len_bytes + val_len_bytes + klen + vlen;

    // 检查是否为大命令，如果是则绕过缓冲区直接写入
    if (total_needed >= LARGE_CMD_THRESHOLD) {
      printf("大命令直接写入\n");
        // 先刷新缓冲区，确保命令顺序一致
        flushAofBuffer();

        // 构造命令数据
        char cmd_data[AOF_BUF_SIZE];  // 使用足够大的缓冲区来构建整个命令
        int pos = 0;

        // 添加命令码（1字节）
        cmd_data[pos++] = (uint8_t)type;

        // 添加键长度（VLQ编码）
        memcpy(cmd_data + pos, vlq, key_len_bytes);
        pos += key_len_bytes;

        // 添加值长度（VLQ编码）
        memcpy(cmd_data + pos, vlq + key_len_bytes, val_len_bytes);
        pos += val_len_bytes;

        // 添加键内容
        if (klen > 0) {
            memcpy(cmd_data + pos, key, klen);
            pos += klen;
        }

        // 添加值内容
        if (vlen > 0) {
            memcpy(cmd_data + pos, value, vlen);
            pos += vlen;
        }

        // 直接写入大命令
        if (write_all(aof_fd, cmd_data, pos) < 0) {
            fprintf(stderr, "AOF错误：无法写入大命令: %s\n", strerror(errno));
        }
        return;
    }

    // 小命令：尝试追加到缓冲区
    if (aofBuffer.len + total_needed > AOF_BUF_SIZE) {
        flushAofBuffer(); // 缓冲区满，先flush
    }

    // 添加命令码（1字节）
    aof_buf[aofBuffer.len++] = (uint8_t)type;

    // 添加键长度（VLQ编码）
    memcpy(aof_buf + aofBuffer.len, vlq, key_len_bytes);
    aofBuffer.len += key_len_bytes;

    // 添加值长度（VLQ编码）
    memcpy(aof_buf + aofBuffer.len, vlq + key_len_bytes, val_len_bytes);
    aofBuffer.len += val_len_bytes;

    // 添加键内容
    if (klen > 0) {
        memcpy(aof_buf + aofBuffer.len, key, klen);
        aofBuffer.len += klen;
    }

    // 添加值内容
    if (vlen > 0) {
        memcpy(aof_buf + aofBuffer.len, value, vlen);
        aofBuffer.len += vlen;
    }
#else
    fprintf(stderr, "错误：多引擎模式下请使用 appendToAofBufferToEngine()\n");
#endif
}

/**
 * 将AOF缓冲区写入文件（在事件循环结束前调用）
 * 更新：使用write_all确保所有数据都写入，简化逻辑
 * 注意：此函数仅在单引擎模式下使用
 */
int flushAofBuffer() {
#if !ENABLE_MULTI_ENGINE
    if (aof_len > 0 && aof_fd != -1) {
        ssize_t result = write_all(aof_fd, aofBuffer.buf, aofBuffer.len);
        if (result == -1) {
            fprintf(stderr, "错误：写入AOF文件失败: %s\n", strerror(errno));
            return -1;
        }
        // 成功写入后，重置缓冲区
        aofBuffer.len = 0;
    }
    return 0;
#else
    fprintf(stderr, "错误：多引擎模式下请使用引擎特定的flush函数\n");
    return -1;
#endif
}

/**
 * 刷新指定引擎的AOF缓冲区到文件（多引擎模式）
 * @param engine_type 引擎类型: 0=array, 1=hash, 2=rbtree
 * @return 成功返回0，失败返回-1
 */
static int flushAofBufferToEngine(int engine_type) {
#if ENABLE_MULTI_ENGINE
    char* aof_buf = aofBuffer[engine_type].buf;
    if (aofBuffer[engine_type].len > 0) {
        int target_fd = -1;
        if (engine_type == 0) {
            #if ENABLE_ARRAY
            target_fd = aof_fd_array;
            #endif
        } else if (engine_type == 1) {
            #if ENABLE_HASH
            target_fd = aof_fd_hash;
            #endif
        } else if (engine_type == 2) {
            #if ENABLE_RBTREE
            target_fd = aof_fd_rbtree;
            #endif
        }

        if (target_fd != -1) {
            ssize_t result = write_all(target_fd, aof_buf, aofBuffer[engine_type].len);
            if (result == -1) {
                fprintf(stderr, "错误：写入AOF文件失败: %s\n", strerror(errno));
                return -1;
            }
            // 成功写入后，重置缓冲区
            aofBuffer[engine_type].len = 0;
        }
    }
    return 0;
#else
    return flushAofBuffer();
#endif
}

/**
 * FSYNC线程函数 - 每秒执行一次同步
 */
void* fsync_thread_func(void* arg) {
    time_t current_time;

    while (fsync_running) {
        sleep(1);

        time(&current_time);

#if ENABLE_MULTI_ENGINE
        // 多引擎模式：同步所有引擎的AOF文件
        #if ENABLE_ARRAY
        if (aof_fd_array != -1) {
            fsync(aof_fd_array);
        }
        #endif
        #if ENABLE_HASH
        if (aof_fd_hash != -1) {
            fsync(aof_fd_hash);
        }
        #endif
        #if ENABLE_RBTREE
        if (aof_fd_rbtree != -1) {
            fsync(aof_fd_rbtree);
        }
        #endif
#else
        // 单引擎模式：只同步一个文件
        if (aof_fd != -1) {
            fsync(aof_fd);
        }
#endif
        last_fsync_time = current_time;
    }

    return NULL;
}

/**
 * 启动AOF FSYNC后台线程
 */
int start_aof_fsync_process() {
#if ENABLE_MULTI_ENGINE
    // 多引擎模式：为每个引擎打开独立的AOF文件
    #if ENABLE_ARRAY
    aof_fd_array = open(aof_filename_array, O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (aof_fd_array == -1) {
        fprintf(stderr, "错误：无法打开AOF文件 %s\n", aof_filename_array);
        return -1;
    }
    printf("Array引擎AOF文件已打开: %s\n", aof_filename_array);
    #endif

    #if ENABLE_HASH
    aof_fd_hash = open(aof_filename_hash, O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (aof_fd_hash == -1) {
        fprintf(stderr, "错误：无法打开AOF文件 %s\n", aof_filename_hash);
        return -1;
    }
    printf("Hash引擎AOF文件已打开: %s\n", aof_filename_hash);
    #endif

    #if ENABLE_RBTREE
    aof_fd_rbtree = open(aof_filename_rbtree, O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (aof_fd_rbtree == -1) {
        fprintf(stderr, "错误：无法打开AOF文件 %s\n", aof_filename_rbtree);
        return -1;
    }
    printf("Rbtree引擎AOF文件已打开: %s\n", aof_filename_rbtree);
    #endif

    #if ENABLE_SKIPLIST
    aof_fd_skiplist = open(aof_filename_skiplist, O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (aof_fd_skiplist == -1) {
        fprintf(stderr, "错误：无法打开AOF文件 %s\n", aof_filename_skiplist);
        return -1;
    }
    printf("Skiplist引擎AOF文件已打开: %s\n", aof_filename_skiplist);
    #endif
#else
    // 单引擎模式：只打开一个AOF文件
    aof_fd = open(aof_filename, O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (aof_fd == -1) {
        fprintf(stderr, "错误：无法打开AOF文件 %s\n", aof_filename);
        return -1;
    }
    printf("AOF文件已打开: %s\n", aof_filename);
#endif

    // 设置运行标志
    fsync_running = 1;
    time(&last_fsync_time);

    // 创建FSYNC线程
    int result = pthread_create(&fsync_thread, NULL, fsync_thread_func, NULL);
    if (result != 0) {
        fprintf(stderr, "错误：创建FSYNC线程失败\n");
#if ENABLE_MULTI_ENGINE
        #if ENABLE_ARRAY
        if (aof_fd_array != -1) close(aof_fd_array);
        #endif
        #if ENABLE_HASH
        if (aof_fd_hash != -1) close(aof_fd_hash);
        #endif
        #if ENABLE_RBTREE
        if (aof_fd_rbtree != -1) close(aof_fd_rbtree);
        #endif
        #if ENABLE_SKIPLIST
        if (aof_fd_skiplist != -1) close(aof_fd_skiplist);
        #endif
#else
        if (aof_fd != -1) close(aof_fd);
#endif
        return -1;
    }

    printf("AOF FSYNC后台线程已启动\n");
    return 0;
}

void before_sleep() {
    // 刷新AOF缓冲区到文件
#if ENABLE_MULTI_ENGINE
    // 多引擎模式：刷新所有引擎的AOF缓冲区
    #if ENABLE_ARRAY
    flushAofBufferToEngine(0);
    #endif
    #if ENABLE_HASH
    flushAofBufferToEngine(1);
    #endif
    #if ENABLE_RBTREE
    flushAofBufferToEngine(2);
    #endif
    #if ENABLE_SKIPLIST
    flushAofBufferToEngine(3);
    #endif
#else
    flushAofBuffer();
#endif
}

/**
 * 从AOF文件加载数据到指定引擎
 * @param filename 文件名
 * @param engine_type 引擎类型: 0=array, 1=hash, 2=rbtree, 3=skiplist
 * @return 成功返回0，失败返回-1
 */
static int aofLoadToEngine(const char* filename, int engine_type) {
    printf("开始加载AOF文件: %s\n", filename);

    // 检查文件是否存在
    FILE* file = fopen(filename, "rb");
    if (!file) {
        printf("AOF文件不存在或无法打开: %s\n", filename);
        return 0; // 文件不存在是正常的，返回成功
    }

    // 获取文件大小
    fseek(file, 0, SEEK_END);
    long file_size = ftell(file);
    fseek(file, 0, SEEK_SET);

    if (file_size == 0) {
        printf("AOF文件为空: %s\n", filename);
        fclose(file);
        return 0;
    }

    // 分配缓冲区读取整个文件
    char* buffer = (char*)kvs_malloc(file_size);
    if (!buffer) {
        fprintf(stderr, "无法分配内存来加载AOF文件\n");
        fclose(file);
        return -1;
    }

    // 读取文件内容
    size_t bytes_read = fread(buffer, 1, file_size, file);
    if (bytes_read != file_size) {
        fprintf(stderr, "读取AOF文件时发生错误\n");
        kvs_free(buffer);
        fclose(file);
        return -1;
    }

    fclose(file);

    // 解析AOF内容并恢复数据
    long pos = 0;
    while (pos < file_size) {
        // 读取命令码（1字节）
        if (pos >= file_size) break;
        uint8_t cmd_type = buffer[pos++];

        // 解码键长度（VLQ格式）
        if (pos >= file_size) break;
        uint64_t key_len;
        int key_len_bytes = decode_vlq((const uint8_t*)(buffer + pos), &key_len);
        pos += key_len_bytes;

        // 解码值长度（VLQ格式）
        if (pos >= file_size) break;
        uint64_t val_len;
        int val_len_bytes = decode_vlq((const uint8_t*)(buffer + pos), &val_len);
        pos += val_len_bytes;

        // 读取键内容
        if (pos + key_len > file_size) break;
        char* key = NULL;
        if (key_len > 0) {
            key = (char*)kvs_malloc(key_len + 1);
            if (!key) {
                fprintf(stderr, "无法分配内存来存储键\n");
                kvs_free(buffer);
                return -1;
            }
            memcpy(key, buffer + pos, key_len);
            key[key_len] = '\0';
            pos += key_len;
        }

        // 读取值内容
        char* value = NULL;
        if (val_len > 0) {
            if (pos + val_len > file_size) {
                if (key) kvs_free(key);
                kvs_free(buffer);
                return -1;
            }
            value = (char*)kvs_malloc(val_len + 1);
            if (!value) {
                fprintf(stderr, "无法分配内存来存储值\n");
                if (key) kvs_free(key);
                kvs_free(buffer);
                return -1;
            }
            memcpy(value, buffer + pos, val_len);
            value[val_len] = '\0';
            pos += val_len;
        }

        // 根据引擎类型和命令类型执行相应的操作
        int result = 0;
        switch (cmd_type) {
            case AOF_CMD_SET:
#if ENABLE_MULTI_ENGINE
                if (engine_type == 0) {
                    #if ENABLE_ARRAY
                    kvs_array_set(&array_engine, key, value);
                    #endif
                } else if (engine_type == 1) {
                    #if ENABLE_HASH
                    kvs_hash_set(&hash_engine, key, value);
                    #endif
                } else if (engine_type == 2) {
                    #if ENABLE_RBTREE
                    kvs_rbtree_set(&rbtree_engine, key, value);
                    #endif
                } else if (engine_type == 3) {
                    #if ENABLE_SKIPLIST
                    kvs_skiplist_set(&skiplist_engine, key, value);
                    #endif
                }
#else
                kvs_main_set(&global_main_engine, key, value);
#endif
                break;

            case AOF_CMD_MOD:
#if ENABLE_MULTI_ENGINE
                if (engine_type == 0) {
                    #if ENABLE_ARRAY
                    kvs_array_mod(&array_engine, key, value);
                    #endif
                } else if (engine_type == 1) {
                    #if ENABLE_HASH
                    kvs_hash_mod(&hash_engine, key, value);
                    #endif
                } else if (engine_type == 2) {
                    #if ENABLE_RBTREE
                    kvs_rbtree_mod(&rbtree_engine, key, value);
                    #endif
                } else if (engine_type == 3) {
                    #if ENABLE_SKIPLIST
                    kvs_skiplist_mod(&skiplist_engine, key, value);
                    #endif
                }
#else
                kvs_main_mod(&global_main_engine, key, value);
#endif
                break;

            case AOF_CMD_DEL:
#if ENABLE_MULTI_ENGINE
                if (engine_type == 0) {
                    #if ENABLE_ARRAY
                    kvs_array_del(&array_engine, key);
                    #endif
                } else if (engine_type == 1) {
                    #if ENABLE_HASH
                    kvs_hash_del(&hash_engine, key);
                    #endif
                } else if (engine_type == 2) {
                    #if ENABLE_RBTREE
                    kvs_rbtree_del(&rbtree_engine, key);
                    #endif
                } else if (engine_type == 3) {
                    #if ENABLE_SKIPLIST
                    kvs_skiplist_del(&skiplist_engine, key);
                    #endif
                }
#else
                kvs_main_del(&global_main_engine, key);
#endif
                break;

            default:
                fprintf(stderr, "未知的AOF命令类型: %d\n", cmd_type);
                break;
        }

        // 释放分配的内存
        if (key) kvs_free(key);
        if (value) kvs_free(value);
    }

    kvs_free(buffer);
    printf("AOF文件加载完成: %s\n", filename);
    return 0;
}

/**
 * 加载所有引擎的AOF文件（多引擎模式）或单个AOF文件（单引擎模式）
 * @return 成功返回0，失败返回-1
 */
int aofLoadAll() {
#if ENABLE_MULTI_ENGINE
    // 多引擎模式：加载所有引擎的AOF文件
    #if ENABLE_ARRAY
    if (aofLoadToEngine(aof_filename_array, 0) != 0) {
        return -1;
    }
    #endif
    #if ENABLE_HASH
    if (aofLoadToEngine(aof_filename_hash, 1) != 0) {
        return -1;
    }
    #endif
    #if ENABLE_RBTREE
    if (aofLoadToEngine(aof_filename_rbtree, 2) != 0) {
        return -1;
    }
    #endif
    #if ENABLE_SKIPLIST
    if (aofLoadToEngine(aof_filename_skiplist, 3) != 0) {
        return -1;
    }
    #endif
    return 0;
#else
    // 单引擎模式：只加载一个AOF文件
    return aofLoadToEngine(aof_filename, -1);
#endif
}

/**
 * 向指定引擎的AOF文件写入命令（用于多引擎模式）
 * @param engine_type 引擎类型: 0=array, 1=hash, 2=rbtree, 3=skiplist
 * @param type 命令类型
 * @param key 键
 * @param value 值
 */
void appendToAofBufferToEngine(int engine_type, int type, const char* key, const char* value) {
    if (type != AOF_CMD_DEL && (key == NULL || value == NULL)) return;
    if (type == AOF_CMD_DEL && key == NULL) return;

    // REPLICATION BROADCAST
    char cmd_text[4096];
    if (type == AOF_CMD_SET) {
        snprintf(cmd_text, sizeof(cmd_text), "SET %s %s", key, value);
        replication_feed_slaves(cmd_text);
    } else if (type == AOF_CMD_MOD) {
        snprintf(cmd_text, sizeof(cmd_text), "MOD %s %s", key, value);
        replication_feed_slaves(cmd_text);
    } else if (type == AOF_CMD_DEL) {
        snprintf(cmd_text, sizeof(cmd_text), "DEL %s", key);
        replication_feed_slaves(cmd_text);
    }

    char* aof_buf = aofBuffer[engine_type].buf;
    
    int klen = key ? strlen(key) : 0;
    int vlen = (value && type != AOF_CMD_DEL) ? strlen(value) : 0;

    uint8_t vlq[16];
    int key_len_bytes = encode_vlq(klen, vlq);
    int val_len_bytes = encode_vlq(vlen, vlq + key_len_bytes);

    int total_needed = 1 + key_len_bytes + val_len_bytes + klen + vlen;

    // 检查是否为大命令，如果是则绕过缓冲区直接写入
    if (total_needed >= LARGE_CMD_THRESHOLD) {
      printf("大命令直接写入\n");
        // 先刷新缓冲区，确保命令顺序一致
        flushAofBufferToEngine(engine_type);

        // 构造命令数据
        char cmd_data[AOF_BUF_SIZE];  // 使用足够大的缓冲区来构建整个命令
        int pos = 0;

        // 添加命令码（1字节）
        cmd_data[pos++] = (uint8_t)type;

        // 添加键长度（VLQ编码）
        memcpy(cmd_data + pos, vlq, key_len_bytes);
        pos += key_len_bytes;

        // 添加值长度（VLQ编码）
        memcpy(cmd_data + pos, vlq + key_len_bytes, val_len_bytes);
        pos += val_len_bytes;

        // 添加键内容
        if (klen > 0) {
            memcpy(cmd_data + pos, key, klen);
            pos += klen;
        }

        // 添加值内容
        if (vlen > 0) {
            memcpy(cmd_data + pos, value, vlen);
            pos += vlen;
        }

        // 直接写入大命令到对应引擎的AOF文件
        int target_fd = -1;
#if ENABLE_MULTI_ENGINE
        if (engine_type == 0) {
            #if ENABLE_ARRAY
            target_fd = aof_fd_array;
            #endif
        } else if (engine_type == 1) {
            #if ENABLE_HASH
            target_fd = aof_fd_hash;
            #endif
        } else if (engine_type == 2) {
            #if ENABLE_RBTREE
            target_fd = aof_fd_rbtree;
            #endif
        }
#else
        target_fd = aof_fd;
#endif

        if (target_fd != -1 && write_all(target_fd, cmd_data, pos) < 0) {
            fprintf(stderr, "AOF错误：无法写入大命令: %s\n", strerror(errno));
        }
        return;
    }

    // 小命令：尝试追加到缓冲区
    if (aofBuffer[engine_type].len + total_needed > AOF_BUF_SIZE) {
        flushAofBufferToEngine(engine_type); // 缓冲区满，先flush
    }

    // 添加命令码（1字节）
    aof_buf[aofBuffer[engine_type].len++] = (uint8_t)type;

    // 添加键长度（VLQ编码）
    memcpy(aof_buf + aofBuffer[engine_type].len, vlq, key_len_bytes);
    aofBuffer[engine_type].len += key_len_bytes;

    // 添加值长度（VLQ编码）
    memcpy(aof_buf + aofBuffer[engine_type].len, vlq + key_len_bytes, val_len_bytes);
    aofBuffer[engine_type].len += val_len_bytes;

    // 添加键内容
    if (klen > 0) {
        memcpy(aof_buf + aofBuffer[engine_type].len, key, klen);
        aofBuffer[engine_type].len += klen;
    }

    // 添加值内容
    if (vlen > 0) {
        memcpy(aof_buf + aofBuffer[engine_type].len, value, vlen);
        aofBuffer[engine_type].len += vlen;
    }
}
