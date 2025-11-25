#include "kvstore.h"
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

// AOF缓冲区和长度（在kvstore.c中定义）
extern char aof_buf[AOF_BUF_SIZE];
extern int aof_len;

// AOF文件描述符
static int aof_fd = -1;

// 后台fsync线程相关
static pthread_t fsync_thread;
static int fsync_running = 0;
static time_t last_fsync_time = 0;

// 命令码定义
#define CMD_SET 1
#define CMD_MOD 2
#define CMD_DEL 3
#define CMD_SNAP 4  // 快照记录

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
 * @param type 命令类型: CMD_SET, CMD_MOD, CMD_DEL
 * @param key 键
 * @param value 值
 */
void appendToAofBuffer(int type, const char* key, const char* value) {
    if (type != CMD_DEL && (key == NULL || value == NULL)) return;
    if (type == CMD_DEL && key == NULL) return;

    int klen = key ? strlen(key) : 0;
    int vlen = (value && type != CMD_DEL) ? strlen(value) : 0;

    uint8_t vlq[16];
    int key_len_bytes = encode_vlq(klen, vlq);
    int val_len_bytes = encode_vlq(vlen, vlq + key_len_bytes);

    int total_needed = 1 + key_len_bytes + val_len_bytes + klen + vlen;

    if (aof_len + total_needed >= AOF_BUF_SIZE) {
        fprintf(stderr, "AOF缓冲区已满，无法追加命令\n");
        return;
    }

    // 添加命令码（1字节）
    aof_buf[aof_len++] = (uint8_t)type;

    // 添加键长度（VLQ编码）
    memcpy(aof_buf + aof_len, vlq, key_len_bytes);
    aof_len += key_len_bytes;

    // 添加值长度（VLQ编码）
    memcpy(aof_buf + aof_len, vlq + key_len_bytes, val_len_bytes);
    aof_len += val_len_bytes;

    // 添加键内容
    if (klen > 0) {
        memcpy(aof_buf + aof_len, key, klen);
        aof_len += klen;
    }

    // 添加值内容
    if (vlen > 0) {
        memcpy(aof_buf + aof_len, value, vlen);
        aof_len += vlen;
    }
}

/**
 * 将AOF缓冲区写入文件（在事件循环结束前调用）
 */
int flushAofBuffer() {
    if (aof_len > 0 && aof_fd != -1) {
        ssize_t nwritten = write(aof_fd, aof_buf, aof_len);
        if (nwritten == -1) {
            fprintf(stderr, "错误：写入AOF文件失败: %s\n", strerror(errno));
            return -1;
        } else if (nwritten > 0) {
            // 更新aof_buf，保留未写入的部分
            memmove(aof_buf, aof_buf + nwritten, aof_len - nwritten);
            aof_len -= nwritten;
        }
    }
    return 0;
}

/**
 * FSYNC线程函数 - 每秒执行一次同步
 */
void* fsync_thread_func(void* arg) {
    time_t current_time;

    while (fsync_running) {
        sleep(1);

        time(&current_time);

        // 检查是否需要执行fsync（每秒一次）
        if (current_time - last_fsync_time >= 1 && aof_fd != -1) {
            fsync(aof_fd);
            last_fsync_time = current_time;
        } else if (aof_fd != -1) {
            // 即使没有数据也要fsync，确保磁盘上的一致性
            fsync(aof_fd);
        }
    }

    return NULL;
}

/**
 * 启动AOF FSYNC后台线程
 */
int start_aof_fsync_process() {
    // 打开AOF文件
    aof_fd = open("appendonly.aof", O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (aof_fd == -1) {
        fprintf(stderr, "错误：无法打开AOF文件 appendonly.aof\n");
        return -1;
    }

    // 设置运行标志
    fsync_running = 1;
    time(&last_fsync_time);

    // 创建FSYNC线程
    int result = pthread_create(&fsync_thread, NULL, fsync_thread_func, NULL);
    if (result != 0) {
        fprintf(stderr, "错误：创建FSYNC线程失败\n");
        close(aof_fd);
        return -1;
    }

    printf("AOF FSYNC后台线程已启动\n");
    return 0;
}