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
 * @param type 命令类型: CMD_SET, CMD_MOD, CMD_DEL
 * @param key 键
 * @param value 值
 */
void appendToAofBuffer(int type, const char* key, const char* value) {
    if (type != AOF_CMD_DEL && (key == NULL || value == NULL)) return;
    if (type == AOF_CMD_DEL && key == NULL) return;

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
    if (aof_len + total_needed > AOF_BUF_SIZE) {
        flushAofBuffer(); // 缓冲区满，先flush
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
 * 更新：使用write_all确保所有数据都写入，简化逻辑
 */
int flushAofBuffer() {
    if (aof_len > 0 && aof_fd != -1) {
        ssize_t result = write_all(aof_fd, aof_buf, aof_len);
        if (result == -1) {
            fprintf(stderr, "错误：写入AOF文件失败: %s\n", strerror(errno));
            return -1;
        }
        // 成功写入后，重置缓冲区
        aof_len = 0;
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
    aof_fd = open("appendonly.ksf", O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (aof_fd == -1) {
        fprintf(stderr, "错误：无法打开AOF文件 appendonly.ksf\n");
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

void before_sleep() {
    // 刷新AOF缓冲区到文件
    flushAofBuffer();
}