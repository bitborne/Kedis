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
#include <semaphore.h>

// AOF缓冲区和长度（在kvstore.c中定义）
extern char aof_buf[AOF_BUF_SIZE];
extern int aof_len;

// AOF文件描述符
static int aof_fd = -1;

// AOF线程和同步机制
static pthread_t fsync_thread;
static sem_t fsync_sem;  // 用于通知fsync线程执行同步
static int fsync_running = 0; // 控制线程循环的标志

/**
 * 将命令追加到AOF缓冲区
 * @param cmd_str 命令字符串
 */
void appendToAofBuffer(const char* cmd_str) {
    int len = strlen(cmd_str);

    // 检查是否有足够的空间
    if (aof_len + len >= AOF_BUF_SIZE) {
        // 如果缓冲区不够，写入文件但不清空，因为数据还在内存中
        if (aof_fd != -1) {
            write(aof_fd, aof_buf, aof_len);
        }
        aof_len = 0;
    }

    // 如果命令长度超过缓冲区大小，则分块处理
    if (len >= AOF_BUF_SIZE) {
        if (aof_fd != -1) {
            write(aof_fd, cmd_str, len);
        }
        return;
    }

    // 将命令追加到缓冲区
    memcpy(aof_buf + aof_len, cmd_str, len);
    aof_len += len;
}

/**
 * FSYNC线程函数 - 每秒执行一次同步
 */
void* fsync_thread_func(void* arg) {
    while (fsync_running) {
        // 每秒执行一次fsync
        sleep(1);

        // 将当前AOF缓冲区内容写入文件
        if (aof_len > 0 && aof_fd != -1) {
            write(aof_fd, aof_buf, aof_len);
            aof_len = 0; // 清空缓冲区
        }

        // 强制同步到磁盘
        if (aof_fd != -1) {
            fsync(aof_fd);
        }
    }

    return NULL;
}

/**
 * 启动AOF同步线程
 */
int start_aof_fsync_process() {
    // 打开AOF文件
    aof_fd = open("appendonly.aof", O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (aof_fd == -1) {
        fprintf(stderr, "错误：无法打开AOF文件 appendonly.aof\n");
        return -1;
    }

    // 初始化信号量
    if (sem_init(&fsync_sem, 0, 0) != 0) {
        fprintf(stderr, "错误：初始化信号量失败\n");
        close(aof_fd);
        return -1;
    }

    // 设置运行标志
    fsync_running = 1;

    // 创建FSYNC线程
    int result = pthread_create(&fsync_thread, NULL, fsync_thread_func, NULL);
    if (result != 0) {
        fprintf(stderr, "错误：创建FSYNC线程失败\n");
        close(aof_fd);
        sem_destroy(&fsync_sem);
        return -1;
    }

    printf("AOF同步线程已启动\n");
    return 0;
}

/**
 * 向AOF同步线程发送缓冲区快照
 */
int send_aof_buffer_to_child() {
    // 这里不需要做任何事，因为同步由独立线程每秒执行
    return 0;
}