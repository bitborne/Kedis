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
#include <dirent.h>
#include <stdint.h>

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
 * 将变长整数编码为VLQ（Variable Length Quantity）格式
 * @param value 要编码的值
 * @param output 存储编码结果的缓冲区
 * @return 编码后占用的字节数
 */
int encode_vlq(uint64_t value, uint8_t *output) {
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
int decode_vlq(const uint8_t *input, uint64_t *value) {
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
 * 向KSF文件写入一条KV记录
 * @param fd 文件描述符
 * @param k 键
 * @param klen 键长度
 * @param v 值
 * @param vlen 值长度
 * @return 成功返回0，失败返回-1
 */
int ksfWriteOneKv(int fd, const char *k, size_t klen, const char *v, size_t vlen) {
    uint8_t vlq[16];
    int bytes_written;

    // 写入键长度
    int klen_bytes = encode_vlq(klen, vlq);
    bytes_written = write(fd, vlq, klen_bytes);
    if (bytes_written != klen_bytes) {
        return -1;
    }

    // 写入值长度
    int vlen_bytes = encode_vlq(vlen, vlq);
    bytes_written = write(fd, vlq, vlen_bytes);
    if (bytes_written != vlen_bytes) {
        return -1;
    }

    // 写入键内容
    bytes_written = write(fd, k, klen);
    if (bytes_written != (ssize_t)klen) {
        return -1;
    }

    // 写入值内容
    bytes_written = write(fd, v, vlen);
    if (bytes_written != (ssize_t)vlen) {
        return -1;
    }

    return 0;
}

/**
 * 遍历数组结构，将所有KV对写入KSF文件
 * @param fd 文件描述符
 * @return 成功返回0，失败返回-1
 */
#if ENABLE_ARRAY
int ksfWriteArray(int fd) {
    for (int i = 0; i < global_array.total; i++) {
        kvs_array_item_t *item = &global_array.table[i];
        if (item->key != NULL) {  // 只写入非空项
            if (ksfWriteOneKv(fd, item->key, strlen(item->key), item->value, strlen(item->value)) != 0) {
                return -1;
            }
        }
    }
    return 0;
}
#endif

/**
 * 遍历红黑树结构，将所有KV对写入KSF文件
 * @param fd 文件描述符
 * @param node 当前节点
 * @return 成功返回0，失败返回-1
 */
#if ENABLE_RBTREE
int ksfWriteRbtreeRecurse(int fd, rbtree_node *node) {
    if (node == NULL || node->key == NULL) {
        return 0;
    }

    // 递归写入左子树
    if (ksfWriteRbtreeRecurse(fd, node->left) != 0) {
        return -1;
    }

    // 写入当前节点
    if (node->key != NULL && node->value != NULL) {
        if (ksfWriteOneKv(fd, node->key, strlen(node->key), (char*)node->value, strlen((char*)node->value)) != 0) {
            return -1;
        }
    }

    // 递归写入右子树
    if (ksfWriteRbtreeRecurse(fd, node->right) != 0) {
        return -1;
    }

    return 0;
}

int ksfWriteRbtree(int fd) {
    return ksfWriteRbtreeRecurse(fd, global_rbtree.root);
}
#endif

/**
 * 遍历哈希表结构，将所有KV对写入KSF文件
 * @param fd 文件描述符
 * @return 成功返回0，失败返回-1
 */
#if ENABLE_HASH
int ksfWriteHash(int fd) {
    hashtable_t *hash = &global_hash;
    for (int i = 0; i < hash->max_slots; i++) {
        hashnode_t *node = hash->nodes[i];
        while (node != NULL) {
            if (node->key != NULL) {
                if (ksfWriteOneKv(fd, node->key, strlen(node->key), node->value, strlen(node->value)) != 0) {
                    return -1;
                }
            }
            node = node->next;
        }
    }
    return 0;
}
#endif

/**
 * 保存KSF快照
 * @param filename 文件名
 * @return 成功返回0，失败返回-1
 */
int ksfSave(const char *filename) {
    char temp_filename[256];
    snprintf(temp_filename, sizeof(temp_filename), "temp-%d.ksf", getpid());

    int fd = open(temp_filename, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd == -1) {
        fprintf(stderr, "错误：无法创建临时KSF文件 %s\n", temp_filename);
        return -1;
    }

    int result = 0;

#if ENABLE_ARRAY
    if (ksfWriteArray(fd) != 0) {
        result = -1;
    }
#endif

#if ENABLE_RBTREE
    if (ksfWriteRbtree(fd) != 0) {
        result = -1;
    }
#endif

#if ENABLE_HASH
    if (ksfWriteHash(fd) != 0) {
        result = -1;
    }
#endif

    // 确保数据写入磁盘
    fsync(fd);

    // 关闭文件
    close(fd);

    if (result == 0) {
        // 重命名临时文件为最终文件名
        if (rename(temp_filename, filename) != 0) {
            fprintf(stderr, "错误：重命名KSF文件失败: %s\n", strerror(errno));
            // 删除临时文件
            unlink(temp_filename);
            return -1;
        }
        printf("KSF快照保存成功: %s\n", filename);
    } else {
        // 如果写入过程中出现错误，删除临时文件
        unlink(temp_filename);
        fprintf(stderr, "错误：写入KSF快照失败\n");
    }

    return result;
}

/**
 * 后台保存KSF快照（BGSAVE）
 * @return 成功返回0，失败返回-1
 */
int ksfSaveBackground() {
    pid_t pid = fork();
    if (pid == 0) {
        // 子进程：生成时间戳
        time_t now = time(0);
        struct tm *tm = localtime(&now);
        char filename[256];
        snprintf(filename, sizeof(filename), "dump-%04d%02d%02d-%02d%02d%02d.ksf",
                 tm->tm_year + 1900, tm->tm_mon + 1, tm->tm_mday,
                 tm->tm_hour, tm->tm_min, tm->tm_sec);

        // 在子进程中执行KSF保存
        int result = ksfSave(filename);
        exit(result == 0 ? 0 : 1); // 子进程退出码表示成功或失败
    } else if (pid > 0) {
        // 父进程
        printf("BGSAVE子进程已启动，PID: %d\n", pid);
        return 0; // 父进程立即返回
    } else {
        // fork失败
        fprintf(stderr, "错误：fork BGSAVE进程失败\n");
        return -1;
    }
}

/**
 * 获取最新的KSF快照文件
 * @return 最新快照文件名，如果不存在则返回NULL（需要手动释放）
 */
char* getLatestKsfFile() {
    DIR *dir;
    struct dirent *entry;
    char *latest_file = NULL;
    time_t latest_time = 0;

    dir = opendir(".");
    if (dir == NULL) {
        return NULL;
    }

    while ((entry = readdir(dir)) != NULL) {
        if (strncmp(entry->d_name, "dump-", 5) == 0 &&
            strstr(entry->d_name, ".ksf") != NULL) {

            // 解析文件名中的时间戳
            int year, month, day, hour, minute, second;
            if (sscanf(entry->d_name, "dump-%4d%2d%2d-%2d%2d%2d.ksf",
                      &year, &month, &day, &hour, &minute, &second) == 6) {

                struct tm tm_time = {0};
                tm_time.tm_year = year - 1900;
                tm_time.tm_mon = month - 1;
                tm_time.tm_mday = day;
                tm_time.tm_hour = hour;
                tm_time.tm_min = minute;
                tm_time.tm_sec = second;

                time_t file_time = mktime(&tm_time);
                if (file_time > latest_time) {
                    latest_time = file_time;
                    if (latest_file) {
                        free(latest_file);
                    }
                    latest_file = malloc(strlen(entry->d_name) + 1);
                    strcpy(latest_file, entry->d_name);
                }
            }
        }
    }

    closedir(dir);
    return latest_file;
}

/**
 * 从KSF文件加载数据
 * @param filename 文件名
 * @return 成功返回0，失败返回-1
 */
int ksfLoad(const char *filename) {
    int fd = open(filename, O_RDONLY);
    if (fd == -1) {
        fprintf(stderr, "错误：无法打开KSF文件 %s\n", filename);
        return -1;
    }

    uint8_t buffer[16];
    uint64_t klen, vlen;
    char *key = NULL, *value = NULL;
    int result = 0;

    while (1) {
        // 读取键长度
        ssize_t bytes_read = read(fd, buffer, sizeof(buffer));
        if (bytes_read <= 0) {
            break; // 文件结束或出错
        }

        int offset = 0;
        int klen_bytes = decode_vlq(buffer, &klen);
        offset += klen_bytes;

        // 读取值长度
        int vlen_bytes = decode_vlq(buffer + klen_bytes, &vlen);
        offset += vlen_bytes;

        // 为键和值分配内存
        key = malloc(klen + 1);
        value = malloc(vlen + 1);
        if (key == NULL || value == NULL) {
            fprintf(stderr, "错误：内存分配失败\n");
            result = -1;
            break;
        }

        // 读取键内容
        if (read(fd, key, klen) != (ssize_t)klen) {
            fprintf(stderr, "错误：读取键数据失败\n");
            result = -1;
            break;
        }
        key[klen] = '\0';

        // 读取值内容
        if (read(fd, value, vlen) != (ssize_t)vlen) {
            fprintf(stderr, "错误：读取值数据失败\n");
            result = -1;
            break;
        }
        value[vlen] = '\0';

        // 根据当前使用的数据结构插入数据
        // 这里使用数组实现为例，你可以根据实际使用的结构适配
#if ENABLE_ARRAY
        kvs_array_set(&global_array, key, value);
#endif

#if ENABLE_RBTREE
        kvs_rbtree_set(&global_rbtree, key, value);
#endif

#if ENABLE_HASH
        kvs_hash_set(&global_hash, key, value);
#endif

        // 释放内存
        free(key);
        free(value);
        key = NULL;
        value = NULL;
    }

    if (key) free(key);
    if (value) free(value);

    close(fd);

    if (result == 0) {
        printf("KSF数据加载成功: %s\n", filename);
    } else {
        fprintf(stderr, "错误：加载KSF数据失败\n");
    }

    return result;
}