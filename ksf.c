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
#if ENABLE_RBTREE
extern kvs_rbtree_t global_rbtree;
#elif ENABLE_HASH
extern kvs_hash_t global_hash;
#elif ENABLE_ARRAY
extern kvs_array_t global_array;
#endif


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



#if ENABLE_RBTREE
/**
 * 遍历红黑树结构，将所有KV对写入KSF文件
 * @param fd 文件描述符
 * @param node 当前节点
 * @return 成功返回0，失败返回-1
 */
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
    return ksfWriteRbtreeRecurse(fd, global_main_engine.root);
  }
  
  #elif ENABLE_HASH
  /**
   * 遍历哈希表结构，将所有KV对写入KSF文件
   * @param fd 文件描述符
   * @return 成功返回0，失败返回-1
   */
  int ksfWriteHash(int fd) {
      hashtable_t *hash = &global_main_engine;  // 注意：这里使用global_main_engine
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
  #elif ENABLE_ARRAY
  /**
   * 遍历数组结构，将所有KV对写入KSF文件
   * @param fd 文件描述符
   * @return 成功返回0，失败返回-1
   */
  int ksfWriteArray(int fd) {
      for (int i = 0; i < global_main_engine.total; i++) {
          kvs_array_item_t *item = &global_main_engine.table[i];
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

    // 只需要对主引擎进行数据保存
    int result = 0;

    // 通过遍历主引擎中的所有键值对来保存数据
    // 由于我们无法直接遍历所有三种数据结构的通用接口，
    // 我们仍需要根据启用的类型来调用对应的写入函数
    #if ENABLE_RBTREE
        if (ksfWriteRbtree(fd) != 0) {
            result = -1;
        }
    #elif ENABLE_HASH
        if (ksfWriteHash(fd) != 0) {
            result = -1;
        }
    #elif ENABLE_ARRAY
        if (ksfWriteArray(fd) != 0) {
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
        // 在子进程中执行KSF保存到固定文件名
        int result = ksfSave("dump.ksf");
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
 * 从KSF文件加载数据
 * @param filename 文件名
 * @return 成功返回0，失败返回-1
 */
int ksfLoad(const char *filename) {
    printf("开始加载KSF快照文件: %s\n", filename);

    // 检查文件是否存在
    FILE* file = fopen(filename, "rb");
    if (!file) {
        printf("KSF快照文件不存在或无法打开: %s\n", filename);
        return 0; // 文件不存在是正常的，返回成功
    }

    // 获取文件大小
    fseek(file, 0, SEEK_END);
    long file_size = ftell(file);
    fseek(file, 0, SEEK_SET);

    if (file_size == 0) {
        printf("KSF快照文件为空: %s\n", filename);
        fclose(file);
        return 0;
    }

    // 分配缓冲区读取整个文件
    char* buffer = (char*)malloc(file_size);
    if (!buffer) {
        fprintf(stderr, "无法分配内存来加载KSF快照文件\n");
        fclose(file);
        return -1;
    }

    // 读取文件内容
    size_t bytes_read = fread(buffer, 1, file_size, file);
    if (bytes_read != file_size) {
        fprintf(stderr, "读取KSF快照文件时发生错误\n");
        kvs_free(buffer);
        fclose(file);
        return -1;
    }

    fclose(file);

    // 解析KSF内容并恢复数据
    long pos = 0;
    while (pos < file_size) {
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
            key = (char*)malloc(key_len + 1);
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
            value = (char*)malloc(val_len + 1);
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

        // 执行SET操作将KV对加载到主存储引擎中
        kvs_main_set(&global_main_engine, key, value);

        // 释放分配的内存
        if (key) kvs_free(key);
        if (value) kvs_free(value);
    }

    kvs_free(buffer);
    printf("KSF快照文件加载完成: %s\n", filename);
    return 0;
}
