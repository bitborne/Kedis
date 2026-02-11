#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "../../include/kvstore.h"

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
#elif ENABLE_SKIPLIST
extern kvs_skiplist_t skiplist_engine;
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
static ssize_t write_all(int fd, const void* buf, size_t count) {
  const char* p = buf;
  size_t written = 0;
  while (written < count) {
    ssize_t n = write(fd, p + written, count - written);
    if (n <= 0) {
      if (n == -1 && errno == EINTR) continue;  // 被中断，继续写入
      return -1;                                // 错误
    }
    written += n;
  }
  last_write_time = time(NULL);
  return (ssize_t)written;
}

/**
 * mmap 文件映射上下文结构体
 * 用于管理 mmap 映射的生命周期和资源
 */
typedef struct {
  char* mmap_addr;       // mmap() 返回的映射起始地址，指向映射区域的开始
  size_t mmap_size;      // mmap() 实际映射的大小（向上取整到页大小倍数）
  int fd;                // 打开的文件描述符，用于后续的 munmap() 和 close()
  size_t file_size;      // 文件的实际字节大小（可能小于 mmap_size）
  const char* filename;  // 文件名字符串指针，用于打印错误信息
} mmap_context_t;

/**
 * 将变长整数编码为VLQ（Variable Length Quantity）格式
 * @param value 要编码的值
 * @param output 存储编码结果的缓冲区
 * @return 编码后占用的字节数
 */
static int encode_vlq(uint64_t value, uint8_t* output) {
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
static int decode_vlq(const uint8_t* input, uint64_t* value) {
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
 * 打开文件并使用 mmap 映射到内存
 * @param filename 要打开的文件路径
 * @param ctx 输出参数，用于存储 mmap 映射的上下文信息
 * @return 成功返回 0，失败返回 -1
 *
 * 功能说明：
 * 1. 使用 open() 打开文件（只读模式）
 * 2. 使用 fstat() 获取文件大小
 * 3. 计算映射大小（向上取整到页大小倍数）
 * 4. 使用 mmap() 将文件映射到内存
 * 5. 将映射信息存储到 ctx 中
 */
static int mmap_open_file(const char* filename, mmap_context_t* ctx) {
  // 步骤 1：使用 open() 打开文件，O_RDONLY 表示只读模式
  ctx->fd = open(filename, O_RDONLY);

  // 检查 open() 是否成功
  if (ctx->fd == -1) {
    // 如果文件不存在（errno == ENOENT），这是正常的，返回成功
    if (errno == ENOENT) {
      return 0;
    }
    // 其他错误情况，打印错误信息并返回失败
    fprintf(stderr, "错误：无法打开文件 %s: %s\n", filename, strerror(errno));
    return -1;
  }

  // 步骤 2：使用 fstat() 获取文件状态信息
  struct stat stat_buf;
  if (fstat(ctx->fd, &stat_buf) == -1) {
    // fstat() 失败，打印错误信息并清理资源
    fprintf(stderr, "错误：无法获取文件状态 %s: %s\n", filename,
            strerror(errno));
    close(ctx->fd);  // 关闭文件描述符
    return -1;
  }

  // 步骤 3：将文件大小存储到 ctx 中
  ctx->file_size = stat_buf.st_size;
  ctx->filename = filename;

  // 步骤 4：检查文件是否为空
  if (ctx->file_size == 0) {
    // 文件为空，打印信息并清理资源
    printf("文件为空: %s\n", filename);
    close(ctx->fd);  // 关闭文件描述符
    return 0;
  }

  // 步骤 5：获取系统页大小（通常是 4096 字节）
  long page_size = sysconf(_SC_PAGESIZE);

  // 步骤 6：计算 mmap 映射大小（向上取整到页大小倍数）
  // 公式：((file_size + page_size - 1) / page_size) * page_size
  ctx->mmap_size = ((ctx->file_size + page_size - 1) / page_size) * page_size;

  // 步骤 7：执行 mmap 映射
  // 参数说明：
  // - NULL: 让内核选择映射地址
  // - ctx->mmap_size: 映射的大小
  // - PROT_READ: 只读权限
  // - MAP_PRIVATE: 私有映射（不会修改文件）
  // - ctx->fd: 文件描述符
  // - 0: 文件偏移量（从文件开头开始）
  ctx->mmap_addr =
      mmap(NULL, ctx->mmap_size, PROT_READ, MAP_PRIVATE, ctx->fd, 0);

  // 步骤 8：检查 mmap() 是否成功
  if (ctx->mmap_addr == MAP_FAILED) {
    // mmap() 失败，打印错误信息并清理资源
    fprintf(stderr, "错误：mmap 失败 %s: %s\n", filename, strerror(errno));
    close(ctx->fd);  // 关闭文件描述符
    return -1;
  }

  // 步骤 9：打印成功信息
  printf("mmap 映射成功: %s (大小: %zu 字节)\n", filename, ctx->file_size);

  // 步骤 10：返回成功
  return 0;
}

/**
 * 关闭 mmap 映射并释放资源
 * @param ctx mmap 上下文，包含映射信息
 *
 * 功能说明：
 * 1. 如果 mmap_addr 非空，调用 munmap() 取消映射
 * 2. 如果 fd 有效，调用 close() 关闭文件
 * 3. 清零 ctx 中的所有字段，防止悬空指针
 */
static void mmap_close_file(mmap_context_t* ctx) {
  // 步骤 1：检查 mmap_addr 是否非空且 mmap_size 大于 0
  if (ctx->mmap_addr != NULL && ctx->mmap_size > 0) {
    // 调用 munmap() 取消映射
    // 参数说明：
    // - ctx->mmap_addr: 映射的起始地址
    // - ctx->mmap_size: 映射的大小
    munmap(ctx->mmap_addr, ctx->mmap_size);

    // 清零映射地址和大小，防止悬空指针
    ctx->mmap_addr = NULL;
    ctx->mmap_size = 0;
  }

  // 步骤 2：检查文件描述符是否有效
  if (ctx->fd != -1) {
    // 调用 close() 关闭文件描述符
    close(ctx->fd);

    // 清零文件描述符
    ctx->fd = -1;
  }

  // 步骤 3：清零文件大小和文件名
  ctx->file_size = 0;
  ctx->filename = NULL;
}

/**
 * 使用 mmap 从 AOF 文件加载数据到指定引擎
 * @param filename AOF 文件名
 * @param engine_type 引擎类型: 0=array, 1=hash, 2=rbtree, 3=skiplist
 * @return 成功返回 0，失败返回 -1
 *
 * 功能说明：
 * 1. 使用 mmap 打开文件
 * 2. 直接在映射内存中解析 VLQ 编码的命令
 * 3. 为每个 key/value 创建临时字符串（引用映射内存）
 * 4. 调用引擎 SET 操作
 * 5. 关闭 mmap 映射
 */
static int aofLoadToEngine_mmap(const char* filename, int engine_type) {
  // 步骤 1：打印开始加载信息
  printf("开始加载 AOF 文件（mmap 方式）: %s\n", filename);

  // 步骤 2：初始化 mmap 上下文结构体，所有字段清零
  mmap_context_t ctx = {0};

  // 步骤 3：调用 mmap_open_file() 打开文件并映射到内存
  if (mmap_open_file(filename, &ctx) != 0) {
    // 打开失败，返回错误
    return -1;
  }

  // 步骤 4：检查文件是否为空或不存在
  // 如果 mmap_addr 为 NULL 或 file_size 为 0，说明文件不存在或为空
  if (ctx.mmap_addr == NULL || ctx.file_size == 0) {
    // 文件不存在或为空是正常的，返回成功
    return 0;
  }

  // 步骤 5：初始化解析变量
  size_t pos = 0;              // 当前解析位置（偏移量）
  char* data = ctx.mmap_addr;  // 指向映射内存的指针，用于读取数据
  int cmd_count = 0;           // 已解析的命令计数器

  // 步骤 6：开始解析 AOF 内容
  // 循环条件：当前解析位置小于文件大小
  while (pos < ctx.file_size) {
    // 步骤 6.1：读取命令码（1 字节）
    // 检查是否还有数据可读
    // 从映射内存中读取 1 字节的命令码
    uint8_t cmd_type = (uint8_t)data[pos++];
    // fprintf(stderr, "读取到的命令码: %02X\n", cmd_type);
    // 步骤 6.2：解码键长度（VLQ 格式）
    // 检查是否还有数据可读
    if (pos >= ctx.file_size) break;

    // 声明变量存储键长度
    uint64_t key_len;

    // 调用 decode_vlq() 解码键长度
    // 参数：data + pos 是指向 VLQ 编码数据的指针
    // 返回值：VLQ 编码占用的字节数
    int key_len_bytes = decode_vlq((const uint8_t*)(data + pos), &key_len);

    // fprintf(stderr, "decode: key_len: %zu\n", key_len);
    // 更新解析位置，跳过 VLQ 编码的字节
    pos += key_len_bytes;

    // 步骤 6.3：解码值长度（VLQ 格式）
    // 检查是否还有数据可读
    if (pos >= ctx.file_size) break;

    // 声明变量存储值长度
    uint64_t val_len;

    // 调用 decode_vlq() 解码值长度
    int val_len_bytes = decode_vlq((const uint8_t*)(data + pos), &val_len);
    // fprintf(stderr, "decode: val_len: %zu\n", val_len);

    // 更新解析位置，跳过 VLQ 编码的字节
    pos += val_len_bytes;

    // 步骤 6.4：检查边界条件
    // 计算 key 和 value 占用的总字节数
    // 如果 pos + key_len + val_len 超过文件大小，说明文件格式错误
    if (pos + key_len + val_len + 2 > ctx.file_size) {
      // 打印错误信息
      fprintf(stderr, "错误：AOF 文件格式错误（超出文件边界）\n");

      // 关闭 mmap 映射，释放资源
      mmap_close_file(&ctx);

      // 返回失败
      return -1;
    }

    // 步骤 6.5：获取 key 和 value 的指针（直接引用映射内存，不拷贝）
    // key_ptr 指向映射内存中 key 的起始位置
    robj key = {0};
    key.ptr = (key_len > 0) ? (data + pos) : NULL;
    key.len = (key_len > 0) ? key_len : 0;
    // 更新解析位置，跳过 key 的内容
    pos += key_len + 1; // 还需要跳过 \0

    // val_ptr 指向映射内存中 value 的起始位置
    robj value = {0};
    value.ptr = (val_len > 0) ? (data + pos) : NULL;
    value.len = (val_len > 0) ? val_len : 0;

    // 更新解析位置，跳过 value 的内容
    pos += val_len + 1; // 还需要跳过 \0

    // 步骤 6.6：根据命令类型执行相应的操作
    // 直接传递映射内存中的指针，引擎内部会分配内存并拷贝数据
    // 这样实现了真正的零拷贝，并且支持任意大小的数据（4MB+）

    // 使用 switch 语句处理不同的命令类型
    switch (cmd_type) {
      // 处理 AOF_CMD_SET 命令
      case AOF_CMD_SET:
#if ENABLE_MULTI_ENGINE
        // 多引擎模式：根据 engine_type 调用相应的 SET 函数
        // 直接传递映射内存指针，引擎内部会分配内存并拷贝数据
        if (engine_type == 0) {
// engine_type == 0 表示 Array 引擎
#if ENABLE_ARRAY
          kvs_array_set(&array_engine, &key, &value);
#endif
        } else if (engine_type == 1) {
// engine_type == 1 表示 Hash 引擎
#if ENABLE_HASH
          kvs_hash_set(&hash_engine, &key, &value);
#endif
        } else if (engine_type == 2) {
// engine_type == 2 表示 Rbtree 引擎
#if ENABLE_RBTREE
          kvs_rbtree_set(&rbtree_engine, &key, &value);
#endif
        } else if (engine_type == 3) {
// engine_type == 3 表示 Skiplist 引擎
#if ENABLE_SKIPLIST
          kvs_skiplist_set(&skiplist_engine, &key, &value);
#endif
        }
#else
        // 单引擎模式：调用统一的 SET 函数
        kvs_main_set(&global_main_engine, &key_ptr, &val_ptr);
#endif
        break;  // 退出 switch

      // 处理 AOF_CMD_MOD 命令
      case AOF_CMD_MOD:
#if ENABLE_MULTI_ENGINE
        // 多引擎模式：根据 engine_type 调用相应的 MOD 函数
        // 直接传递映射内存指针，引擎内部会分配内存并拷贝数据
        if (engine_type == 0) {
// engine_type == 0 表示 Array 引擎
#if ENABLE_ARRAY
          kvs_array_mod(&array_engine, &key, &value);
#endif
        } else if (engine_type == 1) {
// engine_type == 1 表示 Hash 引擎
#if ENABLE_HASH
          kvs_hash_mod(&hash_engine, &key, &value);
#endif
        } else if (engine_type == 2) {
// engine_type == 2 表示 Rbtree 引擎
#if ENABLE_RBTREE
          kvs_rbtree_mod(&rbtree_engine, &key, &value);
#endif
        } else if (engine_type == 3) {
// engine_type == 3 表示 Skiplist 引擎
#if ENABLE_SKIPLIST
          kvs_skiplist_mod(&skiplist_engine, &key, &value);
#endif
        }
#else
        // 单引擎模式：调用统一的 MOD 函数
        kvs_main_mod(&global_main_engine, &key, &value);
#endif
        break;  // 退出 switch

      // 处理 AOF_CMD_DEL 命令
      case AOF_CMD_DEL:
#if ENABLE_MULTI_ENGINE
        // 多引擎模式：根据 engine_type 调用相应的 DEL 函数
        // 直接传递映射内存指针，引擎内部会分配内存并拷贝数据
        if (engine_type == 0) {
// engine_type == 0 表示 Array 引擎
#if ENABLE_ARRAY
          kvs_array_del(&array_engine, &key);
#endif
        } else if (engine_type == 1) {
// engine_type == 1 表示 Hash 引擎
#if ENABLE_HASH
          kvs_hash_del(&hash_engine, &key);
#endif
        } else if (engine_type == 2) {
// engine_type == 2 表示 Rbtree 引擎
#if ENABLE_RBTREE
          kvs_rbtree_del(&rbtree_engine, &key);
#endif
        } else if (engine_type == 3) {
// engine_type == 3 表示 Skiplist 引擎
#if ENABLE_SKIPLIST
          kvs_skiplist_del(&skiplist_engine, &key);
#endif
        }
#else
        // 单引擎模式：调用统一的 DEL 函数
        kvs_main_del(&global_main_engine, &key);
#endif
        break;  // 退出 switch

      // 处理未知命令类型
      default:
        // 打印错误信息，显示未知的命令类型
        fprintf(stderr, "未知的 AOF 命令类型: %02X\n", cmd_type);
        break;  // 退出 switch
    }

    // 步骤 6.10：增加命令计数器
    cmd_count++;
  }

  // 步骤 7：关闭 mmap 映射，释放资源
  // 调用 mmap_close_file() 取消映射并关闭文件
  mmap_close_file(&ctx);

  // 步骤 8：打印加载完成信息
  // 显示文件名和已解析的命令数量
  printf("AOF 文件加载完成: %s (共 %d 条命令)\n", filename, cmd_count);

  // 步骤 9：返回成功
  return 0;
}

/**
 * 将命令追加到AOF缓冲区（使用新的二进制格式）
 * 更新：实现混合写入策略（小命令缓冲+大命令直写）
 * 注意：此函数仅在单引擎模式下使用
 * @param type 命令类型: CMD_SET, CMD_MOD, CMD_DEL
 * @param key 键
 * @param value 值
 */
void appendToAofBuffer(int type, const robj* key, const robj* value) {
#if !ENABLE_MULTI_ENGINE
  if (type != AOF_CMD_DEL &&
      (key == NULL || value == NULL || key->ptr == NULL || value->ptr == NULL))
    return;
  if (type == AOF_CMD_DEL && key == NULL) return;

  // // REPLICATION BROADCAST
  // char cmd_text[4096];
  // if (type == AOF_CMD_SET) {
  //     snprintf(cmd_text, sizeof(cmd_text), "SET %s %s", key, value);
  //     replication_feed_slaves(cmd_text);
  // } else if (type == AOF_CMD_MOD) {
  //     snprintf(cmd_text, sizeof(cmd_text), "MOD %s %s", key, value);
  //     replication_feed_slaves(cmd_text);
  // } else if (type == AOF_CMD_DEL) {
  //     snprintf(cmd_text, sizeof(cmd_text), "DEL %s", key);
  //     replication_feed_slaves(cmd_text);
  // }

  char* aof_buf = aofBuffer.buf;

  int klen = key ? key->len + 1 : 0;
  int vlen =
      (value int vlen = (value->ptr && type != AOF_CMD_DEL) ? value->len : 0;
       int vlen = (value->ptr && type != AOF_CMD_DEL) ? value->len : 0;
       type != AOF_CMD_DEL)
          ? value->len + 1
          : 0;

  uint8_t vlq[16];
  int key_len_bytes = encode_vlq(klen, vlq);
  int val_len_bytes = encode_vlq(vlen, vlq + key_len_bytes);

  int total_needed = 1 + key_len_bytes + val_len_bytes + klen + vlen;

  // 检查是否为大命令，如果是则绕过缓冲区直接写入
  if (total_needed >= LARGE_CMD_THRESHOLD) {
    printf("大命令直接写入\n");
    // 先刷新缓冲区，确保命令顺序一致
    flushAofBuffer();

    // 构造命令数据（使用堆上分配，避免栈溢出）
    char* cmd_data = kvs_malloc(total_needed);  // 在堆上分配缓冲区
    if (!cmd_data) {
      fprintf(stderr, "AOF错误：无法分配大命令缓冲区\n");
      return;
    }
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
      memcpy(cmd_data + pos, key->ptr, klen);
      pos += klen;
    }

    // 添加值内容
    if (vlen > 0) {
      memcpy(cmd_data + pos, value->ptr, vlen);
      pos += vlen;
    }

    // 直接写入大命令
    if (write_all(aof_fd, cmd_data, pos) < 0) {
      fprintf(stderr, "AOF错误：无法写入大命令: %s\n", strerror(errno));
    }

    // 释放堆上分配的缓冲区
    kvs_free(cmd_data);
    return;
  }

  // 小命令：尝试追加到缓冲区
  if (aofBuffer.len + total_needed > AOF_BUF_SIZE) {
    flushAofBuffer();  // 缓冲区满，先flush
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
    memcpy(aof_buf + aofBuffer.len, key->ptr, klen);
    aofBuffer.len += klen;
  }

  // 添加值内容
  if (vlen > 0) {
    memcpy(aof_buf + aofBuffer.len, value->ptr, vlen);
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
 * @param engine_type 引擎类型: 0=array, 1=hash, 2=rbtree, 3=skiplist
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
    } else if (engine_type == 3) {
#if ENABLE_SKIPLIST
      target_fd = aof_fd_skiplist;
#endif
    }
    if (target_fd != -1) {
      ssize_t result =
          write_all(target_fd, aof_buf, aofBuffer[engine_type].len);
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
#if ENABLE_SKIPLIST
    if (aof_fd_skiplist != -1) {
      fsync(aof_fd_skiplist);
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
  aof_fd_rbtree =
      open(aof_filename_rbtree, O_WRONLY | O_CREAT | O_APPEND, 0644);
  if (aof_fd_rbtree == -1) {
    fprintf(stderr, "错误：无法打开AOF文件 %s\n", aof_filename_rbtree);
    return -1;
  }
  printf("Rbtree引擎AOF文件已打开: %s\n", aof_filename_rbtree);
#endif

#if ENABLE_SKIPLIST
  aof_fd_skiplist =
      open(aof_filename_skiplist, O_WRONLY | O_CREAT | O_APPEND, 0644);
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
    return 0;  // 文件不存在是正常的，返回成功
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
    robj key = {0};
    key.len = (key_len > 0) ? (key_len) : 0;
    if (key_len > 0) {
      key.ptr = (char*)kvs_malloc(key_len);
      if (!key.ptr) {
        fprintf(stderr, "无法分配内存来存储键\n");
        kvs_free(buffer);
        return -1;
      }
      memcpy(key.ptr, buffer + pos, key_len + 1);
      pos += key_len + 1;
    }

    // 读取值内容
    robj value = {0};
    value.len = (val_len > 0) ? (val_len) : 0;
    if (val_len > 0) {
      if (pos + val_len > file_size) {
        if (key.ptr) kvs_free(key.ptr);
        kvs_free(buffer);
        return -1;
      }
      value.ptr = (char*)kvs_malloc(val_len);
      if (!value.ptr) {
        fprintf(stderr, "无法分配内存来存储值\n");
        if (key.ptr) kvs_free(key.ptr);
        kvs_free(buffer);
        return -1;
      }
      memcpy(value.ptr, buffer + pos, val_len + 1);
      pos += val_len + 1;
    }

    // 根据引擎类型和命令类型执行相应的操作
    // int result = 0;  // unused variable
    switch (cmd_type) {
      case AOF_CMD_SET:
#if ENABLE_MULTI_ENGINE
        if (engine_type == 0) {
#if ENABLE_ARRAY
          kvs_array_set(&array_engine, &key, &value);
#endif
        } else if (engine_type == 1) {
#if ENABLE_HASH
          kvs_hash_set(&hash_engine, &key, &value);
#endif
        } else if (engine_type == 2) {
#if ENABLE_RBTREE
          kvs_rbtree_set(&rbtree_engine, &key, &value);
#endif
        } else if (engine_type == 3) {
#if ENABLE_SKIPLIST
          kvs_skiplist_set(&skiplist_engine, &key, &value);
#endif
        }
#else
        kvs_main_set(&global_main_engine, &key, &value);
#endif
        break;

      case AOF_CMD_MOD:
#if ENABLE_MULTI_ENGINE
        if (engine_type == 0) {
#if ENABLE_ARRAY
          kvs_array_mod(&array_engine, &key, &value);
#endif
        } else if (engine_type == 1) {
#if ENABLE_HASH
          kvs_hash_mod(&hash_engine, &key, &value);
#endif
        } else if (engine_type == 2) {
#if ENABLE_RBTREE
          kvs_rbtree_mod(&rbtree_engine, &key, &value);
#endif
        } else if (engine_type == 3) {
#if ENABLE_SKIPLIST
          kvs_skiplist_mod(&skiplist_engine, &key, &value);
#endif
        }
#else
        kvs_main_mod(&global_main_engine, &key, &value);
#endif
        break;

      case AOF_CMD_DEL:
#if ENABLE_MULTI_ENGINE
        if (engine_type == 0) {
#if ENABLE_ARRAY
          kvs_array_del(&array_engine, &key);
#endif
        } else if (engine_type == 1) {
#if ENABLE_HASH
          kvs_hash_del(&hash_engine, &key);
#endif
        } else if (engine_type == 2) {
#if ENABLE_RBTREE
          kvs_rbtree_del(&rbtree_engine, &key);
#endif
        } else if (engine_type == 3) {
#if ENABLE_SKIPLIST
          kvs_skiplist_del(&skiplist_engine, &key);
#endif
        }
#else
        kvs_main_del(&global_main_engine, &key);
#endif
        break;

      default:
        fprintf(stderr, "未知的AOF命令类型: %d\n", cmd_type);
        break;
    }

    // 释放分配的内存
    if (key.ptr) kvs_free(key.ptr);
    if (value.ptr) kvs_free(value.ptr);
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
 * 加载所有引擎的 AOF 文件（使用 mmap 优化）
 * @return 成功返回 0，失败返回 -1
 *
 * 功能说明：
 * 1. 根据 ENABLE_MULTI_ENGINE 宏决定加载策略
 * 2. 多引擎模式：加载所有引擎的 AOF 文件
 * 3. 单引擎模式：只加载单个引擎的 AOF 文件
 */
int aofLoadAll_mmap() {
#if ENABLE_MULTI_ENGINE
// 多引擎模式：加载所有引擎的 AOF 文件
// 步骤 1：加载 Array 引擎的 AOF 文件
#if ENABLE_ARRAY
  if (aofLoadToEngine_mmap(aof_filename_array, 0) != 0) {
    // 加载失败，返回错误
    return -1;
  }
#endif

// 步骤 2：加载 Hash 引擎的 AOF 文件
#if ENABLE_HASH
  if (aofLoadToEngine_mmap(aof_filename_hash, 1) != 0) {
    // 加载失败，返回错误
    return -1;
  }
#endif

// 步骤 3：加载 Rbtree 引擎的 AOF 文件
#if ENABLE_RBTREE
  if (aofLoadToEngine_mmap(aof_filename_rbtree, 2) != 0) {
    // 加载失败，返回错误
    return -1;
  }
#endif

// 步骤 4：加载 Skiplist 引擎的 AOF 文件
#if ENABLE_SKIPLIST
  if (aofLoadToEngine_mmap(aof_filename_skiplist, 3) != 0) {
    // 加载失败，返回错误
    return -1;
  }
#endif

  // 所有引擎加载成功，返回成功
  return 0;
#else
  // 单引擎模式：只加载一个 AOF 文件
  // 调用 aofLoadToEngine_mmap() 加载主引擎的 AOF 文件
  return aofLoadToEngine_mmap(aof_filename, -1);
#endif
}

/**
 * 向指定引擎的AOF文件写入命令（用于多引擎模式）
 * @param engine_type 引擎类型: 0=array, 1=hash, 2=rbtree, 3=skiplist
 * @param type 命令类型
 * @param key 键
 * @param value 值
 */
void appendToAofBufferToEngine(int engine_type, int type, const robj* key,
                               const robj* value) {
  if (type != AOF_CMD_DEL && (!key || !value || !key->ptr || !value->ptr))
    return;
  if (type == AOF_CMD_DEL && (!key || !key->ptr)) return;

  // REPLICATION BROADCAST
  // char cmd_text[4096];
  // if (type == AOF_CMD_SET) {
  //     snprintf(cmd_text, sizeof(cmd_text), "SET %s %s", key, value);
  //     replication_feed_slaves(cmd_text);
  // } else if (type == AOF_CMD_MOD) {
  //     snprintf(cmd_text, sizeof(cmd_text), "MOD %s %s", key, value);
  //     replication_feed_slaves(cmd_text);
  // } else if (type == AOF_CMD_DEL) {
  //     snprintf(cmd_text, sizeof(cmd_text), "DEL %s", key);
  //     replication_feed_slaves(cmd_text);
  // }

  char* aof_buf = aofBuffer[engine_type].buf;
  // printf("-->aof 1\n");
  int klen = key->ptr ? key->len : 0;
  int vlen = (type != AOF_CMD_DEL) ? value->len : 0;

  uint8_t vlq[16];
  int key_len_bytes = encode_vlq(klen, vlq);
  int val_len_bytes = encode_vlq(vlen, vlq + key_len_bytes);

  int total_needed = 1 + key_len_bytes + val_len_bytes + klen + vlen + 2;
  // printf("-->aof end\n");

  // 检查是否为大命令，如果是则绕过缓冲区直接写入
  if (total_needed >= LARGE_CMD_THRESHOLD) {
    printf("大命令直接写入\n");
    // 先刷新缓冲区，确保命令顺序一致
    flushAofBufferToEngine(engine_type);

    // 构造命令数据（使用堆上分配，避免栈溢出）
    char* cmd_data = kvs_malloc(total_needed);  // 在堆上分配缓冲区
    if (!cmd_data) {
      fprintf(stderr, "AOF错误：无法分配大命令缓冲区\n");
      return;
    }
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
      memcpy(cmd_data + pos, key->ptr, klen);  // klen = key->len + 1 // 已经带上\0了
      pos += klen;
      cmd_data[pos] = '\0';
      pos++;
    }

    // 添加值内容
    if (vlen > 0) {
      memcpy(cmd_data + pos, value->ptr, vlen);
      pos += vlen;
      cmd_data[pos] = '\0';
      pos++;
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
    } else if (engine_type == 3) {
#if ENABLE_SKIPLIST
      target_fd = aof_fd_skiplist;
#endif
    }
#else
    target_fd = aof_fd;
#endif

    if (target_fd != -1 && write_all(target_fd, cmd_data, pos) < 0) {
      fprintf(stderr, "AOF错误：无法写入大命令: %s\n", strerror(errno));
    }

    // 释放堆上分配的缓冲区
    kvs_free(cmd_data);
    return;
  } else {

    // [小命令]: 尝试追加到缓冲区
    if (aofBuffer[engine_type].len + total_needed > AOF_BUF_SIZE) {
      flushAofBufferToEngine(engine_type);  // 缓冲区满，先flush
    }
  
    // 添加命令码（1字节）
    aof_buf[aofBuffer[engine_type].len++] = (uint8_t)type;
  
    // 添加键长度（VLQ编码）
    memcpy(aof_buf + aofBuffer[engine_type].len, vlq, key_len_bytes);
    aofBuffer[engine_type].len += key_len_bytes;
  
    // 添加值长度（VLQ编码）
    memcpy(aof_buf + aofBuffer[engine_type].len, vlq + key_len_bytes,
           val_len_bytes);
    aofBuffer[engine_type].len += val_len_bytes;
  
    // 添加键内容
    if (klen > 0) {
      memcpy(aof_buf + aofBuffer[engine_type].len, key->ptr, klen);
      aofBuffer[engine_type].len += klen;
      aof_buf[aofBuffer[engine_type].len] = '\0';
      aofBuffer[engine_type].len++;
    }
    
    // 添加值内容
    if (vlen > 0) {
      memcpy(aof_buf + aofBuffer[engine_type].len, value->ptr, vlen);
      aofBuffer[engine_type].len += vlen;
      aof_buf[aofBuffer[engine_type].len] = '\0';
      aofBuffer[engine_type].len++;
    }
  }
}
