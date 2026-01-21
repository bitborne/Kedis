#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include "../../include/kvs_protocol.h"
#include "../../kvstore.h"

/*
 * 解析单条命令的token
 */
// int kvs_split_token(char* msg, char* tokens[]) {
//   if (msg == NULL || tokens == NULL) return -1;

//   int len = strlen(msg);
//   if (len > 0 && msg[len - 1] == '\n') {
//     if (len > 1 && msg[len - 2] == '\r') {
//       msg[len - 2] = '\0';  // Handle CRLF
//     } else {
//       msg[len - 1] = '\0';  // Handle LF only
//     }
//   }

//   int idx = 0;
//   char* p = msg;
  
//   while (*p && idx < KVS_MAX_TOKENS) {
//     // 跳过空格
//     while (*p == ' ') p++;
//     if (*p == '\0') break;
    
//     // 检查是否是引号包裹
//     if (*p == '"') {
//       p++;  // 跳过开始的引号
//       tokens[idx++] = p;  // 参数开始
      
//       // 查找结束引号
//       while (*p && *p != '"') {
//         // 处理转义字符
//         if (*p == '\\' && *(p + 1)) {
//           p++;
//           if (*p == 'r') {
//             *p = '\r';
//           } else if (*p == 'n') {
//             *p = '\n';
//           } else if (*p == 't') {
//             *p = '\t';
//           } else if (*p == '\\') {
//             *p = '\\';
//           } else if (*p == '"') {
//             *p = '"';
//           }
//         }
//         p++;
//       }
      
//       if (*p == '"') {
//         *p = '\0';  // 终止参数
//         p++;  // 跳过结束引号
//       }
//     } else {
//       // 普通参数（无引号）
//       tokens[idx++] = p;
      
//       // 查找下一个空格或结束
//       while (*p && *p != ' ') p++;
      
//       if (*p) {
//         *p = '\0';  // 临时终止
//         p++;
//       }
//     }
//   }

//   return idx;
// }

// /*
//  * 分割多条命令
//  */
// int kvs_split_multicmd(char* msg, char* commands[], int max_commands) {
//   if (msg == NULL || commands == NULL || max_commands <= 0) return -1;

//   int cmd_count = 0;
//   char* start = msg;
//   char* pos = msg;
//   int len = strlen(msg);

//   while (pos < msg + len && cmd_count < max_commands) {
//     // 查找命令分隔符 & 或 &&
//     if (*pos == '&') {
//       // 检查是单个&还是&&
//       int is_andand = 0;
//       if (pos + 1 < msg + len && *(pos + 1) == '&') {
//         is_andand = 1;
//       }

//       // 临时结束字符串以分割命令
//       *pos = '\0';

//       // 复制命令到数组
//       commands[cmd_count] = kvs_malloc(strlen(start) + 1);
//       if (commands[cmd_count] == NULL) {
//         // 释放已分配的内存
//         for (int i = 0; i < cmd_count; i++) {
//           kvs_free(commands[i]);
//         }
//         return -1;
//       }
//       strcpy(commands[cmd_count], start);

//       // 恢复字符
//       *pos = '&';

//       // 跳过&符
//       pos++;
//       if (is_andand) {
//         pos++;  // 跳过第二个&
//       }

//       // 跳过可能的空格
//       while (*pos == ' ' || *pos == '\t') pos++;
//       start = pos;
//       cmd_count++;
//     } else {
//       pos++;
//     }
//   }

//   // 添加最后一个命令（或唯一命令）
//   if (start < msg + len && cmd_count < max_commands) {
//     commands[cmd_count] = kvs_malloc(strlen(start) + 1);
//     if (commands[cmd_count] == NULL) {
//       for (int i = 0; i < cmd_count; i++) {
//         kvs_free(commands[i]);
//       }
//       return -1;
//     }
//     strcpy(commands[cmd_count], start);
//     cmd_count++;
//   }

//   return cmd_count;
// }

// /*
//  * 分析命令分隔符的类型
//  * 返回 0 表示 & (并行执行)
//  * 返回 1 表示 && (顺序执行，前一个成功才执行下一个)
//  */
// int get_command_separator_type(char* msg, int position, int msg_len) {
//   if (position >= msg_len - 1) {
//     return 0;  // 默认为并行执行
//   }

//   if (msg[position] == '&' && msg[position + 1] == '&') {
//     return 1;  // 顺序执行
//   } else {
//     return 0;  // 并行执行
//   }
// }

/* ---------------- 从 proactor.c 迁移过来的 RESP 协议解析逻辑 ---------------- */

void kvs_resp_reset(struct conn* c) {
  // 释放旧参数
  for (int i = 0; i < c->argc; i++) {
    if (c->argv[i].ptr) {
      kvs_free(c->argv[i].ptr);
      c->argv[i].ptr = NULL;
    }
  }

  if (c->seg_buf) {
    kvs_free(c->seg_buf);
    c->seg_buf = NULL;
  }

  c->argc = 0;
  c->multibulk_len = 0;
  c->bulk_len = 0;
  c->seg_used = 0;
  c->resp_state = ST_RESP_HDR;
}

void kvs_resp_free_resources(struct conn* c) {
  // 释放当前正在解析的 buffer
  if (c->seg_buf) {
    kvs_free(c->seg_buf);
    c->seg_buf = NULL;
  }

  // 释放已解析的参数
  for (int i = 0; i < c->argc; i++) {
    if (c->argv[i].ptr) {
      kvs_free(c->argv[i].ptr);
      c->argv[i].ptr = NULL;
    }
  }
  
  // wbuf 是由网络层分配和管理的，这里我们只负责 argv 相关的内存
}

/* --------------  RESP 流式解析：啃掉 data[]，返回是否完成一条完整命令 -------------- */
int kvs_resp_feed(struct conn* c) {
  // 使用环形缓冲区获取可用数据量
  size_t initial_len = ring_buffer_available(c);
  size_t len = initial_len;
  
  // 临时缓冲区用于 peek 数据（最多 peek IOP_SIZE 字节）
  char temp_buf[IOP_SIZE];
  size_t done = 0;

  // 调试信息
  // fprintf(stderr, "kvs_resp_feed: fd=%d, available=%zu\n", c->fd, len);

  // 目标, 把目前收到的数据, 全部处理成 RESP
  while (done < initial_len) {
    // 更新 len 为当前可用数据量
    len = ring_buffer_available(c);
    
    // 从环形缓冲区 peek 数据
    size_t peek_len = len;
    if (peek_len > IOP_SIZE) peek_len = IOP_SIZE;
    
    if (peek_len == 0) {
      // 没有更多数据
      // fprintf(stderr, "kvs_resp_feed: no more data, done=%zu\n", done);
      return done;
    }
    
    int peeked = ring_buffer_peek(c, temp_buf, peek_len);
    if (peeked < 0) {
      // fprintf(stderr, "kvs_resp_feed: ring_buffer_peek failed\n");
      return -1;  // peek 失败
    }
    
    // data 指向 temp_buf 的开头，因为 ring_buffer_peek 已经从 tail 开始读取
    char* data = temp_buf;
    size_t remaining = peeked;
    
    // fprintf(stderr, "kvs_resp_feed: done=%zu, peeked=%d, remaining=%zu\n", done, peeked, remaining);
    
    // 调试：打印 temp_buf 的内容
    // fprintf(stderr, "kvs_resp_feed: temp_buf (first 32 bytes): ");
    // for (int i = 0; i < 32 && i < peeked; i++) {
    //     fprintf(stderr, "%02x ", (unsigned char)temp_buf[i]);
    // }
    // fprintf(stderr, "\n");
    
    switch (c->resp_state) {
      case ST_RESP_HDR: {
        // 期待 *<argc>\r\n
        char* nl = memchr(data, '\n', remaining);
        if (!nl) {
          // fprintf(stderr, "kvs_resp_feed: ST_RESP_HDR, no newline found\n");
          return done;  // 还没收全一行
        }

        if (nl <= data || *(nl - 1) != '\r') {
          // fprintf(stderr, "kvs_resp_feed: ST_RESP_HDR, invalid format\n");
          return -1;  // 格式错误
        }

        char prefix = *data;
        long num = strtol(data + 1, NULL, 10);  // 跳过前缀解析数字

        if (prefix == '*') {
          c->multibulk_len = num; // 找到了想要的值
          c->argc = 0; // 接下来开始解析各个段咯,argc是已经收取的段数量
          if (num <= 0 || num > MAX_ARGC) {
            // fprintf(stderr, "kvs_resp_feed: ST_RESP_HDR, invalid argc=%ld\n", num);
            return -1;
          }
          c->resp_state = ST_RESP_BULK_LEN;  // 接下来期待参数长度
          // fprintf(stderr, "kvs_resp_feed: ST_RESP_HDR, parsed argc=%ld\n", num);
        } else {
          // 如果不是 *, 可能直接是 Inline command? 这里只支持标准 RESP 数组
          // fprintf(stderr, "kvs_resp_feed: ST_RESP_HDR, invalid prefix=%c\n", prefix);
          return -1;
        }

        size_t line_len = (nl - data) + 1;
        done += line_len;
        ring_buffer_skip(c, line_len);  // 跳过这行
        // fprintf(stderr, "kvs_resp_feed: ST_RESP_HDR, skipped %zu bytes, done=%zu\n", line_len, done);
        break;
      }
      case ST_RESP_BULK_LEN: {
        // 期待 $<len>\r\n
        char* nl = memchr(data, '\n', remaining);
        if (!nl) {
          // fprintf(stderr, "kvs_resp_feed: ST_RESP_BULK_LEN, no newline found\n");
          return done;
        }

        if (nl <= data || *(nl - 1) != '\r' || *data != '$') {
          // fprintf(stderr, "kvs_resp_feed: ST_RESP_BULK_LEN, invalid format\n");
          return -1; // 检查是否合法(RESP协议)
        }

        long len_val = strtol(data + 1, NULL, 10);
        c->bulk_len = len_val; // 获取到想要的了!

        if (len_val < 0) {  // NULL Bulk String ($ -1)
          // fprintf(stderr, "kvs_resp_feed: ST_RESP_BULK_LEN, null bulk string\n");
          return -1;
        }
        if (len_val > MAX_SEG_SIZE) {
          // fprintf(stderr, "kvs_resp_feed: ST_RESP_BULK_LEN, bulk too large=%ld\n", len_val);
          return -1; // 超过1GB的 Key 或者 Value,不读
        }

        // 分配内存准备接收数据
        c->seg_buf = kvs_malloc(len_val + 1);  // +1 for null terminator
        if (!c->seg_buf) {
          // fprintf(stderr, "kvs_resp_feed: ST_RESP_BULK_LEN, malloc failed\n");
          return -1;  // 内存分配失败
        }
        c->seg_buf[len_val] = '\0';
        c->seg_used = 0;

        c->resp_state = ST_RESP_BULK_DATA;
        size_t line_len = (nl - data) + 1;
        done += line_len;
        ring_buffer_skip(c, line_len);
        // fprintf(stderr, "kvs_resp_feed: ST_RESP_BULK_LEN, parsed bulk_len=%ld, skipped %zu bytes, done=%zu\n", len_val, line_len, done);
        break;
      }
      case ST_RESP_BULK_DATA: {
        // 读取 bulk_len 字节
        size_t want = c->bulk_len - c->seg_used;
        size_t avail = remaining;
        size_t cp = (want < avail) ? want : avail;

        // 从环形缓冲区读取数据到 seg_buf
        int read_len = ring_buffer_read(c, c->seg_buf + c->seg_used, cp);
        if (read_len < 0) {
          // fprintf(stderr, "kvs_resp_feed: ST_RESP_BULK_DATA, ring_buffer_read failed\n");
          return -1;
        }
        
        c->seg_used += read_len;
        done += read_len;

        // fprintf(stderr, "kvs_resp_feed: ST_RESP_BULK_DATA, read %d bytes, seg_used=%zu, bulk_len=%ld\n", read_len, c->seg_used, c->bulk_len);

        if (c->seg_used == (size_t)c->bulk_len) {
          // 数据读完了，期待 \r\n
          if (ring_buffer_available(c) < 2) {
            // 还没收到 \r\n，等待
            // fprintf(stderr, "kvs_resp_feed: ST_RESP_BULK_DATA, waiting for CRLF\n");
            return done;
          }

          char crlf[2];
          ring_buffer_read(c, crlf, 2);
          
          if (crlf[0] != '\r' || crlf[1] != '\n') {
            // fprintf(stderr, "kvs_resp_feed: ST_RESP_BULK_DATA, invalid CRLF\n");
            return -1;
          }
          done += 2;
          // fprintf(stderr, "kvs_resp_feed: ST_RESP_BULK_DATA, read CRLF, done=%zu\n", done);

          // 参数完整，存入 argv
          c->argv[c->argc++] = (robj){c->seg_buf, c->bulk_len};
          c->seg_buf = NULL;  // 权责移交

          // fprintf(stderr, "kvs_resp_feed: ST_RESP_BULK_DATA, argument complete, argc=%d\n", c->argc);

          if (c->argc == c->multibulk_len) {
            // 所有参数解析完毕
            // fprintf(stderr, "kvs_resp_feed: PARSE_OK\n");
            return PARSE_OK;
          } else {
            // 继续下一个参数
            c->resp_state = ST_RESP_BULK_LEN;
            // fprintf(stderr, "kvs_resp_feed: ST_RESP_BULK_DATA, continue to next parameter\n");
          }
        }
        break;
      }
    }
    
    // 更新 len 为剩余可用数据量（用于 peek）
    len = ring_buffer_available(c);
    // fprintf(stderr, "kvs_resp_feed: end of loop, done=%zu, initial_len=%zu, len=%zu\n", done, initial_len, len);
  }

  return 0;  // 需要更多数据
}
