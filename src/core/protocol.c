#include "../../include/kvstore.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>


void kvs_resp_reset(struct conn* c) {
  int argc_done = c->argc_done; // 先保存，因为后续要清零

  c->rlen = 0;                  // 重置读缓冲区有效数据长度
  c->wlen = c->wbuf_off = c->bulk_sent = 0;       // 重置写缓冲区长度和已发送长度
  c->bulk_p = NULL;
  c->send_st = ST_SEND_NOTSET;
  c->resp_state = ST_RESP_HDR;  // 重置 RESP 解析状态为等待解析命令头
  c->bulk_len = 0;              // 重置 bulk data 长度
  c->argc = 0;                  // 重置期望的参数个数
  c->argc_done = 0;             // 重置已解析完成的参数个数

  // 按需释放已使用的参数内存（RBUF_REF 的直接丢弃，cmd_buf 无需 free）
  int reset_slots = argc_done;
  if (reset_slots < MAX_ARGC && c->argv[reset_slots].ptr)
    reset_slots++;
  for (int i = 0; i < reset_slots; i++) {
    if (c->argv[i].ptr && !(c->argv[i].flags & ROBJ_FLAG_RBUF_REF)
        && c->argv[i].ptr != c->cmd_buf) {
      kvs_free(c->argv[i].ptr);  // 释放参数内存
    }
    c->argv[i].ptr = NULL;     // 清空指针
    c->argv[i].len = 0;  // 清空长度
    c->argv[i].flags = 0;
  }

  c->bulk_done = 0;   // 重置 bulk data 已解析长度
  c->parse_done = 0;  // 重置已解析的数据位置
}

void kvs_resp_free_resources(struct conn* c) {

  // 释放已解析的参数（RBUF_REF 和 cmd_buf 的跳过）
  for (int i = 0; i < c->argc; i++) {
    if (c->argv[i].ptr && !(c->argv[i].flags & ROBJ_FLAG_RBUF_REF)
        && c->argv[i].ptr != c->cmd_buf) {
      kvs_free(c->argv[i].ptr);
      c->argv[i].ptr = NULL;
    }
  }

  // wbuf 是由网络层分配和管理的，这里我们只负责 argv 相关的内存
}

/* --------------  pipeline 模式下释放当前命令资源，保留 rbuf  -------------- */
void kvs_resp_pipeline_next(struct conn* c) {
  for (int i = 0; i < c->argc; i++) {
    if (c->argv[i].ptr && !(c->argv[i].flags & ROBJ_FLAG_RBUF_REF)
        && c->argv[i].ptr != c->cmd_buf) {
      kvs_free(c->argv[i].ptr);
    }
    c->argv[i].ptr = NULL;
    c->argv[i].len = 0;
    c->argv[i].flags = 0;
  }
  c->argc = 0;
  c->argc_done = 0;
  c->bulk_len = 0;
  c->bulk_done = 0;
  c->resp_state = ST_RESP_HDR;
}

static inline char* find_crlf(const char* s, size_t len) {
    if (len <= 16) {
        const char* end = s + len - 1;
        for (const char* p = s; p < end; p++) {
            if (p[0] == '\r' && p[1] == '\n') return (char*)p;
        }
        return NULL;
    }
    const char* p = s;
    const char* end = s + len - 1;
    while (p < end) {
        p = memchr(p, '\r', end - p + 1);
        if (!p) return NULL;
        if (p[1] == '\n') return (char*)p;
        p++;
    }
    return NULL;
}

// 小数字快速 atoi，无溢出检查，适用于 argc 等 1-3 位小数字
static inline int64_t fast_atoi_small(const char* s, size_t len) {
    int64_t n = 0;
    for (size_t i = 0; i < len; i++) {
        char c = s[i];
        if (c < '0' || c > '9') {
            if (i == 0) return -1;
            break;
        }
        n = n * 10 + (c - '0');
    }
    return n;
}

// 安全快速 atoi 实现，带溢出检查
// 返回 >=0 表示成功，<0 表示错误或溢出
static inline int64_t fast_atoi_safe(const char* s, size_t len) {
    int64_t n = 0;
    for (size_t i = 0; i < len; i++) {
        char c = s[i];
        if (c < '0' || c > '9') {
            // 如果第一个字符就是非数字，返回错误
            if (i == 0) return -1;
            break;
        }
        int digit = c - '0';
        // 溢出检查：如果 n > (INT64_MAX - digit) / 10，则 n*10+digit 会溢出
        if (n > (INT64_MAX - digit) / 10) {
            return -1;  // 溢出
        }
        n = n * 10 + digit;
    }
    return n;
}

/* 在 memmove rbuf 前，将所有指向 rbuf 的 argv 指针升级到堆内存 */
static inline void rbuf_ref_upgrade(struct conn* c) {
    for (int i = 0; i < c->argc_done; i++) {
        if (c->argv[i].ptr && (c->argv[i].flags & ROBJ_FLAG_RBUF_REF)) {
            char* old_ptr = c->argv[i].ptr;
            size_t old_len = c->argv[i].len;
            c->argv[i].ptr = kvs_malloc(old_len + 1);
            if (c->argv[i].ptr) {
                memcpy(c->argv[i].ptr, old_ptr, old_len);
                c->argv[i].ptr[old_len] = '\0';
            }
            c->argv[i].flags &= ~ROBJ_FLAG_RBUF_REF;
        }
    }
}

/* --------------  RESP 流式解析：啃掉 data[]，返回是否完成一条完整命令
 * -------------- */
int kvs_resp_feed(struct conn* c) {
  // 读进来的数据放在

  while (c->parse_done < c->rlen && c->resp_state != ST_RESP_OK) {
    switch (c->resp_state) {
      case ST_RESP_HDR: {
        // 检查是否以 * 开头（Array 格式）
        // DEBUG
        if (c->rbuf[c->parse_done] != '*') {
            kvs_logError("The first char must be *");
          goto error;  // 协议错误：不是 Array 格式
        }

        // 查找 \r\n，确定命令头结束位置
        size_t remaining = c->rlen - c->parse_done;
        char* end = find_crlf(c->rbuf + c->parse_done, remaining);
        if (!end) {
          // 找不到 \r\n，数据不足，保留未解析的数据在 rbuf 中
          goto continue_recv;  // 需要更多数据
        }

        // 提取 argc（参数个数）
        char* ptr = c->rbuf + c->parse_done + 1;  // 跳过 '*'
        size_t num_len = end - ptr;  // 数字字符串长度

        // argc 只有 1-3 位，使用无溢出检查的快速路径
        int64_t parsed_argc = fast_atoi_small(ptr, num_len);
        if (parsed_argc < 0 || parsed_argc > MAX_ARGC) {
          kvs_logError("Argc convert error: invalid or out of range");
          goto error;  // 解析错误：数字格式错误或超出范围
        }
        c->argc = (int)parsed_argc;

        // 检查解析是否成功（数字长度应该大于0）
        if (num_len == 0 || c->argc <= 0) {
          kvs_logError("Argc convert error");
          goto error;  // 解析错误：数字格式错误
        }

        // 更新 parse_done 到命令头结束位置（跳过 \r\n）
        c->parse_done = end + 2 - c->rbuf;

        // 切换到 ST_RESP_BULK_LEN 状态，准备解析第一个参数的长度
        c->resp_state = ST_RESP_BULK_LEN;
        break;
      }
      case ST_RESP_BULK_LEN: {
        // 检查是否以 $ 开头（Bulk String 格式）

        // DEBUG
        if (c->rbuf[c->parse_done] != '$') {
          kvs_logError("Bulk should start with $");
          goto error; // 协议错误：不是 Bulk String 格式
        }

        // 查找 \r\n，确定长度头结束位置
        size_t remaining = c->rlen - c->parse_done;
        char* end = find_crlf(c->rbuf + c->parse_done, remaining);
        if (!end) {
          // 找不到 \r\n，数据不足
          // 保留未解析的 $<len> 部分在 rbuf 中，下次继续解析
          // 注意：不要重置 rlen 为 0，而是设置 parse_done 为 0
          goto continue_recv;
          // size_t remaining = c->rlen - c->parse_done;
          // memmove(c->rbuf, c->rbuf + c->parse_done, remaining);
          // c->rlen = remaining;
          // c->parse_done = 0;
          // return RESP_CONTINUE_REMAINING_RECV;
        }

        // 提取 bulk_len（bulk data 长度）
        char* ptr = c->rbuf + c->parse_done + 1;  // 跳过 '$'
        size_t num_len = end - ptr;  // 数字字符串长度

        // 使用安全的 atoi 解析，检查溢出和负数
        int64_t parsed_len = fast_atoi_safe(ptr, num_len);
        if (parsed_len < 0) {
          kvs_logError("Bulk len convert error: negative or overflow");
          goto error;
        }
        c->bulk_len = (size_t)parsed_len;

        // 检查解析是否成功
        if (num_len == 0) {
          kvs_logError("Bulk len convert error");
          goto error;
        }

        // 更新 parse_done 到长度头结束位置（跳过 \r\n）
        c->parse_done = end + 2 - c->rbuf;

        // 处理 NULL bulk string（bulk_len == -1）
        if (c->bulk_len == (size_t)-1) {
          kvs_logError("服务端不应该收到 $-1\\r\\n\n");
          c->argv[c->argc_done].ptr = NULL;  // NULL 指针
          c->argv[c->argc_done].len = 0;     // 长度为 0
          c->argv[c->argc_done].flags = 0;
          c->argc_done++;                    // 已解析参数个数加 1

          // 检查是否所有参数解析完毕
          if (c->argc_done == c->argc) {
            c->resp_state = ST_RESP_OK;  // 切换到完成状态
          }
          // 否则继续解析下一个参数（保持在 ST_RESP_BULK_LEN 状态）
          break;
        }

        // 检查 bulk_len 是否超过最大限制
        if (c->bulk_len > MAX_SEG_SIZE) {
          kvs_logError("Bulk too big");
          goto error;  // 数据过大，拒绝处理
        }

        // 分配内存存储 bulk data（+1 用于 null terminator）
        // argv[0] 命令名内联到 cmd_buf，避免短命令的 malloc/free
        if (c->argc_done == 0 && c->bulk_len < sizeof(c->cmd_buf)) {
          c->argv[0].ptr = c->cmd_buf;
        } else {
          c->argv[c->argc_done].ptr = kvs_malloc(c->bulk_len + 1);
          if (!c->argv[c->argc_done].ptr) {
            kvs_logError("Bulk malloc fail");
            goto error;  // 内存分配失败
          }
        }
        c->argv[c->argc_done].len = c->bulk_len;        // 记录长度
        c->argv[c->argc_done].ptr[c->bulk_len] = '\0';  // 添加 null terminator
        c->argv[c->argc_done].flags = 0;

        // 切换到 ST_RESP_BULK_DATA 状态，准备接收 bulk data
        c->bulk_done = 0;  // 重置已接收的 bulk data 长度
        c->resp_state = ST_RESP_BULK_DATA;
        break;
      }
      case ST_RESP_BULK_DATA: {
        // 计算还需要接收多少 bulk data

        // fprintf(stderr, "-->bulk_data\n");

        size_t want = c->bulk_len - c->bulk_done;

        // 计算 rbuf 中还有多少数据可用
        size_t avail = c->rlen - c->parse_done;

        // 【零拷贝快速路径】整个 bulk 数据（含 \r\n）已经到达且尚未拷贝
        // 注意：argv[0] 是命令名，下游用 strcasecmp/djb2 做字符串比较，需要 \0 结尾，
        // 因此命令名不走零拷贝，总是 malloc+copy 保证 null-terminated。
        if (c->argc_done > 0 && c->bulk_done == 0 && avail >= c->bulk_len + 2) {
            // 直接让 argv 指向 rbuf 内部
            c->argv[c->argc_done].ptr = c->rbuf + c->parse_done;
            c->argv[c->argc_done].len = c->bulk_len;
            c->argv[c->argc_done].flags = ROBJ_FLAG_RBUF_REF;
            c->parse_done += c->bulk_len;
            // 检查 \r\n
            if (c->rbuf[c->parse_done] != '\r' ||
                c->rbuf[c->parse_done + 1] != '\n') {
                kvs_logError("Bulk should end with \\r\\n");
                goto error;
            }
            // 将 \r 覆写为 \0，使零拷贝指针对引擎的 strcmp/strlen 安全
            c->rbuf[c->parse_done] = '\0';
            c->parse_done += 2;
            c->argc_done++;

            if (c->argc_done == c->argc) {
                c->resp_state = ST_RESP_OK;
            } else {
                c->resp_state = ST_RESP_BULK_LEN;
            }
            break;
        }

        // 计算本次可以复制的数据量（取 want 和 avail 的较小值）
        size_t cp = (want < avail) ? want : avail;
        // fprintf(stderr, "cp == %d\n", cp);
        // 从 rbuf 复制数据到 argv[argc_done].ptr
        if (cp > 0) {

          memcpy(c->argv[c->argc_done].ptr + c->bulk_done,
            c->rbuf + c->parse_done, cp);
        }

          // fprintf(stderr, "bulk_done1 == %d\n", c->bulk_done);
          // 更新 bulk_done（已接收的 bulk data 长度）
          c->bulk_done += cp;
        // fprintf(stderr, "bulk_done2 == %d\n", c->bulk_done);

        // fprintf(stderr, "parse_done1 == %d\n", c->parse_done);
        // 更新 parse_done（rbuf 中已处理的数据位置）
        c->parse_done += cp;
        // fprintf(stderr, "parse_done2 == %d\n", c->parse_done);

        // 检查 bulk data 是否接收完成
        // fprintf(stderr, "c->bulk_done:%d != c->bulk_len: %d\n", c->bulk_done, c->bulk_len);
        if (c->bulk_done == c->bulk_len) {
          // bulk data 收全了，现在检查是否有 \r\n

          // 检查 rbuf 中是否有足够的数据接收 \r\n
          // fprintf(stderr, "data:--> 1\n");
          if (c->parse_done + 2 > c->rlen) {
            // fprintf(stderr, "data:--> 01");
            // 缺失\r\n，等待更多数据
            // 保留已接收的部分 \r\n;
            goto continue_recv;
          }

          // fprintf(stderr, "data:--> 2\n");
          // 检查 \r\n 是否正确
          if (c->rbuf[c->parse_done] != '\r' ||
            c->rbuf[c->parse_done + 1] != '\n') {
              kvs_logError("Bulk should end with \\r\\n");
              goto error;  // 协议错误：缺少 \r\n
          }
          // fprintf(stderr, "data:--> 3\n");

          // 跳过 \r\n（2 字节）
          c->parse_done += 2;
          // fprintf(stderr, "跳过\\r\\n: c->parse_done=%d\n", c->parse_done);
          // 参数解析完成，更新 argc_done
          c->argc_done++;

          // 检查是否所有参数解析完毕
          if (c->argc_done == c->argc) {
            // 所有参数解析完毕，切换到完成状态
            // fprintf(stderr, "change to: OK\n");
            c->resp_state = ST_RESP_OK;
          } else {
            // 继续解析下一个参数，切换到 ST_RESP_BULK_LEN 状态
            c->resp_state = ST_RESP_BULK_LEN;
          }
        }
        // 否则，bulk data 还没收全，继续接收（保持在 ST_RESP_BULK_DATA 状态）
        break;
      }
      case ST_RESP_OK: {
        // 命令解析完成，不需要做任何处理
        // 这个状态只是标记，实际逻辑在循环结束后处理

        // fprintf(stderr, "-->OK\n");
        // fprintf(stderr, "不应该来这\n");
        // c->parse_done = c->rlen; // 出循环体
        break;
      }
    }

  }
  // 循环结束，检查是否所有参数解析完毕
  if (c->resp_state == ST_RESP_OK) {
    // 所有参数解析完毕
    // 检查是否所有数据都已处理
    // fprintf(stderr, "c->parse_done: %zu    c->rlen: %zu\n", c->parse_done, c->rlen);
    // fprintf(stderr, "state == OK: ==> parse done = %zu\n rlen = %zu\n", c->parse_done, c->rlen);
    if (c->parse_done >= c->rlen) {
      // 所有数据都已处理，重置 rlen 和 parse_done
      // c->rlen = 0;
      // c->parse_done = 0;

      // fprintf(stderr, "应该来这\n");
      c->resp_state = ST_RESP_HDR;
      c->rlen = c->parse_done = c->bulk_len = 0;
      c->argc_done = 0;
      return RESP_PARSE_OK;

    } else {
      // 还有未解析的数据（例如：一条命令解析完毕，但 rbuf 中还有下一条命令的部分数据）
      // 移动未解析的数据到 rbuf 开头
      // 粘包才走这里( rlen = remaining, parse_done = 0 )
        // fprintf(stderr, "[循环外]确实出现粘包!\n");
        // [原本以为走到这个分支是不可能的, 故原本`go to error`]
        // [但实际,如果粘包,确实会走在这个分支]
        // goto error;
        c->resp_state = ST_RESP_HDR;
        c->argc_done = 0;
        // rlen, parse_done, 在循环内判断出粘包的时候已初始化;
        return RESP_PARSE_OK;

    }
    return RESP_PARSE_OK;
  } else if (c->parse_done < c->rlen) {
    // fprintf(stderr, "c->parse_done: %zu    c->rlen: %zu\n", c->parse_done, c->rlen);
    // 还有未解析的数据，继续解析
    // 这种情况不应该发生，因为 while 循环会继续处理
    goto continue_recv;
  } else {
    // fprintf(stderr, "c->parse_done: %zu    c->rlen: %zu\n", c->parse_done, c->rlen);
    // 数据耗尽，但未解析完毕，需要更多数据
    if (c->parse_done > 0 && c->parse_done < c->rlen) {
      // // 数据耗尽, 但剩下的内容不便解析, 移动未解析的数据到 rbuf 开头（如果有）
      // fprintf(stderr, "-->a\n");
      // size_t remaining = c->rlen - c->parse_done;
      // memmove(c->rbuf, c->rbuf + c->parse_done, remaining);
      // c->rlen = remaining;
      // c->parse_done = 0;
      goto continue_recv;

    } else if (c->parse_done >= c->rlen) {
      // fprintf(stderr, "-->b\n");
      // 所有数据都已处理，重置 rlen
      // 在重置前先把指向 rbuf 的 argv 升级到堆内存
      rbuf_ref_upgrade(c);
      c->rlen = 0;
      c->parse_done = 0;
      goto continue_recv;
  }
    // fprintf(stderr, "-->c\n");
    // fprintf(stderr, "bulk_len: %zu\nargc_done: %d\nbulk_done: %zu\n", c->bulk_len, c->argc_done, c->bulk_done);

    goto continue_recv;  // 需要更多数据
  }

  continue_recv: {
    if (c->rlen >= (IOP_SIZE - 256)) {
        if (c->parse_done > 0) {
            rbuf_ref_upgrade(c);
            size_t remaining = c->rlen - c->parse_done;
            memmove(c->rbuf, c->rbuf + c->parse_done, remaining);
            c->rlen = remaining;
            c->parse_done = 0;
        } else {
            kvs_logError("Protocol error: rbuf full without parse progress");
            goto error;
        }
    }
    return RESP_CONTINUE_RECV;
  }

  error:
    c->rlen = c->parse_done = 0;
    c->resp_state = ST_RESP_HDR;
    return RESP_ERROR;
}
