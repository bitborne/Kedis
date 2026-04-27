#include "../../include/kvstore.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>


void proto_parser_reset(proto_parser_t *p) {
  if (!p) return;
  int argc_done = p->argc_done;
  if (argc_done < MAX_ARGC && p->argv[argc_done].ptr)
    argc_done++;
  for (int i = 0; i < argc_done; i++) {
    if (p->argv[i].ptr && !(p->argv[i].flags & ROBJ_FLAG_RBUF_REF)
        && p->argv[i].ptr != p->cmd_buf) {
      kvs_free(p->argv[i].ptr);
    }
    p->argv[i].ptr = NULL;
    p->argv[i].len = 0;
    p->argv[i].flags = 0;
  }
  p->resp_state = ST_RESP_HDR;
  p->bulk_len = p->argc = p->argc_done = p->bulk_done = 0;
  p->parse_done = 0;
}

void kvs_resp_reset(struct conn* c) {
  c->rlen = 0;
  c->wlen = c->wbuf_off = c->bulk_sent = 0;
  c->bulk_p = NULL;
  c->send_st = ST_SEND_NOTSET;

  if (c->parser) {
    proto_parser_reset(c->parser);
  }
}

void kvs_resp_free_resources(struct conn* c) {
  if (c->parser) {
    for (int i = 0; i < c->parser->argc; i++) {
      if (c->parser->argv[i].ptr && !(c->parser->argv[i].flags & ROBJ_FLAG_RBUF_REF)
          && c->parser->argv[i].ptr != c->parser->cmd_buf) {
        kvs_free(c->parser->argv[i].ptr);
        c->parser->argv[i].ptr = NULL;
      }
    }
  }
}

/* --------------  pipeline 模式下释放当前命令资源，保留 rbuf  -------------- */
void kvs_resp_pipeline_next(struct conn* c) {
  if (!c->parser) return;
  for (int i = 0; i < c->parser->argc; i++) {
    if (c->parser->argv[i].ptr && !(c->parser->argv[i].flags & ROBJ_FLAG_RBUF_REF)
        && c->parser->argv[i].ptr != c->parser->cmd_buf) {
      kvs_free(c->parser->argv[i].ptr);
    }
    c->parser->argv[i].ptr = NULL;
    c->parser->argv[i].len = 0;
    c->parser->argv[i].flags = 0;
  }
  c->parser->argc = 0;
  c->parser->argc_done = 0;
  c->parser->bulk_len = 0;
  c->parser->bulk_done = 0;
  c->parser->resp_state = ST_RESP_HDR;
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
    if (!c->parser) return;
    for (int i = 0; i < c->parser->argc_done; i++) {
        if (c->parser->argv[i].ptr && (c->parser->argv[i].flags & ROBJ_FLAG_RBUF_REF)) {
            char* old_ptr = c->parser->argv[i].ptr;
            size_t old_len = c->parser->argv[i].len;
            c->parser->argv[i].ptr = kvs_malloc(old_len + 1);
            if (c->parser->argv[i].ptr) {
                memcpy(c->parser->argv[i].ptr, old_ptr, old_len);
                c->parser->argv[i].ptr[old_len] = '\0';
            }
            c->parser->argv[i].flags &= ~ROBJ_FLAG_RBUF_REF;
        }
    }
}

/* --------------  RESP 流式解析：啃掉 chunk[]，返回已消费字节数  -------------- */
size_t proto_feed(proto_parser_t *p, const char *chunk, size_t chunk_len) {
  if (!p || !chunk || chunk_len == 0) return 0;

  while (p->parse_done < chunk_len && p->resp_state != ST_RESP_OK) {
    switch (p->resp_state) {
      case ST_RESP_HDR: {
        // 检查是否以 * 开头（Array 格式）
        if (chunk[p->parse_done] != '*') {
            kvs_logError("The first char must be *");
          goto error;  // 协议错误：不是 Array 格式
        }

        // 查找 \r\n，确定命令头结束位置
        size_t remaining = chunk_len - p->parse_done;
        char* end = find_crlf(chunk + p->parse_done, remaining);
        if (!end) {
          // 找不到 \r\n，数据不足，保留未解析的数据在 chunk 中
          goto done;  // 需要更多数据
        }

        // 提取 argc（参数个数）
        char* ptr = (char*)chunk + p->parse_done + 1;  // 跳过 '*'
        size_t num_len = end - ptr;  // 数字字符串长度

        // argc 只有 1-3 位，使用无溢出检查的快速路径
        int64_t parsed_argc = fast_atoi_small(ptr, num_len);
        if (parsed_argc < 0 || parsed_argc > MAX_ARGC) {
          kvs_logError("Argc convert error: invalid or out of range");
          goto error;  // 解析错误：数字格式错误或超出范围
        }
        p->argc = (int)parsed_argc;

        // 检查解析是否成功（数字长度应该大于0）
        if (num_len == 0 || p->argc <= 0) {
          kvs_logError("Argc convert error");
          goto error;  // 解析错误：数字格式错误
        }

        // 更新 parse_done 到命令头结束位置（跳过 \r\n）
        p->parse_done = end + 2 - chunk;

        // 切换到 ST_RESP_BULK_LEN 状态，准备解析第一个参数的长度
        p->resp_state = ST_RESP_BULK_LEN;
        break;
      }
      case ST_RESP_BULK_LEN: {
        // 检查是否以 $ 开头（Bulk String 格式）
        if (chunk[p->parse_done] != '$') {
          kvs_logError("Bulk should start with $");
          goto error; // 协议错误：不是 Bulk String 格式
        }

        // 查找 \r\n，确定长度头结束位置
        size_t remaining = chunk_len - p->parse_done;
        char* end = find_crlf(chunk + p->parse_done, remaining);
        if (!end) {
          // 找不到 \r\n，数据不足
          goto done;
        }

        // 提取 bulk_len（bulk data 长度）
        char* ptr = (char*)chunk + p->parse_done + 1;  // 跳过 '$'
        size_t num_len = end - ptr;  // 数字字符串长度

        // 使用安全的 atoi 解析，检查溢出和负数
        int64_t parsed_len = fast_atoi_safe(ptr, num_len);
        if (parsed_len < 0) {
          kvs_logError("Bulk len convert error: negative or overflow");
          goto error;
        }
        p->bulk_len = (size_t)parsed_len;

        // 检查解析是否成功
        if (num_len == 0) {
          kvs_logError("Bulk len convert error");
          goto error;
        }

        // 更新 parse_done 到长度头结束位置（跳过 \r\n）
        p->parse_done = end + 2 - chunk;

        // 处理 NULL bulk string（bulk_len == -1）
        if (p->bulk_len == (size_t)-1) {
          kvs_logError("服务端不应该收到 $-1\\r\\n\n");
          p->argv[p->argc_done].ptr = NULL;  // NULL 指针
          p->argv[p->argc_done].len = 0;     // 长度为 0
          p->argv[p->argc_done].flags = 0;
          p->argc_done++;                    // 已解析参数个数加 1

          // 检查是否所有参数解析完毕
          if (p->argc_done == p->argc) {
            p->resp_state = ST_RESP_OK;  // 切换到完成状态
          }
          // 否则继续解析下一个参数（保持在 ST_RESP_BULK_LEN 状态）
          break;
        }

        // 检查 bulk_len 是否超过最大限制
        if (p->bulk_len > MAX_SEG_SIZE) {
          kvs_logError("Bulk too big");
          goto error;  // 数据过大，拒绝处理
        }

        // 分配内存存储 bulk data（+1 用于 null terminator）
        // argv[0] 命令名内联到 cmd_buf，避免短命令的 malloc/free
        if (p->argc_done == 0 && p->bulk_len < sizeof(p->cmd_buf)) {
          p->argv[0].ptr = p->cmd_buf;
        } else {
          p->argv[p->argc_done].ptr = kvs_malloc(p->bulk_len + 1);
          if (!p->argv[p->argc_done].ptr) {
            kvs_logError("Bulk malloc fail");
            goto error;  // 内存分配失败
          }
        }
        p->argv[p->argc_done].len = p->bulk_len;        // 记录长度
        p->argv[p->argc_done].ptr[p->bulk_len] = '\0';  // 添加 null terminator
        p->argv[p->argc_done].flags = 0;

        // 切换到 ST_RESP_BULK_DATA 状态，准备接收 bulk data
        p->bulk_done = 0;  // 重置已接收的 bulk data 长度
        p->resp_state = ST_RESP_BULK_DATA;
        break;
      }
      case ST_RESP_BULK_DATA: {
        // 计算还需要接收多少 bulk data
        size_t want = p->bulk_len - p->bulk_done;

        // 计算 chunk 中还有多少数据可用
        size_t avail = chunk_len - p->parse_done;

        // 【零拷贝快速路径】整个 bulk 数据（含 \r\n）已经到达且尚未拷贝
        // 注意：argv[0] 是命令名，下游用 strcasecmp/djb2 做字符串比较，需要 \0 结尾，
        // 因此命令名不走零拷贝，总是 malloc+copy 保证 null-terminated。
        if (p->argc_done > 0 && p->bulk_done == 0 && avail >= p->bulk_len + 2) {
            // 直接让 argv 指向 chunk 内部
            p->argv[p->argc_done].ptr = (char*)chunk + p->parse_done;
            p->argv[p->argc_done].len = p->bulk_len;
            p->argv[p->argc_done].flags = ROBJ_FLAG_RBUF_REF;
            p->parse_done += p->bulk_len;
            // 检查 \r\n
            if (chunk[p->parse_done] != '\r' ||
                chunk[p->parse_done + 1] != '\n') {
                kvs_logError("Bulk should end with \\r\\n");
                goto error;
            }
            // 将 \r 覆写为 \0，使零拷贝指针对引擎的 strcmp/strlen 安全
            // NOTE: caller must ensure chunk is writable (rbuf is writable)
            // This is a design constraint - proto_feed requires writable buffer
            *((char*)chunk + p->parse_done) = '\0';
            p->parse_done += 2;
            p->argc_done++;

            if (p->argc_done == p->argc) {
                p->resp_state = ST_RESP_OK;
            } else {
                p->resp_state = ST_RESP_BULK_LEN;
            }
            break;
        }

        // 计算本次可以复制的数据量（取 want 和 avail 的较小值）
        size_t cp = (want < avail) ? want : avail;
        // 从 chunk 复制数据到 argv[argc_done].ptr
        if (cp > 0) {
          memcpy(p->argv[p->argc_done].ptr + p->bulk_done,
            chunk + p->parse_done, cp);
        }

        // 更新 bulk_done（已接收的 bulk data 长度）
        p->bulk_done += cp;
        // 更新 parse_done（chunk 中已处理的数据位置）
        p->parse_done += cp;

        // 检查 bulk data 是否接收完成
        if (p->bulk_done == p->bulk_len) {
          // bulk data 收全了，现在检查是否有 \r\n
          if (p->parse_done + 2 > chunk_len) {
            // 缺失\r\n，等待更多数据
            goto done;
          }

          // 检查 \r\n 是否正确
          if (chunk[p->parse_done] != '\r' ||
            chunk[p->parse_done + 1] != '\n') {
              kvs_logError("Bulk should end with \\r\\n");
              goto error;  // 协议错误：缺少 \r\n
          }

          // 跳过 \r\n（2 字节）
          p->parse_done += 2;
          // 参数解析完成，更新 argc_done
          p->argc_done++;

          // 检查是否所有参数解析完毕
          if (p->argc_done == p->argc) {
            // 所有参数解析完毕，切换到完成状态
            p->resp_state = ST_RESP_OK;
          } else {
            // 继续解析下一个参数，切换到 ST_RESP_BULK_LEN 状态
            p->resp_state = ST_RESP_BULK_LEN;
          }
        }
        // 否则，bulk data 还没收全，继续接收（保持在 ST_RESP_BULK_DATA 状态）
        break;
      }
      case ST_RESP_OK: {
        // 命令解析完成，不需要做任何处理
        break;
      }
    }
  }

done:
  return p->parse_done;

error:
  p->resp_state = ST_RESP_HDR;
  p->parse_done = 0;
  return (size_t)-1;
}

int proto_cmd_ready(const proto_parser_t *p) {
  return p && p->resp_state == ST_RESP_OK;
}

int proto_take_cmd(proto_parser_t *p, int *argc_out, robj **argv_out) {
  if (!proto_cmd_ready(p)) return 0;
  *argc_out = p->argc;
  *argv_out = p->argv;
  /* Upgrade RBUF_REF argv entries to heap memory before handing to business layer */
  for (int i = 0; i < p->argc; i++) {
    if (p->argv[i].flags & ROBJ_FLAG_RBUF_REF) {
      char *old = p->argv[i].ptr;
      size_t old_len = p->argv[i].len;
      p->argv[i].ptr = kvs_malloc(old_len + 1);
      if (p->argv[i].ptr) {
        memcpy(p->argv[i].ptr, old, old_len);
        p->argv[i].ptr[old_len] = '\0';
      }
      p->argv[i].flags &= ~ROBJ_FLAG_RBUF_REF;
    }
  }
  /* Reset parser state for next command, but DO NOT free argv (caller now owns them) */
  p->resp_state = ST_RESP_HDR;
  p->argc_done = 0;
  p->bulk_len = p->bulk_done = 0;
  p->parse_done = 0;
  return p->argc;
}

void proto_free_argv(int argc, robj *argv) {
  for (int i = 0; i < argc; i++) {
    if (argv[i].ptr && !(argv[i].flags & ROBJ_FLAG_RBUF_REF)) {
      kvs_free(argv[i].ptr);
      argv[i].ptr = NULL;
    }
  }
}

/* --------------  旧兼容包装器  -------------- */
int kvs_resp_feed(struct conn* c) {
  if (!c->parser) return RESP_ERROR;
  size_t consumed = proto_feed(c->parser, c->rbuf_ptr, c->rlen);

  if (consumed == (size_t)-1) {
    c->rlen = c->parser->parse_done = 0;
    c->parser->resp_state = ST_RESP_HDR;
    return RESP_ERROR;
  }

  if (c->parser->resp_state == ST_RESP_OK) {
    /* 兼容：同步 parser->argv 到 c->argv，业务层仍从 c->argv 读取 */
    c->argc = c->parser->argc;
    c->argc_done = c->parser->argc_done;
    for (int i = 0; i < c->parser->argc; i++) {
      c->argv[i] = c->parser->argv[i];
    }
    if (c->parser->parse_done >= c->rlen) {
      c->rlen = 0;
      c->parser->parse_done = 0;
      return RESP_PARSE_OK;
    } else {
      c->parser->resp_state = ST_RESP_HDR;
      c->parser->argc_done = 0;
      return RESP_PARSE_OK;
    }
  }

  /* For CONTINUE_RECV: handle rbuf compaction if buffer is nearly full */
  if (c->rlen >= (IOP_SIZE - 256)) {
    if (c->parser->parse_done > 0) {
      /* upgrade rbuf refs before memmove */
      for (int i = 0; i < c->parser->argc_done; i++) {
        if (c->parser->argv[i].ptr && (c->parser->argv[i].flags & ROBJ_FLAG_RBUF_REF)) {
          char* old_ptr = c->parser->argv[i].ptr;
          size_t old_len = c->parser->argv[i].len;
          c->parser->argv[i].ptr = kvs_malloc(old_len + 1);
          if (c->parser->argv[i].ptr) {
            memcpy(c->parser->argv[i].ptr, old_ptr, old_len);
            c->parser->argv[i].ptr[old_len] = '\0';
          }
          c->parser->argv[i].flags &= ~ROBJ_FLAG_RBUF_REF;
        }
      }
      size_t remaining = c->rlen - c->parser->parse_done;
      memmove(c->rbuf_ptr, c->rbuf_ptr + c->parser->parse_done, remaining);
      c->rlen = remaining;
      c->parser->parse_done = 0;
    } else {
      kvs_logError("Protocol error: rbuf full without parse progress");
      c->rlen = c->parser->parse_done = 0;
      c->parser->resp_state = ST_RESP_HDR;
      return RESP_ERROR;
    }
  }
  return RESP_CONTINUE_RECV;
}
