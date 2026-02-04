#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../../include/kvs_protocol.h"
#include "../../kvstore.h"

/* ---------------- 从 proactor.c 迁移过来的 RESP 协议解析逻辑 ----------------
 */
/*
int fd;         // TCP 套接字
int state;      // io_uring 状态：ST_RECV / ST_SEND / ST_CLOSE
int next_free;  // 空闲链表中的下一个连接索引

char rbuf[IOP_SIZE];  // 读缓冲区（16 KB）
size_t rlen;             // 缓冲区内有效数据长度
size_t parse_done;       // 缓冲区内已解析长度

resp_state_t resp_state;
size_t bulk_len;        // 当前段长度 (需要读取的长度)
char* bulk_data;
int argc;    // 期望的参数个数 (argc)
robj argv[MAX_ARGC];  // 命令段数组 (每个 ptr 都需要 malloc)
size_t bulk_done;   // 当前 bulk 已解析长度
 */
void kvs_resp_reset(struct conn* c) {
  c->rlen = 0;                  // 重置读缓冲区有效数据长度
  c->wlen = c->wdone = 0;       // 重置写缓冲区长度和已发送长度
  c->resp_state = ST_RESP_HDR;  // 重置 RESP 解析状态为等待解析命令头
  c->bulk_len = 0;              // 重置 bulk data 长度
  c->argc = 0;                  // 重置期望的参数个数
  c->argc_done = 0;             // 重置已解析完成的参数个数

  // 释放所有已分配的参数内存
  for (int i = 0; i < MAX_ARGC; i++) {
    if (c->argv[i].ptr) {
      kvs_free(c->argv[i].ptr);  // 释放参数内存
      c->argv[i].ptr = NULL;     // 清空指针
    }
    c->argv[i].len = 0;  // 清空长度
  }

  c->bulk_done = 0;   // 重置 bulk data 已解析长度
  c->parse_done = 0;  // 重置已解析的数据位置
}

void kvs_resp_free_resources(struct conn* c) {
  // 释放当前正在解析的 buffer
  // if (c->seg_buf) {
  //   kvs_free(c->seg_buf);
  //   c->seg_buf = NULL;
  // }

  // 释放已解析的参数
  for (int i = 0; i < c->argc; i++) {
    if (c->argv[i].ptr) {
      kvs_free(c->argv[i].ptr);
      c->argv[i].ptr = NULL;
    }
  }

  // wbuf 是由网络层分配和管理的，这里我们只负责 argv 相关的内存
}

/* --------------  RESP 流式解析：啃掉 data[]，返回是否完成一条完整命令
 * -------------- */
int kvs_resp_feed(struct conn* c) {
  // 读进来的数据放在

  // fprintf(stderr, "--> while\n");
  // fprintf(stderr, "--> while: resp_state: %d\n", c->resp_state);
  // fprintf(stderr, "c->parse_done: %zu < c->rlen: %zu", c->parse_done, c->rlen);
  while (c->parse_done < c->rlen) {
    switch (c->resp_state) {
      case ST_RESP_HDR: {
        // 检查是否以 * 开头（Array 格式）
        // DEBUG
        fprintf(stderr, "-->hdr\n");
        if (c->rbuf[c->parse_done] != '*') {
          goto error;  // 协议错误：不是 Array 格式
        }

        // 查找 \r\n，确定命令头结束位置
        char* end = strstr(c->rbuf + c->parse_done, "\r\n");
        if (!end) {
          // 找不到 \r\n，数据不足，保留未解析的数据在 rbuf 中
          goto continue_recv;  // 需要更多数据
        }

        // 提取 argc（参数个数）
        char* ptr = c->rbuf + c->parse_done + 1;  // 跳过 '*'
        char* endptr;
        c->argc = (int)strtol(ptr, &endptr, 10);  // 解析数字

        fprintf(stderr, "c->argc=%d\n", c->argc);
        // 检查解析是否成功（endptr 应该指向 \r）
        if (endptr != end) {
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
        fprintf(stderr, "-->bulk_len\n");
        
        // fprintf(stderr, "c->parse_down:这个位置是:%s\n", c->rbuf[c->parse_done]);
        if (c->rbuf[c->parse_done] != '$') {
          goto error; // 协议错误：不是 Bulk String 格式
        }
        fprintf(stderr, "-->bulk_len1\n");
        
        // 查找 \r\n，确定长度头结束位置
        char* end = strstr(c->rbuf + c->parse_done, "\r\n");
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
        
        fprintf(stderr, "-->bulk_len2\n");
        // 提取 bulk_len（bulk data 长度）
        char* ptr = c->rbuf + c->parse_done + 1;  // 跳过 '$'
        char* endptr;
        c->bulk_len = (size_t)strtol(ptr, &endptr, 10);  // 解析数字
        
        // 检查解析是否成功（endptr 应该指向 \r）
        if (endptr != end) {
          goto error;
          // return -1;  // 解析错误：数字格式错误
        }
        fprintf(stderr, "-->bulk_len3\n");

        // 更新 parse_done 到长度头结束位置（跳过 \r\n）
        c->parse_done = end + 2 - c->rbuf;

        // 处理 NULL bulk string（bulk_len == -1）
        if (c->bulk_len == (size_t)-1) {
          c->argv[c->argc_done].ptr = NULL;  // NULL 指针
          c->argv[c->argc_done].len = 0;     // 长度为 0
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
          goto error;  // 数据过大，拒绝处理
        }

        // 分配内存存储 bulk data（+1 用于 null terminator）
        c->argv[c->argc_done].ptr = kvs_malloc(c->bulk_len + 1);
        if (!c->argv[c->argc_done].ptr) {
          goto error;  // 内存分配失败
        }
        c->argv[c->argc_done].len = c->bulk_len;        // 记录长度
        c->argv[c->argc_done].ptr[c->bulk_len] = '\0';  // 添加 null terminator

        // 切换到 ST_RESP_BULK_DATA 状态，准备接收 bulk data
        c->bulk_done = 0;  // 重置已接收的 bulk data 长度
        c->resp_state = ST_RESP_BULK_DATA;
        break;
      }
      case ST_RESP_BULK_DATA: {
        // 计算还需要接收多少 bulk data
        
        fprintf(stderr, "-->bulk_data\n");

        size_t want = c->bulk_len - c->bulk_done;

        // 计算 rbuf 中还有多少数据可用
        size_t avail = c->rlen - c->parse_done;

        // 计算本次可以复制的数据量（取 want 和 avail 的较小值）
        size_t cp = (want < avail) ? want : avail;
        fprintf(stderr, "cp == %d\n", cp);
        // 从 rbuf 复制数据到 argv[argc_done].ptr
        if (cp > 0) {

          memcpy(c->argv[c->argc_done].ptr + c->bulk_done,
            c->rbuf + c->parse_done, cp);
        }
          
        fprintf(stderr, "bulk_done1 == %d\n", c->bulk_done);
        // 更新 bulk_done（已接收的 bulk data 长度）
        c->bulk_done += cp;
        fprintf(stderr, "bulk_done2 == %d\n", c->bulk_done);
        
        fprintf(stderr, "parse_done1 == %d\n", c->parse_done);
        // 更新 parse_done（rbuf 中已处理的数据位置）
        c->parse_done += cp;
        fprintf(stderr, "parse_done2 == %d\n", c->parse_done);

        // 检查 bulk data 是否接收完成
        fprintf(stderr, "c->bulk_done:%d != c->bulk_len: %d\n", c->bulk_done, c->bulk_len);
        if (c->bulk_done == c->bulk_len) {
          // bulk data 收全了，现在检查是否有 \r\n

          // 检查 rbuf 中是否有足够的数据接收 \r\n
          fprintf(stderr, "data:--> 1\n");
          if (c->parse_done + 2 > c->rlen) {
            fprintf(stderr, "data:--> 01");
            // 缺失\r\n，等待更多数据
            // 保留已接收的部分 \r\n;
            goto continue_recv;
          }
          
          fprintf(stderr, "data:--> 2\n");
          // 检查 \r\n 是否正确
          if (c->rbuf[c->parse_done] != '\r' ||
            c->rbuf[c->parse_done + 1] != '\n') {
              goto error;  // 协议错误：缺少 \r\n
            }
          fprintf(stderr, "data:--> 3\n");
            
          // 跳过 \r\n（2 字节）
          c->parse_done += 2;
          fprintf(stderr, "跳过\\r\\n: c->parse_done=%d\n", c->parse_done);
          // 参数解析完成，更新 argc_done
          c->argc_done++;

          // 检查是否所有参数解析完毕
          if (c->argc_done == c->argc) {
            // 所有参数解析完毕，切换到完成状态
            fprintf(stderr, "change to: OK\n");
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
        fprintf(stderr, "不应该来这\n");
        break;
      }
    }

  }
  // 循环结束，检查是否所有参数解析完毕
  if (c->resp_state == ST_RESP_OK) {
    // 所有参数解析完毕
    // 检查是否所有数据都已处理
    fprintf(stderr, "c->parse_done: %zu    c->rlen: %zu\n", c->parse_done, c->rlen);
    if (c->parse_done >= c->rlen) {
      // 所有数据都已处理，重置 rlen 和 parse_done
      // c->rlen = 0;
      // c->parse_done = 0;
    
      fprintf(stderr, "应该来这\n");
      return RESP_PARSE_OK;

    } else {
      // 还有未解析的数据（例如：一条命令解析完毕，但 rbuf 中还有下一条命令的部分数据）
      // 移动未解析的数据到 rbuf 开头
      
        fprintf(stderr, "不应该来这啊,你不可能出循环的!\n");
        goto error;
      // size_t remaining = c->rlen - c->parse_done;
      // memmove(c->rbuf, c->rbuf + c->parse_done, remaining);
      // c->rlen = remaining;
      // c->parse_done = 0;
      // return RESP_CONTINUE_REMAINING_RECV;
    }
    return RESP_PARSE_OK;
  } else if (c->parse_done < c->rlen) {
    fprintf(stderr, "c->parse_done: %zu    c->rlen: %zu\n", c->parse_done, c->rlen);
    // 还有未解析的数据，继续解析
    // 这种情况不应该发生，因为 while 循环会继续处理
    goto continue_recv;
  } else {
    fprintf(stderr, "c->parse_done: %zu    c->rlen: %zu\n", c->parse_done, c->rlen);
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
      fprintf(stderr, "-->b\n");
      // 所有数据都已处理，重置 rlen
      c->rlen = 0;
      c->parse_done = 0;
      goto continue_recv;
  }
    fprintf(stderr, "-->c\n");
    fprintf(stderr, "bulk_len: %zu\nargc_done: %d\nbulk_done: %zu\n", c->bulk_len, c->argc_done, c->bulk_done);

    goto continue_recv;  // 需要更多数据
  }

  continue_recv:
    size_t remaining = c->rlen - c->parse_done;
    memmove(c->rbuf, c->rbuf + c->parse_done, remaining);
    memset(c->rbuf + remaining, 0, c->rlen - remaining);
    c->rlen = remaining;
    c->parse_done = 0;
    fprintf(stderr, "留下了谁? %d bytes: %*s\n", remaining , remaining, c->rbuf);
    return RESP_CONTINUE_RECV;

  error:
    return RESP_ERROR;
}
