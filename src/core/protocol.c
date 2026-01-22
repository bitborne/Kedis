#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include "../../include/kvs_protocol.h"
#include "../../kvstore.h"



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
  
  // 重置流式接收状态
  c->streaming_recv = 0;           // 退出流式模式，回到正常模式
  c->remaining_bulk_len = 0;       // 重置剩余 bulk data 长度
  c->need_crlf = 0;                // 重置 \r\n 标记
  
  // 重置流式发送状态
  c->streaming_send = 0;           // 退出流式发送模式，回到正常模式
  c->streaming_data = NULL;        // 重置数据源指针（不负责释放）
  c->streaming_len = 0;            // 重置数据总长度
  c->streaming_sent = 0;           // 重置已发送的字节数
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
  size_t len = c->r_len;
  char* data = c->frame;
  size_t done = 0;

  // 目标, 把 r_len: 目前收到的数据, 全部处理成 RESP
  while (done < len) {
    switch (c->resp_state) {
      case ST_RESP_HDR: {
        // 期待 *<argc>\r\n
        char* p = data + done;
        char* nl = memchr(p, '\n', len - done);
        if (!nl) return done;  // 还没收全一行

        if (nl <= p || *(nl - 1) != '\r') return -1;  // 格式错误

        char prefix = *p;
        long num = strtol(p + 1, NULL, 10);  // 跳过前缀解析数字

        if (prefix == '*') {
          c->multibulk_len = num; // 找到了想要的值
          c->argc = 0; // 接下来开始解析各个段咯,argc是已经收取的段数量
          if (num <= 0 || num > MAX_ARGC) return -1;
          c->resp_state = ST_RESP_BULK_LEN;  // 接下来期待参数长度
        } else {
          // 如果不是 *, 可能直接是 Inline command? 这里只支持标准 RESP 数组
          return -1;
        }

        done += (nl - p) + 1;  // 跳过这行
        break;
      }
      case ST_RESP_BULK_LEN: {
        // 期待 $<len>\r\n
        char* p = data + done;  // 当前所在位置
        char* nl = memchr(p, '\n', len - done);
        if (!nl) return done;

        if (nl <= p || *(nl - 1) != '\r' || *p != '$') return -1; // 检查是否合法(RESP协议)

        long len_val = strtol(p + 1, NULL, 10);
        c->bulk_len = len_val; // 获取到想要的了!

        if (len_val < 0) {  // NULL Bulk String ($ -1)
          return -1;
        }
        if (len_val > MAX_SEG_SIZE) return -1; // 超过1GB的 Key 或者 Value,不读

        // 分配内存准备接收数据
        c->seg_buf = kvs_malloc(len_val + 1);  // +1 for null terminator
        if (!c->seg_buf) {
          return -1;  // 内存分配失败
        }
        c->seg_buf[len_val] = '\0';
        c->seg_used = 0;

        c->resp_state = ST_RESP_BULK_DATA;
        done += (nl - p) + 1;
        break;
      }
      case ST_RESP_BULK_DATA: {
        // want: 还需要多少 bulk data（从 seg_used 到 bulk_len 的距离）
        size_t want = c->bulk_len - c->seg_used;
        
        // avail: frame 中还有多少数据可用（从 done 到 len 的距离）
        size_t avail = len - done;
        
        // cp: 这次复制多少数据（取 want 和 avail 的较小值，避免越界）
        size_t cp = (want < avail) ? want : avail;

        // 从 frame 复制数据到 seg_buf
        memcpy(c->seg_buf + c->seg_used, data + done, cp);
        
        // 更新 seg_used（已经接收的 bulk data 长度）
        c->seg_used += cp;
        
        // 更新 done（frame 中已经处理的数据位置）
        done += cp;

        // 检查 bulk data 是否收全
        if (c->seg_used == (size_t)c->bulk_len) {
          // bulk data 收全了，现在检查 \r\n
          
          // 检查 frame 中是否有足够的数据接收 \r\n
          if (done + 2 > len) {
            // frame 中的数据已经处理完了，但还没收到 \r\n
            
            // 移除 frame 中已处理的数据
            int left = len - done;
            if (left > 0 && done > 0) {
              memmove(c->frame, c->frame + done, left);
            }
            c->r_len = left;
            
            // 设置流式接收状态
            c->streaming_recv = 1;           // 进入流式模式
            c->remaining_bulk_len = 0;       // bulk data 已经收全
            c->need_crlf = 1;                // 还需要接收 \r\n
            
            // 返回特殊值，告诉主循环需要流式接收
            return NEED_STREAMING_RECV;
          }
          
          // 检查 \r\n 是否正确
          if (data[done] != '\r' || data[done + 1] != '\n') {
            return -1;  // 协议错误
          }
          done += 2;
          
          // 参数完整，存入 argv
          c->argv[c->argc++] = (robj){c->seg_buf, c->bulk_len};
          c->seg_buf = NULL;  // 权责移交，argv 负责 seg_buf 的内存
          
          // 检查是否所有参数都解析完毕
          if (c->argc == c->multibulk_len) {
            // 所有参数解析完毕
            
            // 移除 frame 中已处理的数据
            int left = len - done;
            if (left > 0) {
              memmove(c->frame, c->frame + done, left);
            }
            c->r_len = left;
            
            return PARSE_OK;
          } else {
            // 继续下一个参数
            c->resp_state = ST_RESP_BULK_LEN;
          }
        } else {
          // bulk data 没收全，且 frame 中的数据已经处理完了
          
          // 移除 frame 中已处理的数据
          int left = len - done;
          if (left > 0 && done > 0) {
            memmove(c->frame, c->frame + done, left);
          }
          c->r_len = left;
          
          // 设置流式接收状态
          c->streaming_recv = 1;                           // 进入流式模式
          c->remaining_bulk_len = c->bulk_len - c->seg_used;  // 还需要接收多少 bulk data
          c->need_crlf = 1;                                // 收全 bulk data 后还需要 \r\n
          
          // 返回特殊值，告诉主循环需要流式接收
          return NEED_STREAMING_RECV;
        }
        break;
      }
    }
  }

  // 循环结束（数据耗尽），移除已处理数据
  int left = len - done;
  if (left > 0 && done > 0) {
    memmove(c->frame, c->frame + done, left);
  }
  c->r_len = left;

  return 0;  // 需要更多数据
}
