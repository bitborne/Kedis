// proactor.c - Proactor network model with RESP protocol and io_uring
#define _GNU_SOURCE
#include <assert.h>
#include <ctype.h>
#include <errno.h>
#include <liburing.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include "kvstore.h"

/* ---------------- 常量定义 ---------------- */
#define MAX_CONNS 100000                   // 最大并发连接数
#define BACKLOG 4096                       // listen 队列长度
#define IOP_SIZE (16 * 1024)               // 每次 recv/send 的帧大小（16 KB）
#define RING_ENTRIES 8192                  // io_uring 队列深度
#define MAX_SEG_SIZE (1024 * 1024 * 1024)  // 单段最大 1 GB（可自行调大）
#define MAX_ARGC 64                        // 最大参数个数
#define RESP_BUF_SIZE (64 * 1024)          // 响应缓冲区大小

/* ---------------- 连接状态机 ---------------- */
#define ST_RECV 1
#define ST_SEND 2
#define ST_CLOSE 3

/* ---------------- RESP 状态机枚举 ---------------- */
typedef enum {
  ST_RESP_HDR,        // 等待解析 *<argc> (命令开始)
  ST_RESP_BULK_LEN,   // 等待解析 $<len> (参数长度)
  ST_RESP_BULK_DATA,  // 正在收 bulk 内容
} resp_state_t;

/* ---------------- 段对象：只挂指针，不拷贝数据 ---------------- */
typedef struct {
  char* ptr;   // 指向堆内存
  size_t len;  // 段长度
} robj;

/* ---------------- 连接上下文 ---------------- */
struct conn {
  int fd;         // TCP 套接字
  int state;      // io_uring 状态：ST_RECV / ST_SEND / ST_CLOSE
  int next_free;  // 空闲链表中的下一个连接索引

  /* ---- 读流 ---- */
  char frame[IOP_SIZE];  // 读缓冲区（16 KB）
  int r_len;             // 缓冲区内有效数据长度

  /* RESP 状态机 */
  resp_state_t resp_state;
  long bulk_len;        // 当前段剩余长度 (需要读取的长度)
  int multibulk_len;    // 期望的参数个数 (argc)
  int argc;             // 已收段数
  robj argv[MAX_ARGC];  // 命令段数组 (每个 ptr 都需要 malloc)

  /* ---- 当前正在解析的参数 ---- */
  char* seg_buf;    // 当前参数的 buffer
  size_t seg_used;  // 当前参数已填入的字节数

  /* ---- 写回 ---- */
  char* wbuf;       // 回包缓冲（+OK\r\n 或 $len\r\n...）
  int wlen, wdone;  // 总长度 & 已发长度
};

/* ---------------- 连接池 ---------------- */
struct conn_pool {
  struct conn* conns;  // 连接数组
  int free_head;       // 空闲链表头（索引）
  int free_count;      // 空闲连接数
};

/* ---------------- 全局变量 ---------------- */
static struct conn_pool g_conn_pool;  // 全局连接池
static struct io_uring g_ring;        // 全局 io_uring 实例
static msg_handler g_kvs_handler;     // KV 协议处理器

/* ---------------- 外部函数声明 ---------------- */
extern void before_sleep(void);
extern __thread int current_processing_fd;

/* ---------------- 连接池管理 ---------------- */
static void conn_pool_init(struct conn_pool* pool, int max_conns) {
  pool->conns = kvs_malloc(max_conns * sizeof(struct conn));
  if (!pool->conns) {
    perror("kvs_malloc conn pool");
    exit(1);
  }

  // 初始化空闲链表
  for (int i = 0; i < max_conns; i++) {
    pool->conns[i].fd = -1;
    pool->conns[i].next_free = i + 1;
  }
  pool->conns[max_conns - 1].next_free = -1;

  pool->free_head = 0;
  pool->free_count = max_conns;
}

static struct conn* conn_pool_alloc(struct conn_pool* pool) {
  if (pool->free_count == 0) {
    return NULL;  // 连接池耗尽
  }

  int idx = pool->free_head;
  pool->free_head = pool->conns[idx].next_free;
  pool->free_count--;

  return &pool->conns[idx];
}

static void conn_pool_free(struct conn_pool* pool, struct conn* c) {
  int idx = c - pool->conns;
  c->fd = -1;
  c->next_free = pool->free_head;
  pool->free_head = idx;
  pool->free_count++;
}

/* ---------------- 工具：拿 SQE 并填 user_data ---------------- */
static struct io_uring_sqe* sqe_prep(struct io_uring* ring, struct conn* c) {
  struct io_uring_sqe* sqe = io_uring_get_sqe(ring);
  if (!sqe) {
    fprintf(stderr, "get_sqe failed\n");
    exit(1);
  }
  io_uring_sqe_set_data(sqe, c);  // 后面 CQE 能反解出 conn
  return sqe;
}

/* ---------------- 提交异步 accept ---------------- */
static void post_accept(struct io_uring* ring, int listenfd) {
  // 为 accept 额外 malloc 地址信息，避免踩栈
  struct sockaddr_in* addr = kvs_malloc(sizeof(*addr));
  socklen_t* len = kvs_malloc(sizeof(*len));
  *len = sizeof(*addr);

  struct io_uring_sqe* sqe = io_uring_get_sqe(ring);
  io_uring_prep_accept(sqe, listenfd, (struct sockaddr*)addr, len, 0);
  /* 魔法值：accept 事件的 user_data 固定为 -1，主循环据此识别 */
  struct conn* dummy = (struct conn*)-1;
  io_uring_sqe_set_data(sqe, dummy);
}

/* ---------------- 提交异步 recv ---------------- */
static void post_recv_frame(struct io_uring* ring, struct conn* c) {
  // 如果 buffer 快满了，移动数据到开头
  if (c->r_len == IOP_SIZE) {
    fprintf(stderr, "Buffer full, connection too slow or msg too big\n");
    // 这里简单处理：关闭连接，或者扩容。Demo 中选择不做处理等待出错。
  }
  struct io_uring_sqe* sqe = sqe_prep(ring, c);
  // 从 r_len 处开始接收，最大接收 IOP_SIZE - r_len
  io_uring_prep_recv(sqe, c->fd, c->frame + c->r_len, IOP_SIZE - c->r_len, 0);
}

/* ---------------- 提交异步 send：回 RESP 包 ---------------- */
static void post_send_resp(struct io_uring* ring, struct conn* c) {
  struct io_uring_sqe* sqe = sqe_prep(ring, c);
  io_uring_prep_send(sqe, c->fd, c->wbuf + c->wdone, c->wlen - c->wdone, 0);
}

/* ---------------- 提交异步 close ---------------- */
static void post_close(struct io_uring* ring, struct conn* c) {
  struct io_uring_sqe* sqe = sqe_prep(ring, c);
  io_uring_prep_close(sqe, c->fd);
}

/* ---------------- 释放连接资源 ---------------- */
static void conn_free(struct conn* c) {
  if (c->fd >= 0) {
    close(c->fd);
  }

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

  if (c->wbuf) {
    kvs_free(c->wbuf);
    c->wbuf = NULL;
  }

  c->fd = -1;
}

/* ---------------- 监听端口 ---------------- */
static int init_listen(uint16_t port) {
  int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    perror("socket");
    return -1;
  }

  int one = 1;
  setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
  setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one));

  struct sockaddr_in addr = {0};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  addr.sin_addr.s_addr = INADDR_ANY;

  if (bind(fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
    perror("bind");
    close(fd);
    return -1;
  }

  listen(fd, BACKLOG);
  return fd;
}

/* ---------------- 重置连接解析状态 ---------------- */
static void conn_reset(struct conn* c) {
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

/* --------------  RESP 流式解析：啃掉 data[]，返回已用字节 -------------- */
#define PARSE_OK 1
static int resp_feed(struct conn* c) {
  size_t len = c->r_len;
  char* data = c->frame;
  size_t done = 0;

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
          c->multibulk_len = num;
          c->argc = 0;
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
        char* p = data + done;
        char* nl = memchr(p, '\n', len - done);
        if (!nl) return done;

        if (nl <= p || *(nl - 1) != '\r') return -1;

        if (*p != '$') return -1;  // 必须是 $

        long len_val = strtol(p + 1, NULL, 10);
        c->bulk_len = len_val;

        if (len_val < 0) {  // NULL Bulk String ($ -1)
          // 这里暂不支持作为参数的 NULL，简单当作空字符串或报错
          // 标准 Redis 协议中参数通常不为 NULL
          return -1;
        }
        if (len_val > MAX_SEG_SIZE) return -1;

        // 分配内存准备接收数据
        c->seg_buf = kvs_malloc(len_val + 1);  // +1 for null terminator
        if (!c->seg_buf) {
          return -1;  // 内存分配失败
        }
        c->seg_buf[len_val] = '\0';  // 方便调试打印
        c->seg_used = 0;

        c->resp_state = ST_RESP_BULK_DATA;
        done += (nl - p) + 1;
        break;
      }
      case ST_RESP_BULK_DATA: {
        // 读取 bulk_len 字节
        size_t want = c->bulk_len - c->seg_used;
        size_t avail = len - done;
        size_t cp = (want < avail) ? want : avail;

        memcpy(c->seg_buf + c->seg_used, data + done, cp);
        c->seg_used += cp;
        done += cp;

        if (c->seg_used == (size_t)c->bulk_len) {
          // 数据读完了，期待 \r\n
          if (done + 2 > len) {
            // 还没收到 \r\n，等待
            return done;
          }

          if (data[done] != '\r' || data[done + 1] != '\n') {
            return -1;
          }
          done += 2;

          // 参数完整，存入 argv
          c->argv[c->argc++] = (robj){c->seg_buf, c->bulk_len};
          c->seg_buf = NULL;  // 权责移交

          if (c->argc == c->multibulk_len) {
            // 所有参数解析完毕

            // 移除已处理数据
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

/* --------------  协议转换：RESP -> kvs_protocol  -------------- */
static int resp_to_kvs(struct conn* c, char* kvs_msg, int max_len) {
  if (c->argc == 0) return -1;

  int offset = 0;
  for (int i = 0; i < c->argc; i++) {
    if (offset + c->argv[i].len + 1 > max_len) {
      return -1;  // 缓冲区不足
    }
    memcpy(kvs_msg + offset, c->argv[i].ptr, c->argv[i].len);
    offset += c->argv[i].len;
    if (i < c->argc - 1) {
      kvs_msg[offset++] = ' ';  // 参数间用空格分隔
    }
  }
  kvs_msg[offset] = '\0';
  return offset;
}

/* --------------  协议转换：kvs_protocol -> RESP  -------------- */
static int kvs_to_resp(const char* kvs_resp, int kvs_len, char* resp_buf,
                       int max_len) {
  // kvs_protocol 返回格式分析：
  // - "OK\r\n" -> RESP: "+OK\r\n"
  // - "ERROR\r\n" -> RESP: "-ERROR\r\n"
  // - "value\r\n" -> RESP: "$5\r\nvalue\r\n"
  // - "ERROR / Not Exist\r\n" -> RESP: "-ERROR / Not Exist\r\n"
  // - "UNKNOWN COMMAND\r\n" -> RESP: "-UNKNOWN COMMAND\r\n"

  if (kvs_len <= 0) {
    // 空响应
    return snprintf(resp_buf, max_len, "$0\r\n\r\n");
  }

  // 检查是否是 OK 响应
  if (kvs_len >= 2 && strcmp(kvs_resp, "OK\r\n") == 0) {
    return snprintf(resp_buf, max_len, "+OK\r\n");
  }

  // 检查是否是错误响应（以 ERROR 开头或 UNKNOWN）
  if (strncmp(kvs_resp, "ERROR", 5) == 0 ||
      strncmp(kvs_resp, "UNKNOWN", 7) == 0 ||
      strncmp(kvs_resp, "READONLY", 8) == 0) {
    // 去掉末尾的 \r\n，加上 RESP 错误前缀
    int len = kvs_len;
    if (kvs_resp[len - 1] == '\n') len--;
    if (kvs_resp[len - 1] == '\r') len--;
    return snprintf(resp_buf, max_len, "-%.*s\r\n", len, kvs_resp);
  }

  // 检查是否是存在性响应
  if (strncmp(kvs_resp, "YES", 3) == 0 || strncmp(kvs_resp, "NO", 2) == 0) {
    // 作为整数返回：1 表示存在，0 表示不存在
    int exist = (kvs_resp[0] == 'Y') ? 1 : 0;
    return snprintf(resp_buf, max_len, ":%d\r\n", exist);
  }

  // 其他情况作为 bulk string 返回（去掉末尾的 \r\n）
  int len = kvs_len;
  if (kvs_resp[len - 1] == '\n') len--;
  if (kvs_resp[len - 1] == '\r') len--;

  if (len == 0) {
    return snprintf(resp_buf, max_len, "$0\r\n\r\n");
  }

  int needed = snprintf(resp_buf, max_len, "$%d\r\n", len);
  if (needed < 0 || needed + len + 2 > max_len) return -1;
  memcpy(resp_buf + needed, kvs_resp, len);
  needed += len;
  memcpy(resp_buf + needed, "\r\n", 2);
  return needed + 2;
}

/* --------------  业务入口：处理命令  -------------- */
static int processCommand(struct conn* c) {
  // 将 RESP 命令转换为 kvs_protocol 格式
  char kvs_msg[1024] = {0};
  int kvs_len = resp_to_kvs(c, kvs_msg, sizeof(kvs_msg));
  if (kvs_len < 0) {
    c->wlen = snprintf(c->wbuf, RESP_BUF_SIZE, "-ERR command too long\r\n");
    return 0;
  }

  // // 调试日志（使用 stderr 避免缓冲）
  // fprintf(stderr, "[DEBUG]: kvs_msg = [%s], len = %d, argc = %d\n", kvs_msg,
  //         kvs_len, c->argc);

  // 调用 kvs_handler 处理
  char kvs_resp[1024] = {0};
  int ret = g_kvs_handler(kvs_msg, kvs_len, kvs_resp);

  // 调试日志（使用 stderr 避免缓冲）
  // fprintf(stderr, "[DEBUG]: kvs_resp = [%s], len = %d\n", kvs_resp, ret);

  // 将 kvs_response 转换为 RESP 格式
  c->wlen = kvs_to_resp(kvs_resp, ret, c->wbuf, RESP_BUF_SIZE);
  if (c->wlen < 0) {
    c->wlen = snprintf(c->wbuf, RESP_BUF_SIZE, "-ERR internal error\r\n");
  }

  return 0;
}

/* --------------  proactor_start：主入口  -------------- */
int proactor_start(unsigned short port, msg_handler handler) {
  int listenfd = init_listen(port);
  if (listenfd < 0) {
    return -1;
  }

  g_kvs_handler = handler;
  signal(SIGPIPE, SIG_IGN);

  /* 初始化连接池 */
  conn_pool_init(&g_conn_pool, MAX_CONNS);

  /* 初始化 io_uring */
  io_uring_queue_init(RING_ENTRIES, &g_ring, 0);
  post_accept(&g_ring, listenfd);

  printf("Proactor server listening on port %d...\n", port);

  while (1) {
    io_uring_submit(&g_ring);
    struct io_uring_cqe* cqe;
    if (io_uring_wait_cqe(&g_ring, &cqe) < 0) break;

    struct conn* c = (struct conn*)io_uring_cqe_get_data(cqe);
    int res = cqe->res;
    io_uring_cqe_seen(&g_ring, cqe);

    /* -------- accept 事件 -------- */
    if (c == (struct conn*)-1) {
      if (res >= 0) {  // 新连接成功
        struct conn* nc = conn_pool_alloc(&g_conn_pool);
        if (!nc) {
          close(res);
          fprintf(stderr, "Max conns reached, rejecting connection\n");
        } else {
          nc->fd = res;
          nc->state = ST_RECV;
          nc->wbuf = kvs_malloc(RESP_BUF_SIZE);
          if (!nc->wbuf) {
            conn_pool_free(&g_conn_pool, nc);
            close(res);
            fprintf(stderr, "Failed to alloc write buffer\n");
          } else {
            nc->r_len = 0;
            nc->wlen = nc->wdone = 0;
            conn_reset(nc);                // 清空解析状态
            post_recv_frame(&g_ring, nc);  // 投递第一个 recv
          }
        }
        post_accept(&g_ring, listenfd);  // 继续监听
      } else {
        if (res != -EAGAIN && res != -EINTR) perror("accept");
        post_accept(&g_ring, listenfd);
      }
      continue;
    }

    /* -------- 读写错误 -------- */
    if (res < 0) {
      if (res == -EAGAIN || res == -EINTR) {  // 可重试
        if (c->state == ST_RECV) post_recv_frame(&g_ring, c);
        if (c->state == ST_SEND) post_send_resp(&g_ring, c);
      } else {  // 致命错误
        conn_free(c);
        conn_pool_free(&g_conn_pool, c);
      }
      continue;
    }

    /* -------- 正常业务 -------- */
    switch (c->state) {
      case ST_RECV: {
        if (res == 0) {  // EOF
          conn_free(c);
          conn_pool_free(&g_conn_pool, c);
          break;
        } else if (res > 0) {
          c->r_len += res;  // 更新有效数据长度
        }

        int n = resp_feed(c);  // 喂给 RESP 状态机

        if (n < 0) {
          conn_free(c);
          conn_pool_free(&g_conn_pool, c);  // 协议错误
        } else if (n == PARSE_OK) {         // 整条命令完整
          current_processing_fd = c->fd;
          processCommand(c);
          current_processing_fd = -1;

          conn_reset(c);  // 清空解析状态（释放旧参数），准备下一次

          c->state = ST_SEND;
          c->wdone = 0;
          post_send_resp(&g_ring, c);  // 发送响应
        } else {
          // 数据不够，继续接收
          post_recv_frame(&g_ring, c);
        }
        break;
      }
      case ST_SEND:
        c->wdone += res;
        if (c->wdone == c->wlen) {  // 发完
          c->state = ST_RECV;

          // 如果有剩余数据，尝试解析下一条
          if (c->r_len > 0) {
            int n = resp_feed(c);
            if (n == PARSE_OK) {
              current_processing_fd = c->fd;
              processCommand(c);
              current_processing_fd = -1;
              conn_reset(c);
              c->state = ST_SEND;
              c->wdone = 0;
              post_send_resp(&g_ring, c);
            } else if (n < 0) {
              conn_free(c);
              conn_pool_free(&g_conn_pool, c);
            } else {
              // 依然不够
              post_recv_frame(&g_ring, c);
            }
          } else {
            post_recv_frame(&g_ring, c);  // 准备下一条命令
          }
        } else {
          post_send_resp(&g_ring, c);
        }
        break;
      case ST_CLOSE:
        conn_free(c);
        conn_pool_free(&g_conn_pool, c);
        break;
    }  // switch

    before_sleep();
  } /* while */

  close(listenfd);
  io_uring_queue_exit(&g_ring);

  // 释放连接池
  if (g_conn_pool.conns) {
    kvs_free(g_conn_pool.conns);
    g_conn_pool.conns = NULL;
  }

  return 0;
}