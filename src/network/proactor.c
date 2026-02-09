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
// [DEBUG]
int g_100 = 0;
#include "../../include/kvs_protocol.h"
#include "../../kvstore.h"

/* ---------------- 常量定义 ---------------- */
#define MAX_CONNS 100000   // 最大并发连接数
#define BACKLOG 4096       // listen 队列长度
#define RING_ENTRIES 8192  // io_uring 队列深度

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
// extern __thread int current_processing_fd;

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
  // fprintf(stderr, "-->get sqe\n");
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

/* ---------------- 提交异步 close ---------------- */
static void post_close(struct io_uring* ring, struct conn* c) {
  struct io_uring_sqe* sqe = sqe_prep(ring, c);
  io_uring_prep_close(sqe, c->fd);
}
/* ---------------- 提交异步 recv ---------------- */
static void post_recv_frame(struct io_uring* ring, struct conn* c) {
  // 获取一个 SQE（提交队列条目）
  struct io_uring_sqe* sqe = sqe_prep(ring, c);
  // fprintf(stderr, "before post: rlen = %zu\n", c->rlen);
  io_uring_prep_recv(sqe, c->fd, c->rbuf + c->rlen, IOP_SIZE - c->rlen, 0);
}

/* ---------------- 提交异步 send：回 RESP 包 ---------------- */
static void post_send_resp(struct io_uring* ring, struct conn* c) {
  // 获取一个 SQE（提交队列条目）
  struct io_uring_sqe* sqe = sqe_prep(ring, c);
  // if (g_100++ <= 100) fprintf(stderr, "--> c->wbuf : %*s\n", c->wlen, c->wbuf);
  io_uring_prep_send(sqe, c->fd, c->wbuf, c->wlen, 0);
}

/* ---------------- 释放连接资源 ---------------- */
static void conn_free(struct conn* c) {
  if (c->fd >= 0) {
    close(c->fd);
  }

  // 释放协议相关资源
  kvs_resp_free_resources(c);

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

/* --------------  业务入口：处理解析好的argv,并准备wbuf  -------------- */
static int processCommand(struct conn* c) {
  // 直接调用核心逻辑
  // 核心逻辑会根据 c->argv 处理命令，并将结果写入 c->wbuf
  return g_kvs_handler(c);
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
            // conn_reset(nc); -> kvs_resp_reset
            kvs_resp_reset(nc);
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
      // 来了一个请求, 处理一下
      case ST_RECV: {
        if (res == 0) {  // EOF，客户端关闭连接
          conn_free(c);
          conn_pool_free(&g_conn_pool, c);
          break;
        } else if (res > 0) {
          // 将新接收的数据追加到 rbuf 的末尾
          // 注意：c->rlen 是当前 rbuf 中的有效数据长度
          // 新数据从 c->rbuf + c->rlen 的位置开始写入

          // [可以作为最高级日志级别的打印信息]
          // fprintf(stderr, "[DEBUG]: r->len == %zu\n", c->rlen);
          // fprintf(stderr, "[DEBUG]: recv (%d bytes):\n%.*s\n====\n", res, res, c->rbuf + c->rlen);
          c->rlen += res;  // 累加接收的字节数（不是覆盖）
          c->parse_done = 0;
          // fprintf(stderr, "=========数据包来啦!整个缓冲区哦!\n%*s\n", IOP_SIZE,c->rbuf); // 调用 kvs_resp_feed 解析数据
          
          int ret = kvs_resp_feed(c);
          // fprintf(stderr, "==> ret = %d\n", ret);
          if (ret == RESP_ERROR) {
            // 协议错误，关闭连接
            fprintf(stderr, "kvs_resp_feed: RESP parse error\n");
            conn_free(c);
            conn_pool_free(&g_conn_pool, c);
          } else if (ret == RESP_PARSE_OK) {
            // 解析完成，处理命令
            processCommand(c);
            // 切换到发送状态
            c->state = ST_SEND;
            // 投递发送请求
            // fprintf(stderr, "==> after process wbuf: %*s\n", c->wbuf, RESP_BUF_SIZE);
            post_send_resp(&g_ring, c);
          } else if (ret == RESP_CONTINUE_RECV) {
            // ret == 0，需要更多数据
            // 提交 recv 请求，等待更多数据
            post_recv_frame(&g_ring, c);
          }
        }
        break;
      }

      // 发我们准备好的数据过去
      case ST_SEND: {
        if (c->wlen != res) { // 数据未发完
          fprintf(stderr, "wlen != sent\n");
          // 简易处理, 不管提交了多少, 整个 wbuf 全部重新提交
          post_send_resp(&g_ring, c);  // 重新提交 
          break;
        }
        c->wlen = 0;
        memset(c->wbuf, 0, RESP_BUF_SIZE);
        // fprintf(stderr, "======= start =======\n");
        if (c->send_st == ST_SEND_SMALL) {
          // fprintf(stderr, "=======  small \n");
          // 可以断言数据已经发完了
          c->state = ST_RECV;
          c->bulk_p = c->bulk_data = NULL;
          c->bulk_sent = 0;
          c->bulk_tt = 0;
          memset(c->wbuf, 0, RESP_BUF_SIZE);
          c->hdr_len = 0;
          c->wlen = 0;
          post_recv_frame(&g_ring, c);
          break;
        } else {
          // fprintf(stderr, "=======  big \n");
            if (c->send_st == ST_SEND_HDR_SENT) {
              c->bulk_sent += res - c->hdr_len;
              c->send_st = ST_SEND_BULK;
            }
            else if (c->send_st == ST_SEND_BULK) {
              // fprintf(stderr, "走这里吧\n");
              c->bulk_sent += res;
            } else {
              fprintf(stderr, "error: ST_SEND_NOTSET\n");
              conn_free(c);
              conn_pool_free(&g_conn_pool, c);
            }
            c->bulk_p = c->bulk_data + c->bulk_sent;
            if (c->bulk_sent < c->bulk_tt) {
              // 1. 计算当前这一波该发多少
              // 逻辑：剩下的数据量 和 缓冲区大小，谁小取谁
              size_t remain = c->bulk_tt - c->bulk_sent;
              size_t avail = RESP_BUF_SIZE - c->wlen;
              size_t cp = (avail < remain ? avail : remain);
              // 2. 将数据拷贝到定长缓冲区（或直接从原内存地址发送）
              // memcpy(fixed_buffer, data.ptr() + sent_offset, chunk_size);
              memcpy(c->wbuf + c->wlen, c->bulk_p, cp);
              c->wlen += cp;
              if (remain <= avail) {
                c->wbuf[c->wlen - 2] = '\r';
                c->wbuf[c->wlen - 1] = '\n';
              } else if (remain == avail + 1) {
                c->wbuf[c->wlen - 1] = '\r';
              } else if (remain == 1) {
                c->wbuf[c->wlen] = '\n';
              }
              // if (g_100++ <= 100) fprintf(stderr, "这里死循环?\n");
              // if (g_100++ <= 100) fprintf(stderr, "bulk_sent : %zu\n", c->bulk_sent);
              // if (g_100++ <= 100) fprintf(stderr, "p - buf: %zu\n", c->bulk_p - c->wbuf);
              post_send_resp(&g_ring, c);
            } else {
              // bulk 发完了!
              c->state = ST_RECV;
              c->bulk_p = c->bulk_data = NULL;
              c->bulk_sent = 0;
              c->bulk_tt = 0;
              memset(c->wbuf, 0, RESP_BUF_SIZE);
              c->hdr_len = 0;
              c->wlen = 0;
              post_recv_frame(&g_ring, c);
            }
          }
          break;
        }
      case ST_CLOSE: {
        conn_free(c);
        conn_pool_free(&g_conn_pool, c);
        break;
      }
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
