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
  fprintf(stderr, "before post: rlen = %zu\n", c->rlen);
  io_uring_prep_recv(sqe, c->fd, c->rbuf + c->rlen, IOP_SIZE - c->rlen, 0);
}

/* ---------------- 提交异步 send：回 RESP 包 ---------------- */
static void post_send_resp(struct io_uring* ring, struct conn* c) {
  // 获取一个 SQE（提交队列条目）
  struct io_uring_sqe* sqe = sqe_prep(ring, c);

  // 检查是否还有未发送的数据
  if (c->wdone < c->wlen) {
    // 还有 wbuf 中的数据未发送，继续发送 wbuf
    io_uring_prep_send(sqe, c->fd, c->wbuf + c->wdone, c->wlen - c->wdone, 0);
  } else {
    // wbuf 中的数据已发送完毕
    // wbuf 中的数据已发送完毕，需要填充下一帧数据

    // 检查是否是 Bulk String 格式（以 $ 开头）
    if (c->wlen > 0 && c->wbuf[0] == '$' && c->bulk_data != NULL) {
      // 这是 Bulk String 格式，需要流式发送大数据

      // 计算数据的长度（从 RESP 头部中解析）
      // RESP 头部格式：$<len>\r\n
      size_t data_len = 0;
      char* end = strstr(c->wbuf, "\r\n");
      if (end) {
        data_len = strtoul(c->wbuf + 1, NULL, 10);
      }

      // 检查是否还有数据未发送
      if (c->data_sent < data_len) {
        // 还有数据未发送，从 bulk_data 中拷贝下一帧到 wbuf

        // 计算这次可以拷贝的数据量
        size_t remaining = data_len - c->data_sent;  // 剩余数据长度
        size_t copy_size =
            (remaining < RESP_BUF_SIZE) ? remaining : RESP_BUF_SIZE;

        // 从 bulk_data + data_sent 的位置开始拷贝数据到 wbuf
        memcpy(c->wbuf, c->bulk_data + c->data_sent, copy_size);

        // 更新 wbuf 的有效长度
        c->wlen = copy_size;
        c->wdone = 0;  // 重置已发送位置

        // 投递发送 wbuf 的请求
        io_uring_prep_send(sqe, c->fd, c->wbuf, c->wlen, 0);
      } else {
        // 数据已全部发送完毕，现在发送最后的 \r\n

        // 将 \r\n 写入 wbuf
        memcpy(c->wbuf, "\r\n", 2);
        c->wlen = 2;
        c->wdone = 0;

        // 投递发送 \r\n 的请求
        io_uring_prep_send(sqe, c->fd, c->wbuf, c->wlen, 0);
      }
    } else {
      // 不是 Bulk String 格式，或者已经发送完毕
      // 不需要做任何操作，发送完成
    }
  }
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
          c->rlen += res;  // 累加接收的字节数（不是覆盖）
          c->parse_done = 0;
          // fprintf(stderr, "=========数据包来啦!整个缓冲区哦!\n%*s", IOP_SIZE,
          // c->rbuf); 调用 kvs_resp_feed 解析数据
          int ret = kvs_resp_feed(c);
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
        // 更新已发送的字节数
        c->wdone += res;

        // 检查当前帧是否发送完毕
        if (c->wdone == c->wlen) {
          // 当前帧发送完毕

          // 检查是否是 Bulk String 格式（以 $ 开头）
          int is_bulk_string = (c->wlen > 0 && c->wbuf[0] == '$' && c->bulk_data != NULL);

          if (is_bulk_string) {
            // 这是 Bulk String，需要更新 data_sent

            // 从 RESP 头部中提取数据长度
            size_t data_len = 0;
            char* end = strstr(c->wbuf, "\r\n");
            if (end) {
              data_len = strtoul(c->wbuf + 1, NULL, 10);
            }

            // 检查当前帧是否是头部帧
            int is_header_frame = (strstr(c->wbuf, "\r\n") != NULL);

            if (is_header_frame) {
              // 这是头部帧，不需要更新 data_sent
            } else {
              // 这是数据帧，更新 data_sent
              c->data_sent += c->wlen;
            }

            // 检查是否还有数据未发送
            if (c->data_sent < data_len) {
              // 还有数据未发送，继续发送
              post_send_resp(&g_ring, c);
            } else {
              // 数据已全部发送完毕（包括 \r\n），直接重置状态并切换到接收状态
                // 重置写缓冲区状态
                c->wlen = 0;
                c->wdone = 0;
                c->bulk_data = NULL;
                c->data_sent = 0;
                
                // 重置 RESP 解析状态
                c->resp_state = ST_RESP_HDR;
                c->argc = 0;
                c->argc_done = 0;
                c->bulk_len = 0;
                c->bulk_done = 0;
                
                // 切换到接收状态
                c->state = ST_RECV;
                post_recv_frame(&g_ring, c);
                // 如果 rlen 中还有数据（粘包），尝试解析下一条命令
              //   if (c->rlen > 0) {
              //     int n = kvs_resp_feed(c);
              //     if (n == RESP_PARSE_OK) {
              //       c->wlen = 0;
              //       c->wdone = 0;
              //       processCommand(c);
              //       c->state = ST_SEND;
              //       post_send_resp(&g_ring, c);
              //     }
              //   } else {
              //     // 没有数据，等待新的接收
              //     io_uring_prep_recv(sqe, c->fd, c->rbuf + c->rlen, 
              // IOP_SIZE - c->rlen, 0);
              //   }
            }
          } else {
            // 不是 Bulk String，发送完毕

            // 检查是否是 \r\n 帧
            int is_crlf_frame = (c->wlen == 2 && c->wbuf[0] == '\r' && c->wbuf[1] == '\n');

            if (is_crlf_frame || !is_bulk_string) {
              // 所有数据已发送完毕（\r\n 帧或非 Bulk String）

              // 重置写缓冲区状态
              c->wlen = 0;
              c->wdone = 0;
              c->bulk_data = NULL;
              c->data_sent = 0;

              // 重置 RESP 解析状态
              c->resp_state = ST_RESP_HDR;
              c->argc = 0;
              c->argc_done = 0;
              c->bulk_len = 0;
              c->bulk_done = 0;

              // 切换到接收状态
              c->state = ST_RECV;
              post_recv_frame(&g_ring, c);

              // // 如果 rlen 中还有数据（粘包），尝试解析下一条命令
              // if (c->rlen > 0) {
              //   int n = kvs_resp_feed(c);
              //   if (n == RESP_PARSE_OK) {
              //     c->wlen = 0;
              //     c->wdone = 0;
              //     processCommand(c);
              //     c->state = ST_SEND;
              //     post_send_resp(&g_ring, c);
              //   } else if (n == RESP_ERROR) {
              //     conn_free(c);
              //     conn_pool_free(&g_conn_pool, c);
              //   } else {
              //     post_recv_frame(&g_ring, c);
              //   }
              // } else {
              //   post_recv_frame(&g_ring, c);
              // }
            }
          }
        } else {
          // 当前帧未发送完毕，继续发送
          post_send_resp(&g_ring, c);
        }
        break;
      }
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
