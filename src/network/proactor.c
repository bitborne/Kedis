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

#include "../../kvstore.h"
#include "../../include/kvs_protocol.h"

/* ---------------- 常量定义 ---------------- */
#define MAX_CONNS 100000                   // 最大并发连接数
#define BACKLOG 4096                       // listen 队列长度
#define RING_ENTRIES 8192                  // io_uring 队列深度

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
    // 初始化流式接收相关字段
    pool->conns[i].recv_buffer = NULL;
    pool->conns[i].recv_buffer_size = 0;
    pool->conns[i].recv_head = 0;
    pool->conns[i].recv_tail = 0;
    pool->conns[i].is_streaming_recv = 0;
    pool->conns[i].bulk_target = NULL;
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

/* ---------------- 提交异步 close ---------------- */
static void post_close(struct io_uring* ring, struct conn* c);

/* ---------------- 提交异步 recv ---------------- */
static void post_recv_frame(struct io_uring* ring, struct conn* c) {
  // 确定缓冲区和写入位置
  char* buffer;
  size_t buffer_size;
  size_t write_pos;
  
  if (c->recv_buffer) {
    // 使用环形缓冲区
    buffer = c->recv_buffer;
    buffer_size = c->recv_buffer_size;
    write_pos = c->recv_head;
  } else {
    // 使用默认 frame 缓冲区
    buffer = c->frame;
    buffer_size = IOP_SIZE;
    write_pos = c->r_len;
  }
  
  // 计算可用空间
  size_t read_pos = c->recv_buffer ? c->recv_tail : 0;
  size_t available = (read_pos > write_pos) ? (read_pos - write_pos - 1) : (buffer_size - write_pos + read_pos);
  
  // 如果缓冲区满，尝试扩容
  if (available == 0) {
    if (c->recv_buffer == NULL) {
      // 使用默认 frame 时，可以扩容
      size_t new_size = buffer_size * 2;
      if (new_size <= 16 * 1024 * 1024) {  // 最大 16MB
        char* new_buffer = kvs_malloc(new_size);
        if (new_buffer) {
          memcpy(new_buffer, buffer, write_pos);
          kvs_free(c->recv_buffer);  // 如果之前有环形缓冲区，释放它
          c->recv_buffer = new_buffer;
          c->recv_buffer_size = new_size;
          c->recv_head = write_pos;
          c->recv_tail = 0;
          buffer = new_buffer;
          buffer_size = new_size;
          read_pos = 0;  // 新缓冲区，tail 重置为 0
          available = buffer_size - 1;
        } else {
          fprintf(stderr, "Failed to expand recv buffer, closing connection\n");
          c->state = ST_CLOSE;
          post_close(ring, c);
          return;
        }
      } else {
        fprintf(stderr, "Recv buffer full and cannot expand, closing connection\n");
        c->state = ST_CLOSE;
        post_close(ring, c);
        return;
      }
    } else {
      fprintf(stderr, "Ring buffer full, closing connection\n");
      c->state = ST_CLOSE;
      post_close(ring, c);
      return;
    }
  }
  
  // 计算本次接收长度
  size_t recv_len = available;
  if (c->recv_state == RECV_STATE_BULK_DATA && c->recv_bulk_remaining > 0) {
    // 优先接收 bulk 数据
    recv_len = (c->recv_bulk_remaining < available) ? c->recv_bulk_remaining : available;
  }
  
  // 投递接收请求
  struct io_uring_sqe* sqe = sqe_prep(ring, c);
  io_uring_prep_recv(sqe, c->fd, buffer + write_pos, recv_len, 0);
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

  // 释放流式接收资源
  if (c->recv_buffer) {
    kvs_free(c->recv_buffer);
    c->recv_buffer = NULL;
  }
  if (c->bulk_target) {
    kvs_free(c->bulk_target);
    c->bulk_target = NULL;
  }

  // 释放协议相关资源
  kvs_resp_free_resources(c);

  if (c->wbuf) {
    kvs_free(c->wbuf);
    c->wbuf = NULL;
  }

  c->fd = -1;
}

/* ---------------- 环形缓冲区函数 ---------------- */

// 写入环形缓冲区
int ring_buffer_write(struct conn* c, const char* data, size_t len) {
  if (!c || !data || len == 0) {
    return -1;
  }

  char* buffer;
  size_t buffer_size;
  
  // 确定缓冲区
  if (c->recv_buffer) {
    buffer = c->recv_buffer;
    buffer_size = c->recv_buffer_size;
  } else {
    buffer = c->frame;
    buffer_size = IOP_SIZE;
  }

  size_t head = c->recv_head;
  size_t tail = c->recv_tail;

  // 计算可用空间
  size_t available = (tail > head) ? (tail - head - 1) : (buffer_size - head + tail);

  if (len > available) {
    return -1;  // 缓冲区满
  }

  // 写入数据（可能分两段）
  size_t first_part = (head + len <= buffer_size) ? len : (buffer_size - head);
  memcpy(buffer + head, data, first_part);
  c->recv_head = (head + first_part) % buffer_size;

  if (len > first_part) {
    size_t second_part = len - first_part;
    memcpy(buffer, data + first_part, second_part);
    c->recv_head = (c->recv_head + second_part) % buffer_size;
  }

  return 0;
}

// 从环形缓冲区读取数据
int ring_buffer_read(struct conn* c, char* data, size_t len) {
  if (!c || !data || len == 0) {
    return -1;
  }

  char* buffer;
  size_t buffer_size;
  
  // 确定缓冲区
  if (c->recv_buffer) {
    buffer = c->recv_buffer;
    buffer_size = c->recv_buffer_size;
  } else {
    buffer = c->frame;
    buffer_size = IOP_SIZE;
  }

  size_t head = c->recv_head;
  size_t tail = c->recv_tail;

  // 计算可读数据量
  size_t readable = (head >= tail) ? (head - tail) : (buffer_size - tail + head);

  if (len > readable) {
    len = readable;  // 只能读取可用的数据
  }

  if (len == 0) {
    return 0;  // 缓冲区空
  }

  // 读取数据（可能分两段）
  size_t first_part = (tail + len <= buffer_size) ? len : (buffer_size - tail);
  memcpy(data, buffer + tail, first_part);
  c->recv_tail = (tail + first_part) % buffer_size;

  if (len > first_part) {
    size_t second_part = len - first_part;
    memcpy(data + first_part, buffer, second_part);
    c->recv_tail = (c->recv_tail + second_part) % buffer_size;
  }

  return len;
}

// 从环形缓冲区查看数据但不移动 tail 指针
int ring_buffer_peek(struct conn* c, char* data, size_t len) {
  if (!c || !data || len == 0) {
    return -1;
  }

  char* buffer;
  size_t buffer_size;
  
  // 确定缓冲区
  if (c->recv_buffer) {
    buffer = c->recv_buffer;
    buffer_size = c->recv_buffer_size;
  } else {
    buffer = c->frame;
    buffer_size = IOP_SIZE;
  }

  size_t head = c->recv_head;
  size_t tail = c->recv_tail;

  // 计算可读数据量
  size_t readable = (head >= tail) ? (head - tail) : (buffer_size - tail + head);

  if (len > readable) {
    len = readable;  // 只能查看可用的数据
  }

  if (len == 0) {
    return 0;  // 缓冲区空
  }

  // 查看数据（可能分两段）
  size_t first_part = (tail + len <= buffer_size) ? len : (buffer_size - tail);
  memcpy(data, buffer + tail, first_part);

  if (len > first_part) {
    size_t second_part = len - first_part;
    memcpy(data + first_part, buffer, second_part);
  }

  return len;
}

// 跳过环形缓冲区中的数据（移动 tail 指针）
int ring_buffer_skip(struct conn* c, size_t len) {
  if (!c || len == 0) {
    return -1;
  }

  char* buffer;
  size_t buffer_size;
  
  // 确定缓冲区
  if (c->recv_buffer) {
    buffer = c->recv_buffer;
    buffer_size = c->recv_buffer_size;
  } else {
    buffer = c->frame;
    buffer_size = IOP_SIZE;
  }

  size_t head = c->recv_head;
  size_t tail = c->recv_tail;

  // 计算可读数据量
  size_t readable = (head >= tail) ? (head - tail) : (buffer_size - tail + head);

  if (len > readable) {
    return -1;  // 不能跳过比可用数据更多的字节
  }

  c->recv_tail = (tail + len) % buffer_size;
  return 0;
}

// 获取环形缓冲区中的可用数据量
size_t ring_buffer_available(struct conn* c) {
  if (!c) {
    return 0;
  }

  char* buffer;
  size_t buffer_size;
  
  // 确定缓冲区
  if (c->recv_buffer) {
    buffer = c->recv_buffer;
    buffer_size = c->recv_buffer_size;
  } else {
    buffer = c->frame;
    buffer_size = IOP_SIZE;
  }

  size_t head = c->recv_head;
  size_t tail = c->recv_tail;

  return (head >= tail) ? (head - tail) : (buffer_size - tail + head);
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
            nc->r_len = 0;
            nc->wlen = nc->wdone = 0;
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
        if (res == 0) {  // EOF
          conn_free(c);
          conn_pool_free(&g_conn_pool, c);
          break;
        } else if (res > 0) {
          // 写入环形缓冲区
          if (ring_buffer_write(c, c->frame, res) < 0) {
            fprintf(stderr, "Failed to write to ring buffer, closing connection\n");
            conn_free(c);
            conn_pool_free(&g_conn_pool, c);
            break;
          }
        }
        
        // resp_feed -> kvs_resp_feed
        int n = kvs_resp_feed(c);  // 喂给 RESP 状态机

        if (n < 0) {
          conn_free(c);
          conn_pool_free(&g_conn_pool, c);  // 协议错误
        } else if (n == PARSE_OK) {         // 整条命令完整
          current_processing_fd = c->fd;
          c->wlen = 0;
          processCommand(c);
          current_processing_fd = -1;

          // conn_reset -> kvs_resp_reset
          kvs_resp_reset(c);  // 清空解析状态（释放旧参数），准备下一次

          c->state = ST_SEND;
          c->wdone = 0;
          post_send_resp(&g_ring, c);  // 发送响应
        } else {
          // 数据不够，继续接收
          post_recv_frame(&g_ring, c);
        }
        break;
      }

      // 发我们准备好的数据过去
      case ST_SEND:
        c->wdone += res;
        if (c->wdone == c->wlen) {  // 发完
          c->state = ST_RECV;

          // 如果有剩余数据，尝试解析下一条
          if (ring_buffer_available(c) > 0) {
            int n = kvs_resp_feed(c);
            if (n == PARSE_OK) {
              current_processing_fd = c->fd;
              c->wlen = 0; // 重置写缓冲区长度
              processCommand(c);
              current_processing_fd = -1;
              kvs_resp_reset(c);
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
