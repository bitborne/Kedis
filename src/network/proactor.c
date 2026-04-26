// proactor.c - Proactor network model with RESP protocol and io_uring
#define _GNU_SOURCE
#include "../../include/kvstore.h"
#include "../../include/echo_mode.h"
#include "../../include/kvs_rdma_sync.h"  /* SLAVE_STATE_* 宏定义 */
#include <assert.h>
#include <ctype.h>
#include <errno.h>
#include <liburing.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <time.h>

extern kv_config g_config;

/* ---------------- 常量定义 ---------------- */
#define MAX_CONNS 100000   // 最大并发连接数
#define BACKLOG 4096       // listen 队列长度
#define RING_ENTRIES 8192  // io_uring 队列深度

/* ---------------- SQE 操作类型标签 ----------------
 * 利用指针 8 字节对齐特性，低 3 位存 op 类型。
 * kmem_alloc 代码层面不保证对齐，但 x86_64 上 mmap/malloc
 * 实际返回页对齐地址。运行时断言兜底。
 */
#define OP_MASK     0x7ULL
#define OP_RECV     1
#define OP_SEND     2
#define OP_ACCEPT   3
#define OP_EVENTFD  4

static inline void assert_pointer_aligned(void *p) {
  if (((uintptr_t)p & OP_MASK) != 0) {
    kvs_logError("FATAL: pointer %p is not 8-byte aligned, cannot use low bits for op tag", p);
    abort();
  }
}

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
static int g_listenfd = -1;           // 监听 fd
static volatile int g_proactor_running = 1;  // 运行标志，用于优雅退出

/* eventfd 相关 */
static int g_event_fd = -1;           // eventfd（用于 RDMA 完成通知）
static struct conn *g_event_conn = NULL;  // eventfd 对应的 conn 结构
static uint64_t g_event_buf;          // eventfd 读取缓冲区

/* accept 上下文 - 用于保存 addr 和 len，嵌入结构体避免额外分配 */
#define ACCEPT_CTX_MAGIC 0xACCE0000
struct accept_ctx {
  int magic;  // 魔数标记，用于识别 accept_ctx
  struct sockaddr_in addr;
  socklen_t len;
};

/* 从节点同步管理 */
extern int slave_sync_get_eventfd(void);
extern void slave_sync_drain_backlog(msg_handler handler);

/* ---------------- 外部函数声明 ---------------- */
extern void before_sleep(uint64_t now_ns);

/* ---------------- 优雅退出支持 ---------------- */
void proactor_stop(void) {
  g_proactor_running = 0;
}
// extern __thread int current_processing_fd;

/* ---------------- accept 上下文管理 ---------------- */
static void accept_ctx_free(struct accept_ctx* ctx) {
  if (ctx) {
    kvs_free(ctx);
  }
}

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

  assert_pointer_aligned(pool->conns);
}

static struct conn* conn_pool_alloc(struct conn_pool* pool) {
  if (pool->free_count == 0) {
    return NULL;  // 连接池耗尽
  }

  int idx = pool->free_head;
  pool->free_head = pool->conns[idx].next_free;
  pool->free_count--;

  struct conn* c = &pool->conns[idx];
  c->recv_inflight = 0;
  c->send_inflight = 0;
  return c;
}

static void conn_pool_free(struct conn_pool* pool, struct conn* c) {
  int idx = c - pool->conns;
  c->fd = -1;
  c->next_free = pool->free_head;
  c->recv_inflight = 0;
  c->send_inflight = 0;
  pool->free_head = idx;
  pool->free_count++;
}

/* ---------------- 工具：拿 SQE 并填带标签的 user_data ---------------- */
static struct io_uring_sqe* sqe_prep_tagged(struct io_uring* ring, struct conn* c, int op) {
  struct io_uring_sqe* sqe = io_uring_get_sqe(ring);
  if (!sqe) {
    kvs_logError("get_sqe failed");
    exit(1);
  }
  uint64_t ud = ((uint64_t)c & ~OP_MASK) | (op & OP_MASK);
  io_uring_sqe_set_data64(sqe, ud);
  return sqe;
}

/* ---------------- 提交异步 accept ---------------- */
static void post_accept(struct io_uring* ring, int listenfd) {
  // 为 accept 额外 malloc 地址信息，避免踩栈
  struct accept_ctx* ctx = kvs_malloc(sizeof(*ctx));
  if (!ctx) {
    kvs_logError("Failed to alloc accept_ctx");
    return;
  }
  assert_pointer_aligned(ctx);
  ctx->magic = ACCEPT_CTX_MAGIC;  // 设置魔数标记
  ctx->len = sizeof(ctx->addr);

  struct io_uring_sqe* sqe = io_uring_get_sqe(ring);
  if (!sqe) {
    kvs_logError("Failed to get sqe for accept");
    accept_ctx_free(ctx);
    return;
  }
  io_uring_prep_accept(sqe, listenfd, (struct sockaddr*)&ctx->addr, &ctx->len, 0);
  uint64_t ud = ((uint64_t)ctx & ~OP_MASK) | OP_ACCEPT;
  io_uring_sqe_set_data64(sqe, ud);
}

// /* ---------------- 提交异步 close ---------------- */
// static void post_close(struct io_uring* ring, struct conn* c) {
//   struct io_uring_sqe* sqe = sqe_prep(ring, c);
//   io_uring_prep_close(sqe, c->fd);
// }
/* ---------------- 提交异步 recv ---------------- */
static void post_recv_frame(struct io_uring* ring, struct conn* c) {
  struct io_uring_sqe* sqe = sqe_prep_tagged(ring, c, OP_RECV);
  io_uring_prep_recv(sqe, c->fd, c->rbuf + c->rlen, IOP_SIZE - c->rlen, 0);
  c->recv_inflight++;
}

/* ---------------- 提交异步 send：回 RESP 包 ---------------- */
static void post_send_resp(struct io_uring* ring, struct conn* c) {
  struct io_uring_sqe* sqe = sqe_prep_tagged(ring, c, OP_SEND);
  io_uring_prep_send(sqe, c->fd, c->wbuf + c->wbuf_off, c->wlen - c->wbuf_off, 0);
  c->send_inflight++;
}

/* ---------------- 提交异步 read：用于 eventfd ---------------- */
static void post_read_eventfd(struct io_uring* ring, int fd, void* buf) {
  struct io_uring_sqe* sqe = io_uring_get_sqe(ring);
  if (!sqe) {
    kvs_logError("get_sqe failed for eventfd");
    return;
  }
  io_uring_prep_read(sqe, fd, buf, 8, 0);
  uint64_t ud = ((uint64_t)g_event_conn & ~OP_MASK) | OP_EVENTFD;
  io_uring_sqe_set_data64(sqe, ud);
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
static int init_listen(uint16_t port, const char* bind_addr) {

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
  /* 解析绑定地址 */
  if (bind_addr == NULL || strlen(bind_addr) == 0) {
      /* 默认：监听所有地址 */
      addr.sin_addr.s_addr = INADDR_ANY;
      bind_addr = "0.0.0.0";
  } else if (strcmp(bind_addr, "0.0.0.0") == 0) {
      /* 显式指定 0.0.0.0 */
      addr.sin_addr.s_addr = INADDR_ANY;
  } else {
      /* 使用 inet_pton 转换指定 IP */
      if (inet_pton(AF_INET, bind_addr, &addr.sin_addr) != 1) {
          kvs_logError("Invalid bind address: %s", bind_addr);
          close(fd);
          return -1;
      }
  }


  if (bind(fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
    perror("bind");
    close(fd);
    return -1;
  }

  listen(fd, BACKLOG);
  return fd;
}

#if !ENABLE_ECHO_MODE
/* --------------  业务入口：处理解析好的argv,并准备wbuf  -------------- */
static int processCommand(struct conn* c) {
  // 直接调用核心逻辑
  // 核心逻辑会根据 c->argv 处理命令，并将结果写入 c->wbuf
  return g_kvs_handler(c);
}

/* --------------  pipeline 主循环：批量解析并执行命令  -------------- */
static void run_pipeline(struct io_uring* ring, struct conn* c) {
  while (1) {
    int ret = kvs_resp_feed(c);
    if (ret == RESP_ERROR) {
      kvs_logError("kvs_resp_feed: RESP parse error");
      conn_free(c);
      conn_pool_free(&g_conn_pool, c);
      return;
    } else if (ret == RESP_CONTINUE_RECV) {
      kvs_logDebug("RESP continue recv");
      flush_all_aof_buffers_now();
      if (c->wlen > 0) {
        c->state = ST_SEND;
        post_send_resp(ring, c);
      } else {
        post_recv_frame(ring, c);
      }
      return;
    } else if (ret == RESP_PARSE_OK) {
      kvs_logDebug("RESP parse OK");
      processCommand(c);
      kvs_resp_pipeline_next(c);
      if (c->send_st == ST_SEND_HDR_SENT) {
        flush_all_aof_buffers_now();
        c->state = ST_SEND;
        post_send_resp(ring, c);
        return;
      }
      if (c->wbuf_full) {
        flush_all_aof_buffers_now();
        c->state = ST_SEND;
        post_send_resp(ring, c);
        return;
      }
      continue;
    }
  }
}
#endif

/* --------------  proactor_start：主入口  -------------- */
int proactor_start(unsigned short port, msg_handler handler) {
  int listenfd = init_listen(port, g_config.bind_addr);
  if (listenfd < 0) {
    return -1;
  }

  g_kvs_handler = handler;
  g_listenfd = listenfd;
  signal(SIGPIPE, SIG_IGN);

  /* 初始化连接池 */
  conn_pool_init(&g_conn_pool, MAX_CONNS);

  /* 初始化 io_uring */
  {
    int ret = io_uring_queue_init(RING_ENTRIES, &g_ring, IORING_SETUP_SQPOLL);
    if (ret < 0) {
      kvs_logWarn("IORING_SETUP_SQPOLL not available (ret=%d), falling back", ret);
      ret = io_uring_queue_init(RING_ENTRIES, &g_ring, 0);
      if (ret < 0) {
        kvs_logError("io_uring_queue_init failed: %d", ret);
        return -1;
      }
    }
  }
  post_accept(&g_ring, listenfd);

  /* 初始化从节点同步系统（如果是从节点）
   *
   * 【v3.0 架构】检查是否已初始化
   * 在主函数中，slave_sync_init() 可能在 proactor_start() 之前调用，
   * 以确保 eventfd 在 RDMA 线程启动前创建。
   * 这里通过 slave_sync_get_eventfd() 检查是否已初始化。
   */
  if (g_config.replica_mode == REPLICA_MODE_SLAVE) {
    extern int slave_sync_get_eventfd(void);
    g_event_fd = slave_sync_get_eventfd();

    if (g_event_fd < 0) {
      /* 尚未初始化，进行初始化 */
      extern int slave_sync_init(void);
      g_event_fd = slave_sync_init();
      kvs_logWarn("Proactor: 在 proactor_start 中初始化 eventfd=%d", g_event_fd);
    } else {
      kvs_logInfo("Proactor: 检测到 eventfd=%d 已初始化，直接注册到 io_uring", g_event_fd);
    }

    if (g_event_fd >= 0) {
      /* 创建 eventfd 对应的 conn 结构 */
      g_event_conn = kvs_calloc(1, sizeof(struct conn));
      if (g_event_conn) {
        g_event_conn->fd = g_event_fd;
        g_event_conn->state = ST_RECV;
        g_event_conn->rlen = 0;
        /* 注册 eventfd 到 io_uring */
        post_read_eventfd(&g_ring, g_event_fd, &g_event_buf);
        kvs_logInfo("Proactor: eventfd=%d 已注册到 io_uring", g_event_fd);
      }
    }
  }

  kvs_logInfo("Proactor server listening on port %d...", port);

  struct timespec last_before_sleep = {0};

  while (g_proactor_running) {
    if (io_uring_sq_ready(&g_ring) > 0) {
        io_uring_submit(&g_ring);
    }
    struct io_uring_cqe* cqe;

    // 【优雅退出】使用 100ms 超时等待，定期检查退出标志
    struct __kernel_timespec ts = {.tv_sec = 0, .tv_nsec = 100000000};
    int ret = io_uring_wait_cqe_timeout(&g_ring, &cqe, &ts);

    if (ret == -ETIME) {
      before_sleep(0);
      continue;
    }
    if (ret < 0) break;

    unsigned int count = 0;
    unsigned int head;
    io_uring_for_each_cqe(&g_ring, head, cqe) {
      uint64_t ud = io_uring_cqe_get_data64(cqe);
      int op = ud & OP_MASK;
      void* ptr = (void*)(ud & ~OP_MASK);
      int res = cqe->res;
      count++;

      switch (op) {
        case OP_ACCEPT: {
          struct accept_ctx* accept_ctx = (struct accept_ctx*)ptr;
          int new_fd = res;
          accept_ctx_free(accept_ctx);
          if (new_fd >= 0) {
            struct conn* nc = conn_pool_alloc(&g_conn_pool);
            if (!nc) {
              close(new_fd);
              kvs_logError("Max conns reached, rejecting connection");
            } else {
              nc->fd = new_fd;
              nc->state = ST_RECV;
              int nodelay = 1;
              setsockopt(new_fd, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));
              nc->wbuf = kvs_malloc(RESP_BUF_SIZE);
              if (!nc->wbuf) {
                conn_pool_free(&g_conn_pool, nc);
                close(new_fd);
                kvs_logError("Failed to alloc write buffer");
              } else {
                kvs_resp_reset(nc);
                post_recv_frame(&g_ring, nc);
              }
            }
            post_accept(&g_ring, listenfd);
          } else {
            if (new_fd != -EAGAIN && new_fd != -EINTR) perror("accept");
            post_accept(&g_ring, listenfd);
          }
          break;
        }

        case OP_EVENTFD: {
          if (g_event_conn && ptr == g_event_conn) {
            if (res > 0) {
              uint64_t notify_val = g_event_buf;
              kvs_logInfo("[Proactor] 收到 RDMA 完成通知，值=%lu", (unsigned long)notify_val);
              extern int slave_sync_get_state(void);
              int current_state = slave_sync_get_state();
              if (current_state == SLAVE_STATE_READY) {
                extern void slave_sync_drain_backlog(msg_handler handler);
                slave_sync_drain_backlog(g_kvs_handler);
                kvs_logInfo("[Proactor] 积压队列回放完成");
              } else if (current_state == SLAVE_STATE_IDLE) {
                kvs_logWarn("[Proactor] RDMA 同步失败（状态为 IDLE），无需回放");
              } else if (current_state == SLAVE_STATE_SYNCING) {
                kvs_logWarn("[Proactor] 收到通知但状态仍为 SYNCING，等待下次通知");
              } else {
                kvs_logError("[Proactor] 未知同步状态: %d", current_state);
              }
              post_read_eventfd(&g_ring, g_event_fd, &g_event_buf);
            } else if (res < 0) {
              if (res == -EAGAIN || res == -EINTR) {
                kvs_logDebug("[Proactor] eventfd 可重试错误: %d，重新投递", res);
                post_read_eventfd(&g_ring, g_event_fd, &g_event_buf);
              } else {
                kvs_logError("[Proactor] eventfd 错误: %d，尝试恢复", res);
                post_read_eventfd(&g_ring, g_event_fd, &g_event_buf);
              }
            } else {
              kvs_logWarn("[Proactor] eventfd 返回 0（对端关闭？），重新注册");
              post_read_eventfd(&g_ring, g_event_fd, &g_event_buf);
            }
          }
          break;
        }

        case OP_RECV: {
          struct conn* c = (struct conn*)ptr;
          c->recv_inflight--;
          if (res < 0) {
            if (res == -EAGAIN || res == -EINTR) {
              post_recv_frame(&g_ring, c);
            } else {
              conn_free(c);
              conn_pool_free(&g_conn_pool, c);
            }
            break;
          }
          if (res == 0) {
            conn_free(c);
            conn_pool_free(&g_conn_pool, c);
            break;
          }
          c->rlen += res;
#if ENABLE_ECHO_MODE
          echo_handler(c);
          post_send_resp(&g_ring, c);
#else
          run_pipeline(&g_ring, c);
#endif
          break;
        }

        case OP_SEND: {
          struct conn* c = (struct conn*)ptr;
          c->send_inflight--;
          if (res < 0) {
            if (res == -EAGAIN || res == -EINTR) {
              post_send_resp(&g_ring, c);
            } else {
              conn_free(c);
              conn_pool_free(&g_conn_pool, c);
            }
            break;
          }
          if (c->wlen - c->wbuf_off != (size_t)res) {
            c->wbuf_off += res;
            post_send_resp(&g_ring, c);
            break;
          }
          c->wbuf_off = 0;
          if (c->send_st == ST_SEND_SMALL) {
            c->wlen = 0;
            kvs_logDebug("SEND: Small response");
            c->state = ST_RECV;
            c->bulk_p = c->bulk_data = NULL;
            c->bulk_sent = 0;
            c->bulk_tt = 0;
            c->hdr_len = 0;
            c->send_st = ST_SEND_NOTSET;
            c->wbuf_full = 0;
            if (c->rlen > c->parse_done) {
              run_pipeline(&g_ring, c);
            } else {
              post_recv_frame(&g_ring, c);
            }
            break;
          }
          if (c->send_st == ST_SEND_HDR_SENT) {
            c->bulk_sent += res - c->hdr_len;
            c->send_st = ST_SEND_BULK;
          } else if (c->send_st == ST_SEND_BULK) {
            c->bulk_sent += res;
          } else {
            kvs_logError("ST_SEND unknown state\n");
            conn_free(c);
            conn_pool_free(&g_conn_pool, c);
            break;
          }
          c->wlen = 0;
          if (c->bulk_sent < c->bulk_tt) {
            c->bulk_p = c->bulk_data + c->bulk_sent;
            size_t remain = c->bulk_tt - c->bulk_sent;
            size_t cp = (remain < RESP_BUF_SIZE) ? remain : RESP_BUF_SIZE;
            memcpy(c->wbuf, c->bulk_p, cp);
            c->wlen = cp;
            if (remain <= RESP_BUF_SIZE) {
              c->wbuf[c->wlen - 2] = '\r';
              c->wbuf[c->wlen - 1] = '\n';
            } else if (remain == RESP_BUF_SIZE + 1) {
              c->wbuf[c->wlen - 1] = '\r';
            } else if (remain == 1) {
              c->wbuf[c->wlen] = '\n';
            }
            post_send_resp(&g_ring, c);
          } else {
            c->state = ST_RECV;
            c->bulk_p = c->bulk_data = NULL;
            c->bulk_sent = 0;
            c->bulk_tt = 0;
            c->hdr_len = 0;
            c->wlen = 0;
            c->send_st = ST_SEND_NOTSET;
            c->wbuf_full = 0;
            if (c->rlen > c->parse_done) {
              run_pipeline(&g_ring, c);
            } else {
              post_recv_frame(&g_ring, c);
            }
          }
          break;
        }

        default: {
          kvs_logWarn("[Step1-FALLBACK] CQE with untagged user_data=%p, op=%d", ptr, op);
          struct conn* c = (struct conn*)ptr;
          if (c != NULL && c != g_event_conn) {
            struct accept_ctx* accept_ctx = (struct accept_ctx*)c;
            if (accept_ctx->magic == ACCEPT_CTX_MAGIC) {
              int new_fd = res;
              accept_ctx_free(accept_ctx);
              if (new_fd >= 0) {
                struct conn* nc = conn_pool_alloc(&g_conn_pool);
                if (!nc) {
                  close(new_fd);
                  kvs_logError("Max conns reached, rejecting connection");
                } else {
                  nc->fd = new_fd;
                  nc->state = ST_RECV;
                  int nodelay = 1;
                  setsockopt(new_fd, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));
                  nc->wbuf = kvs_malloc(RESP_BUF_SIZE);
                  if (!nc->wbuf) {
                    conn_pool_free(&g_conn_pool, nc);
                    close(new_fd);
                    kvs_logError("Failed to alloc write buffer");
                  } else {
                    kvs_resp_reset(nc);
                    post_recv_frame(&g_ring, nc);
                  }
                }
                post_accept(&g_ring, listenfd);
              } else {
                if (new_fd != -EAGAIN && new_fd != -EINTR) perror("accept");
                post_accept(&g_ring, listenfd);
              }
              break;
            }
          }
          if (g_event_conn && c == g_event_conn) {
            if (res > 0) {
              uint64_t notify_val = g_event_buf;
              kvs_logInfo("[Proactor] 收到 RDMA 完成通知，值=%lu", (unsigned long)notify_val);
              extern int slave_sync_get_state(void);
              int current_state = slave_sync_get_state();
              if (current_state == SLAVE_STATE_READY) {
                extern void slave_sync_drain_backlog(msg_handler handler);
                slave_sync_drain_backlog(g_kvs_handler);
                kvs_logInfo("[Proactor] 积压队列回放完成");
              } else if (current_state == SLAVE_STATE_IDLE) {
                kvs_logWarn("[Proactor] RDMA 同步失败（状态为 IDLE），无需回放");
              } else if (current_state == SLAVE_STATE_SYNCING) {
                kvs_logWarn("[Proactor] 收到通知但状态仍为 SYNCING，等待下次通知");
              } else {
                kvs_logError("[Proactor] 未知同步状态: %d", current_state);
              }
              post_read_eventfd(&g_ring, g_event_fd, &g_event_buf);
            } else if (res < 0) {
              if (res == -EAGAIN || res == -EINTR) {
                kvs_logDebug("[Proactor] eventfd 可重试错误: %d，重新投递", res);
                post_read_eventfd(&g_ring, g_event_fd, &g_event_buf);
              } else {
                kvs_logError("[Proactor] eventfd 错误: %d，尝试恢复", res);
                post_read_eventfd(&g_ring, g_event_fd, &g_event_buf);
              }
            } else {
              kvs_logWarn("[Proactor] eventfd 返回 0（对端关闭？），重新注册");
              post_read_eventfd(&g_ring, g_event_fd, &g_event_buf);
            }
            break;
          }
          if (res < 0) {
            if (res == -EAGAIN || res == -EINTR) {
              if (c->state == ST_RECV) post_recv_frame(&g_ring, c);
              if (c->state == ST_SEND) post_send_resp(&g_ring, c);
            } else {
              conn_free(c);
              conn_pool_free(&g_conn_pool, c);
            }
            break;
          }
          switch (c->state) {
            case ST_RECV: {
              if (res == 0) {
                conn_free(c);
                conn_pool_free(&g_conn_pool, c);
                break;
              } else if (res > 0) {
                c->rlen += res;
#if ENABLE_ECHO_MODE
                echo_handler(c);
                post_send_resp(&g_ring, c);
#else
                run_pipeline(&g_ring, c);
#endif
              }
              break;
            }
            case ST_SEND: {
              if (c->wlen - c->wbuf_off != (size_t)res) {
                c->wbuf_off += res;
                post_send_resp(&g_ring, c);
                break;
              }
              c->wbuf_off = 0;
              if (c->send_st == ST_SEND_SMALL) {
                c->wlen = 0;
                kvs_logDebug("SEND: Small response");
                c->state = ST_RECV;
                c->bulk_p = c->bulk_data = NULL;
                c->bulk_sent = 0;
                c->bulk_tt = 0;
                c->hdr_len = 0;
                c->send_st = ST_SEND_NOTSET;
                c->wbuf_full = 0;
                if (c->rlen > c->parse_done) {
                  run_pipeline(&g_ring, c);
                } else {
                  post_recv_frame(&g_ring, c);
                }
                break;
              }
              if (c->send_st == ST_SEND_HDR_SENT) {
                c->bulk_sent += res - c->hdr_len;
                c->send_st = ST_SEND_BULK;
              } else if (c->send_st == ST_SEND_BULK) {
                c->bulk_sent += res;
              } else {
                kvs_logError("ST_SEND unknown state\n");
                conn_free(c);
                conn_pool_free(&g_conn_pool, c);
                break;
              }
              c->wlen = 0;
              if (c->bulk_sent < c->bulk_tt) {
                c->bulk_p = c->bulk_data + c->bulk_sent;
                size_t remain = c->bulk_tt - c->bulk_sent;
                size_t cp = (remain < RESP_BUF_SIZE) ? remain : RESP_BUF_SIZE;
                memcpy(c->wbuf, c->bulk_p, cp);
                c->wlen = cp;
                if (remain <= RESP_BUF_SIZE) {
                  c->wbuf[c->wlen - 2] = '\r';
                  c->wbuf[c->wlen - 1] = '\n';
                } else if (remain == RESP_BUF_SIZE + 1) {
                  c->wbuf[c->wlen - 1] = '\r';
                } else if (remain == 1) {
                  c->wbuf[c->wlen] = '\n';
                }
                post_send_resp(&g_ring, c);
              } else {
                c->state = ST_RECV;
                c->bulk_p = c->bulk_data = NULL;
                c->bulk_sent = 0;
                c->bulk_tt = 0;
                c->hdr_len = 0;
                c->wlen = 0;
                c->send_st = ST_SEND_NOTSET;
                c->wbuf_full = 0;
                if (c->rlen > c->parse_done) {
                  run_pipeline(&g_ring, c);
                } else {
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
          }
          break;
        }
      }
    }
    io_uring_cq_advance(&g_ring, count);

    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    uint64_t now_ns = (uint64_t)now.tv_sec * 1000000000ULL + now.tv_nsec;
    uint64_t last_ns = (uint64_t)last_before_sleep.tv_sec * 1000000000ULL + last_before_sleep.tv_nsec;
    if (count == 0 || now_ns - last_ns >= 1000000ULL) { // 至少每 1ms 调用一次
      before_sleep(now_ns);
      last_before_sleep = now;
    }
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
