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
struct io_uring g_ring;        // 全局 io_uring 实例
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
extern int slave_sync_take_repl_fd(void);

/* ---------------- 外部函数声明 ---------------- */
extern void before_sleep(void);

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
  pool->conns = kvs_calloc(max_conns, sizeof(struct conn));
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
  c->pause_recv = 0;
  c->wlen = 0;
  c->iov_data = NULL;
  c->iov_len = 0;
  c->has_bulk_suffix = 0;
  c->iov_base = NULL;
  c->iov_needs_free = 0;
  c->is_slave = 0;
  c->dead = 0;

  /* Ensure parser and rbuf_ptr are initialized */
  if (!c->parser) {
    c->parser = kvs_calloc(1, sizeof(proto_parser_t));
  }
  c->rbuf_cap = IOP_SIZE;
  if (!c->rbuf_ptr) {
    c->rbuf_ptr = c->rbuf_embedded;
  }
  c->rlen = 0;
  c->rbuf_off = 0;
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
  io_uring_prep_recv(sqe, c->fd, c->rbuf_ptr + c->rbuf_off + c->rlen, c->rbuf_cap - (c->rbuf_off + c->rlen), 0);
  c->recv_inflight++;
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

/* ---------------- 将外部 fd 附加到连接池，由 io_uring 管理 ---------------- */
struct conn* conn_attach_fd(int fd) {
  struct conn* c = conn_pool_alloc(&g_conn_pool);
  if (!c) {
    close(fd);
    return NULL;
  }
  c->fd = fd;
  c->state = ST_RECV;
  c->rlen = 0;
  c->rbuf_off = 0;
  if (c->parser) {
    kvs_resp_reset(c->parser);
  }
  int nodelay = 1;
  setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));
  post_recv_frame(&g_ring, c);
  return c;
}

/* ---------------- Step 2: 按需补投 recv ----------------
 * 当连接存活、没有 inflight recv、且 rbuf 还有空间时，补投 recv。
 * 调用位置：OP_RECV case 末尾、OP_SEND case 中 send 完成后。
 */
static void maybe_post_recv(struct io_uring* ring, struct conn* c) {
  if (c->fd >= 0 && c->recv_inflight == 0 && c->rbuf_off + c->rlen < IOP_SIZE) {
    post_recv_frame(ring, c);
  }
}

/* ---------------- 释放连接资源 ---------------- */
static void conn_free(struct conn* c) {
  /* 释放广播大命令时分配的 iov_base */
  if (c->iov_needs_free && c->iov_base) {
    kvs_free(c->iov_base);
    c->iov_base = NULL;
    c->iov_needs_free = 0;
  }

  if (c->fd >= 0) {
    close(c->fd);
  }

  // 释放协议相关资源
  if (c->parser) {
    kvs_resp_free_resources(c->parser);
    kvs_free(c->parser);
    c->parser = NULL;
  }

  if (c->fd >= 0) {
    close(c->fd);
  }
  c->fd = -1;
}

/* ---------------- 安全释放连接：如果有 inflight send，延迟释放 ---------------- */
static void conn_close_or_defer(struct conn_pool* pool, struct conn* c) {
  c->state = ST_CLOSE;

  /* 清理从节点注册表中的 dead 连接 */
  if (c->is_slave) {
    extern void slave_cleanup_dead(void);
    slave_cleanup_dead();
  }

  int defer = (c->send_inflight > 0 || c->has_bulk_suffix || c->iov_len > 0);
  kvs_logDebug("[conn_close_or_defer] fd=%d send_inflight=%d has_bulk=%d iov_len=%zu dead=%d -> %s",
               c->fd, c->send_inflight, c->has_bulk_suffix, c->iov_len, c->dead,
               defer ? "defer" : "free");
  if (defer) {
    /* 还有未发完的数据，不能 close(fd)，否则发送 RST */
    c->dead = 1;
  } else {
    conn_free(c);
    conn_pool_free(pool, c);
  }
}

/* ---------------- Step 3: 统一刷 send 队列 ---------------- */
void flush_send_queue(struct io_uring* ring, struct conn* c) {
  if (c->fd < 0 || c->send_inflight) {
    kvs_logDebug("[flush_send_queue] fd=%d skip (inflight=%d)", c->fd, c->send_inflight);
    return;
  }

  struct io_uring_sqe* sqe;

  if (c->wlen > 0) {
    kvs_logDebug("[flush_send_queue] fd=%d send wbuf wlen=%zu", c->fd, c->wlen);
    sqe = sqe_prep_tagged(ring, c, OP_SEND);
    if (!sqe) return;
    io_uring_prep_send(sqe, c->fd, c->wbuf, c->wlen, 0);
    c->send_inflight++;
  } else if (c->iov_len > 0) {
    kvs_logDebug("[flush_send_queue] fd=%d send iov_len=%zu", c->fd, c->iov_len);
    sqe = sqe_prep_tagged(ring, c, OP_SEND);
    if (!sqe) return;
    io_uring_prep_send(sqe, c->fd, c->iov_data, c->iov_len, 0);
    c->send_inflight++;
  } else if (c->has_bulk_suffix) {
    kvs_logDebug("[flush_send_queue] fd=%d send bulk_suffix \\r\\n", c->fd);
    memcpy(c->wbuf, "\r\n", 2);
    c->wlen = 2;
    c->has_bulk_suffix = 0;
    sqe = sqe_prep_tagged(ring, c, OP_SEND);
    if (!sqe) return;
    io_uring_prep_send(sqe, c->fd, c->wbuf, c->wlen, 0);
    c->send_inflight++;
  }
}

/* ---------------- 命令处理：提取为函数，支持 OP_SEND 完成后续处理 ---------------- */
static int process_commands(struct io_uring* ring, struct conn* c) {
  kvs_logDebug("[process_commands] fd=%d enter rlen=%zu rbuf_off=%zu wlen=%zu send_inflight=%d",
               c->fd, c->rlen, c->rbuf_off, c->wlen, c->send_inflight);

  /* 惰性紧缩 rbuf */
  if (c->rbuf_off > 0 && c->rbuf_off + c->rlen >= IOP_SIZE - 256) {
    memmove(c->rbuf_ptr, c->rbuf_ptr + c->rbuf_off, c->rlen);
    c->rbuf_off = 0;
  }

  while (1) {
    size_t consumed = proto_feed(c->parser, c->rbuf_ptr + c->rbuf_off, c->rlen);
    if (consumed == (size_t)-1) {
      kvs_logDebug("[process_commands] fd=%d parse error", c->fd);
      conn_close_or_defer(&g_conn_pool, c);
      return -1;
    }

    if (proto_cmd_ready(c->parser)) {
      /* wbuf 快满时暂停处理，等 OP_SEND 完成、wbuf 清空后再续处理。
       * 必须在 proto_take_cmd 之前检查，否则命令被取出后无法回退。 */
      if (c->wlen > RESP_BUF_SIZE - 512) {
        kvs_logDebug("[process_commands] fd=%d wbuf nearly full wlen=%zu > %d, return 1",
                     c->fd, c->wlen, RESP_BUF_SIZE - 512);
        return 1;
      }

      int argc;
      robj *argv;
      proto_take_cmd(c->parser, &argc, &argv);
      reply_builder_t rb = {.nc = c};
      g_kvs_handler(&rb, argc, argv);

      if (c->state == ST_CLOSE) {
        kvs_logDebug("[process_commands] fd=%d ST_CLOSE after handler, closing", c->fd);
        proto_free_argv(argc, argv);
        conn_close_or_defer(&g_conn_pool, c);
        return -1;
      }

      proto_free_argv(argc, argv);
      flush_send_queue(ring, c);

      c->rbuf_off += consumed;
      c->rlen -= consumed;
      if (c->rlen > 0) {
        continue;
      }
      c->rbuf_off = 0;
      continue;
    }

    /* 没有未消费数据，正常退出循环 */
    if (c->rlen == 0) {
      c->rbuf_off = 0;
      break;
    }

    /* 数据不足但还有未消费数据：紧缩 rbuf 腾出接收空间，等待更多数据 */
    if (c->rbuf_off > 0) {
      memmove(c->rbuf_ptr, c->rbuf_ptr + c->rbuf_off, c->rlen);
      c->rbuf_off = 0;
      kvs_logDebug("[process_commands] fd=%d rbuf compacted, rlen=%zu\n", c->fd, c->rlen);
    }

    /* 保护：rbuf 已满且紧缩后 parser 仍无进度，视为协议错误 */
    if (c->rbuf_off + c->rlen >= (IOP_SIZE - 256) && c->parser->parse_done == 0) {
      kvs_logDebug("[process_commands] fd=%d rbuf full no progress, closing\n", c->fd);
      conn_close_or_defer(&g_conn_pool, c);
      return -1;
    }

    /* 大 key/value 续传：parser 消费了全部数据但命令仍未完整 */
    if (c->rbuf_off + c->rlen >= IOP_SIZE && c->parser->parse_done == c->rlen) {
      c->rbuf_off = 0;
      c->rlen = 0;
      c->parser->parse_done = 0;
    }

    break;
  }
  return 0;
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

  while (g_proactor_running) {
    if (io_uring_sq_ready(&g_ring) > 0) {
        io_uring_submit(&g_ring);
    }
    struct io_uring_cqe* cqe;

    // 【优雅退出】使用 100ms 超时等待，定期检查退出标志
    struct __kernel_timespec ts = {.tv_sec = 0, .tv_nsec = 100000000};
    int ret = io_uring_wait_cqe_timeout(&g_ring, &cqe, &ts);

    if (ret == -ETIME) {
      if (g_config.aof_enabled) before_sleep();
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
              nc->rlen = 0;
              nc->rbuf_off = 0;
              if (nc->parser) {
                kvs_resp_reset(nc->parser);
              }
              post_recv_frame(&g_ring, nc);
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

                int repl_fd = slave_sync_take_repl_fd();
                if (repl_fd >= 0) {
                  struct conn *nc = conn_attach_fd(repl_fd);
                  if (nc) {
                    kvs_logInfo("[Proactor] REPLCONF 连接已注册 fd=%d", repl_fd);
                  } else {
                    kvs_logError("[Proactor] conn_attach_fd 失败 fd=%d", repl_fd);
                  }
                }
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
          kvs_logDebug("[OP_RECV] fd=%d res=%d recv_inflight=%d rlen=%zu",
                       c->fd, res, c->recv_inflight, c->rlen);
          if (res < 0) {
            if (res == -EAGAIN || res == -EINTR) {
              post_recv_frame(&g_ring, c);
            } else {
              kvs_logDebug("[OP_RECV] fd=%d recv error %d, closing", c->fd, res);
              conn_close_or_defer(&g_conn_pool, c);
            }
            break;
          }
          if (res == 0) {
            kvs_logDebug("[OP_RECV] fd=%d peer closed", c->fd);
            conn_close_or_defer(&g_conn_pool, c);
            break;
          }
          c->rlen += res;

          if (c->is_slave) {
            /* 从节点连接：主节点单向推送写命令，不期望接收数据。
             * 从节点执行命令后可能回复 +OK，直接丢弃以避免 parse error 关闭连接。
             * 对端正常关闭（res == 0）或错误在前面已处理。 */
            c->rbuf_off = 0;
            c->rlen = 0;
          } else {
#if ENABLE_ECHO_MODE
            {
              reply_builder_t rb = {.nc = c};
              echo_handler(&rb);
            }
#else
            {
              int ret = process_commands(&g_ring, c);
              if (ret < 0) {
                break;
              }
            }
#endif
          }
          flush_send_queue(&g_ring, c);
          maybe_post_recv(&g_ring, c);
          break;
        }

        case OP_SEND: {
          struct conn* c = (struct conn*)ptr;
          c->send_inflight--;
          kvs_logDebug("[OP_SEND] fd=%d res=%d send_inflight=%d wlen=%zu iov_len=%zu",
                       c->fd, res, c->send_inflight, c->wlen, c->iov_len);
          if (res < 0) {
            if (res == -EAGAIN || res == -EINTR) {
              flush_send_queue(&g_ring, c);
            } else {
              kvs_logDebug("[OP_SEND] fd=%d send error %d, closing", c->fd, res);
              conn_close_or_defer(&g_conn_pool, c);
            }
            break;
          }

          if (c->wlen > 0) {
            if ((size_t)res >= c->wlen) {
              c->wlen = 0;
            } else {
              memmove(c->wbuf, c->wbuf + res, c->wlen - res);
              c->wlen -= res;
            }
          } else if (c->iov_len > 0) {
            if ((size_t)res >= c->iov_len) {
              c->iov_data = NULL;
              c->iov_len = 0;
            } else {
              c->iov_data += res;
              c->iov_len -= res;
            }
          }

          if (c->dead && c->send_inflight == 0) {
            kvs_logDebug("[OP_SEND] fd=%d dead and no inflight, freeing", c->fd);
            conn_free(c);
            conn_pool_free(&g_conn_pool, c);
            break;
          }

          if (c->wlen > 0 || c->iov_len > 0) {
            flush_send_queue(&g_ring, c);
          } else if (c->has_bulk_suffix) {
            memcpy(c->wbuf, "\r\n", 2);
            c->wlen = 2;
            c->has_bulk_suffix = 0;
            flush_send_queue(&g_ring, c);
          }

          /* wbuf 已空，rbuf 还有数据，继续处理剩余命令 */
          if (c->rlen > 0 && c->wlen == 0 && c->iov_len == 0 && !c->has_bulk_suffix) {
            kvs_logDebug("[OP_SEND] fd=%d continue processing rlen=%zu", c->fd, c->rlen);
            int ret = process_commands(&g_ring, c);
            if (ret < 0) {
              break;
            }
          }

          /* 释放广播大命令时分配的 iov_base */
          if (c->iov_needs_free && c->wlen == 0 && c->iov_len == 0 && !c->has_bulk_suffix) {
            kvs_free(c->iov_base);
            c->iov_base = NULL;
            c->iov_needs_free = 0;
          }

          maybe_post_recv(&g_ring, c);
          break;
        }

        default: {
          break;
    }
      }
    }
    io_uring_cq_advance(&g_ring, count);
    if (g_config.aof_enabled) before_sleep();
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
