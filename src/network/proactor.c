// proactor.c - Proactor network model with RESP protocol and io_uring
#define _GNU_SOURCE
#include "../../include/kvstore.h"
#include "../../include/kvs_rdma_sync.h"  /* SLAVE_STATE_* 宏定义 */
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
#include <arpa/inet.h>
#include <unistd.h>

extern kv_config g_config;

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
static int g_listenfd = -1;           // 监听 fd

/* eventfd 相关 */
static int g_event_fd = -1;           // eventfd（用于 RDMA 完成通知）
static struct conn *g_event_conn = NULL;  // eventfd 对应的 conn 结构
static uint64_t g_event_buf;          // eventfd 读取缓冲区

/* 从节点同步管理 */
extern int slave_sync_get_eventfd(void);
extern void slave_sync_drain_backlog(msg_handler handler);

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
    kvs_logError("get_sqe failed");
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

// /* ---------------- 提交异步 close ---------------- */
// static void post_close(struct io_uring* ring, struct conn* c) {
//   struct io_uring_sqe* sqe = sqe_prep(ring, c);
//   io_uring_prep_close(sqe, c->fd);
// }
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

/* ---------------- 提交异步 read：用于 eventfd ---------------- */
static void post_read_eventfd(struct io_uring* ring, int fd, void* buf) {
  struct io_uring_sqe* sqe = io_uring_get_sqe(ring);
  if (!sqe) {
    kvs_logError("get_sqe failed for eventfd");
    return;
  }
  io_uring_prep_read(sqe, fd, buf, 8, 0);
  /* 使用特殊标记识别 eventfd */
  io_uring_sqe_set_data(sqe, g_event_conn);
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

/* --------------  业务入口：处理解析好的argv,并准备wbuf  -------------- */
static int processCommand(struct conn* c) {
  // 直接调用核心逻辑
  // 核心逻辑会根据 c->argv 处理命令，并将结果写入 c->wbuf
  return g_kvs_handler(c);
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
  io_uring_queue_init(RING_ENTRIES, &g_ring, 0);
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
          kvs_logError("Max conns reached, rejecting connection");
        } else {
          nc->fd = res;
          nc->state = ST_RECV;
          nc->wbuf = kvs_malloc(RESP_BUF_SIZE);
          if (!nc->wbuf) {
            conn_pool_free(&g_conn_pool, nc);
            close(res);
            kvs_logError("Failed to alloc write buffer");
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

    /* =========================================================================
     * eventfd 事件处理（RDMA 同步完成通知）
     *
     * 【v3.0 架构】线程间通信机制
     *
     * 触发流程：
     *   RDMA 线程完成同步 -> 更新 g_sync_state -> 写入 eventfd ->
     *   io_uring 检测到可读 -> 这里处理 -> 回放积压队列
     *
     * 设计要点：
     *   - 无锁：eventfd 是单生产者（RDMA 线程）单消费者（主线程）
     *   - 幂等：即使多次通知，状态检查防止重复回放
     *   - 容错：错误后重新投递 read 请求，确保不错过后续通知
     * ========================================================================= */
    if (g_event_conn && c == g_event_conn) {
      if (res > 0) {
        /* 成功读取通知值（8 字节 uint64_t） */
        uint64_t notify_val = g_event_buf;
        kvs_logInfo("[Proactor] 收到 RDMA 完成通知，值=%lu", (unsigned long)notify_val);

        /* ------------------------------------------------------------------
         * 【关键】状态检查，防止重复处理或异常状态处理
         *
         * 场景1: 正常完成 -> READY -> 回放积压
         * 场景2: RDMA 失败 -> IDLE -> 积压已清空，无需回放
         * 场景3: 重复通知 -> 状态已为 READY，积压为空，安全
         * ------------------------------------------------------------------ */
        extern int slave_sync_get_state(void);
        int current_state = slave_sync_get_state();

        if (current_state == SLAVE_STATE_READY) {
          /* 正常路径：RDMA 成功，需要回放积压队列 */
          extern void slave_sync_drain_backlog(msg_handler handler);
          slave_sync_drain_backlog(g_kvs_handler);
          kvs_logInfo("[Proactor] 积压队列回放完成");
        } else if (current_state == SLAVE_STATE_IDLE) {
          /* RDMA 线程可能失败了，积压队列已被清空 */
          kvs_logWarn("[Proactor] RDMA 同步失败（状态为 IDLE），无需回放");
        } else if (current_state == SLAVE_STATE_SYNCING) {
          /* 竞态条件：通知早于状态更新到达
           * 策略：继续等待下一次通知（RDMA 线程会再发一次） */
          kvs_logWarn("[Proactor] 收到通知但状态仍为 SYNCING，等待下次通知");
        } else {
          /* 未知状态，记录错误 */
          kvs_logError("[Proactor] 未知同步状态: %d", current_state);
        }

        /* 重新投递 read 请求，支持多次同步（如 REPLICAOF 切换到新主） */
        post_read_eventfd(&g_ring, g_event_fd, &g_event_buf);

      } else if (res < 0) {
        /* ------------------------------------------------------------------
         * 错误处理
         * -EAGAIN: 非阻塞模式下无数据可读（不应发生，io_uring 已触发）
         * -EINTR:  被信号中断，可以安全重试
         * 其他：    严重错误，记录并尝试恢复
         * ------------------------------------------------------------------ */
        if (res == -EAGAIN || res == -EINTR) {
          /* 可重试错误，重新投递 read 请求 */
          kvs_logDebug("[Proactor] eventfd 可重试错误: %d，重新投递", res);
          post_read_eventfd(&g_ring, g_event_fd, &g_event_buf);
        } else {
          /* 严重错误，记录但不退出（eventfd 可能仍可恢复） */
          kvs_logError("[Proactor] eventfd 错误: %d，尝试恢复", res);
          post_read_eventfd(&g_ring, g_event_fd, &g_event_buf);
        }
      } else {
        /* res == 0: 对端关闭 eventfd（不应发生，eventfd 是匿名管道） */
        kvs_logWarn("[Proactor] eventfd 返回 0（对端关闭？），重新注册");
        post_read_eventfd(&g_ring, g_event_fd, &g_event_buf);
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
        //   debug("r->len == %zu", c->rlen);
        //   debug("recv (%d bytes):\n%.*s\n====", res, res, c->rbuf + c->rlen);

          c->rlen += res;  // 累加接收的字节数（不是覆盖）
          c->parse_done = 0;
          
          int ret = kvs_resp_feed(c);
          // fprintf(stderr, "==> ret = %d\n", ret);
          if (ret == RESP_ERROR) {
            // 协议错误，关闭连接
            kvs_logError("kvs_resp_feed: RESP parse error");
            conn_free(c);
            conn_pool_free(&g_conn_pool, c);
          } else if (ret == RESP_PARSE_OK) {
            kvs_logDebug("RESP parse OK");
            // 解析完成，处理命令
            processCommand(c);
            // 切换到发送状态
            c->state = ST_SEND;
            // 投递发送请求
            // fprintf(stderr, "==> after process wbuf: %*s\n", c->wbuf, RESP_BUF_SIZE);
            post_send_resp(&g_ring, c);
          } else if (ret == RESP_CONTINUE_RECV) {
              kvs_logDebug("RESP continue recv");
            // ret == 0，需要更多数据
            // 提交 recv 请求，等待更多数据
            post_recv_frame(&g_ring, c);
          }
        }
        break;
      }

      // 发我们准备好的数据过去
      case ST_SEND: {
        if (c->wlen != res) { // 数据未发完(比较少见)
          kvs_logError("wlen != sent");
          // 简易处理, 不管提交了多少, 整个 wbuf 全部重新提交
          post_send_resp(&g_ring, c);  // 重新提交 
          break;
        }
        c->wlen = 0;
        memset(c->wbuf, 0, RESP_BUF_SIZE);
        // fprintf(stderr, "======= start =======\n");
        if (c->send_st == ST_SEND_SMALL) {
          kvs_logDebug("SEND: Small response");
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
            if (c->send_st == ST_SEND_HDR_SENT) {
              kvs_logDebug("SEND: Header sented");
              c->bulk_sent += res - c->hdr_len;
              c->send_st = ST_SEND_BULK;
            }
            else if (c->send_st == ST_SEND_BULK) {
              kvs_logDebug("SEND: Bulk data sending");
              c->bulk_sent += res;
            } else {
              kvs_logError("ST_SEND_NOTSET\n");
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
              post_send_resp(&g_ring, c);
            } else {
              // bulk 发完了!
              kvs_logDebug("SEND: Bulk data OVER!");
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
