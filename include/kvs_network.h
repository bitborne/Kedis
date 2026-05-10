#ifndef __KVS_NETWORK_H__
#define __KVS_NETWORK_H__

#include <stddef.h>
#include <string.h>
#include <stdio.h>
#include "kvs_protocol.h"
#include "kvs_log.h"

/* ---------------- 常量定义 ---------------- */
#define IOP_SIZE (4 * 1024)               // 每次 recv/send 的帧大小（16 KB）
#define RESP_BUF_SIZE (4 * 1024)          // 响应缓冲区大小

/* ---------------- 连接状态机 ---------------- */
#define ST_RECV 1
#define ST_SEND 2
#define ST_CLOSE 3

/* ---------------- 连接上下文（struct conn 保持为主名，net_conn_t 为别名） ---------------- */
/*
 * 警告：以下偏移量被 mirror/src/uprobe_mirror.bpf.c 硬编码引用。
 * 任何修改（增删字段、改变顺序、调整对齐）都必须同步更新该 BPF 文件中的
 * CONN_FD_OFFSET / CONN_RLEN_OFFSET / CONN_PARSE_DONE_OFFSET / CONN_RBUF_OFFSET。
 */
struct conn {
  /* === 热字段：事件循环高频访问，集中在前 64-128 字节 === */
  int fd;                // TCP 套接字
  int state;             // io_uring 状态：ST_RECV / ST_SEND / ST_CLOSE
  int next_free;         // 空闲链表中的下一个连接索引
  size_t rlen;           // 未消费数据长度（从 rbuf_off 开始）
  size_t rbuf_off;       // 未消费数据起始偏移量

  /* === 冷字段 / 大缓冲区：低频或间接访问 === */

  /* Step 1: inflight 计数 */
  int recv_inflight;
  int send_inflight;

  /* === 新字段：追加在末尾，不影响旧字段偏移 === */
  /* Receive side - pointerized for future registered buffers */
  char *rbuf_ptr;
  size_t rbuf_cap;
  char rbuf_embedded[IOP_SIZE];  /* Default points here */

  /* Send buffer: 单 wbuf 批量累积，替代 send_slot 队列 */
  char wbuf[RESP_BUF_SIZE];
  size_t wlen;

  /* 大响应零拷贝分片：wbuf 满后引用外部数据 */
  char *iov_data;
  size_t iov_len;
  int has_bulk_suffix;   // 标记 iov_data 发完后还需发送 \r\n

  int dead;              // 连接待释放（有 inflight send 时延迟释放）

  int pause_recv;

  /* Layer associations */
  proto_parser_t *parser;
  void *app_ctx;

  int is_slave;            // 标记该连接是从节点同步通道

  /* 新增：支持广播大命令时 iov_data 的内存自管理 */
  char *iov_base;          // iov_data 的原始分配指针（如果需要释放）
  int iov_needs_free;      // 标记 iov_base 是否需要 kvs_free
};

typedef struct conn net_conn_t;
typedef struct conn conn_t;

/* ---------------- 回复构建器 ---------------- */
typedef struct {
  net_conn_t *nc;
} reply_builder_t;

// 消息处理回调函数定义
typedef int (*msg_handler)(reply_builder_t *rb, int argc, robj *argv);

// 网络模型启动函数声明
extern int reactor_start(unsigned short port, msg_handler handler);
extern int proactor_start(unsigned short port, msg_handler handler);
extern int ntyco_start(unsigned short port, msg_handler handler);

/* 全局 io_uring 实例（定义在 proactor.c） */
struct io_uring;
extern struct io_uring g_ring;

/* 统一刷 send 队列（定义在 proactor.c） */
extern void flush_send_queue(struct io_uring *ring, struct conn *c);

/* ---------------- 内存分配前向声明（供 inline 回复构建器使用） ---------------- */
extern void* kvs_malloc(size_t size);
extern void kvs_free(void *ptr);

/* ---------------- 回复构建器接口（替代 add_reply_*） ---------------- */
static inline int rb_add_reply_str_len(reply_builder_t *rb, const char *str, size_t len) {
  net_conn_t *nc = rb->nc;
  if (!nc || !str) return -1;

  if (nc->wlen + len > RESP_BUF_SIZE) {
    kvs_logDebug("[rb_add_reply_str_len] fd=%d overflow wlen=%zu + len=%zu > %d\n",
                 nc->fd, nc->wlen, len, RESP_BUF_SIZE);
    nc->state = ST_CLOSE;  // wbuf 溢出会导致协议流错位，必须关闭连接
    return -1;
  }
  memcpy(nc->wbuf + nc->wlen, str, len);
  nc->wlen += len;
  return 0;
}

static inline void rb_add_reply_str(reply_builder_t *rb, const char *str) {
  rb_add_reply_str_len(rb, str, strlen(str));
}

static inline void rb_add_reply_error(reply_builder_t *rb, const char *err) {
  rb_add_reply_str(rb, "-ERR ");
  rb_add_reply_str(rb, err);
  rb_add_reply_str(rb, "\r\n");
}

static inline void rb_add_reply_status(reply_builder_t *rb, const char *status) {
  rb_add_reply_str(rb, "+");
  rb_add_reply_str(rb, status);
  rb_add_reply_str(rb, "\r\n");
}

static inline void rb_add_reply_bulk_len(reply_builder_t *rb, char *data, size_t len) {
  char buf[32];
  int n = snprintf(buf, sizeof(buf), "$%zu\r\n", len);

  net_conn_t *nc = rb->nc;
  if (nc->wlen + n > RESP_BUF_SIZE) {
    nc->state = ST_CLOSE;
    return;
  }
  memcpy(nc->wbuf + nc->wlen, buf, n);
  nc->wlen += n;

  size_t space = RESP_BUF_SIZE - nc->wlen;
  if (len <= space) {
    // 能完全放入 wbuf
    memcpy(nc->wbuf + nc->wlen, data, len);
    nc->wlen += len;
    if (nc->wlen + 2 > RESP_BUF_SIZE) {
      nc->state = ST_CLOSE;
      return;
    }
    memcpy(nc->wbuf + nc->wlen, "\r\n", 2);
    nc->wlen += 2;
  } else {
    // wbuf 装不下，填充 wbuf 后剩余部分挂 iov_data
    if (space > 0) {
      memcpy(nc->wbuf + nc->wlen, data, space);
      nc->wlen += space;
      data += space;
      len -= space;
    }
    nc->iov_data = data;
    nc->iov_len = len;
    nc->has_bulk_suffix = 1;  // 发送完 iov_data 后需要补发 \r\n
  }
}

static inline void rb_add_reply_bulk(reply_builder_t *rb, char *data) {
  rb_add_reply_bulk_len(rb, data, strlen(data));
}

#endif // __KVS_NETWORK_H__
