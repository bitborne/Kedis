#ifndef __KVS_NETWORK_H__
#define __KVS_NETWORK_H__

#include <stddef.h>
#include <sys/socket.h>  /* for struct msghdr / iovec */

/* ---------------- 常量定义 ---------------- */
#define IOP_SIZE (4 * 1024)               // 每次 recv/send 的帧大小（16 KB）
#define MAX_ARGC (8)                        // 最大参数个数
#define RESP_BUF_SIZE (4 * 1024)          // 响应缓冲区大小
#define MAX_SEG_SIZE (1024 * 1024 * 1024)  // 单段最大 1 GB

/* ---------------- 连接状态机 ---------------- */
#define ST_RECV 1
#define ST_SEND 2
#define ST_CLOSE 3

/* ---------------- RESP 状态机枚举 ---------------- */
typedef enum {
  ST_RESP_HDR,        // 等待解析 *<argc> (命令开始)
  ST_RESP_BULK_LEN,   // 等待解析 $<len> (参数长度)
  ST_RESP_BULK_DATA,  // 正在收 bulk 内容
  ST_RESP_OK
} resp_state_t;

typedef enum {
  ST_SEND_SMALL,   // 等待解析 $<len> (参数长度)
  ST_SEND_HDR_SENT,        // 等待解析 *<argc> (命令开始)
  ST_SEND_BULK,  // 正在收 bulk 内容
  ST_SEND_NOTSET
} send_state_t;

/* ---------------- 段对象：只挂指针，不拷贝数据 ---------------- */
#define ROBJ_FLAG_RBUF_REF 0x01  // ptr 直接指向 rbuf 内部，无需 free

typedef struct {
  char* ptr;   // 指向数据（可能是堆内存，也可能直接指向 rbuf）
  size_t len;  // 段长度
  unsigned int flags;  // 标志位
} robj;

/* ---------------- 连接上下文 ---------------- */
/*
 * 警告：struct conn 的字段布局被 mirror/src/uprobe_mirror.bpf.c 硬编码引用。
 * 任何修改（增删字段、改变顺序、调整对齐）都必须同步更新该 BPF 文件中的
 * CONN_FD_OFFSET / CONN_RLEN_OFFSET / CONN_PARSE_DONE_OFFSET / CONN_RBUF_OFFSET。
 */
struct conn {
  /* === 热字段：事件循环高频访问，集中在前 64-128 字节 === */
  int fd;                // TCP 套接字
  int state;             // io_uring 状态：ST_RECV / ST_SEND / ST_CLOSE
  int next_free;         // 空闲链表中的下一个连接索引
  size_t rlen;           // 缓冲区内有效数据长度
  size_t wlen;           // wbuf 有效数据总长度
  size_t wbuf_off;       // wbuf 已发送偏移（保持 wbuf 指针不动）
  size_t parse_done;     // 缓冲区内已解析长度
  resp_state_t resp_state;
  size_t bulk_len;       // 当前段长度 (需要读取的长度)
  int argc;              // 期望的参数个数 (argc)
  int argc_done;         // 已解析完成的参数个数 (用于跟踪解析进度)
  size_t bulk_done;      // 当前 bulk 已解析长度
  send_state_t send_st;
  size_t bulk_sent;      // 已发送的数据长度（用于流式发送跟踪）
  size_t bulk_tt;
  size_t hdr_len;

  /* === 冷字段 / 大缓冲区：低频或间接访问 === */
  char rbuf[IOP_SIZE];   // 读缓冲区（16 KB）
  char cmd_buf[16];      // argv[0] 短命令名内联缓冲，避免 malloc
  robj argv[MAX_ARGC];   // 命令段数组 (每个 ptr 都需要 malloc)
  char* bulk_data;       // 大数据源指针（用于流式发送）
  char* bulk_p;
  char* wbuf;            // 回包缓冲（+OK\r\n 或 $len\r\n...）

  /* sendmsg 零拷贝用的持久化结构（避免栈上 msghdr 被提前释放） */
  struct iovec send_iov[2];
  struct msghdr send_msg;
};

// 消息处理回调函数定义
typedef int (*msg_handler)(struct conn* c);

// 网络模型启动函数声明
extern int reactor_start(unsigned short port, msg_handler handler);
extern int proactor_start(unsigned short port, msg_handler handler);
extern int ntyco_start(unsigned short port, msg_handler handler);

// RESP 协议回复函数声明（供命令处理使用）
extern void add_reply_error(struct conn* c, const char* err);    // 发送错误回复 (-ERR ...)
extern void add_reply_status(struct conn* c, const char* status); // 发送状态回复 (+OK ...)
extern void add_reply_bulk(struct conn* c, char* data);   // 发送批量字符串回复 ($len...)
extern void add_reply_bulk_len(struct conn* c, char* data, size_t len); // 带已知长度的批量字符串回复
extern void add_reply_str(struct conn* c, const char* str);     // 发送原始字符串
extern void add_reply_str_len(struct conn* c, const char* str, size_t len); // 带已知长度的原始字符串

#endif // __KVS_NETWORK_H__