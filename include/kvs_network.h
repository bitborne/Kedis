#ifndef __KVS_NETWORK_H__
#define __KVS_NETWORK_H__

#include <stddef.h>

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
  char rbuf[IOP_SIZE];  // 读缓冲区（16 KB）
  size_t rlen;             // 缓冲区内有效数据长度
  size_t parse_done;       // 缓冲区内已解析长度

  /* RESP 状态机 */
  resp_state_t resp_state;
  size_t bulk_len;        // 当前段长度 (需要读取的长度)
  int argc;              // 期望的参数个数 (argc)
  int argc_done;         // 已解析完成的参数个数 (用于跟踪解析进度)
  size_t bulk_done;      // 当前 bulk 已解析长度
  robj argv[MAX_ARGC];   // 命令段数组 (每个 ptr 都需要 malloc)
  
  
  /* ---- 写回 ---- */
  char* bulk_data;       // 大数据源指针（用于流式发送）
  char* bulk_p;
  char* wbuf;       // 回包缓冲（+OK\r\n 或 $len\r\n...）
  size_t wlen;  // wbuf 有效长度 & 已发长度
  size_t bulk_sent;  // 已发送的数据长度（用于流式发送跟踪）
  size_t bulk_tt;
  size_t hdr_len;
  send_state_t send_st;
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
extern void add_reply_str(struct conn* c, const char* str);     // 发送原始字符串

#endif // __KVS_NETWORK_H__