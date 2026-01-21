#ifndef __KVS_NETWORK_H__
#define __KVS_NETWORK_H__

#include <stddef.h>
#include "kvs_constants.h"

/* ---------------- 常量定义 ---------------- */
#define IOP_SIZE (16 * 1024)               // 每次 recv/send 的帧大小（16 KB）
#define MAX_ARGC 64                        // 最大参数个数
#define RESP_BUF_SIZE (64 * 1024)          // 响应缓冲区大小
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

// 消息处理回调函数定义
typedef int (*msg_handler)(struct conn* c);

// 网络模型启动函数声明
extern int reactor_start(unsigned short port, msg_handler handler);
extern int proactor_start(unsigned short port, msg_handler handler);
extern int ntyco_start(unsigned short port, msg_handler handler);

#endif // __KVS_NETWORK_H__