#ifndef __KVS_NETWORK_H__
#define __KVS_NETWORK_H__

#include <stddef.h>
#include "kvs_constants.h"

/* ---------------- 常量定义 ---------------- */
#define IOP_SIZE (44)               // 每次 recv/send 的帧大小（16 KB）
#define MAX_ARGC (8)                        // 最大参数个数
#define RESP_BUF_SIZE (100)          // 响应缓冲区大小
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
  char* bulk_data;       //  当前段起始位置 
  int argc;              // 期望的参数个数 (argc)
  int argc_done;         // 已解析完成的参数个数 (用于跟踪解析进度)
  size_t bulk_done;      // 当前 bulk 已解析长度
  robj argv[MAX_ARGC];   // 命令段数组 (每个 ptr 都需要 malloc)


  /* ---- 写回 ---- */
  char* wbuf;       // 回包缓冲（+OK\r\n 或 $len\r\n...）
  int wlen, wdone;  // 总长度 & 已发长度

  /* ---- 流式发送状态 ---- */
  int streaming_send;          // 标记是否正在进行流式发送（0: 正常模式，1: 流式模式）
                                // 正常模式：数据先进入 wbuf，再一次性发送
                                // 流式模式：数据直接从数据源分多次发送
  const char* streaming_data;  // 流式发送的数据源指针（指向实际数据，不复制）
  size_t streaming_len;        // 流式发送的数据总长度
  size_t streaming_sent;       // 流式发送已发送的字节数
};

// 消息处理回调函数定义
typedef int (*msg_handler)(struct conn* c);

// 网络模型启动函数声明
extern int reactor_start(unsigned short port, msg_handler handler);
extern int proactor_start(unsigned short port, msg_handler handler);
extern int ntyco_start(unsigned short port, msg_handler handler);

#endif // __KVS_NETWORK_H__