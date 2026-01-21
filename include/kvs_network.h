#ifndef __KVS_NETWORK_H__
#define __KVS_NETWORK_H__

#include <stddef.h>
#include "kvs_constants.h"

/* ---------------- 常量定义 ---------------- */
#define IOP_SIZE (128 * 1024)              // 每次 recv/send 的帧大小（128 KB）
#define MAX_ARGC 64                        // 最大参数个数
#define RESP_BUF_SIZE (32 * 1024 * 1024)   // 响应缓冲区大小（32 MB，支持大数据测试）
#define MAX_SEG_SIZE (1024 * 1024 * 1024)  // 单段最大 1 GB

/* ---------------- 连接状态机 ---------------- */
#define ST_RECV 1
#define ST_SEND 2
#define ST_CLOSE 3

/* ---------------- 流式发送状态机 ---------------- */
typedef enum {
    STREAM_STATE_IDLE,       // 空闲状态，未启用流式发送
    STREAM_STATE_HEADER,     // 发送头部（如 "$len\r\n"）
    STREAM_STATE_DATA,       // 发送数据主体
    STREAM_STATE_TAIL,       // 发送尾部（如 "\r\n"）
    STREAM_STATE_DONE        // 发送完成
} stream_state_t;

/* ---------------- 流式接收状态机 ---------------- */
typedef enum {
    RECV_STATE_IDLE,           // 空闲状态
    RECV_STATE_HEADER,         // 接收头部（如 "*<argc>\r\n"）
    RECV_STATE_BULK_LEN,       // 接收 bulk 长度（如 "$<len>\r\n"）
    RECV_STATE_BULK_DATA,      // 接收 bulk 数据主体
    RECV_STATE_TAIL,           // 接收尾部（如 "\r\n"）
    RECV_STATE_DONE            // 接收完成
} recv_state_t;

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

  /* ---- 流式发送相关 ---- */
  int is_streaming;           // 是否正在流式发送（0=否，1=是）
  stream_state_t stream_state;// 流式发送状态
  const char* stream_data;    // 流式发送的数据源（指向实际数据）
  size_t stream_total_len;    // 总数据长度
  size_t stream_sent_len;     // 已发送长度
  size_t stream_chunk_size;   // 每次发送的块大小（默认 IOP_SIZE）
  char stream_header[256];    // 流式发送的头部缓冲区
  int header_len;             // 头部实际长度
  int header_sent;            // 头部已发送长度
  char stream_tail[8];        // 流式发送的尾部缓冲区（"\r\n"）
  int tail_sent;              // 尾部已发送长度

  /* ---- 流式接收相关 ---- */
  int is_streaming_recv;           // 是否正在流式接收（0=否，1=是）
  recv_state_t recv_state;        // 流式接收状态
  char* recv_buffer;               // 接收缓冲区（环形），NULL 表示使用默认 frame
  size_t recv_buffer_size;         // 缓冲区大小
  size_t recv_head;                // 写指针（数据写入位置）
  size_t recv_tail;                // 读指针（数据读取位置）
  size_t recv_total_len;           // 期望接收的总长度
  size_t recv_processed_len;       // 已处理的数据长度
  size_t recv_bulk_remaining;      // bulk 数据剩余长度
  char* bulk_target;               // bulk 数据目标缓冲区（直接写入）
  size_t bulk_target_len;          // 目标缓冲区长度
  size_t bulk_target_used;         // 已写入目标缓冲区的长度
};

// 消息处理回调函数定义
typedef int (*msg_handler)(struct conn* c);

// 网络模型启动函数声明
extern int reactor_start(unsigned short port, msg_handler handler);
extern int proactor_start(unsigned short port, msg_handler handler);
extern int ntyco_start(unsigned short port, msg_handler handler);

// 环形缓冲区函数声明（用于流式接收）
extern int ring_buffer_write(struct conn* c, const char* data, size_t len);
extern int ring_buffer_read(struct conn* c, char* data, size_t len);
extern int ring_buffer_peek(struct conn* c, char* data, size_t len);
extern int ring_buffer_skip(struct conn* c, size_t len);
extern size_t ring_buffer_available(struct conn* c);

#endif // __KVS_NETWORK_H__