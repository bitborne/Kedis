#ifndef __KVS_PROTOCOL_H__
#define __KVS_PROTOCOL_H__

#include <stddef.h>

/* ---------------- 协议层常量 ---------------- */
#define MAX_ARGC (8)                        // 最大参数个数
#define MAX_SEG_SIZE (1024 * 1024 * 1024)  // 单段最大 1 GB

/* ---------------- RESP 状态机枚举 ---------------- */
typedef enum {
  ST_RESP_HDR,        // 等待解析 *<argc> (命令开始)
  ST_RESP_BULK_LEN,   // 等待解析 $<len> (参数长度)
  ST_RESP_BULK_DATA,  // 正在收 bulk 内容
  ST_RESP_OK
} resp_state_t;

/* ---------------- 段对象：只挂指针，不拷贝数据 ---------------- */
typedef struct {
  char* ptr;   // 指向数据（可能是堆内存，也可能直接指向 rbuf）
  size_t len;  // 段长度
  unsigned int flags;  // 标志位
} robj;

/* ---------------- 协议解析器（从 conn 抽离，支持未来注册缓冲区） ---------------- */
typedef struct {
  resp_state_t resp_state;
  size_t bulk_len;
  size_t bulk_done;
  int argc;
  int argc_done;
  size_t parse_done;
  robj argv[MAX_ARGC];
  char cmd_buf[16];
} proto_parser_t;

/* ---------------- 协议层接口 ---------------- */
extern void proto_parser_reset(proto_parser_t *p);
extern size_t proto_feed(proto_parser_t *p, const char *data, size_t len);
extern int proto_cmd_ready(const proto_parser_t *p);
extern int proto_take_cmd(proto_parser_t *p, int *argc_out, robj **argv_out);
extern void proto_free_argv(int argc, robj *argv);

// 重置 RESP 解析状态
void kvs_resp_reset(proto_parser_t *p);

// 释放 RESP 解析过程中申请的内存
void kvs_resp_free_resources(proto_parser_t *p);

#endif // __KVS_PROTOCOL_H__
