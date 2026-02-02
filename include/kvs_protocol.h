#ifndef __KVS_PROTOCOL_H__
#define __KVS_PROTOCOL_H__

#include "kvs_constants.h"
#include "kvs_network.h"

/* ---------------- 返回值定义 ---------------- */
#define NEED_STREAMING_RECV 2  // 需要流式接收（数据直接写入 seg_buf）

// 从 proactor.c 迁移过来的 RESP 解析核心函数
// 啃掉 conn->frame 中的数据，填充 conn->argv
int kvs_resp_feed(struct conn* c);

// 重置 RESP 解析状态 (原 conn_reset 的核心逻辑)
void kvs_resp_reset(struct conn* c);

// 释放 RESP 解析过程中申请的内存 (原 conn_free 的核心逻辑)
void kvs_resp_free_resources(struct conn* c);


#endif // __KVS_PROTOCOL_H__