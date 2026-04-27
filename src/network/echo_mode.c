#include "echo_mode.h"
#include "kvstore.h"

#if ENABLE_ECHO_MODE

void echo_handler(reply_builder_t *rb) {
    net_conn_t *nc = rb->nc;
    // 将本次接收到的所有数据原封不动写入回复构建器
    rb_add_reply_str_len(rb, nc->rbuf_ptr, nc->rlen);
    nc->rlen = 0;
}

#endif
