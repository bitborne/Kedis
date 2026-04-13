#include "echo_mode.h"
#include "kvstore.h"

#if ENABLE_ECHO_MODE

void echo_handler(struct conn *c) {
    // 将本次接收到的所有数据原封不动拷贝到发送缓冲区
    memcpy(c->wbuf, c->rbuf, c->rlen);
    c->wlen = c->rlen;

    // 重置接收缓冲区，准备下一次 recv
    c->rlen = 0;

    // 切换到发送状态，走 SMALL 响应路径
    c->state = ST_SEND;
    c->send_st = ST_SEND_SMALL;
}

#endif
