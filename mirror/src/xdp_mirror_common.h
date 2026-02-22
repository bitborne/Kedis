#ifndef __XDP_MIRROR_COMMON_H__
#define __XDP_MIRROR_COMMON_H__

#define CHUNK_SIZE 1024
#define EVENT_DATA 2  // 我们现在只需要 DATA 事件

struct packet_event {
    __u32 type;
    __u32 src_ip;
    __u32 dst_ip;
    __u16 src_port;
    __u16 dst_port;
    __u32 seq;         // [新增] TCP序列号，用于乱序重排
    __u32 chunk_len;   // 当前分块的长度
    __u8 data[CHUNK_SIZE];
};

#endif