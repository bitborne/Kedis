#ifndef __XDP_MIRROR_COMMON_H__
#define __XDP_MIRROR_COMMON_H__

#define CHUNK_SIZE 65536
#define EVENT_HEADER 1
#define EVENT_DATA 2

struct packet_event {
    __u32 type;
    __u32 src_ip;
    __u32 dst_ip;
    __u16 src_port;
    __u16 dst_port;
    __u32 payload_len;  // HEADER: 总长度, DATA: 当前 chunk 长度
    __u32 offset;       // DATA 事件专用: 在 payload 中的偏移
    __u32 chunk_len;    // DATA 事件专用: 当前 chunk 实际长度
    __u8 data[CHUNK_SIZE];
};

#endif
