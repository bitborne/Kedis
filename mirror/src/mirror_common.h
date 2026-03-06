#ifndef __MIRROR_COMMON_H__
#define __MIRROR_COMMON_H__

#define CHUNK_SIZE 65536
#define EVENT_HEADER 1
#define EVENT_DATA 2

struct packet_event {
    __u32 type;
    __u32 src_ip;
    __u32 dst_ip;
    __u16 src_port;
    __u16 dst_port;
    __u32 payload_len;
    __u32 offset;
    __u32 chunk_len;
    __u8 data[CHUNK_SIZE];
};

#endif