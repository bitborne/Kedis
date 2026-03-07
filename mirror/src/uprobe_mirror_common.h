#ifndef __UPROBE_MIRROR_COMMON_H__
#define __UPROBE_MIRROR_COMMON_H__

#define IOP_SIZE 4096

// 更新的事件结构 - 增加 parse_done
struct uprobe_event {
    int fd;              // socket fd
    __u32 rlen;          // 总数据长度
    __u32 parse_done;    // 已解析的字节数（剩余数据从该位置开始）
    __u8 data[IOP_SIZE]; // 原始数据 rbuf
};

#endif
