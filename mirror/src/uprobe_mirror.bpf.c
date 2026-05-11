#include "vmlinux.h"
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_tracing.h>
#include <bpf/bpf_core_read.h>

#include "uprobe_mirror_common.h"

char LICENSE[] SEC("license") = "Dual BSD/GPL";

struct {
    __uint(type, BPF_MAP_TYPE_RINGBUF);
    __uint(max_entries, 256 * 1024 * 1024);
} rb SEC(".maps");

// 调试计数器 (key 0-7, 与数组 max_entries=8 严格对应)
struct {
    __uint(type, BPF_MAP_TYPE_ARRAY);
    __uint(max_entries, 8);
    __type(key, __u32);
    __type(value, __u64);
} debug_stats SEC(".maps");

/*
 * 警告：以下偏移量必须与 include/kvs_network.h 中的 struct conn 布局严格同步。
 * 当前偏移基于 64 位系统布局（热字段在前，大缓冲区在后）。
 * struct conn {
 *   int fd;                // offset 0
 *   int state;             // offset 4
 *   int next_free;         // offset 8
 *   // padding 4
 *   size_t rlen;           // offset 16
 *   size_t rbuf_off;       // offset 24
 *   int recv_inflight;     // offset 32
 *   int send_inflight;     // offset 36
 *   char *rbuf_ptr;        // offset 40   <-- 接收缓冲区指针
 *   size_t rbuf_cap;       // offset 48
 *   char rbuf_embedded[4096]; // offset 56
 *   ...
 * };
 */
#define CONN_FD_OFFSET         0
#define CONN_RLEN_OFFSET       16
#define CONN_RBUF_OFF_OFFSET   24
#define CONN_RBUF_PTR_OFFSET   40

SEC("uprobe/process_commands")
int BPF_KPROBE(uprobe_process_commands, void *ring, void *c)
{
    __u32 key0 = 0;
    __u64 *cnt0 = bpf_map_lookup_elem(&debug_stats, &key0);
    if (cnt0) __sync_fetch_and_add(cnt0, 1);

    if (!c) {
        __u32 key = 1;
        __u64 *cnt = bpf_map_lookup_elem(&debug_stats, &key);
        if (cnt) __sync_fetch_and_add(cnt, 1);
        return 0;
    }

    // 读取 fd
    int fd = 0;
    if (bpf_probe_read_user(&fd, sizeof(fd), (void *)c + CONN_FD_OFFSET) < 0) {
        __u32 key = 2;
        __u64 *cnt = bpf_map_lookup_elem(&debug_stats, &key);
        if (cnt) __sync_fetch_and_add(cnt, 1);
        return 0;
    }

    if (fd < 0) {
        __u32 key = 3;
        __u64 *cnt = bpf_map_lookup_elem(&debug_stats, &key);
        if (cnt) __sync_fetch_and_add(cnt, 1);
        return 0;
    }

    // 读取 rlen
    __u64 rlen64 = 0;
    if (bpf_probe_read_user(&rlen64, sizeof(rlen64), (void *)c + CONN_RLEN_OFFSET) < 0) {
        __u32 key = 4;
        __u64 *cnt = bpf_map_lookup_elem(&debug_stats, &key);
        if (cnt) __sync_fetch_and_add(cnt, 1);
        return 0;
    }

    // 读取 rbuf_off
    __u64 rbuf_off64 = 0;
    if (bpf_probe_read_user(&rbuf_off64, sizeof(rbuf_off64), (void *)c + CONN_RBUF_OFF_OFFSET) < 0) {
        __u32 key = 4;
        __u64 *cnt = bpf_map_lookup_elem(&debug_stats, &key);
        if (cnt) __sync_fetch_and_add(cnt, 1);
        return 0;
    }

    // 读取 rbuf_ptr（接收缓冲区指针）
    void *rbuf_ptr = NULL;
    if (bpf_probe_read_user(&rbuf_ptr, sizeof(rbuf_ptr), (void *)c + CONN_RBUF_PTR_OFFSET) < 0) {
        __u32 key = 4;
        __u64 *cnt = bpf_map_lookup_elem(&debug_stats, &key);
        if (cnt) __sync_fetch_and_add(cnt, 1);
        return 0;
    }

    if (!rbuf_ptr) {
        __u32 key = 5;
        __u64 *cnt = bpf_map_lookup_elem(&debug_stats, &key);
        if (cnt) __sync_fetch_and_add(cnt, 1);
        return 0;
    }

    // 边界检查
    if (rlen64 == 0 || rlen64 > IOP_SIZE) {
        __u32 key = 5;
        __u64 *cnt = bpf_map_lookup_elem(&debug_stats, &key);
        if (cnt) __sync_fetch_and_add(cnt, 1);
        return 0;
    }

    __u32 rlen = (__u32)rlen64;
    __u32 rbuf_off = (__u32)rbuf_off64;

    // 实际未消费数据起始位置：rbuf_ptr + rbuf_off
    void *data_ptr = rbuf_ptr + rbuf_off;

    // 分配 ring buffer 事件
    struct uprobe_event *e = bpf_ringbuf_reserve(&rb, sizeof(*e), 0);
    if (!e) {
        __u32 key = 6;
        __u64 *cnt = bpf_map_lookup_elem(&debug_stats, &key);
        if (cnt) __sync_fetch_and_add(cnt, 1);
        return 0;
    }

    // 填充事件
e->fd = fd;
    e->rlen = rlen;
    e->parse_done = 0;  // data 已指向有效数据起始，无需额外偏移

    // 读取实际数据到事件
    bpf_probe_read_user(e->data, rlen, data_ptr);

    bpf_ringbuf_submit(e, 0);

    // key 7: 成功
    __u32 key7 = 7;
    __u64 *cnt7 = bpf_map_lookup_elem(&debug_stats, &key7);
    if (cnt7) __sync_fetch_and_add(cnt7, 1);

    return 0;
}
