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

// 调试计数器
struct {
    __uint(type, BPF_MAP_TYPE_ARRAY);
    __uint(max_entries, 8);
    __type(key, __u32);
    __type(value, __u64);
} debug_stats SEC(".maps");

// conn 结构体偏移量（基于实际计算）
#define CONN_FD_OFFSET         0
#define CONN_RBUF_OFFSET       12
#define CONN_RLEN_OFFSET       4112
#define CONN_PARSE_DONE_OFFSET 4120

SEC("uprobe/kvs_resp_feed")
int BPF_KPROBE(uprobe_kvs_resp_feed, void *c)
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
    if (bpf_probe_read_user(&fd, sizeof(fd), (void*)c + CONN_FD_OFFSET) < 0) {
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
    if (bpf_probe_read_user(&rlen64, sizeof(rlen64), (void*)c + CONN_RLEN_OFFSET) < 0) {
        __u32 key = 4;
        __u64 *cnt = bpf_map_lookup_elem(&debug_stats, &key);
        if (cnt) __sync_fetch_and_add(cnt, 1);
        return 0;
    }
    
    // 读取 parse_done
    __u64 parse_done64 = 0;
    if (bpf_probe_read_user(&parse_done64, sizeof(parse_done64), (void*)c + CONN_PARSE_DONE_OFFSET) < 0) {
        __u32 key = 5;
        __u64 *cnt = bpf_map_lookup_elem(&debug_stats, &key);
        if (cnt) __sync_fetch_and_add(cnt, 1);
        return 0;
    }
    
    // 边界检查
    if (rlen64 == 0 || rlen64 > IOP_SIZE) {
        __u32 key = 6;
        __u64 *cnt = bpf_map_lookup_elem(&debug_stats, &key);
        if (cnt) __sync_fetch_and_add(cnt, 1);
        return 0;
    }
    
    // 确保 parse_done <= rlen
    if (parse_done64 > rlen64) {
        parse_done64 = 0;  // 异常情况，从头开始
    }
    
    __u32 rlen = (__u32)rlen64;
    __u32 parse_done = (__u32)parse_done64;
    
    // 计算实际有效数据：从 parse_done 到 rlen
    __u32 valid_len = rlen - parse_done;
    if (valid_len == 0) {
        return 0;  // 没有新数据
    }
    
    // 分配 ring buffer 事件
    struct uprobe_event *e = bpf_ringbuf_reserve(&rb, sizeof(*e), 0);
    if (!e) {
        __u32 key = 7;
        __u64 *cnt = bpf_map_lookup_elem(&debug_stats, &key);
        if (cnt) __sync_fetch_and_add(cnt, 1);
        return 0;
    }
    
    // 填充事件
    e->fd = fd;
    e->rlen = rlen;
    e->parse_done = parse_done;
    
    // 读取 rbuf 数据（全部读取，用户态根据 parse_done 处理）
    void *rbuf_ptr = (void*)c + CONN_RBUF_OFFSET;
    bpf_probe_read_user(e->data, rlen, rbuf_ptr);
    
    bpf_ringbuf_submit(e, 0);
    
    // 计数 8: 成功
    __u32 key8 = 8;
    __u64 *cnt8 = bpf_map_lookup_elem(&debug_stats, &key8);
    if (cnt8) __sync_fetch_and_add(cnt8, 1);
    
    return 0;
}
