#include "vmlinux.h"
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_endian.h>

#define CHUNK_SIZE 256
#define EVENT_HEADER 1
#define EVENT_DATA   2

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

#define ETH_P_IP 0x0800

char LICENSE[] SEC("license") = "Dual BSD/GPL";

struct {
    __uint(type, BPF_MAP_TYPE_RINGBUF);
    __uint(max_entries, 256 * 1024);
} rb SEC(".maps");

SEC("tc")
int mirror_forward(struct __sk_buff *skb)
{
    void *data_end = (void *)(long)skb->data_end;
    void *data = (void *)(long)skb->data;

    struct ethhdr *eth = data;
    if ((void *)(eth + 1) > data_end) return 0;
    if (eth->h_proto != bpf_htons(ETH_P_IP)) return 0;

    struct iphdr *ip = (void *)(eth + 1);
    if ((void *)(ip + 1) > data_end) return 0;
    if (ip->protocol != IPPROTO_TCP) return 0;

    __u32 ip_hdr_len = ip->ihl * 4;
    if (ip_hdr_len < 20 || ip_hdr_len > 60) return 0;

    struct tcphdr *tcp = (void *)((unsigned char *)ip + (ip_hdr_len & 0x3F));
    if ((void *)(tcp + 1) > data_end) return 0;
    if (tcp->dest != bpf_htons(8888)) return 0;

    __u32 tcp_hdr_len = tcp->doff * 4;
    if (tcp_hdr_len < 20 || tcp_hdr_len > 60) return 0;

    __u32 data_offset = sizeof(struct ethhdr) + ip_hdr_len + tcp_hdr_len;
    // 显式检查偏移量
    if (data_offset > 1500) return 0; 
    if (data_offset > skb->len) return 0;
    
    __u32 payload_len = skb->len - data_offset;
    if (payload_len == 0) return 0;
    // 限制最大处理长度，防止验证器爆炸
    // if (payload_len > 2560) payload_len = 2560;

    // 发送 Header
    struct packet_event *e = bpf_ringbuf_reserve(&rb, sizeof(*e), 0);
    if (e) {
        e->type = EVENT_HEADER;
        e->src_ip = ip->saddr; e->dst_ip = ip->daddr;
        e->src_port = bpf_ntohs(tcp->source); e->dst_port = bpf_ntohs(tcp->dest);
        e->payload_len = payload_len; e->offset = 0; e->chunk_len = 0;
        bpf_ringbuf_submit(e, 0);
    }

    // 发送 Data
    #pragma unroll
    for (int i = 0; i < 1000; i++) {
        __u32 cur_offset = i * CHUNK_SIZE;
        if (cur_offset >= payload_len) break;

        __u32 remaining = payload_len - cur_offset;
        __u32 chunk_len = remaining;
        if (chunk_len > CHUNK_SIZE) chunk_len = CHUNK_SIZE;

        // --- 【最终修复逻辑】 ---
        // 1. 即使验证器不确定 chunk_len 的最小边界
        // 2. 我们通过 (len - 1) & 0xFF 确保结果在 0-255
        // 3. 再 + 1，确保结果在 1-256
        __u32 final_len = ((chunk_len - 1) & 0xFF) + 1;

        struct packet_event *de = bpf_ringbuf_reserve(&rb, sizeof(*de), 0);
        if (!de) continue;

        de->type = EVENT_DATA;
        de->offset = cur_offset;
        de->chunk_len = final_len;

        // 这里使用 final_len，验证器现在可以推导出 R4 属于 [1, 256]
        bpf_skb_load_bytes(skb, data_offset + cur_offset, de->data, final_len);
        bpf_ringbuf_submit(de, 0);
    }

    return 0;
}