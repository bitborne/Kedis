#include "vmlinux.h"
#include <bpf/bpf_endian.h>
#include <bpf/bpf_helpers.h>

#include "xdp_mirror_common.h"

char LICENSE[] SEC("license") = "Dual BSD/GPL";

// XDP 返回值定义
#define XDP_PASS 2

#define ETH_P_IP 0x0800

struct {
    __uint(type, BPF_MAP_TYPE_RINGBUF);
    __uint(max_entries, 32 * 1024 * 1024);
} rb SEC(".maps");

SEC("xdp")
int xdp_mirror_forward(struct xdp_md *ctx)
{
    void *data_end = (void *)(long)ctx->data_end;
    void *data = (void *)(long)ctx->data;

    struct ethhdr *eth = data;
    if ((void *)(eth + 1) > data_end) return XDP_PASS;
    if (eth->h_proto != bpf_htons(ETH_P_IP)) return XDP_PASS;

    struct iphdr *ip = (void *)(eth + 1);
    if ((void *)(ip + 1) > data_end) return XDP_PASS;
    if (ip->protocol != IPPROTO_TCP) return XDP_PASS;

    __u32 ip_hdr_len = ip->ihl * 4;
    if (ip_hdr_len < 20 || ip_hdr_len > 60) return XDP_PASS;

    struct tcphdr *tcp = (void *)((unsigned char *)ip + (ip_hdr_len & 0x3F));
    if ((void *)(tcp + 1) > data_end) return XDP_PASS;
    if (tcp->dest != bpf_htons(8888)) return XDP_PASS;

    __u32 tcp_hdr_len = tcp->doff * 4;
    if (tcp_hdr_len < 20 || tcp_hdr_len > 60) return XDP_PASS;

    __u32 data_offset = sizeof(struct ethhdr) + ip_hdr_len + tcp_hdr_len;
    __u32 ctx_len = data_end - data;
    if (data_offset > ctx_len) return XDP_PASS;

    __u32 payload_len = ctx_len - data_offset;
    if (payload_len == 0) return XDP_PASS;

    // 发送 Header 事件
    struct packet_event *e = bpf_ringbuf_reserve(&rb, sizeof(*e), 0);
    if (e) {
        e->type = EVENT_HEADER;
        e->src_ip = ip->saddr;
        e->dst_ip = ip->daddr;
        e->src_port = bpf_ntohs(tcp->source);
        e->dst_port = bpf_ntohs(tcp->dest);
        e->payload_len = payload_len;
        e->offset = 0;
        e->chunk_len = 0;
        bpf_ringbuf_submit(e, 0);
    }

    // 发送 Data 分块
    #pragma unroll
    for (int i = 0; i < 1000; i++) {
        __u32 cur_offset = i * CHUNK_SIZE;
        if (cur_offset >= payload_len) break;

        __u32 remaining = payload_len - cur_offset;
        __u32 chunk_len = remaining;
        if (chunk_len > CHUNK_SIZE) chunk_len = CHUNK_SIZE;

        // 验证器友好：确保 chunk_len 在 [1, CHUNK_SIZE]
        __u32 final_len = ((chunk_len - 1) & 0xFFFF) + 1;

        struct packet_event *de = bpf_ringbuf_reserve(&rb, sizeof(*de), 0);
        if (!de) continue;

        de->type = EVENT_DATA;
        de->src_ip = ip->saddr;
        de->dst_ip = ip->daddr;
        de->src_port = bpf_ntohs(tcp->source);
        de->dst_port = bpf_ntohs(tcp->dest);
        de->payload_len = 0;
        de->offset = cur_offset;
        de->chunk_len = final_len;

        bpf_xdp_load_bytes(ctx, data_offset + cur_offset, de->data, final_len);
        bpf_ringbuf_submit(de, 0);
    }

    return XDP_PASS;
}
