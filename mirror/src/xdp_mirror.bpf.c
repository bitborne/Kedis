#include "vmlinux.h"
#include <bpf/bpf_endian.h>
#include <bpf/bpf_helpers.h>

#include "xdp_mirror_common.h"

char LICENSE[] SEC("license") = "Dual BSD/GPL";

// XDP 返回值定义
#define XDP_ABORTED 0   // 发生错误，丢弃数据包
#define XDP_DROP 1      // 丢弃数据包
#define XDP_PASS 2      // 允许数据包继续传递到网络栈
#define XDP_TX 3        // 从接收接口发送回去
#define XDP_REDIRECT 4  // 重定向到其他接口

#define ICMP_PROTOCOL 1
#define TCP_PROTOCOL 6
#define UDP_PROTOCOL 17

struct {
  __uint(type, BPF_MAP_TYPE_RINGBUF);
  __uint(max_entries, 8 * 1024 * 1024);  // 用户态消费跟不上生产:256*1024 -> 8*1024*1024
} rb SEC(".maps");

// XDP 程序：过滤 ICMP 数据包并统计流量
SEC("xdp")
int xdp_mirror_forward(struct xdp_md* ctx) {
  // Step 1: 获取数据包的起始和结束位置
  void* data = (void*)(long)ctx->data;
  void* data_end = (void*)(long)ctx->data_end;

  // Step 2: 解析以太网头部
  struct ethhdr* eth = data;

  // 边界检查：确保不会越界访问
  if ((void*)(eth + 1) > data_end) return XDP_PASS;  // 数据包太小，直接放行

  // Step 3: 检查是否为 IP 协议 (EtherType = 0x0800)
  if (eth->h_proto != bpf_htons(0x0800))
    return XDP_PASS;  // 不是 IPv4，直接放行

  struct iphdr* ip = (void*)(eth + 1);
  if ((void*)(ip + 1) > data_end) return XDP_PASS;
  if (ip->protocol != IPPROTO_TCP) return XDP_PASS;

  __u32 ip_hdr_len = ip->ihl * 4;
  if (ip_hdr_len < 20 || ip_hdr_len > 60) return XDP_PASS;

  struct tcphdr* tcp = (void*)((unsigned char*)ip + (ip_hdr_len & 0x3F));
  if ((void*)(tcp + 1) > data_end) return XDP_PASS;
  if (tcp->dest != bpf_htons(8888)) return XDP_PASS;

  __u32 tcp_hdr_len = tcp->doff * 4;
  if (tcp_hdr_len < 20 || tcp_hdr_len > 60) return XDP_PASS;

  // 确保数据偏移合法
  __u32 data_offset = sizeof(struct ethhdr) + ip_hdr_len + tcp_hdr_len;
  __u32 ctx_len = ctx->data_end - ctx->data;
  if (data_offset > ctx_len) return XDP_PASS;

  __u32 payload_len = ctx_len - data_offset;
  if (payload_len == 0) return XDP_PASS;

  // [新增] 提取 TCP 序列号，注意网络字节序到主机字节序的转换
  __u32 tcp_seq = bpf_ntohl(tcp->seq);

  // 发送 Data 分块 (移除了 EVENT_HEADER 的发送)
#pragma unroll
  for (int i = 0; i < 1000; i++) {
    __u32 cur_offset = i * CHUNK_SIZE;
    if (cur_offset >= payload_len) break;  //-> chunk_len 必 > 0

    __u32 remaining = payload_len - cur_offset;
    __u32 chunk_len = remaining;  // 计算剩余数据有多少
    if (chunk_len > CHUNK_SIZE) chunk_len = CHUNK_SIZE;
    // 如果超过CHUNK_LEN, 那么先发CHUNK_LEN, 剩下的交给下一次循环

    // 0. 目前,我们人类知道,运行时已经确保了 0 < chunk_len <= CHUNK_SIZE
    // 1. 即使验证器不确定 chunk_len 的最小边界
    // 2. 我们通过 (len - 1) & 0x3FF 确保结果在 0-1023 (0-CHUNK_SIZE)
    // 3. 再 + 1，确保结果在 1-1024
    __u32 final_len = ((chunk_len - 1) & 0x3FF) + 1;
    // 这样验证器就知道, final_len 一定不是0
    // 而 final_len 就等于 chunk_len,就是我们要发送的数据大小

    struct packet_event* de = bpf_ringbuf_reserve(&rb, sizeof(*de), 0);
    if (!de) continue;

    de->type = EVENT_DATA;
    de->src_ip = ip->saddr;
    de->dst_ip = ip->daddr;
    de->src_port = bpf_ntohs(tcp->source);
    de->dst_port = bpf_ntohs(tcp->dest);

    // [新增] 计算当前 Chunk 的绝对序列号
    de->seq = tcp_seq + cur_offset; 
    de->chunk_len = final_len;

    // ============================================

    // 这里使用 final_len，验证器现在可以推导出 R4 属于 [1, 256]
    bpf_xdp_load_bytes(ctx, data_offset + cur_offset, de->data, final_len);
    bpf_ringbuf_submit(de, 0);
  }

  return XDP_PASS;
}