#include <arpa/inet.h>
#include <bpf/bpf.h>
#include <bpf/libbpf.h>
#include <errno.h>
#include <fcntl.h>
#include <linux/if_link.h>
#include <net/if.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include "xdp_mirror_common.h"
#include "mirror_log.h"
#include "xdp_mirror.skel.h"

#define HASH_SIZE 16384
#define FLOW_TIMEOUT_SEC 60

// --- 数据结构 ---

struct ooo_segment {
    __u32 seq;
    __u32 len;
    __u8 data[CHUNK_SIZE];
    struct ooo_segment* next;
};

struct tcp_stream {
    struct tcp_stream* next; // Hash 冲突链
    __u32 src_ip, dst_ip;
    __u16 src_port, dport;

    __u32 expected_seq;
    bool seq_initialized;
    struct ooo_segment* ooo_list;
    time_t last_update;
};

// --- 全局变量 ---

static volatile bool exiting = false;
static int slave_fd = -1;
static struct tcp_stream* flow_table[HASH_SIZE] = {0};
static long long total_sent_bytes = 0;

static void sig_handler(int sig) {
    debug("total_sent: %lld\n", total_sent_bytes);
    fprintf(stderr, "收到信号 %d，正在退出...\n", sig);
    exiting = true;     
}

// --- 工具函数 ---

static unsigned int hash_5tuple(__u32 sip, __u32 dip, __u16 sport, __u16 dport) {
    return (sip ^ dip ^ sport ^ dport) % HASH_SIZE;
}

static void free_ooo_list(struct ooo_segment* list) {
    while (list) {
        struct ooo_segment* tmp = list;
        list = list->next;
        free(tmp);
    }
}

static void send_to_slave(const __u8* data, __u32 len) {
    if (slave_fd < 0) return;
    ssize_t total = 0;
    while (total < len) {
        ssize_t n = send(slave_fd, data + total, len - total, MSG_NOSIGNAL);
        if (n <= 0) {
            if (errno == EINTR || errno == EAGAIN) continue;
            mirror_logError("Slave socket error, closing...");
            close(slave_fd);
            slave_fd = -1;
            break;
        }
        total += n;
    }
    total_sent_bytes += total;
}

// --- 流管理逻辑 ---

static void check_ooo_list(struct tcp_stream* stream) {
    while (stream->ooo_list && stream->ooo_list->seq == stream->expected_seq) {
        struct ooo_segment* ready = stream->ooo_list;
        send_to_slave(ready->data, ready->len);
        stream->expected_seq += ready->len;
        
        stream->ooo_list = ready->next;
        free(ready);
    }
}

static struct tcp_stream* get_stream(__u32 sip, __u32 dip, __u16 sport, __u16 dport) {
    unsigned int idx = hash_5tuple(sip, dip, sport, dport);
    struct tcp_stream* s = flow_table[idx];
    while (s) {
        if (s->src_ip == sip && s->dst_ip == dip && s->src_port == sport && s->dport == dport) {
            s->last_update = time(NULL);
            return s;
        }
        s = s->next;
    }
    
    struct tcp_stream* new_s = calloc(1, sizeof(struct tcp_stream));
    if (!new_s) return NULL;
    new_s->src_ip = sip; new_s->dst_ip = dip;
    new_s->src_port = sport; new_s->dport = dport;
    new_s->last_update = time(NULL);
    new_s->next = flow_table[idx];
    flow_table[idx] = new_s;
    return new_s;
}

// 清理过期连接，防止内存溢出
static void cleanup_expired_streams() {
    time_t now = time(NULL);
    for (int i = 0; i < HASH_SIZE; i++) {
        struct tcp_stream **curr = &flow_table[i];
        while (*curr) {
            struct tcp_stream *entry = *curr;
            if (now - entry->last_update > FLOW_TIMEOUT_SEC) {
                mirror_logInfo("Expiring flow %u -> %u", entry->src_port, entry->dport);
                *curr = entry->next;
                free_ooo_list(entry->ooo_list);
                free(entry);
            } else {
                curr = &entry->next;
            }
        }
    }
}

// --- 事件回调 ---

static int handle_event(void* ctx, void* data, size_t data_sz) {
    struct packet_event* e = data;
    struct tcp_stream* s = get_stream(e->src_ip, e->dst_ip, e->src_port, e->dst_port);
    if (!s) return 0;

    if (!s->seq_initialized) {
        s->expected_seq = e->seq;
        s->seq_initialized = true;
    }

    if (e->seq == s->expected_seq) {
        send_to_slave(e->data, e->chunk_len);
        s->expected_seq += e->chunk_len;
        check_ooo_list(s);
    } else if (e->seq > s->expected_seq) {
        // 插入乱序链表（保持升序）
        struct ooo_segment* new_seg = malloc(sizeof(struct ooo_segment));
        new_seg->seq = e->seq; new_seg->len = e->chunk_len;
        memcpy(new_seg->data, e->data, e->chunk_len);

        struct ooo_segment **p = &s->ooo_list;
        while (*p && (*p)->seq < e->seq) p = &(*p)->next;
        if (*p && (*p)->seq == e->seq) { free(new_seg); } // 重复包
        else { new_seg->next = *p; *p = new_seg; }
    }
    return 0;
}

// --- 主函数 ---

int main(int argc, char** argv) {
    if (argc < 3) {
        printf("Usage: %s <ifname> <slave_ip:port>\n", argv[0]);
        return 1;
    }

    signal(SIGINT, sig_handler);
    signal(SIGTERM, sig_handler);

    // 解析从节点地址并连接
    char ip[64]; int port;
    sscanf(argv[2], "%[^:]:%d", ip, &port);
    struct sockaddr_in addr = { .sin_family = AF_INET, .sin_port = htons(port) };
    inet_pton(AF_INET, ip, &addr.sin_addr);
    
    slave_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (connect(slave_fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("Connect slave failed");
        return 1;
    }
    mirror_logInfo("Connected to slave %s:%d", ip, port);

    // 加载 BPF
    struct xdp_mirror_bpf* skel = xdp_mirror_bpf__open_and_load();
    int ifindex = if_nametoindex(argv[1]);
    bpf_xdp_attach(ifindex, bpf_program__fd(skel->progs.xdp_mirror_forward), XDP_FLAGS_SKB_MODE, NULL);

    struct ring_buffer* rb = ring_buffer__new(bpf_map__fd(skel->maps.rb), handle_event, NULL, NULL);
    
    printf("XDP Mirroring started. Monitoring flows...\n");

    time_t last_cleanup = time(NULL);
    while (!exiting) {
        int err = ring_buffer__poll(rb, 100);
        if (err < 0 && err != -EINTR) break;

        // 每 10 秒执行一次清理和统计打印
        if (time(NULL) - last_cleanup > 10) {
            cleanup_expired_streams();
            printf("\r[Stats] Total Sent: %lld bytes\n", total_sent_bytes);
            fflush(stdout);
            last_cleanup = time(NULL);
        }
    }

    // 清理退出
    bpf_xdp_detach(ifindex, XDP_FLAGS_SKB_MODE, NULL);
    xdp_mirror_bpf__destroy(skel);
    close(slave_fd);
    printf("\nExit safely.\n");
    return 0;
}