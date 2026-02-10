#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <signal.h>
#include <time.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <bpf/libbpf.h>
#include <bpf/bpf.h>
#include <net/if.h>
#include "mirror.skel.h"

#define LOG_FILE "a.log"
// #define SLAVE_IP "172.20.10.2"
// #define SLAVE_PORT 9999
#define CHUNK_SIZE 256
#define MAX_PAYLOAD 65536

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

// 重组缓冲区
struct reassembly_buffer {
    __u32 src_ip;
    __u32 dst_ip;
    __u16 src_port;
    __u16 dst_port;
    __u32 total_len;
    __u32 received_len;
    __u8 data[MAX_PAYLOAD];
    int active;
};

static volatile bool exiting = false;
static int log_fd = -1;
static int slave_fd = -1;
static struct reassembly_buffer reasm = {0};

static void get_timestamp(char *buf, size_t len)
{
    time_t now = time(NULL);
    struct tm *tm_info = localtime(&now);
    strftime(buf, len, "%Y-%m-%d %H:%M:%S", tm_info);
}

static void flush_buffer(void)
{
    if (!reasm.active || reasm.received_len == 0)
        return;
    
    char src_str[16], dst_str[16];
    inet_ntop(AF_INET, &reasm.src_ip, src_str, sizeof(src_str));
    inet_ntop(AF_INET, &reasm.dst_ip, dst_str, sizeof(dst_str));
    
    printf("[Complete] %s:%d -> %s:%d, total: %u bytes\n",
           src_str, reasm.src_port, dst_str, reasm.dst_port, reasm.received_len);
    
    // 写入日志
    if (log_fd >= 0) {
        dprintf(log_fd, "--- %s:%d -> %s:%d (len=%u) ---\n",
                src_str, reasm.src_port, dst_str, reasm.dst_port, reasm.received_len);
        write(log_fd, reasm.data, reasm.received_len);
        write(log_fd, "\n", 1);
    }
    
    // 发送到从节点
    if (slave_fd >= 0) {
        ssize_t sent = 0;
        while (sent < reasm.received_len) {
            ssize_t n = send(slave_fd, reasm.data + sent, 
                           reasm.received_len - sent, 0);
            if (n < 0) {
                if (errno == EAGAIN) continue;
                perror("send");
                close(slave_fd);
                slave_fd = -1;
                break;
            }
            sent += n;
        }
        printf("Forwarded %zd bytes to slave\n", sent);
    }
    
    // 清空缓冲区
    memset(&reasm, 0, sizeof(reasm));
}

static int handle_event(void *ctx, void *data, size_t data_sz)
{
    struct packet_event *e = data;
    
    if (e->type == EVENT_HEADER) {
        // 新数据包开始，先刷新旧缓冲区
        if (reasm.active) {
            printf("Warning: incomplete packet, flushing...\n");
            flush_buffer();
        }
        
        // 初始化新缓冲区
        reasm.active = 1;
        reasm.src_ip = e->src_ip;
        reasm.dst_ip = e->dst_ip;
        reasm.src_port = e->src_port;
        reasm.dst_port = e->dst_port;
        reasm.total_len = e->payload_len;
        fprintf(stderr, "初始化: reasm.total_len == payload_len: %u\n", reasm.total_len);
        reasm.received_len = 0;
        
        if (reasm.total_len > MAX_PAYLOAD) {
            printf("Warning: payload too large (%u), truncating to %d\n",
                   reasm.total_len, MAX_PAYLOAD);
            reasm.total_len = MAX_PAYLOAD;
        }
        
        printf("[Header] Expecting %u bytes\n", reasm.total_len);
        
    } else if (e->type == EVENT_DATA) {
        if (!reasm.active) {
            printf("Warning: data without header, dropping\n");
            return 0;
        }
        
        // 拷贝分片到正确位置
        if (e->offset + e->chunk_len > MAX_PAYLOAD) {
            printf("Warning: chunk offset %u out of bounds\n", e->offset);
            return 0;
        }
        
        memcpy(reasm.data + e->offset, e->data, e->chunk_len);
        reasm.received_len += e->chunk_len;
        
        printf("[Chunk] offset=%u, len=%u, total_received=%u\n",
               e->offset, e->chunk_len, reasm.received_len);
        
        // 如果收齐了，立即发送
        if (reasm.received_len >= reasm.total_len) {
            flush_buffer();
        }
    }
    
    return 0;
}

static int connect_slave(const char* slave_ip, uint16_t slave_port)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("socket");
        return -1;
    }

    struct sockaddr_in addr = {
        .sin_family = AF_INET,
        .sin_port = htons(slave_port),
    };
    inet_pton(AF_INET, slave_ip, &addr.sin_addr);

    printf("Connecting to slave %s:%d...\n", slave_ip, slave_port);
    if (connect(fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("connect");
        close(fd);
        return -1;
    }
    printf("Connected to slave!\n");
    return fd;
}

static void sig_handler(int sig)
{
    printf("\n收到信号 %d，正在退出...\n", sig);
    exiting = true;
}

int main(int argc, char **argv)
{
    struct mirror_bpf *skel;
    int err;
    int ifindex;
    LIBBPF_OPTS(bpf_tc_hook, hook);
    LIBBPF_OPTS(bpf_tc_opts, opts_ingress);

    if (argc != 4) {
        fprintf(stderr, "Usage: %s <ifname> <slave_ip> <slave_port>\n", argv[0]);
        return 1;
    }

    const char* slave_ip = argv[2];
    uint16_t slave_port = atoi(argv[3]);

    ifindex = if_nametoindex(argv[1]);
    if (ifindex == 0) {
        fprintf(stderr, "Invalid interface: %s\n", argv[1]);
        return 1;
    }

    signal(SIGINT, sig_handler);
    signal(SIGTERM, sig_handler);

    slave_fd = connect_slave(slave_ip, slave_port);
    if (slave_fd < 0) {
        fprintf(stderr, "Failed to connect to slave\n");
        return 1;
    }

    skel = mirror_bpf__open_and_load();
    if (!skel) {
        fprintf(stderr, "Failed to open/load BPF skeleton\n");
        return 1;
    }

    log_fd = open(LOG_FILE, O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (log_fd < 0) {
        fprintf(stderr, "Failed to open %s: %s\n", LOG_FILE, strerror(errno));
    }

    char timestamp[64];
    get_timestamp(timestamp, sizeof(timestamp));
    if (log_fd >= 0) {
        dprintf(log_fd, "\n========================================\n");
        dprintf(log_fd, "  Test Run: %s\n", timestamp);
        dprintf(log_fd, "========================================\n\n");
    }
    printf("========================================\n");
    printf("  Test Run: %s\n", timestamp);
    printf("========================================\n");

    struct ring_buffer *rb = ring_buffer__new(
        bpf_map__fd(skel->maps.rb),
        handle_event,
        NULL,
        NULL
    );
    if (!rb) {
        fprintf(stderr, "Failed to create ring buffer\n");
        err = -1;
        goto cleanup;
    }

    hook.ifindex = ifindex;
    hook.attach_point = BPF_TC_INGRESS;
    err = bpf_tc_hook_create(&hook);
    if (err && err != -EEXIST) {
        fprintf(stderr, "Failed to create TC hook: %d\n", err);
        goto cleanup;
    }    

    opts_ingress.prog_fd = bpf_program__fd(skel->progs.mirror_forward);
    err = bpf_tc_attach(&hook, &opts_ingress);
    if (err) {
        fprintf(stderr, "Failed to attach TC: %d\n", err);
        goto cleanup;
    }
    
    printf("Running on %s. Press Ctrl+C to exit.\n", argv[1]);

    while (!exiting) {
        err = ring_buffer__poll(rb, 100);
        if (err < 0 && err != -EINTR) {
            fprintf(stderr, "Error polling: %d\n", err);
            break;
        }
    }

    // 退出前刷新剩余数据
    if (reasm.active) {
        printf("Flushing incomplete buffer...\n");
        flush_buffer();
    }

    printf("\nDetaching...\n");

    if (log_fd >= 0) {
        get_timestamp(timestamp, sizeof(timestamp));
        dprintf(log_fd, "\n[Ended at %s]\n", timestamp);
        dprintf(log_fd, "========================================\n\n");
    }

    opts_ingress.flags = opts_ingress.prog_fd = opts_ingress.prog_id = 0;
    bpf_tc_detach(&hook, &opts_ingress);

cleanup:
    if (log_fd >= 0) close(log_fd);
    if (slave_fd >= 0) close(slave_fd);
    ring_buffer__free(rb);
    mirror_bpf__destroy(skel);
    return err != 0;
}