#include <arpa/inet.h>
#include <bpf/bpf.h>
#include <bpf/libbpf.h>
#include <errno.h>
#include <fcntl.h>
#include <linux/if_link.h>
#include <net/if.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#include "xdp_mirror_common.h"
#include "mirror_log.h"
#include "xdp_mirror.skel.h"

#define HASH_SIZE 16384
#define MAX_PAYLOAD 131017
#define FLOW_TIMEOUT_SEC 60
#define RECONNECT_INTERVAL_SEC 5

// 每流的重组缓冲区
struct reassembly_buffer {
    struct reassembly_buffer* next;
    __u32 src_ip, dst_ip;
    __u16 src_port, dst_port;
    __u32 total_len;
    __u32 received_len;
    time_t last_update;
    int slave_fd;           // 该流专用的从节点 socket
    bool active;
    __u8 data[MAX_PAYLOAD];
};

static volatile bool exiting = false;
static struct reassembly_buffer* flow_table[HASH_SIZE] = {0};
static long long total_complete = 0;
static long long total_sent = 0;
static long long total_dropped = 0;

// 从节点配置
static char target_ip[64];
static int target_port;

static void sig_handler(int sig) {
    mirror_logInfo("收到信号 %d，正在准备安全退出...", sig);
    mirror_logInfo("统计: complete=%lld, sent=%lld, dropped=%lld",
                   total_complete, total_sent, total_dropped);
    exiting = true;
}

static unsigned int hash_5tuple(__u32 sip, __u32 dip, __u16 sport, __u16 dport) {
    return (sip ^ dip ^ sport ^ dport) % HASH_SIZE;
}

// 连接到从节点，返回 socket fd
static int connect_slave() {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        mirror_logError("创建 socket 失败: %s", strerror(errno));
        return -1;
    }

    // 禁用 Nagle 算法，确保数据立即发送
    int flag = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));

    // 设置非阻塞连接超时
    struct timeval tv = {.tv_sec = 3, .tv_usec = 0};
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    struct sockaddr_in addr = {
        .sin_family = AF_INET,
        .sin_port = htons(target_port),
    };
    inet_pton(AF_INET, target_ip, &addr.sin_addr);

    mirror_logInfo("正在连接从节点 %s:%d...", target_ip, target_port);
    if (connect(fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        mirror_logError("连接从节点失败: %s", strerror(errno));
        close(fd);
        return -1;
    }

    mirror_logInfo("成功连接到从节点");
    return fd;
}

// 发送数据到从节点
static void send_to_slave(int fd, const __u8* data, __u32 len,
                          __u16 src_port, __u16 dst_port) {
    if (fd < 0) return;

    ssize_t total = 0;
    while (total < len && !exiting) {
        ssize_t n = send(fd, data + total, len - total, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EINTR || errno == EAGAIN) {
                usleep(1000);
                continue;
            }
            mirror_logError("发送失败 (fd=%d, port=%d->%d): %s",
                           fd, src_port, dst_port, strerror(errno));
            close(fd);
            return;
        }
        if (n == 0) {
            mirror_logError("从节点断开连接 (fd=%d)", fd);
            close(fd);
            return;
        }
        total += n;
    }
    total_sent += total;
    mirror_logDebug("成功发送 %zd/%u 字节 (fd=%d, port=%d->%d)",
                   total, len, fd, src_port, dst_port);
}

// [修改] 刷新缓冲区：降级模式，不完整也尝试发送
static void flush_buffer(struct reassembly_buffer* buf) {
    if (!buf || !buf->active) return;

    char src_str[16], dst_str[16];
    inet_ntop(AF_INET, &buf->src_ip, src_str, sizeof(src_str));
    inet_ntop(AF_INET, &buf->dst_ip, dst_str, sizeof(dst_str));

    // [修改] 降级模式：不完整也尝试发送已收到的数据
    if (buf->received_len != buf->total_len) {
        mirror_logWarn("[INCOMPLETE] 流 %s:%d -> %s:%d 不完整: 期望 %u, 收到 %u, 尝试发送部分数据",
                      src_str, buf->src_port, dst_str, buf->dst_port,
                      buf->total_len, buf->received_len);
        total_dropped += (buf->total_len - buf->received_len);  // 只统计未收到的部分
    } else {
        mirror_logInfo("[COMPLETE] %s:%d -> %s:%d, 总长度 %u 字节",
                      src_str, buf->src_port, dst_str, buf->dst_port, buf->total_len);
    }
    
    total_complete += buf->received_len;

    // 发送数据（即使不完整也发送）
    if (buf->slave_fd >= 0 && buf->received_len > 0) {
        send_to_slave(buf->slave_fd, buf->data, buf->received_len,
                     buf->src_port, buf->dst_port);
    } else if (buf->slave_fd < 0) {
        mirror_logError("[ERROR] 流 %s:%d -> %s:%d 没有有效的从节点连接",
                       src_str, buf->src_port, dst_str, buf->dst_port);
    }

    // [修复] 保持长连接，不关闭 slave_fd
    // 连接将在流超时清理时关闭

    // 重置缓冲区状态（但保留连接）
    buf->active = false;
    buf->total_len = 0;
    buf->received_len = 0;
}

// 查找或创建流缓冲区
static struct reassembly_buffer* get_or_create_flow(
    __u32 sip, __u32 dip, __u16 sport, __u16 dport, int create) {

    unsigned int idx = hash_5tuple(sip, dip, sport, dport);
    struct reassembly_buffer* buf = flow_table[idx];

    // 遍历链表查找
    while (buf) {
        if (buf->src_ip == sip && buf->dst_ip == dip &&
            buf->src_port == sport && buf->dst_port == dport) {
            buf->last_update = time(NULL);
            return buf;
        }
        buf = buf->next;
    }

    if (!create) return NULL;

    // 创建新节点
    struct reassembly_buffer* new_buf = calloc(1, sizeof(struct reassembly_buffer));
    if (!new_buf) {
        mirror_logError("分配内存失败: %s", strerror(errno));
        return NULL;
    }

    new_buf->src_ip = sip;
    new_buf->dst_ip = dip;
    new_buf->src_port = sport;
    new_buf->dst_port = dport;
    new_buf->last_update = time(NULL);
    new_buf->slave_fd = -1;
    new_buf->active = false;

    // 为该流建立独立的从节点连接
    new_buf->slave_fd = connect_slave();
    if (new_buf->slave_fd < 0) {
        mirror_logWarn("为流 %u -> %u 建立从节点连接失败", sport, dport);
    } else {
        mirror_logInfo("为流 %u -> %u 建立从节点连接成功 (fd=%d)",
                      sport, dport, new_buf->slave_fd);
    }

    // 头插法插入哈希桶
    new_buf->next = flow_table[idx];
    flow_table[idx] = new_buf;

    return new_buf;
}

// 处理 ringbuf 事件
static int handle_event(void* ctx, void* data, size_t data_sz) {
    if (exiting) return -1;

    struct packet_event* e = data;

    // 日志：收到事件
    mirror_logDebug("收到事件: type=%u, %u.%u.%u.%u:%d -> %u.%u.%u.%u:%d",
                   e->type,
                   (e->src_ip >> 0) & 0xFF, (e->src_ip >> 8) & 0xFF,
                   (e->src_ip >> 16) & 0xFF, (e->src_ip >> 24) & 0xFF, e->src_port,
                   (e->dst_ip >> 0) & 0xFF, (e->dst_ip >> 8) & 0xFF,
                   (e->dst_ip >> 16) & 0xFF, (e->dst_ip >> 24) & 0xFF, e->dst_port);

    mirror_logDebug("\n==============\n%.*s\n==============\n", data_sz, e->data);
    
    if (e->type == EVENT_HEADER) {
        // 收到 Header：查找或创建流
        struct reassembly_buffer* buf = get_or_create_flow(
            e->src_ip, e->dst_ip, e->src_port, e->dst_port, 1);
        if (!buf) {
            mirror_logError("无法为流创建缓冲区");
            return 0;
        }

        // 如果该流还有未完成的数据，先强制 flush（或丢弃）
        if (buf->active) {
            mirror_logWarn("流 %u -> %u 上一个请求未完成，强制 flush",
                          buf->src_port, buf->dst_port);
            flush_buffer(buf);
        }

        // 初始化新请求
        buf->active = true;
        buf->total_len = e->payload_len;
        buf->received_len = 0;
        buf->last_update = time(NULL);

        if (buf->total_len > MAX_PAYLOAD) {
            mirror_logWarn("Payload 过大 (%u > %d)，截断",
                          buf->total_len, MAX_PAYLOAD);
            buf->total_len = MAX_PAYLOAD;
        }

        // 检查从节点连接
        if (buf->slave_fd < 0) {
            mirror_logWarn("尝试为流 %u -> %u 重新建立从节点连接",
                          buf->src_port, buf->dst_port);
            buf->slave_fd = connect_slave();
        }

        mirror_logInfo("[HEADER] 流 %u -> %u 期望 %u 字节",
                      buf->src_port, buf->dst_port, buf->total_len);

    } else if (e->type == EVENT_DATA) {
        // 收到 Data：只查找，不创建
        struct reassembly_buffer* buf = get_or_create_flow(
            e->src_ip, e->dst_ip, e->src_port, e->dst_port, 0);

        if (!buf || !buf->active) {
            mirror_logWarn("[DATA] 没有找到对应的 Header，丢弃 (offset=%u, len=%u)",
                          e->offset, e->chunk_len);
            return 0;
        }

        // 边界检查
        if (e->offset + e->chunk_len > MAX_PAYLOAD) {
            mirror_logError("[DATA] offset %u + len %u 超出边界",
                           e->offset, e->chunk_len);
            return 0;
        }

        // 拷贝数据到正确位置
        memcpy(buf->data + e->offset, e->data, e->chunk_len);
        buf->received_len += e->chunk_len;
        buf->last_update = time(NULL);

        mirror_logDebug("[CHUNK] 流 %u -> %u offset=%u, len=%u, 已收 %u/%u",
                       buf->src_port, buf->dst_port, e->offset, e->chunk_len,
                       buf->received_len, buf->total_len);

        // 检查是否收齐
        if (buf->received_len >= buf->total_len) {
            if (buf->received_len > buf->total_len) {
                mirror_logWarn("[WARN] 流 %u -> %u 收到数据超过期望: %u > %u",
                              buf->src_port, buf->dst_port,
                              buf->received_len, buf->total_len);
            }
            flush_buffer(buf);
        }
    }

    return 0;
}

// 清理过期流
static void cleanup_expired_flows() {
    time_t now = time(NULL);
    int cleaned = 0;

    for (int i = 0; i < HASH_SIZE; i++) {
        struct reassembly_buffer** curr = &flow_table[i];
        while (*curr) {
            struct reassembly_buffer* entry = *curr;

            // 超时或程序退出时清理
            if ((entry->active && now - entry->last_update > FLOW_TIMEOUT_SEC) || exiting) {
                if (!exiting) {
                    mirror_logInfo("清理超时流 %u -> %u (活跃 %ld 秒)",
                                  entry->src_port, entry->dst_port,
                                  now - entry->last_update);
                }

                // 如果是活跃流，尝试 flush（可能不完整会被丢弃）
                if (entry->active && entry->received_len > 0) {
                    flush_buffer(entry);
                }

                // 优雅关闭 socket
                if (entry->slave_fd >= 0) {
                    shutdown(entry->slave_fd, SHUT_WR);
                    usleep(10000);  // 等待 10ms 让数据发送
                    close(entry->slave_fd);
                }

                // 从链表移除
                *curr = entry->next;
                free(entry);
                cleaned++;
            } else {
                curr = &entry->next;
            }
        }
    }

    if (cleaned > 0) {
        mirror_logInfo("清理了 %d 个流", cleaned);
    }
}

// [新增] 读取 BPF 丢包统计
static void print_drop_stats(struct xdp_mirror_bpf* skel) {
    int drop_stats_fd = bpf_map__fd(skel->maps.drop_stats);
    if (drop_stats_fd < 0) return;
    
    __u32 key;
    __u64 count;
    
    key = 0;  // data chunk 丢包
    if (bpf_map_lookup_elem(drop_stats_fd, &key, &count) == 0 && count > 0) {
        mirror_logWarn("[DROP STATS] Data chunks dropped: %llu", count);
    }
    
    key = 1;  // header 丢包
    if (bpf_map_lookup_elem(drop_stats_fd, &key, &count) == 0 && count > 0) {
        mirror_logWarn("[DROP STATS] Headers dropped: %llu", count);
    }
}

// 统计打印
static void print_stats(struct xdp_mirror_bpf* skel) {
    mirror_logInfo("[STATS] 完成: %lld, 发送: %lld, 丢弃: %lld bytes",
                  total_complete, total_sent, total_dropped);
    print_drop_stats(skel);
}

int main(int argc, char** argv) {
    if (argc != 4) {
        fprintf(stderr, "Usage: %s <ifname> <slave_ip> <slave_port>\n", argv[0]);
        return 1;
    }

    const char* ifname = argv[1];
    strncpy(target_ip, argv[2], sizeof(target_ip) - 1);
    target_port = atoi(argv[3]);

    mirror_logInfo("========================================");
    mirror_logInfo("XDP Mirror 启动");
    mirror_logInfo("网卡: %s", ifname);
    mirror_logInfo("从节点: %s:%d", target_ip, target_port);
    mirror_logInfo("模式: 降级模式 (不完整也发送)");
    mirror_logInfo("Socket: 每五元组独立连接");
    mirror_logInfo("========================================");

    signal(SIGINT, sig_handler);
    signal(SIGTERM, sig_handler);

    // 加载 BPF
    struct xdp_mirror_bpf* skel = xdp_mirror_bpf__open_and_load();
    if (!skel) {
        mirror_logError("加载 BPF 程序失败");
        return 1;
    }

    int ifindex = if_nametoindex(ifname);
    if (ifindex == 0) {
        mirror_logError("无效网卡: %s", ifname);
        return 1;
    }

    // 附加 XDP 程序
    int err = bpf_xdp_attach(ifindex,
                             bpf_program__fd(skel->progs.xdp_mirror_forward),
                             XDP_FLAGS_SKB_MODE, NULL);
    if (err) {
        mirror_logError("附加 XDP 程序失败: %d", err);
        return 1;
    }
    mirror_logInfo("XDP 程序已附加到 %s", ifname);

    // 创建 ring buffer
    struct ring_buffer* rb = ring_buffer__new(
        bpf_map__fd(skel->maps.rb), handle_event, NULL, NULL);
    if (!rb) {
        mirror_logError("创建 ring buffer 失败");
        goto cleanup;
    }

    mirror_logInfo("开始监听...");

    time_t last_cleanup = time(NULL);
    time_t last_stats = time(NULL);

    while (!exiting) {
        err = ring_buffer__poll(rb, 100);
        if (err < 0 && err != -EINTR) {
            mirror_logError("ring_buffer 轮询错误: %d", err);
            break;
        }

        time_t now = time(NULL);

        // 每 5 秒清理过期流
        if (now - last_cleanup > 5) {
            cleanup_expired_flows();
            last_cleanup = now;
        }

        // 每 10 秒打印统计
        if (now - last_stats > 10) {
            print_stats(skel);
            last_stats = now;
        }
    }

cleanup:
    mirror_logInfo("正在清理资源...");

    // 分离 XDP
    bpf_xdp_detach(ifindex, XDP_FLAGS_SKB_MODE, NULL);

    // 清理所有流
    exiting = true;
    cleanup_expired_flows();

    ring_buffer__free(rb);
    
    mirror_logInfo("已安全退出");
    // [修改] 在销毁 skel 之前打印最终统计
    print_drop_stats(skel);
    print_stats(skel);
    
    xdp_mirror_bpf__destroy(skel);

    return 0;
}