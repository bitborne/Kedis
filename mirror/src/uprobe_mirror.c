#include <arpa/inet.h>
#include <bpf/bpf.h>
#include <bpf/libbpf.h>
#include <errno.h>
#include <fcntl.h>
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

#include "uprobe_mirror_common.h"
#include "mirror_log.h"
#include "uprobe_mirror.skel.h"

// 动态获取函数地址
#include <elf.h>
#include <fcntl.h>
#include <sys/stat.h>

#define FD_TABLE_SIZE 1024     // fd 哈希表大小
#define FLOW_TIMEOUT_SEC 300   // 流超时时间（秒）- 增加到5分钟

// fd -> 从节点连接的映射
struct fd_flow {
    int fd;              // 原始连接的 fd（作为 key）
    int slave_fd;        // 到从节点的 socket
    time_t last_active;  // 最后活跃时间
    bool active;         // 是否活跃
    struct fd_flow *next; // 链表指针
};

static volatile bool exiting = false;
static struct fd_flow *fd_table[FD_TABLE_SIZE] = {0};

// 统计
static long long total_events = 0;
static long long total_bytes = 0;
static long long total_sent = 0;
static long long total_dropped = 0;

// 从节点配置
static char target_ip[64];
static int target_port;

static void sig_handler(int sig) {
    mirror_logInfo("收到信号 %d，正在准备安全退出...", sig);
    mirror_logInfo("统计: events=%lld, bytes=%lld, sent=%lld, dropped=%lld",
                   total_events, total_bytes, total_sent, total_dropped);
    exiting = true;
}

/**
 * 从 ELF 文件中获取符号地址
 * 简单实现：通过读取 nm 命令输出获取地址
 */
static size_t get_symbol_offset(const char* path, const char* symbol) {
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "nm '%s' 2>/dev/null | grep ' %s$' | head -1", path, symbol);
    
    FILE* fp = popen(cmd, "r");
    if (!fp) {
        mirror_logError("无法执行 nm 命令");
        return 0;
    }
    
    char line[256];
    size_t addr = 0;
    if (fgets(line, sizeof(line), fp)) {
        // 格式: 0000000000006910 T kvs_resp_feed
        sscanf(line, "%zx", &addr);
    }
    
    pclose(fp);
    return addr;
}

static unsigned int hash_fd(int fd) {
    return ((unsigned int)fd) % FD_TABLE_SIZE;
}

// 连接到从节点
static int connect_slave() {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        mirror_logError("创建 socket 失败: %s", strerror(errno));
        return -1;
    }

    int flag = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));

    struct timeval tv = {.tv_sec = 3, .tv_usec = 0};
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    struct sockaddr_in addr = {
        .sin_family = AF_INET,
        .sin_port = htons(target_port),
    };
    inet_pton(AF_INET, target_ip, &addr.sin_addr);

    mirror_logDebug("正在连接从节点 %s:%d...", target_ip, target_port);
    if (connect(fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        mirror_logError("连接从节点失败: %s", strerror(errno));
        close(fd);
        return -1;
    }

    mirror_logDebug("成功连接到从节点 (fd=%d)", fd);
    return fd;
}

// 发送数据到从节点
static int send_to_slave(int slave_fd, const __u8* data, __u32 len) {
    if (slave_fd < 0) return -1;

    ssize_t total = 0;
    while (total < len) {
        ssize_t n = send(slave_fd, data + total, len - total, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EINTR || errno == EAGAIN) {
                usleep(1000);
                continue;
            }
            mirror_logError("发送失败 (fd=%d): %s", slave_fd, strerror(errno));
            return -1;
        }
        if (n == 0) {
            mirror_logError("从节点断开连接 (fd=%d)", slave_fd);
            return -1;
        }
        total += n;
    }
    return 0;
}

// 查找或创建 fd 对应的流
static struct fd_flow* find_or_create_flow(int fd) {
    unsigned int idx = hash_fd(fd);
    struct fd_flow *flow = fd_table[idx];
    
    // 查找现有流
    while (flow) {
        if (flow->fd == fd) {
            return flow;
        }
        flow = flow->next;
    }
    
    // 创建新流
    flow = calloc(1, sizeof(struct fd_flow));
    if (!flow) {
        mirror_logError("分配内存失败");
        return NULL;
    }
    
    flow->fd = fd;
    flow->slave_fd = -1;
    flow->active = false;
    flow->last_active = time(NULL);
    
    // 插入哈希表（头插法）
    flow->next = fd_table[idx];
    fd_table[idx] = flow;
    
    mirror_logInfo("[NEW FLOW] fd=%d", fd);
    
    return flow;
}

// 关闭流的连接
static void close_flow(struct fd_flow *flow) {
    if (!flow) return;
    
    if (flow->slave_fd >= 0) {
        close(flow->slave_fd);
        flow->slave_fd = -1;
    }
    flow->active = false;
    
    mirror_logInfo("[CLOSE FLOW] fd=%d", flow->fd);
}

// 处理 ringbuf 事件
static int handle_event(void* ctx, void* data, size_t data_sz) {
    (void)ctx;
    
    struct uprobe_event* e = data;
    
    // 计算有效数据长度（BPF 程序已经只复制了新数据，从 parse_done 开始）
    __u32 valid_len = e->rlen - e->parse_done;
    
    if (valid_len == 0 || valid_len > IOP_SIZE) {
        return 0;  // 没有新数据或数据异常
    }
    
    total_events++;
    total_bytes += valid_len;
    
    // 查找或创建流
    struct fd_flow *flow = find_or_create_flow(e->fd);
    if (!flow) {
        total_dropped += valid_len;
        return 0;
    }
    
    // 如果是新流或连接已断开，建立到从节点的连接
    if (!flow->active || flow->slave_fd < 0) {
        flow->slave_fd = connect_slave();
        if (flow->slave_fd < 0) {
            mirror_logError("无法为 fd=%d 建立从节点连接", e->fd);
            total_dropped += valid_len;
            return 0;
        }
        flow->active = true;
        mirror_logInfo("[FLOW ACTIVE] fd=%d -> slave_fd=%d", e->fd, flow->slave_fd);
    }
    
    // 【修复】BPF 程序已经只复制了新数据到 e->data，直接使用 e->data 而不需要再跳过 parse_done
    if (send_to_slave(flow->slave_fd, e->data, valid_len) < 0) {
        // 发送失败，关闭连接，下次重试
        close(flow->slave_fd);
        flow->slave_fd = -1;
        flow->active = false;
        total_dropped += valid_len;
        return 0;
    }
    
    total_sent += valid_len;
    flow->last_active = time(NULL);
    
    mirror_logDebug("[FORWARD] fd=%d, len=%u (rlen=%u, parse_done=%u), total_sent=%lld", 
                    e->fd, valid_len, e->rlen, e->parse_done, total_sent);
    
    return 0;
}

// 清理超时流
static void cleanup_expired_flows() {
    time_t now = time(NULL);
    int cleaned = 0;
    
    for (int i = 0; i < FD_TABLE_SIZE; i++) {
        struct fd_flow **curr = &fd_table[i];
        while (*curr) {
            struct fd_flow *entry = *curr;
            
            if (now - entry->last_active > FLOW_TIMEOUT_SEC) {
                // 超时，关闭并移除
                close_flow(entry);
                *curr = entry->next;
                free(entry);
                cleaned++;
            } else {
                curr = &entry->next;
            }
        }
    }
    
    if (cleaned > 0) {
        mirror_logInfo("清理了 %d 个超时流", cleaned);
    }
}

// 关闭所有流
static void cleanup_all_flows() {
    for (int i = 0; i < FD_TABLE_SIZE; i++) {
        while (fd_table[i]) {
            struct fd_flow *entry = fd_table[i];
            close_flow(entry);
            fd_table[i] = entry->next;
            free(entry);
        }
    }
}

// 读取调试统计
static void print_debug_stats(struct uprobe_mirror_bpf* skel) {
    int debug_fd = bpf_map__fd(skel->maps.debug_stats);
    if (debug_fd < 0) return;
    
    __u32 key;
    __u64 count;
    const char *labels[] = {
        "总调用", "c=NULL", "读fd失败", "fd<0", 
        "读rlen失败", "读parse_done失败", "rlen无效", "ringbuf满", "成功"
    };
    
    for (int i = 0; i < 9; i++) {
        key = i;
        if (bpf_map_lookup_elem(debug_fd, &key, &count) == 0 && count > 0) {
            mirror_logInfo("[DEBUG] %s: %llu", labels[i], count);
        }
    }
}

// 统计打印
static void print_stats(struct uprobe_mirror_bpf* skel) {
    mirror_logInfo("[STATS] events=%lld, bytes=%lld, sent=%lld, dropped=%lld",
                  total_events, total_bytes, total_sent, total_dropped);
    print_debug_stats(skel);
}

int main(int argc, char** argv) {
    if (argc != 5) {
        fprintf(stderr, "Usage: %s <kvstore_path> <pid> <slave_ip> <slave_port>\n", argv[0]);
        fprintf(stderr, "Example: %s ../kvstore 12345 127.0.0.1 9999\n", argv[0]);
        fprintf(stderr, "Use 'pidof kvstore' to get PID\n");
        return 1;
    }

    const char *kvstore_path = argv[1];
    pid_t target_pid = atoi(argv[2]);
    strncpy(target_ip, argv[3], sizeof(target_ip) - 1);
    target_port = atoi(argv[4]);

    // 检查 kvstore 文件是否存在
    if (access(kvstore_path, X_OK) != 0) {
        fprintf(stderr, "Error: Cannot access %s: %s\n", kvstore_path, strerror(errno));
        return 1;
    }

    // 检查 PID 是否有效
    if (target_pid <= 0) {
        fprintf(stderr, "Error: Invalid PID %d\n", target_pid);
        return 1;
    }
    if (kill(target_pid, 0) != 0) {
        fprintf(stderr, "Error: Process PID=%d does not exist\n", target_pid);
        return 1;
    }

    mirror_logInfo("========================================");
    mirror_logInfo("Uprobe Mirror 启动");
    mirror_logInfo("目标程序: %s", kvstore_path);
    mirror_logInfo("目标进程: PID=%d", target_pid);
    mirror_logInfo("从节点: %s:%d", target_ip, target_port);
    mirror_logInfo("探测点: kvs_resp_feed (uprobe)");
    mirror_logInfo("流管理: 按 fd 区分，超时 %d 秒 (5分钟)", FLOW_TIMEOUT_SEC);
    mirror_logInfo("========================================");

    signal(SIGINT, sig_handler);
    signal(SIGTERM, sig_handler);

    // 动态获取 kvs_resp_feed 符号地址
    size_t func_offset = get_symbol_offset(kvstore_path, "kvs_resp_feed");
    if (func_offset == 0) {
        mirror_logError("无法获取 %s 的地址，使用默认地址 0x6910", "kvs_resp_feed");
        func_offset = 0x6910;  // 后备默认值
    }
    
    mirror_logInfo("函数 %s 的偏移量: 0x%lx", "kvs_resp_feed", func_offset);

    // 加载 BPF
    struct uprobe_mirror_bpf* skel = uprobe_mirror_bpf__open_and_load();
    if (!skel) {
        mirror_logError("加载 BPF 程序失败");
        return 1;
    }
    
    mirror_logInfo("尝试附加 uprobe: %s + 0x%lx", kvstore_path, func_offset);
    
    // 附加 uprobe 到指定 PID
    struct bpf_link *link = bpf_program__attach_uprobe(
        skel->progs.uprobe_kvs_resp_feed,
        false,        // retprobe = false (entry)
        target_pid,   // 指定 PID
        kvstore_path,
        func_offset   // 函数偏移量
    );
    if (!link) {
        mirror_logError("附加 uprobe kvs_resp_feed 失败");
        goto cleanup;
    }

    mirror_logInfo("uprobe 已附加到 PID=%d:%s:kvs_resp_feed (offset=0x%lx)", 
                   target_pid, kvstore_path, func_offset);

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
        int err = ring_buffer__poll(rb, 100);
        if (err < 0 && err != -EINTR) {
            mirror_logError("ring_buffer 轮询错误: %d", err);
            break;
        }

        time_t now = time(NULL);

        // 每 5 秒清理超时流
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

    cleanup_all_flows();
    ring_buffer__free(rb);
    bpf_link__destroy(link);
    
    mirror_logInfo("已安全退出");
    print_debug_stats(skel);
    print_stats(skel);
    
    uprobe_mirror_bpf__destroy(skel);

    return 0;
}
