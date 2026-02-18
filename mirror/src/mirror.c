#include "mirror_log.h"
#include "mirror.skel.h"

#include <arpa/inet.h>
#include <bpf/bpf.h>
#include <bpf/libbpf.h>
#include <errno.h>
#include <fcntl.h>
#include <net/if.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include "mirror_common.h"

// #define SLAVE_IP "172.20.10.2"
// #define SLAVE_PORT 9999
// #define CHUNK_SIZE 1024

//  定义哈希表大小，建议为 2 的幂次方
#define HASH_SIZE 16384
#define MAX_PAYLOAD 131017

long long total_complete = 0;
long long total_sent = 0;

// 重组缓冲区
struct reassembly_buffer {
  struct reassembly_buffer* next;  // [新增] 链表指针
  __u32 src_ip;
  __u32 dst_ip;
  __u16 src_port;
  __u16 dst_port;
  __u32 total_len;
  __u32 received_len;
  // [新增] 用于超时清理（虽然本例暂未实现清理线程，但这是必须的）
  time_t last_update;
  __u8 data[MAX_PAYLOAD];
  int active;
};

static volatile bool exiting = false;
// static int log_fd = -1;
static int slave_fd = -1;

// static struct reassembly_buffer reasm = {0};
// 全局 Hash 表
static struct reassembly_buffer* flow_table[HASH_SIZE] = {0};

// [新增] 简单的哈希函数：计算五元组 Hash
static unsigned int hash_5tuple(__u32 sip, __u32 dip, __u16 sport, __u16 dport) {
  // 简单的异或混合，生产环境可以使用 Jenkins Hash 或 MurmurHash
  return (sip ^ dip ^ sport ^ dport) % HASH_SIZE;
}

// [新增] 查找或创建流缓冲区的辅助函数
static struct reassembly_buffer* get_or_create_flow(__u32 sip, __u32 dip, __u16 sport, __u16 dport, int create) {
  unsigned int idx = hash_5tuple(sip, dip, sport, dport);
  struct reassembly_buffer* buf = flow_table[idx];

  // 遍历链表查找
  while (buf) {
    if (buf->src_ip == sip && buf->dst_ip == dip && buf->src_port == sport &&
        buf->dst_port == dport) {
      buf->last_update = time(NULL);
      return buf;
    }
    buf = buf->next;
  }

  // 未找到，且不允许创建（用于 Data 包查找）
  if (!create) return NULL;

  // 创建新节点
  struct reassembly_buffer* new_buf = calloc(1, sizeof(struct reassembly_buffer));
  if (!new_buf) {
    perror("calloc");
    return NULL;
  }

  new_buf->src_ip = sip;
  new_buf->dst_ip = dip;
  new_buf->src_port = sport;
  new_buf->dst_port = dport;
  new_buf->last_update = time(NULL);

  // 头插法插入哈希桶
  new_buf->next = flow_table[idx];
  flow_table[idx] = new_buf;

  return new_buf;
}

// [新增] 从哈希表中移除并释放流
static void remove_flow(struct reassembly_buffer* target) {
  if (!target) return;

  unsigned int idx = hash_5tuple(target->src_ip, target->dst_ip, target->src_port, target->dst_port);
  struct reassembly_buffer* curr = flow_table[idx];
  struct reassembly_buffer* prev = NULL;

  while (curr) {
    if (curr == target) {
      if (prev)
        prev->next = curr->next;
      else
        flow_table[idx] = curr->next;

      free(curr);
      return;
    }
    prev = curr;
    curr = curr->next;
  }
}

static void get_timestamp(char* buf, size_t len) {
  time_t now = time(NULL);
  struct tm* tm_info = localtime(&now);
  strftime(buf, len, "%Y-%m-%d %H:%M:%S", tm_info);
}

static void flush_buffer(struct reassembly_buffer *buf) {
  if (!buf || !buf->active || buf->received_len == 0) return;


  // [新增] 完整性检查
    // 如果收到的长度 不等于 头部声明的总长度
    // 说明中间有丢包，或者包被截断了
  if (buf->received_len != buf->total_len) {
    // 记录丢包日志（仅在调试时打开，生产环境可计数）
    // fprintf(stderr, "[Drop] Flow incomplete: expected %u, got %u. Dropping to keep connection alive.\n", 
    //         buf->total_len, buf->received_len);

    // 既然这个包坏了，就别发了，直接从哈希表移除，重置状态
    // 这样虽然丢失了这一个 SET 命令，但 TCP 连接还活着，后续的命令还能发
    remove_flow(buf);
    return;
  }

  
  char src_str[16], dst_str[16];
  inet_ntop(AF_INET, &buf->src_ip, src_str, sizeof(src_str));
  inet_ntop(AF_INET, &buf->dst_ip, dst_str, sizeof(dst_str));
  mirror_logInfo("[Complete] %s:%d -> %s:%d, total: %u bytes", src_str, buf->src_port, dst_str, buf->dst_port, buf->received_len);
  total_complete += buf->received_len;

//   // 写入日志
//   if (log_fd >= 0) {
//     // [优化] 增加 dprintf 的错误检查
//     dprintf(log_fd, "--- %s:%d -> %s:%d (len=%u) ---\n",
//             src_str, buf->src_port, dst_str, buf->dst_port, buf->received_len);
//     write(log_fd, buf->data, buf->received_len);
//     write(log_fd, "\n", 1);
//   }

  // 发送到从节点
  if (slave_fd >= 0) {
    ssize_t sent = 0;
    while (sent < buf->received_len) {
      ssize_t n =
          send(slave_fd, buf->data + sent, buf->received_len - sent, 0);
      if (n < 0) {
        if (errno == EAGAIN) continue;
        perror("send");
        close(slave_fd);
        slave_fd = -1;
        break;
      } else if (n == 0) {
        mirror_logError("Client disconnect");
        close(slave_fd);
        slave_fd = -1;
        break;
      }
      sent += n;
    }
    total_sent += sent;
    mirror_logInfo("Forwarded %zd bytes to slave\n", sent);
  }

  remove_flow(buf);
}

static int handle_event(void* ctx, void* data, size_t data_sz) {
  struct packet_event *e = data;
  struct reassembly_buffer *buf = NULL;

  buf = get_or_create_flow(e->src_ip, e->dst_ip, e->src_port, e->dst_port, 1);
  if (e->type == EVENT_HEADER) {
    // [逻辑] 收到 Header，强制查找或创建新流
    buf = get_or_create_flow(e->src_ip, e->dst_ip, e->src_port, e->dst_port, 1);
    if (!buf) return 0; // 内存不足

    // [逻辑] 如果该流已经处于 Active 状态（说明上一个请求没收完），先强制 Flush
    if (buf->active) {
        mirror_logError("Incomplete packet for flow, flushing old data...");
        flush_buffer(buf); 
        // Flush 后 buf 可能被 free，需要重新获取
        buf = get_or_create_flow(e->src_ip, e->dst_ip, e->src_port, e->dst_port, 1);
    }

    // 初始化新缓冲区(ip port已经在创建时初始化了)
    buf->active = 1;
    buf->total_len = e->payload_len;
    buf->received_len = 0;
    // mirror_logDebug("初始化: buf->total_len == payload_len: %u", buf->total_len);
    if (buf->total_len > MAX_PAYLOAD) {
        mirror_logWarn("Payload too large (%u), truncating", buf->total_len);
        buf->total_len = MAX_PAYLOAD;
    }

    mirror_logInfo("[Header] Expecting %u bytes", buf->total_len);

  } else if (e->type == EVENT_DATA) {
      
      // [逻辑] 收到 Data，只查找，不创建。找不到 Header 就丢弃。
    buf = get_or_create_flow(e->src_ip, e->dst_ip, e->src_port, e->dst_port, 0);
    if (!buf || !buf->active) {
      mirror_logWarn("Data without header, dropping");
      return 0;
    }
      
    if (e->offset + e->chunk_len > MAX_PAYLOAD) {
      mirror_logWarn("Chunk offset %u out of bounds", e->offset);
      return 0;
    }

    // 拷贝分片到正确位置
    memcpy(buf->data + e->offset, e->data, e->chunk_len);
    buf->received_len += e->chunk_len;

    mirror_logDebug("[Chunk] offset=%u, len=%u, total_received=%u", e->offset,
                 e->chunk_len, buf->received_len);

    // 如果收齐了，立即发送
    if (buf->received_len >= buf->total_len) {
      flush_buffer(buf);
    }
  }

  return 0;
}

static int connect_slave(const char* slave_ip, uint16_t slave_port) {
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

  mirror_logInfo("Connecting to slave %s:%d...", slave_ip, slave_port);
  if (connect(fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
    perror("connect");
    close(fd);
    return -1;
  }
  mirror_logInfo("Connected to slave!\n");
  return fd;
}

static void cleanup_flows() { // 清理整个 table
    mirror_logInfo("Cleaning up remaining flows...");
    for (int i = 0; i < HASH_SIZE; i++) {
        struct reassembly_buffer *curr = flow_table[i];
        while (curr) {
            struct reassembly_buffer *next = curr->next;
            if (curr->active && curr->received_len > 0) {
                flush_buffer(curr); // 尝试最后冲刷一次
            } else {
                free(curr);
            }
            curr = next;
        }
        flow_table[i] = NULL;
    }
}

static void sig_handler(int sig) {
  debug("total_complete: %lld\n", total_complete);
  debug("total_sent: %lld\n", total_sent);
  fprintf(stderr, "收到信号 %d，正在退出...\n", sig);
  exiting = true;
}

int main(int argc, char** argv) {
  struct mirror_bpf* skel;
  int err;
  int ifindex;
  LIBBPF_OPTS(bpf_tc_hook, hook);
  LIBBPF_OPTS(bpf_tc_opts, opts_ingress);

  if (argc != 4) {
    mirror_logError("Usage: %s <ifname> <slave_ip> <slave_port>", argv[0]);
    return 1;
  }

  const char* slave_ip = argv[2];
  uint16_t slave_port = atoi(argv[3]);

  ifindex = if_nametoindex(argv[1]);
  if (ifindex == 0) {
    mirror_logError("Invalid interface: %s", argv[1]);
    return 1;
  }

  signal(SIGINT, sig_handler);
  signal(SIGTERM, sig_handler);

  slave_fd = connect_slave(slave_ip, slave_port);
  if (slave_fd < 0) {
    mirror_logError("Failed to connect to slave\n");
    return 1;
  }

  skel = mirror_bpf__open_and_load();
  if (!skel) {
    mirror_logError("Failed to open/load BPF skeleton");
    return 1;
  }

//   log_fd = open(LOG_FILE, O_WRONLY | O_CREAT | O_APPEND, 0644);
//   if (log_fd < 0) {
    // mirror_logError("Failed to open %s: %s\n", LOG_FILE, strerror(errno));
//   }

  char timestamp[64];
  get_timestamp(timestamp, sizeof(timestamp));
//   if (log_fd >= 0) {
//     dprintf(log_fd, "\n========================================\n");
//     dprintf(log_fd, "  Test Run: %s\n", timestamp);
//     dprintf(log_fd, "========================================\n\n");
//   }
  mirror_logInfo("========================================");
  mirror_logInfo("  Test Run: %s", timestamp);
  mirror_logInfo("========================================");

  struct ring_buffer* rb =
      ring_buffer__new(bpf_map__fd(skel->maps.rb), handle_event, NULL, NULL);
  if (!rb) {
    mirror_logError("Failed to create ring buffer");
    err = -1;
    goto cleanup;
  }

  hook.ifindex = ifindex;
  hook.attach_point = BPF_TC_INGRESS;
  err = bpf_tc_hook_create(&hook);
  if (err && err != -EEXIST) {
    mirror_logError("Failed to create TC hook: %d", err);
    goto cleanup;
  }

  opts_ingress.prog_fd = bpf_program__fd(skel->progs.mirror_forward);
  opts_ingress.flags = BPF_TC_F_REPLACE; // 强制替换现有的程序，不要因为“位置被占了”而报错
  err = bpf_tc_attach(&hook, &opts_ingress);
  if (err) {
    mirror_logError("Failed to attach TC: %d", err);
    goto cleanup;
  }

  fprintf(stderr, "Running on %s. Press Ctrl+C to exit.\n", argv[1]);

  while (!exiting) {
    err = ring_buffer__poll(rb, 100);
    if (err < 0 && err != -EINTR) {
      mirror_logError("Error polling: %d", err);
      break;
    }
  }

  // 退出前刷新(送走)并释放剩余数据
  cleanup_flows();
  fprintf(stderr, "\nDetaching...\n");

//   if (log_fd >= 0) {
//     get_timestamp(timestamp, sizeof(timestamp));
//     dprintf(log_fd, "\n[Ended at %s]\n", timestamp);
//     dprintf(log_fd, "========================================\n\n");
//   }

  opts_ingress.flags = opts_ingress.prog_fd = opts_ingress.prog_id = 0;
  bpf_tc_detach(&hook, &opts_ingress);
  bpf_tc_hook_destroy(&hook);

cleanup:
//   if (log_fd >= 0) close(log_fd);
  if (slave_fd >= 0) close(slave_fd);
  ring_buffer__free(rb);
  mirror_bpf__destroy(skel);
  return err != 0;
}