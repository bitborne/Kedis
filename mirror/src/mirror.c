#include "mirror_log.h"
#include "mirror.skel.h"

#include <arpa/inet.h>
#include <bpf/bpf.h>
#include <bpf/libbpf.h>
#include <errno.h>
#include <fcntl.h>
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
  int slave_fd;  // [新增] 每个流独立的从节点连接
  bool incomplete_warned;  // [新增] 是否已经警告过不完整
};

static volatile bool exiting = false;
// static int log_fd = -1;
// [删除] 不再使用全局 slave_fd，改为每个流独立连接

// 从节点配置
static char target_ip[64];
static int target_port;

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
  new_buf->slave_fd = -1;  // [新增] 初始化为无效 fd

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

// [删除] print_drop_stats 函数暂时未使用

// [新增] 发送数据到从节点（使用流专属连接）
static void send_to_slave(struct reassembly_buffer* buf, const __u8* data, __u32 len) {
  if (buf->slave_fd < 0) return;

  ssize_t total = 0;
  while (total < len && !exiting) {
    ssize_t n = send(buf->slave_fd, data + total, len - total, MSG_NOSIGNAL);
    if (n < 0) {
      if (errno == EINTR || errno == EAGAIN) {
        usleep(1000);
        continue;
      }
      mirror_logError("发送失败 (fd=%d): %s", buf->slave_fd, strerror(errno));
      close(buf->slave_fd);
      buf->slave_fd = -1;
      return;
    }
    if (n == 0) {
      mirror_logError("从节点断开连接 (fd=%d)", buf->slave_fd);
      close(buf->slave_fd);
      buf->slave_fd = -1;
      return;
    }
    total += n;
  }
  total_sent += total;
}

// [新增] 为流建立从节点连接
static int connect_slave_for_flow(struct reassembly_buffer* buf) {
  int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    mirror_logError("创建 socket 失败: %s", strerror(errno));
    return -1;
  }

  // 禁用 Nagle 算法
  int flag = 1;
  setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));

  // 设置连接超时
  struct timeval tv = {.tv_sec = 3, .tv_usec = 0};
  setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

  struct sockaddr_in addr = {
      .sin_family = AF_INET,
      .sin_port = htons(target_port),
  };
  inet_pton(AF_INET, target_ip, &addr.sin_addr);

  if (connect(fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
    mirror_logError("连接从节点失败: %s", strerror(errno));
    close(fd);
    return -1;
  }

  mirror_logDebug("为流 %u -> %u 建立连接成功 (fd=%d)", 
                  buf->src_port, buf->dst_port, fd);
  return fd;
}

static void flush_buffer(struct reassembly_buffer *buf) {
  if (!buf || !buf->active || buf->received_len == 0) return;

  char src_str[16], dst_str[16];
  inet_ntop(AF_INET, &buf->src_ip, src_str, sizeof(src_str));
  inet_ntop(AF_INET, &buf->dst_ip, dst_str, sizeof(dst_str));

  // [修改] 完整性检查：不完整也尝试发送，但记录警告
  if (buf->received_len != buf->total_len) {
    if (!buf->incomplete_warned) {
      mirror_logWarn("[INCOMPLETE] 流 %s:%d -> %s:%d: 期望 %u, 收到 %u, 尝试发送部分数据",
                     src_str, buf->src_port, dst_str, buf->dst_port,
                     buf->total_len, buf->received_len);
      buf->incomplete_warned = true;
    }
    // 继续执行，尝试发送已收到的数据
  } else {
    mirror_logInfo("[Complete] %s:%d -> %s:%d, total: %u bytes", 
                   src_str, buf->src_port, dst_str, buf->dst_port, buf->received_len);
  }
  
  total_complete += buf->received_len;

  // [修改] 使用流专属的 slave_fd
  if (buf->slave_fd >= 0) {
    send_to_slave(buf, buf->data, buf->received_len);
  } else {
    mirror_logWarn("流 %s:%d -> %s:%d 没有有效的从节点连接，数据丢弃",
                   src_str, buf->src_port, dst_str, buf->dst_port);
  }

  // [修复] 保持长连接，不关闭 slave_fd
  // 连接将在流超时清理或程序退出时关闭

  // 重置缓冲区状态（但保留连接和流在哈希表中）
  buf->active = 0;
  buf->total_len = 0;
  buf->received_len = 0;
}

static int handle_event(void* ctx, void* data, size_t data_sz) {
  (void)ctx;  // 保留用于未来扩展
  struct packet_event *e = data;
  struct reassembly_buffer *buf = NULL;

  if (e->type == EVENT_HEADER) {
    // [逻辑] 收到 Header，强制查找或创建新流
    buf = get_or_create_flow(e->src_ip, e->dst_ip, e->src_port, e->dst_port, 1);
    if (!buf) return 0; // 内存不足

    // [逻辑] 如果该流已经处于 Active 状态（说明上一个请求没收完），先强制 Flush
    if (buf->active) {
        mirror_logWarn("流 %u -> %u 上一个请求未完成，强制 flush", 
                       buf->src_port, buf->dst_port);
        flush_buffer(buf); 
        // Flush 后 buf 可能被 free，需要重新获取
        buf = get_or_create_flow(e->src_ip, e->dst_ip, e->src_port, e->dst_port, 1);
    }

    // 初始化新缓冲区
    buf->active = 1;
    buf->total_len = e->payload_len;
    buf->received_len = 0;
    buf->incomplete_warned = false;
    
    if (buf->total_len > MAX_PAYLOAD) {
        mirror_logWarn("Payload too large (%u), truncating", buf->total_len);
        buf->total_len = MAX_PAYLOAD;
    }

    // [修复] 只在需要时建立连接（保持长连接）
    if (buf->slave_fd < 0) {
        buf->slave_fd = connect_slave_for_flow(buf);
        if (buf->slave_fd < 0) {
            mirror_logError("为流 %u -> %u 建立从节点连接失败", 
                           e->src_port, e->dst_port);
            buf->active = 0;
            return 0;
        }
    }

    mirror_logInfo("[Header] 流 %u -> %u 期望 %u 字节", 
                   e->src_port, e->dst_port, buf->total_len);

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

    mirror_logDebug("[Chunk] 流 %u -> %u offset=%u, len=%u, 已收 %u/%u",
                   buf->src_port, buf->dst_port, e->offset,
                   e->chunk_len, buf->received_len, buf->total_len);

    // 如果收齐了，立即发送
    if (buf->received_len >= buf->total_len) {
      flush_buffer(buf);
    }
  }

  return 0;
}

// [删除] connect_slave 函数已不再使用，改为使用 connect_slave_for_flow

static void cleanup_flows() { // 清理整个 table
    mirror_logInfo("Cleaning up remaining flows...");
    int cleaned = 0;
    for (int i = 0; i < HASH_SIZE; i++) {
        while (flow_table[i]) {
            struct reassembly_buffer *curr = flow_table[i];
            // 如果还有未发送的数据，尝试发送
            if (curr->active && curr->received_len > 0) {
                flush_buffer(curr);
            }
            // 关闭连接
            if (curr->slave_fd >= 0) {
                close(curr->slave_fd);
            }
            // 使用 remove_flow 正确移除并释放
            remove_flow(curr);
            cleaned++;
        }
    }
    mirror_logInfo("清理了 %d 个流", cleaned);
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

  // [修改] 保存配置，不再建立全局连接
  strncpy(target_ip, slave_ip, sizeof(target_ip) - 1);
  target_port = slave_port;
  mirror_logInfo("目标从节点: %s:%d", target_ip, target_port);

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
      ring_buffer__new(bpf_map__fd(skel->maps.rb), handle_event, skel, NULL);
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

  opts_ingress.flags = opts_ingress.prog_fd = opts_ingress.prog_id = 0;
  bpf_tc_detach(&hook, &opts_ingress);
  bpf_tc_hook_destroy(&hook);

cleanup:
  // [修改] 不再需要关闭全局 slave_fd，每个流有自己的连接
  ring_buffer__free(rb);
  mirror_bpf__destroy(skel);
  return err != 0;
}