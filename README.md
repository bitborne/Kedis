# KVstore: 高性能 Linux 内核增强型存储系统

[![license](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Platform](https://img.shields.io/badge/platform-Linux-orange.svg)]()
[![Build Status](https://img.shields.io/badge/build-passing-brightgreen.svg)]()

KVstore 是一个面向极致性能设计、基于 **io_uring** 异步 I/O 框架、支持**多存储引擎**、**内核级主从同步**及 **RDMA 硬件加速**的高可靠 Key-Value 存储系统。

本项目深度集成 Linux 内核增强技术（io_uring, eBPF, mmap），旨在构建一个能够支撑百万级并发、亚毫秒级延迟、且具备非侵入式数据同步能力的高性能存储基座。对于志在深入研究高性能服务器架构、内核驱动开发及分布式系统的工程师，本项目提供了一个从用户态到内核态的全栈优化范本。

---

## 目录

- [引言](#引言)
  - [核心架构亮点](#核心架构亮点)
  - [核心组件说明](#核心组件说明)
- [功能特性](#功能特性)
- [架构设计](#架构设计)
  - [网络模型：基于 io_uring 的 Proactor](#网络模型基于-io_uring-的-proactor)
  - [同步体系：eBPF 实时增量 + RDMA 存量全量](#同步体系ebpf-实时增量--rdma-存量全量)
  - [存储引擎：渐进式 Rehash 与多索引结构](#存储引擎渐进式-rehash-与多索引结构)
- [Q&A](#Q&A)
- [快速开始](#快速开始)
  - [环境要求](#环境要求)
  - [编译构建](#编译构建)
  - [运行指南](#运行指南)
- [性能测试](#性能测试)
- [发展路线](#发展路线-roadmap)
- [贡献](#贡献)
- [支持](#支持)
- [许可证](#许可证)

---

## 项目介绍

KVstore 旨在打破传统存储系统在海量并发下的 I/O 瓶颈。通过基于 io_uring 的网络框架减少系统调用开销，也支持多网络框架的跨平台设计，利用 eBPF (XDP/TC) 实现旁路数据镜像，并规划引入 RDMA 实现零拷贝的主从节点存量同步。

### 核心架构亮点

1.  **全异步 I/O 栈**：基于 `io_uring` 实现了纯异步的 Proactor 网络模型，彻底解决了传统 `epoll` 在极高并发下由于频繁上下文切换导致的性能衰减。
2.  **内核旁路同步 (eBPF Mirror)**：在不修改应用层逻辑的前提下，利用 `eBPF` 在内核协议栈直接拦截 RESP 流量，实现对应用无感、极低开销的主从增量同步。
3.  **异构同步双机制**：创新性地设计了“实时增量（eBPF）+ 初始全量（RDMA）”的组合。eBPF 确保主从实时数据一致，RDMA 保证在节点冷启动时，TB 级存量数据能以硬件级速度（Zero-copy）完成镜像复制。
4.  **mmap 快照加载冷启动**：通过 `mmap` 内存映射技术，实现 KSF 快照与 AOF 日志的亚秒级数据加载。
5.  **生产级持久化保证**：支持 **KSF 二进制快照** 与 **AOF 增量日志**，支持 `BGSAVE`，以及自定义自动快照落盘频率。
6.  **非侵入式实时同步**：基于 eBPF 的 `mirror` 模块，在内核层实现流量劫持，支持从节点实时追踪主节点状态。
7.  **智能内存管理**：内置定长内存池，针对 KV 常见数据分布优化。
8.  **基于 RDMA 的存量同步**：引入 RDMA 远程直接内存访问，实现主从同步中存量数据搬运的硬件级零拷贝。

### 核心组件说明

| 目录 | 组件名称 | 核心技术点 |
| :--- | :-- | :--- |
| `src/core/` | 核心调度层 | 流式 RESP 状态机、统一命令分发路由 |
| `src/network/` | 异步网络库 | io_uring 队列管理、固定文件/缓冲区优化、Fallback Reactor |
| `src/engines/` | 多索引存储引擎 | 渐进式 Hash Rehash、SkipList 范围查询、RBTree 稳定查询 |
| `src/persistence/`| 高性能持久化 | VLQ 变长编码 AOF、mmap 优化快照加载、异步 fsync 线程 |
| `src/utils/` | 基础工具链 | 针对 256B 小对象的定长内存池、高性能线程安全日志 |
| `mirror/` | eBPF 同步模块 | XDP/TC 挂载点双版本、内核态 TCP 重组、Ring Buffer 通信 |

---

## 架构设计

### 网络模型：基于 io_uring 的 Proactor

KVstore 采用真正的 Proactor 模式。传统的 `epoll` 属于“通知就绪”，应用仍需调用 `read` 将数据从内核拷贝到用户态。而 `io_uring` 允许应用直接提交请求到提交队列（SQ），内核完成后直接放入完成队列（CQ），系统调用开销减少约 70% 以上。

### 同步体系：eBPF 实时增量 + RDMA 存量全量

本项目将数据同步解耦为两个阶段：
1.  **实时阶段 (Incremental)**：使用 eBPF 程序挂载在网络驱动（XDP）或协议栈（TC）入口。当主节点收到 `SET` 命令包时，eBPF 直接提取 TCP 载荷，不经过用户态存储逻辑，延迟极低。
2.  **初始阶段 (Full Sync)**：针对主从刚建立连接时的存量搬运，引入 **RDMA (Remote Direct Memory Access)** 技术，允许从节点直接读取主节点内存中的快照映像，实现 GB/s 级别的传输带宽。

### 存储引擎：渐进式 Rehash 与多索引结构

-   **Hash 引擎**：实现了 Redis 风格的渐进式 Rehash，通过 `REHASH_STEPS_PER_OP` 机制将扩容压力分散。
-   **SkipList 引擎**：专为范围扫描设计，层级化索引保证了大规模数据下的 $O(\log N)$ 稳定性。

---

## Q&A

### 1. 为什么引入 RDMA？TCP 的瓶颈在哪里？
在分布式存储的**全量数据初始化**场景下，传统的 TCP 同步存在多次内存拷贝和高频协议栈中断开销。RDMA 支持硬件层面的零拷贝（Zero-copy）和内核旁路，允许从节点绕过主节点 CPU 直接读取内存。在 TB 级数据同步时，RDMA 能将 CPU 占用率降至近乎 0，同时同步效率提升一个数量级。

### 2. 为什么将 eBPF 主从同步模块（mirror）独立设计？
`mirror` 模块被设计为一个**协议无关的内核态网络插件**。由于它在内核层仅通过五元组过滤提取载荷，**完全不解析 RESP 协议**，因此具有极强的通用性。它不仅能为本项目服务，还可以无缝迁移到 **Redis、MySQL** 等任何基于 TCP 的数据库，为其实现非侵入式的流量镜像与性能监控。

### 3. 如何解决 io_uring 异步写与 eBPF 旁路镜像的一致性挑战？
这是一个核心的技术空隙：`eBPF` 在网卡收到包时立即镜像，而 `io_uring` 可能因磁盘 I/O 延迟尚未完成写入。KVstore 计划通过在镜像路径中引入基于 **TCP 序列号的屏障机制**，以及从节点的延迟确认逻辑，确保只有在主节点磁盘落地（AOF 确认）后，从节点才正式应用该变更，从而在高并发与最终一致性间取得平衡。

---

## 快速开始

### 环境要求
- **基础工具**: `gcc`, `clang`, `make`
- **系统环境**: Linux 内核版本 5.8+ (支持 io_uring 与 eBPF ring_buf)
- **依赖库**: `liburing`, `libbpf`

### 编译构建
```bash
# 1. 编译主服务器
make
# 2. 编译 eBPF 模块
cd mirror && make prebuild && make all
```

### 运行指南

#### 启动服务器

```bash
# 启动服务器 (使用默认配置)
./kvstore

# 使用自定义配置文件
./kvstore <config-file-path>
```

#### 启动 eBPF 镜像同步

```bash
# XDP 版本
sudo ./mirror/src/xdp_mirror <网络接口名> <从节点IP> <从节点端口>
# TC 版本
sudo ./mirror/src/xdp_mirror <网络接口名> <从节点IP> <从节点端口>
```

---

## 性能测试

*测试时间 2026.2.12*

### Hardware Spec

- **CPU**: 18 × Intel® Core™ Ultra 5 125H
- **MEM**: 32 GiB 内存 (30.9 GiB 可用)
- **Kernel**: Linux 6.18.8-arch2-1 (64 位)
> 内核版本 >= 5.8 以支持 eBPF ring_buf 特性

### 固定大小不同引擎SET (128B, 512B, 1024B)

```bash
python benchmark.py -t 8 -c 5 --command "<A/R/H/S>SET __key__ __value__" --test-time 30 --data-size <128/512/1024> --key-minimum 1 --key-maximum 1048576
```

#### Array

##### 128

![image-20260212162917115](README.assets/image-20260212162917115.png)

##### 512

![image-20260212162934970](README.assets/image-20260212162934970.png)

##### 1024

![image-20260212162950146](README.assets/image-20260212162950146.png)

#### RBtree

##### 128

![image-20260212163047276](README.assets/image-20260212163047276.png)

##### 512

![image-20260212163247599](README.assets/image-20260212163247599.png)

##### 1024

![image-20260212163346868](README.assets/image-20260212163346868.png)

#### Hash

##### 128

![image-20260212174535454](README.assets/image-20260212174535454.png)

##### 512

![image-20260212174648558](README.assets/image-20260212174648558.png)

##### 1024

![image-20260212174742342](README.assets/image-20260212174742342.png)

#### SkipList

##### 128

![image-20260212165105784](README.assets/image-20260212165105784.png)

##### 512

![image-20260212165156867](README.assets/image-20260212165156867.png)

##### 1024

![image-20260212164956516](README.assets/image-20260212164956516.png)

### 固定 size 的单 GET 测试 (热区数据预填充 `hit-rate=0.8` )

```bash
python benchmark.py -t 8 -c 5 --command "<A/R/H/S>GET __key__ __value__" --test-time 30 --data-size 128 --key-minimum 1 --key-maximum 32768 --populate --hit-rate 0.8
```

#### Array

![image-20260212171228478](README.assets/image-20260212171228478.png)

#### RBtree

![image-20260212171238432](README.assets/image-20260212171238432.png)

#### Hash

![image-20260212174918128](README.assets/image-20260212174918128.png)

#### SkipList

![image-20260212175036469](README.assets/image-20260212175036469.png)





### 业务场景模拟(不预热,2:8,5:5, 60s)

#### 2:8

```bash
python benchmark.py -t 8 -c 5 --ratio "<a/r/h/s>set:2,<a/r/h/s>get:8" --test-tim
e 60 --data-size 128 --key-minimum 1 --key-maximum 524288 --hit-rate 0.8
```

##### Array

![image-20260212185701984](README.assets/image-20260212185701984.png)

##### RBtree

![image-20260212185526358](README.assets/image-20260212185526358.png)

##### Hash

![image-20260212190041363](README.assets/image-20260212190041363.png)

##### SkipList

![image-20260212190214645](README.assets/image-20260212190214645.png)

#### 5:5

##### Array

![image-20260212190817121](README.assets/image-20260212190817121.png)

##### RBtree

![image-20260212190651963](README.assets/image-20260212190651963.png)

##### Hash

![image-20260212190949494](README.assets/image-20260212190949494.png)

##### SkipList

![image-20260212191116300](README.assets/image-20260212191116300.png)

### 持久化性能数据

> 测试时间: 2026年2月21日
>
> `commit: dc8a0bffd93ea637f459fe37d0254229d98d1230`

#### 配置文件选项

仅列出可能影响性能的选项

```bash
logfile ""
log-level 2

aof-enabled yes
auto-save-enabled 
auto-save-seconds 10
auto-save-changes 1000
```

#### 测试工具和选项

详见`tests/pers_benchmark.sh`

```bash
# --------------------- 配置参数 ---------------------
HOST="127.0.0.1"
PORT="8888"
THREADS=8
CONNECTIONS_PER_THREAD=50
DATA_SIZE=128
KEY_PREFIX="kv_"
TOTAL_KEYS=1000000
KEY_MIN=1
KEY_MAX=1000000

# 计算每个连接需要处理的请求数
REQUESTS_PER_CONN=$((TOTAL_KEYS / (THREADS * CONNECTIONS_PER_THREAD)))

# 使用SSET作为测试命令
TEST_CMD="SSET"


memtier_benchmark \
    -s ${HOST} \
    -p ${PORT} \
    --command="${TEST_CMD} __key__ __data__" \
    --command-ratio=1 \
    --command-key-pattern=P \
    -t ${THREADS} \
    -c ${CONNECTIONS_PER_THREAD} \
    -n ${REQUESTS_PER_CONN} \
    -d ${DATA_SIZE} \
    --key-prefix=${KEY_PREFIX} \
    --key-minimum=${KEY_MIN} \
    --key-maximum=${KEY_MAX} \
    --hide-histogram \
    --hdr-file-prefix="${OUTPUT_DIR}/${CONFIG_NAME}_hdr" \
    --json-out-file="${OUTPUT_DIR}/${CONFIG_NAME}_result.json" \
    --print-percentiles=50,90,95,99,99.9 \
    2>&1 | tee "${OUTPUT_DIR}/${CONFIG_NAME}_output.log"
```

#### 测试结果

```bash
# 图表生成
cd tests && python3 gen_charts.py ./pers_sset_benchmark_results 
```

<img src="README.assets/image-20260221204018087.png" alt="image-20260221204018087" style="zoom: 67%;" />

<img src="README.assets/image-20260221204037592.png" alt="image-20260221204037592" style="zoom: 67%;" />

<img src="README.assets/image-20260221204053473.png" alt="image-20260221204053473" style="zoom: 67%;" />

<img src="README.assets/image-20260221204109200.png" alt="image-20260221204109200" style="zoom:67%;" />

<img src="README.assets/image-20260221204150706.png" alt="image-20260221204150706" style="zoom:67%;" />

<img src="README.assets/image-20260221204215237.png" alt="image-20260221204215237" style="zoom: 67%;" />

---

## 发展路线 (Roadmap)

- [ ] **RDMA 存量全量克隆**：实现基于 RDMA Read 的大规模存量数据全量同步，压榨硬件带宽极限。
- [ ] **eBPF 挂载点对比研究**：深入对比 **XDP vs TC vs Kprobe** 在高频同步下的 CPU 指令周期消耗与丢包率。
- [ ] **io_uring 固定缓冲区优化**：引入 `IORING_REGISTER_BUFFERS` 实现真正的零拷贝内存路径。
- [ ] **内存池性能测试**：通过 `valgrind` 的 `massif` 工具，对比基于**自研内存池**与 **jemalloc** 的两种 KVstore 的堆内存变化趋势。

---

## 贡献

欢迎任何形式的贡献！在提交 PR 之前，请确保已通过所有 `tests/` 下的一致性测试 `test.sh`。

```bash
# 需要用到 root 权限以启动 eBPF 程序
sudo tests/test.sh
```

## 支持

如有疑问，请通过 GitHub Issues 提交反馈。

## 许可证
- 核心代码：MIT License。
- eBPF 代码：Dual BSD/GPL。

---
*最后更新: 2026-02-23*
