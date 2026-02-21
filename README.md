# KVstore

[![license](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Platform](https://img.shields.io/badge/platform-Linux-orange.svg)]()
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)]()

KVstore 是一个高性能、多引擎、支持持久化及基于 eBPF 主从增量同步的 C 语言 Key-Value 存储系统，深度集成了 io_uring 与 eBPF 内核增强技术。

## Table of Contents

- [Introduction](#introduction)
  - [Key Components](#key-components)
- [Features](#features)
- [架构设计]()
  - [选择io_uring的原因](#为什么最终选择 io_uring？)
- [Get Started](#get-started)
  - [Clone the Repository](#clone-the-repository)
  - [Requirements](#requirements)
  - [Build](#build)
  - [Run and Play](#run-and-play)
  - [Test](#test)
- [Performance](#performance)
  - [Hardware Spec](#hardware-spec)
  - [固定大小不同引擎SET (128B, 512B, 1024B)](#固定大小不同引擎set-128b-512b-1024b)
  - [固定 size 的单 GET 测试 (热区数据预填充 `hit-rate=0.8`)](#固定-size-的单-get-测试-热区数据预填充-hit-rate08-)
  - [业务场景模拟 (不预热, 2:8, 5:5, 60s)](#业务场景模拟不预热2855-60s)
- [Contributing](#contributing)
- [Support](#support)
- [License](#license)

## Introduction

KVstore 旨在构建一个高性能的存储基座，完全兼容 RESP (Redis Serialization Protocol) 协议。它通过模块化设计支持多种存储引擎（Array, Hash, RBTree, SkipList），并集成了 NtyCo 协程、基于 epoll 的 reactor 模型和基于 io_uring 的 proactor模型以处理海量并发连接。此外，项目引入了 eBPF 技术（mirror 模块）, 挂载点为 tc-ingress，并使用 ring-buf 实现内核态到用户态的无上限、高吞吐的数据传输，从而实现在用户态对内核数据流进行镜像转发。

### Key Components

| 目录 | 说明 |
| :--- | :-- |
| `src/` | 核心源代码（协议解析、存储引擎、网络模型、持久化） |
| `include/` | 核心头文件定义 |
| `mirror/` | eBPF 扩展模块，包含独立的 libbpf、bpftool 构建系统 |
| `NtyCo/` | 集成的协程库（子模块），提供高性能 IO 支持 |
| `client/` | 客户端示例（Rust、Python） |
| `tests/` | 自动化测试套件与性能基准测试 |

## Features

- **RESP 协议兼容**: 使用针对 4 个引擎和快照存储落盘的 22 个指令，服务端与客户端完全使用 RESP 通信协议。
- **多引擎架构**: 提供 **Hash, RBTree, SkipList, Array** 多种数据结构实现。
- **mmap 持久化机制**: 集成 AOF (Append Only File) 与 KSF (Snapshot) 两种持久化策略，使用名为 ksf 的自研数据存储格式，加载数据时均采用 **mmap** 减少数据拷贝。
- **协程并发模型**: 基于 NtyCo 实现的 Reactor/Proactor 网络模型，极大提升单机并发吞吐。
- **eBPF 独立镜像模块**: 利用 eBPF 技术在内核态实现数据镜像转发与性能监控，完全支持 Redis 等所有基于 TCP 的网络服务流量转发。

## 架构设计

本项目采用**模块化网络层设计**，支持三种底层网络框架：

| 框架 | 状态 | 说明 |
|:---|:---|:---|
| **io_uring** | ✅ 生产就绪 | 当前主力方案，完整实现 RESP 协议、分片流式收发与解析 |
| epoll | 🚧 实验性 | 基础实现，**未实现 RESP 协议及流式处理** |
| Ntyco | 🚧 实验性 | 基础实现，**未实现 RESP 协议及流式处理** |

> **设计意图**：验证 io_uring 在现代存储场景下的性能优势，同时保留 epoll/Ntyco 实现作为跨平台基础。后续计划统一抽象层，实现真正的跨平台支持。

---

### 为什么最终选择 io_uring？

开发初期对三种框架进行了原型验证，io_uring 在以下方面表现显著优于传统方案：

- 减少 70% 的系统调用开销
- 更低的延迟抖动
- 更简洁的异步编程模型

因此决定将核心功能（RESP 协议、流式解析、分片传输）优先在 io_uring 上完善，epoll/kqueue 保留为**可扩展的架构基础**。


## Get Started

### Clone the Repository

```bash

```

### Requirements

- **基础构建工具**: `gcc`, `clang` (eBPF 编译必需), `make`
- **环境**: Rust 工具链 (可选：用于编译高性能客户端), Python (用于自动化测试), Linux 内核版本 5.8+

### Build

**1. 编译主服务器:**

```bash
# 项目根目录下
make
```

**2. 编译 eBPF 增强模块 (mirror):**

```bash
cd mirror
make help      # 查看编译帮助
make prebuild  # 构建 libbpf 和 bpftool
make all       # 编译 mirror 程序（推荐：一步到位✔）
make vmlinux   # 从当前系统内核提取 vmlinux.h
make clean     # 清理所有构建产物
```

**3. 编译 Rust 客户端:**

```bash
cd client
cargo build --release
```

### Run and Play

**启动服务器:**

```bash
# 指定端口并以 KSF 快照模式初始化
./kvstore 8888 snap

# 指定端口并以 AOF 模式初始化
./kvstore 9999 aof
```

**使用 rust 客户端连接:**

```bash
cd client && cargo run -- -P <本地端口号> -i  
```

**使用 python 测试客户端连接:**

```bash
python3 tests/resp_client.py
```

**开启 mirror 镜像转发**

```bash
# ⚠️ 默认：KV 存储主节点服务开在 8888 端口下
# 网卡名称可通过 ip addr 查看
sudo ./mirror <网卡名> <slave_ip> <slave_port>
```

### Test

运行多引擎一致性测试框架：

```bash
python3 tests/conformance.py
```

## Performance

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

# 输出目录
OUTPUT_DIR="./sset_benchmark_results"
mkdir -p "${OUTPUT_DIR}"

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
cd tests && python3 gen_charts.py ./sset_benchmark_results 
```



<img src="README.assets/image-20260221204018087.png" alt="image-20260221204018087" style="zoom: 67%;" />

<img src="README.assets/image-20260221204037592.png" alt="image-20260221204037592" style="zoom: 67%;" />

<img src="README.assets/image-20260221204053473.png" alt="image-20260221204053473" style="zoom:67%;" />

<img src="README.assets/image-20260221204109200.png" alt="image-20260221204109200" style="zoom:67%;" />

<img src="README.assets/image-20260221204150706.png" alt="image-20260221204150706" style="zoom:67%;" />

<img src="README.assets/image-20260221204215237.png" alt="image-20260221204215237" style="zoom: 67%;" />



## Contributing

欢迎任何形式的贡献！在提交 PR 之前，请确保已通过所有 `tests/` 下的一致性测试 `conformance.py`。

## Support

如有疑问，请通过 GitHub Issues 提交反馈。

## License

- 本项目核心代码采用 [MIT License](LICENSE) 授权。
- eBPF 内核态代码（`mirror/` 模块）采用 **Dual BSD/GPL** 混合授权，以符合 Linux 内核合规性要求。

---
*Last Updated: 2026-02-10*