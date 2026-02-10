# KVstore

[![license](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Platform](https://img.shields.io/badge/platform-Linux-orange.svg)]()
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)]()

KVstore 是一个高性能、多引擎、支持持久化及 eBPF 增强功能的 C 语言 Key-Value 存储系统，深度集成了协程框架与内核增强技术。

## Table of Contents

- [Introduction](#introduction)
- [Features](#features)
- [Get Started](#get-started)
  - [Requirements](#requirements)
  - [Build](#build)
  - [Run and Play](#run-and-play)
  - [Test](#test)
- [Performance](#performance)
  - [Hardware Spec](#hardware-spec)
  - [Benchmarks and QPS numbers](#benchmarks-and-qps-numbers)
- [Contributing](#contributing)
- [Support](#support)
- [License](#license)

## Introduction

KVstore 旨在构建一个高性能的存储基座，完全兼容 RESP (Redis Serialization Protocol) 协议。它通过模块化设计支持多种存储引擎（Hash, RBTree, SkipList），并集成了 NtyCo 协程网络框架以处理海量并发连接。此外，项目引入了 eBPF 技术，实现在内核态对数据流进行监控与镜像转发（mirror 模块）。

### Key Components

| 目录 | 说明 |
| :--- | :-- |
| `src/` | 核心源代码（协议解析、存储引擎、网络模型、持久化） |
| `include/` | 全局头文件定义 |
| `mirror/` | eBPF 扩展模块，包含独立的 libbpf 构建系统 |
| `NtyCo/` | 集成的协程库，提供高性能 IO 支持 |
| `client/` | Rust 语言编写的官方客户端 |
| `tests/` | 自动化测试套件与性能基准测试 |

## Features

- **RESP 协议兼容**: 支持标准 Redis 指令集，无缝对接现有 Redis 客户端。
- **多引擎架构**: 针对不同场景提供 Hash, RBTree, SkipList, Array 等多种存储实现。
- **持久化机制**: 集成 AOF (Append Only File) 与 KSF (Snapshot) 两种持久化策略，确保数据安全。
- **协程并发模型**: 基于 NtyCo 实现的 Reactor/Proactor 网络模型，极大提升单机并发吞吐。
- **eBPF 增强模块**: 利用 eBPF 技术在内核态实现数据镜像转发与性能监控，降低上下文切换开销。

## Get Started

### Clone the Repository

```bash

```

### Requirements

- **基础构建工具**: `gcc`, `clang` (eBPF 编译必需), `make`, `cmake`
- **库依赖**: `libelf-dev`, `zlib1g-dev` (eBPF 组件依赖)
- **环境**: Rust 工具链 (用于编译高性能客户端), Python (用于自动化测试), Linux 内核版本 5.8+

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
make prebuild  # 构建 libbpf 和 bpftool (首次运行必做)
make all       # 编译 mirror 程序
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

### Test

运行多引擎一致性测试框架：

```bash
python3 tests/conformance.py
```

## Performance

### Hardware Spec

- **CPU**: 18 × Intel® Core™ Ultra 5 125H
- **MEM**: 32 GiB 内存 (30.9 GiB 可用)
- **Kernel**: Linux 6.18.8-arch2-1 (64 位)
> 内核版本 >= 5.8 以支持 eBPF ring_buf 特性

### Benchmarks and QPS numbers

*(此处可添加性能压测图表与数据结论)*

### QPS on different payload

## Contributing

欢迎任何形式的贡献！在提交 PR 之前，请确保已通过所有 `tests/` 下的一致性测试。

## Support

如有疑问，请通过 GitHub Issues 提交反馈。

## License

- 本项目核心代码采用 [MIT License](LICENSE) 授权。
- eBPF 内核态代码（`mirror/` 模块）采用 **Dual BSD/GPL** 混合授权，以符合 Linux 内核合规性要求。

---
*Last Updated: 2026-02-10*