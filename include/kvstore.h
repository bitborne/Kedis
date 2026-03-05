#ifndef __KVSTORE_H__
#define __KVSTORE_H__
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#include <sys/mman.h>
#include <sys/time.h>
#include <pthread.h>
#include <unistd.h>

// jemalloc 开关
// #define HAVE_JEMALLOC

// 引擎启用开关
#define ENABLE_ARRAY		1
#define ENABLE_RBTREE		1
#define ENABLE_HASH			1
#define ENABLE_SKIPLIST		1

// 网络模型定义
#define NETWORK_REACTOR 	0
#define NETWORK_PROACTOR	1
#define NETWORK_NTYCO		2

// 当前选择的网络模型
#define NETWORK_SELECT		NETWORK_PROACTOR

// 多引擎模式开关：0=单引擎模式（按优先级选择），1=多引擎模式（同时启用所有引擎）
#define ENABLE_MULTI_ENGINE	1
// 是否使用mmap加载数据文件(ksf, 快照)
#define ENABLE_MMAP 1

#include "config.h"

#include "kvs_network.h"
#include "kvs_protocol.h"

#include "kvs_hash.h"
#include "kvs_rbtree.h"
#include "kvs_array.h"
#include "kvs_skiplist.h"

#include "kvs_aof.h"
#include "kvs_ksf.h"


#include "memory_pool.h"
#include "kvs_log.h"

enum {
    KVS_CMD_START = 0,
    // 统一的KV操作命令（单引擎模式）
    KVS_CMD_SET = KVS_CMD_START,
    KVS_CMD_GET,
    KVS_CMD_DEL,
    KVS_CMD_MOD,
    KVS_CMD_EXIST,

    // 多引擎模式 - Array 引擎命令
    KVS_CMD_ASET,
    KVS_CMD_AGET,
    KVS_CMD_ADEL,
    KVS_CMD_AMOD,
    KVS_CMD_AEXIST,

    // 多引擎模式 - Hash 引擎命令
    KVS_CMD_HSET,
    KVS_CMD_HGET,
    KVS_CMD_HDEL,
    KVS_CMD_HMOD,
    KVS_CMD_HEXIST,

    // 多引擎模式 - RBTREE 引擎命令
    KVS_CMD_RSET,
    KVS_CMD_RGET,
    KVS_CMD_RDEL,
    KVS_CMD_RMOD,
    KVS_CMD_REXIST,

    // 多引擎模式 - Skiplist 引擎命令
    KVS_CMD_SSET,
    KVS_CMD_SGET,
    KVS_CMD_SDEL,
    KVS_CMD_SMOD,
    KVS_CMD_SEXIST,

    // 通用命令（两种模式都支持）
    KVS_CMD_SAVE,
    KVS_CMD_BGSAVE,
    KVS_CMD_SYNC,
    KVS_CMD_REPLICAOF,  // 设置/取消主从复制关系: REPLICAOF <host> <port> 或 REPLICAOF NO ONE

    /*
     * 【方案C新增】RDMASYNC 命令
     *
     * 用途: 从节点发送给主节点，触发fork子进程进行RDMA存量同步
     * 语法: RDMASYNC <engine_type>
     *       engine_type: 0-3表示特定引擎，4表示所有引擎
     *
     * 响应:
     *   +FORKED\r\n      - 主节点已fork子进程
     *   +RDMA_READY <port>\r\n  - 子进程RDMA服务器就绪，携带动态端口
     *   +RDMA_DONE\r\n  - 同步完成
     *   -ERR ...\r\n    - 发生错误
     *
     * 注意事项:
     *   - 此命令只能在主节点执行
     *   - 执行此命令后，当前TCP连接的fd将移交给fork出的子进程
     *   - 父进程立即返回+FORKED，后续通信由子进程处理
     */
    KVS_CMD_RDMASYNC,

    KVS_CMD_COUNT
};

void* kvs_calloc(size_t num, size_t size);
void *kvs_malloc(size_t size);
void kvs_free(void *ptr);

#endif