#ifndef __KVS_KSF_H__
#define __KVS_KSF_H__

#include "kvstore.h"

// KSF持久化相关函数声明
int ksfSave(const char *filename);  // 保存KSF快照
int ksfSaveBackground(void);  // 后台保存KSF快照
int ksfLoad(const char *filename);  // 加载KSF快照

// 多引擎模式下的KSF引擎保存函数
#if ENABLE_MULTI_ENGINE
int ksfSaveAll(void);  // 保存所有引擎的KSF快照
int ksfLoadAll(void);  // 加载所有引擎的KSF快照
#endif

// mmap 优化版本
int ksfLoadAll_mmap(void);  // 使用 mmap 加载所有 KSF 快照

// 单个引擎写入函数（用于 RDMA 同步）
int ksfWriteArray(int fd);
int ksfWriteRbtree(int fd);
int ksfWriteHash(int fd);
int ksfWriteSkiplist(int fd);

// 从内存缓冲区加载引擎数据（用于 RDMA 同步）
int ksf_load_engine_from_buffer(int engine_type, void *buffer, size_t length);

/* ============================================================================
 * 流式 KSF 解析器（用于 RDMA 分段传输）
 * ============================================================================
 *
 * 设计目的：
 *   - 支持边接收 RDMA 数据边解析，无需等待完整文件
 *   - 零拷贝：数据直接流入引擎，无中间缓冲
 *
 * 使用流程：
 *   1. ksf_stream_parser_init() - 创建解析器
 *   2. 循环接收 RDMA 数据块：
 *      ksf_stream_parser_feed() - 喂入数据块，自动解析并插入引擎
 *   3. ksf_stream_parser_finish() - 完成解析，检查是否有不完整数据
 *   4. ksf_stream_parser_free() - 释放解析器
 *
 * 特点：
 *   - 内部自动处理跨块的不完整 entry
 *   - 支持任意大小的数据流（仅受限于 RDMA 缓冲区）
 */

/* 流式解析器状态 */
struct ksf_stream_parser;

/* 创建流式解析器
 * @param engine_type: 目标引擎类型 (0=array, 1=rbtree, 2=hash, 3=skiplist)
 * @return: 解析器上下文，失败返回 NULL
 */
struct ksf_stream_parser *ksf_stream_parser_init(int engine_type);

/* 喂入数据块进行解析
 * @param parser: 解析器上下文
 * @param data: 数据块指针
 * @param len: 数据块长度
 * @return: 成功解析的 entry 数量，<0 表示错误
 *
 * 注意：此函数会复制不完整 entry 的剩余部分到内部缓冲区，
 *       并在下一次调用时自动拼接
 */
int ksf_stream_parser_feed(struct ksf_stream_parser *parser,
                           const void *data, size_t len);

/* 完成解析，检查是否有未处理的数据
 * @param parser: 解析器上下文
 * @return: 0 成功，-1 有未完成的 entry
 */
int ksf_stream_parser_finish(struct ksf_stream_parser *parser);

/* 释放解析器
 * @param parser: 解析器上下文
 */
void ksf_stream_parser_free(struct ksf_stream_parser *parser);

/* 获取已解析的 entry 数量 */
uint64_t ksf_stream_parser_get_count(struct ksf_stream_parser *parser);

#endif // __KVS_KSF_H__
