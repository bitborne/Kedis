/*
 * 流式 KSF 解析器实现（用于 RDMA 分段传输）
 *
 * 设计目的：
 *   - 支持边接收 RDMA 数据边解析，无需等待完整文件
 *   - 零拷贝：数据直接流入引擎，无中间缓冲
 */

#include "../../include/kvstore.h"
#include "../../include/kvs_ksf.h"
#include <string.h>

/* 解析状态 */
typedef enum {
    KSF_PARSE_STATE_KEY_LEN,   /* 正在解析 key_len VLQ */
    KSF_PARSE_STATE_VAL_LEN,   /* 正在解析 val_len VLQ */
    KSF_PARSE_STATE_KEY_DATA,  /* 正在解析 key 数据 */
    KSF_PARSE_STATE_VAL_DATA,  /* 正在解析 value 数据 */
} ksf_parse_state_t;

/* 流式解析器上下文 */
struct ksf_stream_parser {
    int engine_type;           /* 目标引擎类型 */
    uint64_t entry_count;      /* 已解析的 entry 数量 */

    /* 解析状态 */
    ksf_parse_state_t state;

    /* 当前 entry 的解析状态 */
    uint64_t key_len;          /* key 长度（含结尾 \0） */
    uint64_t val_len;          /* value 长度（含结尾 \0） */
    char *key_buf;             /* key 缓冲区（已分配） */
    char *val_buf;             /* value 缓冲区（已分配） */
    size_t key_received;       /* 已接收的 key 字节数 */
    size_t val_received;       /* 已接收的 value 字节数 */
};

/* 重新声明全局引擎变量 */
#if ENABLE_MULTI_ENGINE
  #if ENABLE_ARRAY
  extern kvs_array_t array_engine;
  #endif
  #if ENABLE_HASH
  extern kvs_hash_t hash_engine;
  #endif
  #if ENABLE_RBTREE
  extern kvs_rbtree_t rbtree_engine;
  #endif
  #if ENABLE_SKIPLIST
  extern kvs_skiplist_t skiplist_engine;
  #endif
#else
  #if ENABLE_RBTREE
  extern kvs_rbtree_t global_main_engine;
  #elif ENABLE_HASH
  extern kvs_hash_t global_main_engine;
  #elif ENABLE_ARRAY
  extern kvs_array_t global_main_engine;
  #elif ENABLE_SKIPLIST
  extern kvs_skiplist_t global_main_engine;
  #endif
#endif

/**
 * 重置 entry 解析状态，准备解析下一个 entry
 */
static void ksf_parser_reset_entry(struct ksf_stream_parser *parser) {
    if (parser->key_buf) {
        kvs_free(parser->key_buf);
        parser->key_buf = NULL;
    }
    if (parser->val_buf) {
        kvs_free(parser->val_buf);
        parser->val_buf = NULL;
    }
    parser->key_len = 0;
    parser->val_len = 0;
    parser->key_received = 0;
    parser->val_received = 0;
    parser->state = KSF_PARSE_STATE_KEY_LEN;
}

/**
 * 将当前解析完成的 entry 插入引擎
 */
static int ksf_parser_insert_entry(struct ksf_stream_parser *parser) {
    robj key = {0};
    robj value = {0};

    /* key_len 和 val_len 包含结尾的 \0，但 robj.len 不包含 */
    if (parser->key_len > 0) {
        key.len = (int)(parser->key_len - 1);
        key.ptr = parser->key_buf;
    }
    if (parser->val_len > 0) {
        value.len = (int)(parser->val_len - 1);
        value.ptr = parser->val_buf;
    }

    /* 插入引擎 */
#if ENABLE_MULTI_ENGINE
    switch (parser->engine_type) {
    case 0: /* ENGINE_ARRAY */
#if ENABLE_ARRAY
        kvs_array_set(&array_engine, &key, &value);
#endif
        break;
    case 1: /* ENGINE_RBTREE */
#if ENABLE_RBTREE
        kvs_rbtree_set(&rbtree_engine, &key, &value);
#endif
        break;
    case 2: /* ENGINE_HASH */
#if ENABLE_HASH
        kvs_hash_set(&hash_engine, &key, &value);
#endif
        break;
    case 3: /* ENGINE_SKIPLIST */
#if ENABLE_SKIPLIST
        kvs_skiplist_set(&skiplist_engine, &key, &value);
#endif
        break;
    default:
        kvs_logError("未知的引擎类型: %d\n", parser->engine_type);
        return -1;
    }
#else
    /* 单引擎模式 */
#if ENABLE_RBTREE
    kvs_rbtree_set(&global_main_engine, &key, &value);
#elif ENABLE_HASH
    kvs_hash_set(&global_main_engine, &key, &value);
#elif ENABLE_ARRAY
    kvs_array_set(&global_main_engine, &key, &value);
#elif ENABLE_SKIPLIST
    kvs_skiplist_set(&global_main_engine, &key, &value);
#endif
#endif

    parser->entry_count++;

    /* 注意：不要释放 key_buf 和 val_buf，它们已被引擎接管
     * 只需将指针置空，reset_entry 时不会重复释放 */
    parser->key_buf = NULL;
    parser->val_buf = NULL;

    return 0;
}

/**
 * 尝试从缓冲区解析 VLQ 编码的整数
 * @param buf: 输入缓冲区
 * @param len: 缓冲区长度
 * @param value: 输出解析值
 * @param consumed: 输出消耗的字节数
 * @return: 0=成功，-1=错误，1=需要更多数据
 */
static int parse_vlq(const uint8_t *buf, size_t len, uint64_t *value, size_t *consumed) {
    *value = 0;
    int count = 0;
    int shift = 0;

    while (count < (int)len) {
        uint8_t byte = buf[count];
        *value |= ((uint64_t)(byte & 0x7F)) << shift;
        shift += 7;
        count++;

        /* 最高位为 0 表示 VLQ 结束 */
        if ((byte & 0x80) == 0) {
            *consumed = count;
            return 0;  /* 成功 */
        }

        /* 安全检查：VLQ 不应该超过 10 字节（64位整数） */
        if (count >= 10) {
            return -1;  /* 错误 */
        }
    }

    /* 数据不足，需要更多字节 */
    return 1;
}

/* ============================================================================
 * 公开 API 实现
 * ============================================================================ */

struct ksf_stream_parser *ksf_stream_parser_init(int engine_type) {
    struct ksf_stream_parser *parser =
        (struct ksf_stream_parser *)kvs_calloc(1, sizeof(*parser));
    if (!parser) {
        kvs_logError("流式解析器内存分配失败\n");
        return NULL;
    }

    parser->engine_type = engine_type;
    parser->state = KSF_PARSE_STATE_KEY_LEN;

    kvs_logInfo("流式 KSF 解析器初始化完成，引擎类型: %d\n", engine_type);
    return parser;
}

void ksf_stream_parser_free(struct ksf_stream_parser *parser) {
    if (!parser) return;

    /* 清理当前 entry 的资源 */
    if (parser->key_buf) kvs_free(parser->key_buf);
    if (parser->val_buf) kvs_free(parser->val_buf);

    kvs_free(parser);
}

uint64_t ksf_stream_parser_get_count(struct ksf_stream_parser *parser) {
    return parser ? parser->entry_count : 0;
}

/**
 * 处理数据块，尝试解析尽可能多的完整 entry
 * @return: 成功返回 0，错误返回 -1
 */
int ksf_stream_parser_feed(struct ksf_stream_parser *parser,
                           const void *data, size_t len) {
    if (!parser || !data) return -1;
    if (len == 0) return 0;

    const uint8_t *input = (const uint8_t *)data;
    size_t input_pos = 0;

    while (input_pos < len) {
        size_t available = len - input_pos;

        switch (parser->state) {
        case KSF_PARSE_STATE_KEY_LEN: {
            uint64_t key_len;
            size_t consumed;
            int ret = parse_vlq(input + input_pos, available, &key_len, &consumed);

            if (ret == 0) {
                /* VLQ 解析成功 */
                parser->key_len = key_len;
                input_pos += consumed;
                parser->state = KSF_PARSE_STATE_VAL_LEN;
            } else if (ret == 1) {
                /* 数据不足，停止处理 */
                return 0;
            } else {
                /* 解析错误 */
                kvs_logError("VLQ 解析错误 (key_len)\n");
                return -1;
            }
            break;
        }

        case KSF_PARSE_STATE_VAL_LEN: {
            uint64_t val_len;
            size_t consumed;
            int ret = parse_vlq(input + input_pos, available, &val_len, &consumed);

            if (ret == 0) {
                parser->val_len = val_len;
                input_pos += consumed;

                /* 分配 key 和 value 缓冲区 */
                if (parser->key_len > 0) {
                    parser->key_buf = (char *)kvs_malloc(parser->key_len);
                    if (!parser->key_buf) {
                        kvs_logError("无法分配 key 缓冲区\n");
                        return -1;
                    }
                }
                if (parser->val_len > 0) {
                    parser->val_buf = (char *)kvs_malloc(parser->val_len);
                    if (!parser->val_buf) {
                        kvs_logError("无法分配 value 缓冲区\n");
                        return -1;
                    }
                }

                parser->state = KSF_PARSE_STATE_KEY_DATA;
            } else if (ret == 1) {
                /* 数据不足 */
                return 0;
            } else {
                kvs_logError("VLQ 解析错误 (val_len)\n");
                return -1;
            }
            break;
        }

        case KSF_PARSE_STATE_KEY_DATA: {
            size_t needed = parser->key_len - parser->key_received;
            size_t to_copy = (available < needed) ? available : needed;

            if (to_copy > 0) {
                memcpy(parser->key_buf + parser->key_received, input + input_pos, to_copy);
                parser->key_received += to_copy;
                input_pos += to_copy;
            }

            if (parser->key_received >= parser->key_len) {
                parser->state = KSF_PARSE_STATE_VAL_DATA;
            } else {
                /* key 数据不完整，需要更多数据 */
                return 0;
            }
            break;
        }

        case KSF_PARSE_STATE_VAL_DATA: {
            size_t needed = parser->val_len - parser->val_received;
            size_t to_copy = (available < needed) ? available : needed;

            if (to_copy > 0) {
                memcpy(parser->val_buf + parser->val_received, input + input_pos, to_copy);
                parser->val_received += to_copy;
                input_pos += to_copy;
            }

            if (parser->val_received >= parser->val_len) {
                /* entry 完整，插入引擎 */
                if (ksf_parser_insert_entry(parser) < 0) {
                    return -1;
                }
                ksf_parser_reset_entry(parser);
            } else {
                /* value 数据不完整 */
                return 0;
            }
            break;
        }

        default:
            kvs_logError("未知的解析状态: %d\n", parser->state);
            return -1;
        }
    }

    return 0;
}

int ksf_stream_parser_finish(struct ksf_stream_parser *parser) {
    if (!parser) return -1;

    /* 检查是否有未完成的 entry */
    if (parser->state != KSF_PARSE_STATE_KEY_LEN) {
        kvs_logWarn("流式解析器结束时仍有未完成的 entry (state=%d)\n",
                    parser->state);
        return -1;
    }

    kvs_logInfo("流式 KSF 解析完成，共解析 %lu 个 entry\n",
                (unsigned long)parser->entry_count);
    return 0;
}
