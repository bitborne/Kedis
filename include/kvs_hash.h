#ifndef __KVS_HASH_H__
#define __KVS_HASH_H__

#include "kvs_constants.h"
#include "kvs_network.h"

#if ENABLE_HASH

  #define MAX_KEY_LEN	128
  #define MAX_VALUE_LEN	512
  #define MAX_TABLE_SIZE	1024

  #define HASH_ENABLE_KEY_POINTER	1

  typedef struct hashnode_s {
  #if HASH_ENABLE_KEY_POINTER
    char *key;
    char *value;
  #else
    char key[MAX_KEY_LEN];
    char value[MAX_VALUE_LEN];
  #endif
    struct hashnode_s *next;

  } hashnode_t;

  // 方案二：Rehash 状态枚举
  typedef enum {
    REHASH_STATE_IDLE,
    REHASH_STATE_ACTIVE
  } rehash_state_t;

  typedef struct hashtable_s {

    hashnode_t **nodes;

    int max_slots;
    int count;
    int capacity;
    float load_factor;

    // 方案二：Rehash 相关字段
    hashnode_t **rehash_nodes;  // 扩容表
    int rehash_slots;            // 扩容表大小
    int rehash_index;            // rehash 进度（当前迁移到第几个槽）
    rehash_state_t rehash_state; // rehash 状态

  } hashtable_t;

  typedef struct hashtable_s kvs_hash_t;


  int kvs_hash_create(kvs_hash_t *hash);
  void kvs_hash_destroy(kvs_hash_t *hash);
  int kvs_hash_set(hashtable_t *hash, robj* key, robj* value);
  char* kvs_hash_get(kvs_hash_t *hash, robj* key);
  int kvs_hash_mod(kvs_hash_t *hash, robj* key, robj* value);
  int kvs_hash_del(kvs_hash_t *hash, robj* key);
  int kvs_hash_exist(kvs_hash_t *hash, robj* key);

  // 方案一新增接口
  typedef struct {
    int count;
    int max_slots;
    int capacity;
    float load_factor;
    int max_chain_length;
    int empty_slots;
    float avg_chain_length;
  } kvs_hash_stats_t;

  int kvs_hash_resize(hashtable_t *hash, int new_size);
  int kvs_hash_get_stats(hashtable_t *hash, kvs_hash_stats_t *stats);
  void kvs_hash_print_stats(hashtable_t *hash);

  // 方案二新增接口
  int kvs_hash_start_rehash(hashtable_t *hash);
  int kvs_hash_step_rehash(hashtable_t *hash);
  void kvs_hash_finish_rehash(hashtable_t *hash);
  int kvs_hash_is_rehashing(hashtable_t *hash);

#endif // ENABLE_HASH

#endif // __KVS_HASH_H__
