
#ifndef __KV_STORE_H__
#define __KV_STORE_H__

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <stddef.h>
#include <stdint.h>


#define NETWORK_REACTOR 	0
#define NETWORK_PROACTOR	1
#define NETWORK_NTYCO		2

#define NETWORK_SELECT		NETWORK_PROACTOR



#define KVS_MAX_TOKENS		128

#define ENABLE_ARRAY		1
#define ENABLE_RBTREE		1
#define ENABLE_HASH			1

#define BUFFER_SIZE 1024
#define MAX_MULTICMD_LENGTH 8192  // 增加最大多命令长度

#define INCREMENTAL_PERSISTENCE 1

// AOF相关定义
#define AOF_BUF_SIZE (1024*1024)  // 1MB AOF缓冲区
// 应该是网络框架的问题, 导致命令解析部分在遇到大文件的情况下会出错

#define AOF_SYNC_INTERVAL 1  // AOF同步间隔（秒）

// AOF命令码定义
#define AOF_CMD_SET 1
#define AOF_CMD_MOD 2
#define AOF_CMD_DEL 3

typedef int (*msg_handler)(char *msg, int length, char *response);


extern int reactor_start(unsigned short port, msg_handler handler);
extern int proactor_start(unsigned short port, msg_handler handler);
extern int ntyco_start(unsigned short port, msg_handler handler);

void* kvs_calloc(size_t num, size_t size);
void *kvs_malloc(size_t size);
void kvs_free(void *ptr);


#if ENABLE_ARRAY
#define KVS_ARRAY_SIZE 16384
// 元素
typedef struct kvs_array_item {
  char* key;
  char* value;
} kvs_array_item_t;

// 整个数组
typedef struct kvs_array {
  kvs_array_item_t* table;
  int total;
} kvs_array_t;

int kvs_array_create(kvs_array_t *inst);
void kvs_array_destroy(kvs_array_t *inst);

int kvs_array_set(kvs_array_t *inst, char *key, char *value);
char* kvs_array_get(kvs_array_t *inst, char *key);
int kvs_array_del(kvs_array_t *inst, char *key);
int kvs_array_mod(kvs_array_t *inst, char *key, char *value);
int kvs_array_exist(kvs_array_t *inst, char *key);


#endif


#if ENABLE_RBTREE

#define RED				1
#define BLACK 			2

#define ENABLE_KEY_CHAR		1

#if ENABLE_KEY_CHAR
typedef char* KEY_TYPE;
#else
typedef int KEY_TYPE; // key
#endif

typedef struct _rbtree_node {
	unsigned char color;
	struct _rbtree_node *right;
	struct _rbtree_node *left;
	struct _rbtree_node *parent;
	KEY_TYPE key;
	void *value;
} rbtree_node;

typedef struct _rbtree {
	rbtree_node *root;
	rbtree_node *nil;
} rbtree;


typedef struct _rbtree kvs_rbtree_t;

int kvs_rbtree_create(kvs_rbtree_t *inst);
void kvs_rbtree_destroy(kvs_rbtree_t *inst);
int kvs_rbtree_set(kvs_rbtree_t *inst, char *key, char *value);
char* kvs_rbtree_get(kvs_rbtree_t *inst, char *key);
int kvs_rbtree_del(kvs_rbtree_t *inst, char *key);
int kvs_rbtree_mod(kvs_rbtree_t *inst, char *key, char *value);
int kvs_rbtree_exist(kvs_rbtree_t *inst, char *key);
void kvs_rbtree_save_snapshot(rbtree *T, FILE *file);
// kvs_hash_save_snapshot的声明将在ENABLE_HASH块中定义



#endif


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


typedef struct hashtable_s {

	hashnode_t **nodes; //* change **,

	int max_slots;
	int count;

} hashtable_t;

typedef struct hashtable_s kvs_hash_t;


int kvs_hash_create(kvs_hash_t *hash);
void kvs_hash_destroy(kvs_hash_t *hash);
int kvs_hash_set(hashtable_t *hash, char *key, char *value);
char* kvs_hash_get(kvs_hash_t *hash, char *key);
int kvs_hash_mod(kvs_hash_t *hash, char *key, char *value);
int kvs_hash_del(kvs_hash_t *hash, char *key);
int kvs_hash_exist(kvs_hash_t *hash, char *key);
void kvs_hash_save_snapshot(kvs_hash_t *hash, FILE *file);


#endif

// // 持久化模式定义
// #define PERSIST_MODE_INCREMENTAL 0
// #define PERSIST_MODE_SNAPSHOT    1

// // 持久化相关函数声明
// int kvs_persist_init(int mode);  // 初始化持久化功能，mode: 0-增量, 1-全量
// int kvs_persist_log_operation(const char *operation, const char *key, const char *value);  // 记录操作日志
// int kvs_persist_replay_log(void);  // 回放日志
// int kvs_persist_close(void);  // 关闭持久化功能，确保日志写入完成

// // 全量持久化相关函数声明
// int kvs_snapshot_save(void);  // 保存数据快照
// int kvs_snapshot_load(void);  // 加载数据快照

// KSF持久化相关函数声明
int ksfSave(const char *filename);  // 保存KSF快照
int ksfSaveBackground(void);  // 后台保存KSF快照
int ksfLoad(const char *filename);  // 加载KSF快照

// AOF相关函数声明
void appendToAofBuffer(int type, const char* key, const char* value);  // 添加到AOF缓冲区
int flushAofBuffer(void);  // 刷新AOF缓冲区
int start_aof_fsync_process(void);  // 启动AOF同步线程
void before_sleep(void);  // 事件循环前的处理函数

#endif



