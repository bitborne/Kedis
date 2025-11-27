
#ifndef __KV_STORE_H__
#define __KV_STORE_H__
#define HAVE_JEMALLOC
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

#define INIT_LOAD_AOF 0
#define INIT_LOAD_SNAP 1

typedef int (*msg_handler)(char *msg, int length, char *response);


extern int reactor_start(unsigned short port, msg_handler handler);
extern int proactor_start(unsigned short port, msg_handler handler);
extern int ntyco_start(unsigned short port, msg_handler handler);

void* kvs_calloc(size_t num, size_t size);
void *kvs_malloc(size_t size);
void kvs_free(void *ptr);

#if ENABLE_RBTREE

  #define RED				1
  #define BLACK 			2

  #define ENABLE_KEY_CHAR		1

  #define kvs_main_set(inst, key, value) kvs_rbtree_set(inst, key, value)
  #define kvs_main_get(inst, key) kvs_rbtree_get(inst, key)
  #define kvs_main_del(inst, key) kvs_rbtree_del(inst, key)
  #define kvs_main_mod(inst, key, value) kvs_rbtree_mod(inst, key, value)
  #define kvs_main_exist(inst, key) kvs_rbtree_exist(inst, key)
  #define kvs_main_create(inst) kvs_rbtree_create(inst)
  #define kvs_main_destroy(inst) kvs_rbtree_destroy(inst)

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




#elif ENABLE_HASH

  #define MAX_KEY_LEN	128
  #define MAX_VALUE_LEN	512
  #define MAX_TABLE_SIZE	1024

  #define HASH_ENABLE_KEY_POINTER	1

  #define kvs_main_set(inst, key, value) kvs_hash_set(inst, key, value)
  #define kvs_main_get(inst, key) kvs_hash_get(inst, key)
  #define kvs_main_del(inst, key) kvs_hash_del(inst, key)
  #define kvs_main_mod(inst, key, value) kvs_hash_mod(inst, key, value)
  #define kvs_main_exist(inst, key) kvs_hash_exist(inst, key)
  #define kvs_main_create(inst) kvs_hash_create(inst)
  #define kvs_main_destroy(inst) kvs_hash_destroy(inst)

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



#elif ENABLE_ARRAY
  #define KVS_ARRAY_SIZE 16384

  #define kvs_main_set(inst, key, value) kvs_array_set(inst, key, value)
  #define kvs_main_get(inst, key) kvs_array_get(inst, key)
  #define kvs_main_del(inst, key) kvs_array_del(inst, key)
  #define kvs_main_mod(inst, key, value) kvs_array_mod(inst, key, value)
  #define kvs_main_exist(inst, key) kvs_array_exist(inst, key)
  #define kvs_main_create(inst) kvs_array_create(inst)
  #define kvs_main_destroy(inst) kvs_array_destroy(inst)


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




// KSF持久化相关函数声明
int ksfSave(const char *filename);  // 保存KSF快照
int ksfSaveBackground(void);  // 后台保存KSF快照
int ksfLoad(const char *filename);  // 加载KSF快照

// AOF相关函数声明
void appendToAofBuffer(int type, const char* key, const char* value);  // 添加到AOF缓冲区
int flushAofBuffer(void);  // 刷新AOF缓冲区
int start_aof_fsync_process(void);  // 启动AOF同步线程
void before_sleep(void);  // 事件循环前的处理函数
int aofLoad(const char* filename);

// 复制相关函数声明
int handle_sync_command(int fd);
int init_slave_replication(const char* master_host, int master_port);
int slave_state_machine();
void replication_send_aof_loop();
void check_and_cleanup_replication_clients();

// 从节点状态结构定义
typedef enum {
    REPL_STATE_CONNECTING = 0,
    REPL_STATE_LOADING_KSF,
    REPL_STATE_REPLAYING_AOF,
    REPL_STATE_CONNECTED
} repl_state_t;

typedef struct slave_state {
    repl_state_t state;
    char master_host[256];
    int master_port;
    int master_fd;
    time_t last_reconnect_time;
    off_t current_aof_offset;
    int is_loading_snapshot;
    char *temp_ksf_file;
} slave_state_t;

// 复制客户端结构定义
typedef struct replication_client {
    int fd;
    repl_state_t state;
    time_t last_io_time;
    off_t sent_aof_offset;
    off_t acked_aof_offset;
    struct replication_client *next;
} replication_client_t;

typedef struct replication_state {
    int is_master;
    char **slave_list;
    int slave_count;
    replication_client_t *slaves;
    pthread_mutex_t lock;
    off_t aof_keep_size;
} replication_state_t;

extern replication_state_t replication_info;
extern slave_state_t slave_info;

extern pthread_mutex_t global_kvs_lock;
extern __thread int current_processing_fd;
void replication_feed_slaves(const char *cmd);

#endif



