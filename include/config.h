#ifndef __CONFIG_H__
#define __CONFIG_H__

#include <stdint.h>
#include <stdbool.h>

#define CONFIG_STR_LEN 50

/* 配置项类型 */
typedef enum {
    CONFIG_TYPE_INT,        /* 整数 */
    CONFIG_TYPE_BOOL,       /* 布尔 (yes/no, on/off, 1/0) */
    CONFIG_TYPE_STRING,     /* 字符串 */
    CONFIG_TYPE_ENUM,       /* 枚举（有限选项） */
    CONFIG_TYPE_SIZE        /* 字节大小（支持 K/M/G 后缀） */
} config_type_t;

/* 配置项描述结构 */
typedef struct config_option {
    const char *name;           /* 配置项名称 */
    config_type_t type;         /* 数据类型 */
    void *ptr;                  /* 指向实际变量的指针 */
    union {
        struct { int min, max; } i;           /* INT: 范围限制 */
        struct { const char **options; int *value; } e;  /* ENUM: 选项列表 */
    } limit;
    const char *default_value;  /* 默认值（字符串形式） */
    const char *description;    /* 配置说明 */
} config_option;

/* 初始化模式枚举 */
typedef enum {
    INIT_MODE_SNAPSHOT = 0,     /* 从 RDB 快照加载 */
    INIT_MODE_AOF,              /* 从 AOF 日志加载 */
    INIT_MODE_NONE              /* 空库启动 */
} init_mode_t;

/* 主从模式枚举 */
typedef enum {
    REPLICA_MODE_MASTER = 0,    /* 主节点 */
    REPLICA_MODE_SLAVE          /* 从节点 */
} replica_mode_t;

/* 全局配置结构体 */
typedef struct kv_config {
    /* 网络 */
    char bind_addr[CONFIG_STR_LEN];            /* 绑定IP地址 */
    int port;                   /* 监听端口 */
    
    /* 日志 */
    int log_level;              /* 0=DEBUG, 1=INFO, 2=WARN, 3=ERROR */
    char logfile[CONFIG_STR_LEN];
    
    /* 初始化 */
    init_mode_t init_mode;      /* 启动时加载方式 */
    
    /* 主从复制 */
    replica_mode_t replica_mode;    /* 主还是从 */
    char master_host[CONFIG_STR_LEN];          /* 主节点IP（从节点用） */
    int master_port;            /* 主节点端口（从节点用） */
    char slave_host[CONFIG_STR_LEN];
    int slave_port;
    
    /* 持久化 */
    bool aof_enabled;           /* 是否开启AOF */
    bool auto_save_enabled;
    int auto_save_seconds;
    int auto_save_changes;
    
} kv_config;

/* 全局配置实例 */
extern kv_config g_config;

/* 配置文件默认路径 */
#define DEFAULT_CONFIG_FILE "kvstore.conf"

/* 函数声明 */
int kv_config_init(void);
int kv_config_load(const char *filename);
int kv_config_load_default(void);
char* kv_config_get(const char *name);
void kv_config_print_all(void);

/* 辅助函数：获取初始化模式字符串 */
const char *kv_config_init_mode_str(void);
const char *kv_config_replica_mode_str(void);

#endif /* __CONFIG_H__ */