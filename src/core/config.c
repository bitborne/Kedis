#include "../../include/kvstore.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <errno.h>


/* 全局配置实例 */
kv_config g_config;

/* 枚举选项定义(用于.conf解析) */
static const char *init_mode_options[] = {"snapshot", "aof", "none", NULL};
static const char *replica_mode_options[] = {"master", "slave", NULL};

/* 临时存储枚举值的变量（用于表驱动绑定） */
static int init_mode_value;
static int replica_mode_value;

/* 配置表：核心！所有配置项在这里注册 */
static config_option config_table[] = {
    /* 网络 */
    {
        "bind", CONFIG_TYPE_STRING, g_config.bind_addr,
        .default_value = "127.0.0.1",
        .description = "绑定IP地址"
    },
    {
        "port", CONFIG_TYPE_INT, &g_config.port,
        .limit.i = {1, 65535},
        .default_value = "8888",
        .description = "监听端口"
    },
    
    /* 日志 */
    {
        "log-level", CONFIG_TYPE_INT, &g_config.log_level,
        .limit.i = {0, 3},
        .default_value = "1",
        .description = "日志级别: 0=DEBUG, 1=INFO, 2=WARN, 3=ERROR"
    },
    {
        "logfile", CONFIG_TYPE_STRING, g_config.logfile,
        .default_value = "", // 默认 stdout
        .description = "指定日志文件名 (如为空字符串, 则日志输出到stdout)"
    },
    
    /* 初始化模式 */
    {
        "init-mode", CONFIG_TYPE_ENUM, &g_config.init_mode,
        .limit.e = {init_mode_options, &init_mode_value},
        .default_value = "snapshot",
        .description = "初始化模式: snapshot/aof/none"
    },
    
    /* 主从模式 */
    {
        "replica-mode", CONFIG_TYPE_ENUM, &g_config.replica_mode,
        .limit.e = {replica_mode_options, &replica_mode_value},
        .default_value = "master",
        .description = "节点角色: master/slave"
    },
    {
        "master-host", CONFIG_TYPE_STRING, g_config.master_host,
        .default_value = "",
        .description = "主节点IP（从节点用）"
    },
    {
        "master-port", CONFIG_TYPE_INT, &g_config.master_port,
        .limit.i = {1, 65535},
        .default_value = "8888",
        .description = "主节点端口（从节点用）"
    },
    {
        "slave-host", CONFIG_TYPE_STRING, g_config.slave_host,
        .default_value = "",
        .description = "从节点IP (主节点用)"
    },
    {
        "slave-port", CONFIG_TYPE_INT, &g_config.slave_port,
        .limit.i = {1, 65535},
        .default_value = "9999",
        .description = "从节点端口（主节点用）"
    },
    /* AOF设置 */
    {
        "aof-enabled", CONFIG_TYPE_BOOL, &g_config.aof_enabled,
        .default_value = "no",
        .description = "是否开启AOF持久化: yes/no"
    },
    /* 自动快照保存设置 */
    {
        "auto-save-enabled", CONFIG_TYPE_BOOL, &g_config.auto_save_enabled,
        .default_value = "yes",
        .description = "是否开启自动快照保存: yes/no"
    },
    { /* 多少秒内变化超过多少次执行快照保存 */
        "auto-save-seconds", CONFIG_TYPE_INT, &g_config.auto_save_seconds,
        .limit.i = {1, INT32_MAX},
        .default_value = "300",
        .description = "自动快照保存的时间窗口长度(单位: s)"
    },
    { /* 多少秒内变化超过多少次执行快照保存 */
        "auto-save-changes", CONFIG_TYPE_INT, &g_config.auto_save_changes,
        .limit.i = {1, INT32_MAX},
        .default_value = "100",
        .description = "每个时间窗口内, 自动执行快照保存的最低变化次数"  
    },
    /* 结束标记 */
    {NULL}
};

/* 辅助函数：去除字符串首尾空白 */
static char *trim(char *str) {
    char *end;
    while (isspace((unsigned char)*str)) str++;
    if (*str == '\0') return str;
    end = str + strlen(str) - 1;
    while (end > str && isspace((unsigned char)*end)) end--;
    end[1] = '\0';
    return str;
}

/* 辅助函数: 去除引号 */
static char *unquote(char *str) {
    size_t len = strlen(str);
    if (len >= 2) {
        // 检查是否被相同引号包裹 "..." 或 '...'
        if ((str[0] == '"' && str[len-1] == '"') ||
            (str[0] == '\'' && str[len-1] == '\'')) {
            str[len-1] = '\0';  // 去掉尾部引号
            return str + 1;      // 跳过首部引号
        }
    }
    return str;
}

/* 辅助函数：解析布尔值 */
static int parse_bool(const char *value, bool *result) {
    if (strcasecmp(value, "yes") == 0 || 
        strcasecmp(value, "on") == 0 || 
        strcasecmp(value, "1") == 0 ||
        strcasecmp(value, "true") == 0) {
        *result = true;
        return 0;
    }
    if (strcasecmp(value, "no") == 0 || 
        strcasecmp(value, "off") == 0 || 
        strcasecmp(value, "0") == 0 ||
        strcasecmp(value, "false") == 0) {
        *result = false;
        return 0;
    }
    return -1;
}

/* 辅助函数：解析枚举 */
static int parse_enum(const char *value, const char **options, int *result) {
    for (int i = 0; options[i] != NULL; i++) {
        if (strcasecmp(value, options[i]) == 0) {
            *result = i;
            return 0;
        }
    }
    return -1;
}

/* 辅助函数：解析带单位的大小 */
static int parse_size(const char *value, uint64_t *result) {
    char *endptr;
    unsigned long long num = strtoull(value, &endptr, 10);
    if (endptr == value) return -1;
    
    while (isspace((unsigned char)*endptr)) endptr++;
    
    if (*endptr == '\0') {
        *result = num;
    } else if (*endptr == 'k' || *endptr == 'K') {
        *result = num * 1024ULL;
    } else if (*endptr == 'm' || *endptr == 'M') {
        *result = num * 1024ULL * 1024ULL;
    } else if (*endptr == 'g' || *endptr == 'G') {
        *result = num * 1024ULL * 1024ULL * 1024ULL;
    } else {
        return -1;
    }
    return 0;
}

/* 设置单个配置项的值 */
static int config_set_value(config_option *opt, const char *value) {
    switch (opt->type) {
        case CONFIG_TYPE_INT: {
            char *endptr;
            long val = strtol(value, &endptr, 10);
            if (*endptr != '\0' || endptr == value) {
                kvs_logError("Invalid integer value: %s\n", value);
                return -1;
            }
            if (val < opt->limit.i.min || val > opt->limit.i.max) {
                kvs_logError("Value out of range [%d, %d]: %ld\n", 
                        opt->limit.i.min, opt->limit.i.max, val);
                return -1;
            }
            *(int *)opt->ptr = (int)val;
            break;
        }
        
        case CONFIG_TYPE_BOOL: {
            bool val;
            if (parse_bool(value, &val) < 0) {
                kvs_logError("Invalid boolean value: %s (expected yes/no)\n", value);
                return -1;
            }
            *(bool *)opt->ptr = val;
            break;
        }
        
        case CONFIG_TYPE_STRING: {
            char *dst = (char *)opt->ptr;
            if (strlen(value) == 0) {
                // 空字符串 = 特殊含义（如 stdout）
                strcpy(dst, "");  // 或设置标志位表示"无日志文件"
            } else {
                strncpy(dst, value, CONFIG_STR_LEN - 1);
                dst[CONFIG_STR_LEN - 1] = '\0'; 
            }
            break;
        }
        
        case CONFIG_TYPE_ENUM: {
            int val;
            if (parse_enum(value, opt->limit.e.options, &val) < 0) {
                kvs_logError("Invalid enum value: %s\n", value);
                return -1;
            }
            *(int *)opt->ptr = val;
            *opt->limit.e.value = val;  /* 同步到临时变量 */
            break;
        }
        
        case CONFIG_TYPE_SIZE: {
            uint64_t val;
            if (parse_size(value, &val) < 0) {
                kvs_logError("Invalid size value: %s\n", value);
                return -1;
            }
            *(uint64_t *)opt->ptr = val;
            break;
        }
        
        default:
            kvs_logError("Unknown config type\n");
            return -1;
    }
    return 0;
}

/* 初始化默认值 */
int kv_config_init(void) {
    memset(&g_config, 0, sizeof(g_config));
    
    for (config_option *opt = config_table; opt->name; opt++) {
        if (config_set_value(opt, opt->default_value) < 0) {
            kvs_logError("Failed to set default for %s\n", opt->name);
            return -1;
        }
    }
    
    /* 同步枚举值到实际变量 */
    g_config.init_mode = init_mode_value;
    g_config.replica_mode = replica_mode_value;
    
    return 0;
}

/* 从文件加载配置 */
int kv_config_load(const char *filename) {
    FILE *fp = fopen(filename, "r");
    if (!fp) {
        kvs_logError("Cannot open config file: %s (%s)\n", 
                filename, strerror(errno));
        return -1;
    }
    
    char line[1024];
    int linenum = 0;
    int errors = 0;
    
    while (fgets(line, sizeof(line), fp)) {
        linenum++;
        char *p = trim(line);
        
        /* 跳过空行和注释 */
        if (*p == '\0' || *p == '#') continue;
        
        /* 查找等号或空格分隔 */
        char *key = p;
        char *value = NULL;
        
        /* 支持 "key value" 和 "key=value" 两种格式 */
        char *space = strchr(p, ' ');
        char *tab = strchr(p, '\t');
        char *equal = strchr(p, '=');
        
        char *sep = NULL;
        if (equal) {
            sep = equal;
        } else if (space && tab) {
            sep = (space < tab) ? space : tab;
        } else if (space) {
            sep = space;
        } else if (tab) {
            sep = tab;
        }
        
        if (sep) {
            *sep = '\0';
            value = trim(sep + 1);
            /* 去除引号 */
            if (value) {
                value = unquote(value);
            }
        }
        key = trim(key);
        
        if (*key == '\0') continue;
        
        /* 查找并设置 */
        int found = 0;
        for (config_option *opt = config_table; opt->name; opt++) {
            if (strcasecmp(opt->name, key) == 0) {
                found = 1;
                if (config_set_value(opt, value ? value : "") < 0) {
                    kvs_logError("Error at %s:%d: %s\n", filename, linenum, key);
                    errors++;
                }
                break;
            }
        }
        
        if (!found) {
            kvs_logError("Warning: Unknown config option at %s:%d: %s\n", 
                    filename, linenum, key);
        }
    }
    
    fclose(fp);
    
    /* 同步枚举值 */
    g_config.init_mode = init_mode_value;
    g_config.replica_mode = replica_mode_value;
    
    return errors ? -1 : 0;
}

/* 加载默认路径的配置文件 */
int kv_config_load_default(void) {
    return kv_config_load(DEFAULT_CONFIG_FILE);
}

/* 打印所有配置（调试用） */
void kv_config_print_all(void) {
    printf("# ============ KVStore Configuration ============\n\n");
    
    for (config_option *opt = config_table; opt->name; opt++) {
        printf("# %s\n", opt->description);
        printf("%s %s\n\n", opt->name, kv_config_get(opt->name));
    }
}

/* 辅助函数：获取初始化模式字符串 */
const char *kv_config_init_mode_str(void) {
    const char *modes[] = {"snapshot", "aof", "none"};
    return modes[g_config.init_mode];
}

/* 获取配置值字符串（用于 CONFIG GET） */
char *kv_config_get(const char *name) {
    static char buf[64];
    
    for (config_option *opt = config_table; opt->name; opt++) {
        if (strcasecmp(opt->name, name) != 0) continue;
        
        switch (opt->type) {
            case CONFIG_TYPE_INT:
                snprintf(buf, sizeof(buf), "%d", *(int *)opt->ptr);
                return buf;
                
            case CONFIG_TYPE_BOOL:
                return *(bool *)opt->ptr ? "yes" : "no";
                
            case CONFIG_TYPE_STRING: {
                char *val = (char *)opt->ptr;
                return (val[0] != '\0') ? val : "\"\"";
            }
                
            case CONFIG_TYPE_ENUM: {
                int idx = *(int *)opt->ptr;
                return (char *)opt->limit.e.options[idx];
            }
                
            case CONFIG_TYPE_SIZE:
                snprintf(buf, sizeof(buf), "%llu", 
                        (unsigned long long)*(uint64_t *)opt->ptr);
                return buf;
                
            default:
                return "unknown";
        }
    }
    return NULL;
}

/* 辅助函数：获取主从模式字符串 */
const char *kv_config_replica_mode_str(void) {
    const char *modes[] = {"master", "slave"};
    return modes[g_config.replica_mode];
}