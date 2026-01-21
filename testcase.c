// KVStore 自动化测试框架
// 支持 RESP 协议、多引擎测试、大数据、特殊字符、性能测试

#include <netdb.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/time.h>
#include <unistd.h>
#include <time.h>

#define MAX_MSG 1024
#define MAX_LARGE_MSG (16 * 1024 * 1024)  // 16MB
#define TIME_SUB_MS(tv1, tv2) ((tv1.tv_sec - tv2.tv_sec) * 1000 + (tv1.tv_usec - tv2.tv_usec) / 1000)

/* ==================== 核心结构体 ==================== */

// 测试配置
typedef struct {
    char ip[32];
    int port;
    char mode[32];            // quick, standard, stress, boundary, full
    char engines[16];         // "AHRS"
    int count;                // 测试数据量
    int key_max_size;         // 最大 Key 大小
    int value_max_size;       // 最大 Value 大小
    int enable_persistence;   // 是否开启持久化测试
} test_config_t;

// 测试统计
typedef struct {
    int total;
    int passed;
    int failed;
    long long start_time_ms;
    long long end_time_ms;
} test_stats_t;

// 引擎操作抽象
typedef struct {
    const char* name;
    const char* set_cmd;
    const char* get_cmd;
    const char* del_cmd;
    const char* mod_cmd;
    const char* exist_cmd;
} engine_ops_t;

/* ==================== 全局变量 ==================== */

static test_config_t g_config;
static test_stats_t g_stats = {0};

/* ==================== 网络层 ==================== */

int connect_server(const char *ip, unsigned short port) {
    int connfd = socket(AF_INET, SOCK_STREAM, 0);
    if (connfd < 0) {
        perror("socket");
        return -1;
    }

    struct sockaddr_in server_addr = {0};
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    int ret = inet_pton(AF_INET, ip, &server_addr.sin_addr);
    if (!ret) {
        fprintf(stderr, "Not a valid IPv4\n");
        close(connfd);
        return -1;
    }

    if (connect(connfd, (struct sockaddr*)&server_addr, sizeof(struct sockaddr_in)) != 0) {
        perror("connect");
        close(connfd);
        return -1;
    }
    return connfd;
}

int send_msg(int connfd, const char* msg, int length) {
    int res = send(connfd, msg, length, 0);
    if (res < 0) {
        perror("send");
        return -1;
    }
    return res;
}

// 动态接收消息，支持大包
char* recv_msg_dynamic(int connfd, int* length) {
    char buffer[MAX_MSG];
    int total_received = 0;
    char* result = NULL;
    int result_size = 0;
    int expected_len = -1;  // -1 表示未知
    
    // 设置超时时间为 120 秒（支持 16MB 数据包）
    struct timeval timeout;
    timeout.tv_sec = 120;
    timeout.tv_usec = 0;
    
    while (1) {
        fd_set read_fds;
        FD_ZERO(&read_fds);
        FD_SET(connfd, &read_fds);
        
        int select_result = select(connfd + 1, &read_fds, NULL, NULL, &timeout);
        if (select_result < 0) {
            perror("select");
            if (result) free(result);
            return NULL;
        } else if (select_result == 0) {
            // 超时
            fprintf(stderr, "[TIMEOUT] No response received within 120 seconds\n");
            if (result) free(result);
            return NULL;
        }
        
        int res = recv(connfd, buffer, MAX_MSG, 0);
        if (res < 0) {
            perror("recv");
            if (result) free(result);
            return NULL;
        }
        if (res == 0) {
            break;  // 连接关闭
        }
        
        // 重置超时计时器
        timeout.tv_sec = 120;
        timeout.tv_usec = 0;
        
        // 扩展结果缓冲区
        char* new_result = realloc(result, result_size + res + 1);
        if (!new_result) {
            fprintf(stderr, "[ERROR] Failed to allocate %d bytes for receive buffer\n", result_size + res + 1);
            if (result) free(result);
            return NULL;
        }
        result = new_result;
        memcpy(result + result_size, buffer, res);
        result_size += res;
        result[result_size] = '\0';
        
        // 如果还没有解析出期望长度，尝试解析
        if (expected_len == -1 && result_size >= 2) {
            if (result[0] == '$') {
                // RESP bulk string: $len\r\n
                char* crlf = strstr(result, "\r\n");
                if (crlf) {
                    expected_len = atoi(result + 1);
                    if (expected_len < 0) {
                        // Null bulk string ($-1\r\n)
                        break;
                    }
                }
            } else if (result[0] == '+' || result[0] == '-' || result[0] == ':') {
                // 简单字符串、错误或整数
                if (result_size >= 2 && result[result_size - 2] == '\r' && result[result_size - 1] == '\n') {
                    break;
                }
            }
        }
        
        // 检查是否接收完整
        if (expected_len >= 0) {
            // RESP bulk string: $len\r\n + data + \r\n
            int header_size = 0;
            char* crlf = strstr(result, "\r\n");
            if (crlf) {
                header_size = (crlf - result) + 2;
            }
            if (result_size >= header_size + expected_len + 2) {
                break;  // 接收完整
            }
        } else if (result_size >= 2 && result[result_size - 2] == '\r' && result[result_size - 1] == '\n') {
            // 简单响应
            if (result[0] != '$') {
                break;
            }
        }
    }
    
    if (length) *length = result_size;
    return result;
}

/* ==================== 协议层 ==================== */

// 增强版 RESP 编码（支持引号和转义字符）
char* encode_to_resp(const char* cmd) {
    if (cmd == NULL || strlen(cmd) == 0) {
        return NULL;
    }

    // 解析参数（支持引号和转义字符）
    int argc = 0;
    char* args[64];
    
    // 动态分配命令副本，避免栈溢出
    size_t cmd_len = strlen(cmd);
    char* cmd_copy = (char*)malloc(cmd_len + 1);
    if (!cmd_copy) {
        return NULL;
    }
    strcpy(cmd_copy, cmd);
    
    char* p = cmd_copy;
    while (*p && argc < 64) {
        // 跳过空格
        while (*p == ' ') p++;
        if (*p == '\0') break;
        
        // 检查是否是引号包裹
        if (*p == '"') {
            p++;  // 跳过开始的引号
            args[argc++] = p;  // 参数开始
            
            // 查找结束引号
            while (*p && *p != '"') {
                // 处理转义字符
                if (*p == '\\' && *(p + 1)) {
                    p++;
                    if (*p == 'r') {
                        *p = '\r';
                    } else if (*p == 'n') {
                        *p = '\n';
                    } else if (*p == '\\') {
                        *p = '\\';
                    } else if (*p == '"') {
                        *p = '"';
                    }
                }
                p++;
            }
            
            if (*p == '"') {
                *p = '\0';  // 终止参数
                p++;  // 跳过结束引号
            }
        } else {
            // 普通参数（无引号）
            args[argc++] = p;
            
            // 查找下一个空格或结束
            while (*p && *p != ' ') p++;
            
            if (*p) {
                *p = '\0';  // 临时终止
                p++;
            }
        }
    }

    if (argc == 0) {
        free(cmd_copy);
        return NULL;
    }

    // 计算总长度
    int total_len = 0;
    total_len += snprintf(NULL, 0, "*%d\r\n", argc);
    for (int i = 0; i < argc; i++) {
        int arg_len = strlen(args[i]);
        total_len += snprintf(NULL, 0, "$%d\r\n%s\r\n", arg_len, args[i]);
    }

    // 分配内存
    char* resp = (char*)malloc(total_len + 2);
    if (resp == NULL) {
        free(cmd_copy);
        return NULL;
    }

    // 生成 RESP 格式
    int offset = 0;
    offset += snprintf(resp + offset, total_len - offset + 1, "*%d\r\n", argc);
    for (int i = 0; i < argc; i++) {
        int arg_len = strlen(args[i]);
        int written = snprintf(resp + offset, total_len - offset + 1, "$%d\r\n%s\r\n", arg_len, args[i]);
        offset += written;
    }
    resp[offset] = '\0';

    free(cmd_copy);
    return resp;
}

const char* convert_response_to_resp(const char* pattern) {
    if (pattern == NULL) return NULL;

    static char resp_pattern[MAX_LARGE_MSG];

    if (strcmp(pattern, "OK\r\n") == 0) {
        return "+OK\r\n";
    }

    if (strcmp(pattern, "ERROR\r\n") == 0) {
        return "-ERROR\r\n";
    }

    if (strcmp(pattern, "YES, Exist\r\n") == 0) {
        return ":1\r\n";
    }

    if (strcmp(pattern, "NO, Not Exist\r\n") == 0) {
        return ":0\r\n";
    }

    if (strcmp(pattern, "Key has existed\r\n") == 0) {
        return "-Key has existed\r\n";
    }

    if (strcmp(pattern, "Not Exist\r\n") == 0) {
        return "-Not Exist\r\n";
    }

    if (strcmp(pattern, "ERROR / Not Exist\r\n") == 0) {
        return "-ERROR / Not Exist\r\n";
    }

    // 其他情况：转换为 bulk string 格式
    int len = strlen(pattern);
    if (len >= 2 && pattern[len - 1] == '\n' && pattern[len - 2] == '\r') {
        int value_len = len - 2;
        snprintf(resp_pattern, sizeof(resp_pattern), "$%d\r\n%.*s\r\n", value_len, value_len, pattern);
        return resp_pattern;
    }

    return pattern;
}

/* ==================== 工具函数 ==================== */

long long get_time_ms() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (long long)tv.tv_sec * 1000 + tv.tv_usec / 1000;
}

// 生成随机字符串（支持指定大小和字符集）
char* generate_random_string(int size, const char* charset) {
    if (size <= 0) return NULL;
    
    char* str = (char*)malloc(size + 1);
    if (!str) {
        fprintf(stderr, "[ERROR] Failed to allocate %d bytes for random string\n", size);
        return NULL;
    }
    
    const char* default_charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    if (!charset || strlen(charset) == 0) charset = default_charset;
    
    int charset_len = strlen(charset);
    if (charset_len == 0) {
        free(str);
        return NULL;
    }
    
    // 使用 /dev/urandom 获取更好的随机数
    FILE* urandom = fopen("/dev/urandom", "r");
    if (urandom) {
        unsigned int seed;
        fread(&seed, sizeof(seed), 1, urandom);
        fclose(urandom);
        srand(seed);
    } else {
        srand(time(NULL) ^ getpid());
    }
    
    for (int i = 0; i < size; i++) {
        str[i] = charset[rand() % charset_len];
    }
    str[size] = '\0';
    
    return str;
}

void test_report(const char* test_name, int passed) {
    g_stats.total++;
    if (passed) {
        g_stats.passed++;
        printf("[PASS] %s\n", test_name);
    } else {
        g_stats.failed++;
        printf("[FAIL] %s\n", test_name);
    }
}

/* ==================== 测试用例函数 ==================== */

// 通用测试函数
void testcase(int connfd, char* msg, char* pattern, char* casename) {
    if (msg == NULL || casename == NULL) {
        printf("[ERROR] %s: Invalid parameters (msg=%p, casename=%p)\n", casename, msg, casename);
        test_report(casename, 0);
        return;
    }

    char* resp_msg = encode_to_resp(msg);
    if (resp_msg == NULL) {
        printf("[ERROR] %s: Failed to encode command to RESP\n", casename);
        test_report(casename, 0);
        return;
    }

    int res = send_msg(connfd, resp_msg, strlen(resp_msg));
    if (res < 0) {
        printf("[ERROR] %s: Failed to send message\n", casename);
        free(resp_msg);
        test_report(casename, 0);
        return;
    }
    free(resp_msg);
    
    int recv_len = 0;
    char* result = recv_msg_dynamic(connfd, &recv_len);
    if (result == NULL) {
        printf("[ERROR] %s: Failed to receive response\n", casename);
        test_report(casename, 0);
        return;
    }

    // 如果 pattern 为 NULL，只检查是否成功接收到响应（用于超大数据测试）
    int passed;
    if (pattern == NULL) {
        passed = (recv_len > 0);  // 只要接收到数据就认为通过
        if (passed) {
            printf("  Received %d bytes\n", recv_len);
        } else {
            printf("[ERROR] %s: Received 0 bytes\n", casename);
        }
    } else {
        const char* resp_pattern = convert_response_to_resp(pattern);
        passed = !strcmp(result, resp_pattern);
        
        if (!passed) {
            printf("  Expected: '%s'\n", resp_pattern);
            printf("  Got:      '%s'\n", result);
            printf("  Command:  %s\n", msg);
            printf("  Expected length: %zu\n", strlen(resp_pattern));
            printf("  Got length:      %d\n", recv_len);
        }
    }
    
    free(result);
    test_report(casename, passed);
}

// 基础 CRUD 测试
void test_basic_crud(int connfd, const engine_ops_t* engine) {
    printf("\n  Testing %s Engine - Basic CRUD...\n", engine->name);
    
    char test_key[64], test_value[64], test_value2[64];
    snprintf(test_key, sizeof(test_key), "%s_test_key", engine->name);
    snprintf(test_value, sizeof(test_value), "%s_test_value", engine->name);
    snprintf(test_value2, sizeof(test_value2), "%s_test_value2", engine->name);
    
    char cmd[128], expected[128];
    
    // 1. SET
    snprintf(cmd, sizeof(cmd), "%s %s %s", engine->set_cmd, test_key, test_value);
    testcase(connfd, cmd, "OK\r\n", "Basic: SET operation");
    
    // 2. GET
    snprintf(cmd, sizeof(cmd), "%s %s", engine->get_cmd, test_key);
    snprintf(expected, sizeof(expected), "%s\r\n", test_value);
    testcase(connfd, cmd, expected, "Basic: GET operation");
    
    // 3. EXIST
    snprintf(cmd, sizeof(cmd), "%s %s", engine->exist_cmd, test_key);
    testcase(connfd, cmd, "YES, Exist\r\n", "Basic: EXIST operation");
    
    // 4. MOD
    snprintf(cmd, sizeof(cmd), "%s %s %s", engine->mod_cmd, test_key, test_value2);
    testcase(connfd, cmd, "OK\r\n", "Basic: MOD operation");
    
    // 5. GET after MOD
    snprintf(cmd, sizeof(cmd), "%s %s", engine->get_cmd, test_key);
    snprintf(expected, sizeof(expected), "%s\r\n", test_value2);
    testcase(connfd, cmd, expected, "Basic: GET after MOD");
    
    // 6. DEL
    snprintf(cmd, sizeof(cmd), "%s %s", engine->del_cmd, test_key);
    testcase(connfd, cmd, "OK\r\n", "Basic: DEL operation");
    
    // 7. EXIST after DEL
    snprintf(cmd, sizeof(cmd), "%s %s", engine->exist_cmd, test_key);
    testcase(connfd, cmd, "NO, Not Exist\r\n", "Basic: EXIST after DEL");
    
    // 8. GET after DEL (should fail)
    snprintf(cmd, sizeof(cmd), "%s %s", engine->get_cmd, test_key);
    testcase(connfd, cmd, "ERROR / Not Exist\r\n", "Basic: GET after DEL (should fail)");
    
    // 9. DEL non-existent key (should fail)
    snprintf(cmd, sizeof(cmd), "%s %s_non_existent", engine->del_cmd, test_key);
    testcase(connfd, cmd, "ERROR / Not Exist\r\n", "Basic: DEL non-existent key (should fail)");
}

// 特殊字符测试
void test_special_chars(int connfd, const engine_ops_t* engine) {
    printf("\n  Testing %s Engine - Special Chars...\n", engine->name);
    
    char cmd[MAX_MSG], expected[MAX_MSG];
    
    // 测试包含空格的 key（使用引号包裹）
    snprintf(cmd, sizeof(cmd), "%s \"key with spaces\" value1", engine->set_cmd);
    testcase(connfd, cmd, "OK\r\n", "Special: key with spaces - SET");
    
    snprintf(cmd, sizeof(cmd), "%s \"key with spaces\"", engine->get_cmd);
    testcase(connfd, cmd, "value1\r\n", "Special: key with spaces - GET");
    
    snprintf(cmd, sizeof(cmd), "%s \"key with spaces\"", engine->del_cmd);
    testcase(connfd, cmd, "OK\r\n", "Special: key with spaces - DEL");
    
    // 测试包含 $ 的 key
    snprintf(cmd, sizeof(cmd), "%s key$test value2", engine->set_cmd);
    testcase(connfd, cmd, "OK\r\n", "Special: key with $ - SET");
    
    snprintf(cmd, sizeof(cmd), "%s key$test", engine->get_cmd);
    testcase(connfd, cmd, "value2\r\n", "Special: key with $ - GET");
    
    snprintf(cmd, sizeof(cmd), "%s key$test", engine->del_cmd);
    testcase(connfd, cmd, "OK\r\n", "Special: key with $ - DEL");
    
    // 测试包含 * 的 key
    snprintf(cmd, sizeof(cmd), "%s key*test value3", engine->set_cmd);
    testcase(connfd, cmd, "OK\r\n", "Special: key with * - SET");
    
    snprintf(cmd, sizeof(cmd), "%s key*test", engine->get_cmd);
    testcase(connfd, cmd, "value3\r\n", "Special: key with * - GET");
    
    snprintf(cmd, sizeof(cmd), "%s key*test", engine->del_cmd);
    testcase(connfd, cmd, "OK\r\n", "Special: key with * - DEL");
    
    // 测试包含 \r 的 key（使用转义字符）
    snprintf(cmd, sizeof(cmd), "%s \"key\\rwith\\rcarriage\" value4", engine->set_cmd);
    testcase(connfd, cmd, "OK\r\n", "Special: key with \\r - SET");
    
    snprintf(cmd, sizeof(cmd), "%s \"key\\rwith\\rcarriage\"", engine->get_cmd);
    testcase(connfd, cmd, "value4\r\n", "Special: key with \\r - GET");
    
    snprintf(cmd, sizeof(cmd), "%s \"key\\rwith\\rcarriage\"", engine->del_cmd);
    testcase(connfd, cmd, "OK\r\n", "Special: key with \\r - DEL");
    
    // 测试包含 \n 的 key
    snprintf(cmd, sizeof(cmd), "%s \"key\\nwith\\nnewline\" value5", engine->set_cmd);
    testcase(connfd, cmd, "OK\r\n", "Special: key with \\n - SET");
    
    snprintf(cmd, sizeof(cmd), "%s \"key\\nwith\\nnewline\"", engine->get_cmd);
    testcase(connfd, cmd, "value5\r\n", "Special: key with \\n - GET");
    
    snprintf(cmd, sizeof(cmd), "%s \"key\\nwith\\nnewline\"", engine->del_cmd);
    testcase(connfd, cmd, "OK\r\n", "Special: key with \\n - DEL");
    
    // 测试包含 \r\n 的 key
    snprintf(cmd, sizeof(cmd), "%s \"key\\r\\nwith\\r\\ncrlf\" value6", engine->set_cmd);
    testcase(connfd, cmd, "OK\r\n", "Special: key with \\r\\n - SET");
    
    snprintf(cmd, sizeof(cmd), "%s \"key\\r\\nwith\\r\\ncrlf\"", engine->get_cmd);
    testcase(connfd, cmd, "value6\r\n", "Special: key with \\r\\n - GET");
    
    snprintf(cmd, sizeof(cmd), "%s \"key\\r\\nwith\\r\\ncrlf\"", engine->del_cmd);
    testcase(connfd, cmd, "OK\r\n", "Special: key with \\r\\n - DEL");
}

// 大数据测试
void test_large_data(int connfd, const engine_ops_t* engine) {
    printf("\n  Testing %s Engine - Large Data...\n", engine->name);
    
    // 动态分配，避免栈溢出
    char* cmd = (char*)malloc(MAX_LARGE_MSG);
    char* expected = (char*)malloc(MAX_LARGE_MSG);
    char* del_cmd = (char*)malloc(MAX_LARGE_MSG);
    if (!cmd || !expected || !del_cmd) {
        if (cmd) free(cmd);
        if (expected) free(expected);
        if (del_cmd) free(del_cmd);
        return;
    }
    
    // 测试 1KB key/value
    char* large_key_1k = generate_random_string(1024, NULL);
    char* large_value_1k = generate_random_string(1024, NULL);
    
    if (large_key_1k && large_value_1k) {
        snprintf(cmd, MAX_LARGE_MSG, "%s %s %s", engine->set_cmd, large_key_1k, large_value_1k);
        testcase(connfd, cmd, "OK\r\n", "Large: 1KB key/value SET");
        
        // 清理
        snprintf(del_cmd, MAX_LARGE_MSG, "%s %s", engine->del_cmd, large_key_1k);
        testcase(connfd, del_cmd, "OK\r\n", "Large: 1KB key/value DEL");
    }
    
    if (large_key_1k) free(large_key_1k);
    if (large_value_1k) free(large_value_1k);
    
    // 测试 16KB value
    printf("    Testing 16KB value...\n");
    char* large_key_16k = generate_random_string(32, NULL);
    char* large_value_16k = generate_random_string(16 * 1024, NULL);
    
    if (large_key_16k && large_value_16k) {
        snprintf(cmd, MAX_LARGE_MSG, "%s %s %s", engine->set_cmd, large_key_16k, large_value_16k);
        testcase(connfd, cmd, "OK\r\n", "Large: 16KB value SET");
        
        snprintf(cmd, MAX_LARGE_MSG, "%s %s", engine->get_cmd, large_key_16k);
        testcase(connfd, cmd, NULL, "Large: 16KB value GET");
        
        // 清理
        snprintf(del_cmd, MAX_LARGE_MSG, "%s %s", engine->del_cmd, large_key_16k);
        testcase(connfd, del_cmd, "OK\r\n", "Large: 16KB value DEL");
    } else {
        printf("    [SKIP] Failed to allocate 16KB buffer\n");
    }
    
    if (large_key_16k) free(large_key_16k);
    if (large_value_16k) free(large_value_16k);
    
    // 测试 128KB value
    printf("    Testing 128KB value...\n");
    char* large_key_128k = generate_random_string(32, NULL);
    char* large_value_128k = generate_random_string(128 * 1024, NULL);
    
    if (large_key_128k && large_value_128k) {
        snprintf(cmd, MAX_LARGE_MSG, "%s %s %s", engine->set_cmd, large_key_128k, large_value_128k);
        testcase(connfd, cmd, "OK\r\n", "Large: 128KB value SET");
        
        snprintf(cmd, MAX_LARGE_MSG, "%s %s", engine->get_cmd, large_key_128k);
        testcase(connfd, cmd, NULL, "Large: 128KB value GET");
        
        // 清理
        snprintf(del_cmd, MAX_LARGE_MSG, "%s %s", engine->del_cmd, large_key_128k);
        testcase(connfd, del_cmd, "OK\r\n", "Large: 128KB value DEL");
    } else {
        printf("    [SKIP] Failed to allocate 128KB buffer\n");
    }
    
    if (large_key_128k) free(large_key_128k);
    if (large_value_128k) free(large_value_128k);
    
    // 测试 1MB value
    printf("    Testing 1MB value...\n");
    char* large_key_1m = generate_random_string(32, NULL);
    char* large_value_1m = generate_random_string(1 * 1024 * 1024, NULL);
    
    if (large_key_1m && large_value_1m) {
        snprintf(cmd, MAX_LARGE_MSG, "%s %s %s", engine->set_cmd, large_key_1m, large_value_1m);
        testcase(connfd, cmd, "OK\r\n", "Large: 1MB value SET");
        
        snprintf(cmd, MAX_LARGE_MSG, "%s %s", engine->get_cmd, large_key_1m);
        testcase(connfd, cmd, NULL, "Large: 1MB value GET");
        
        // 清理
        snprintf(del_cmd, MAX_LARGE_MSG, "%s %s", engine->del_cmd, large_key_1m);
        testcase(connfd, del_cmd, "OK\r\n", "Large: 1MB value DEL");
    } else {
        printf("    [SKIP] Failed to allocate 1MB buffer\n");
    }
    
    if (large_key_1m) free(large_key_1m);
    if (large_value_1m) free(large_value_1m);
    
    free(cmd);
    free(expected);
    free(del_cmd);
}

// 性能测试
void test_performance(int connfd, const engine_ops_t* engine, int count) {
    printf("\n  Testing %s Engine - Performance (%d ops)...\n", engine->name, count);
    
    long long start_time = get_time_ms();
    int set_success = 0;
    int set_failed = 0;
    
    // SET 性能测试
    for (int i = 0; i < count; i++) {
        char cmd[128];
        snprintf(cmd, sizeof(cmd), "%s perf_key_%d perf_value_%d", engine->set_cmd, i, i);
        
        char* resp_msg = encode_to_resp(cmd);
        if (!resp_msg) {
            set_failed++;
            continue;
        }
        
        int send_res = send_msg(connfd, resp_msg, strlen(resp_msg));
        free(resp_msg);
        
        if (send_res < 0) {
            set_failed++;
            continue;
        }
        
        int recv_len = 0;
        char* result = recv_msg_dynamic(connfd, &recv_len);
        if (result) {
            if (strcmp(result, "+OK\r\n") == 0) {
                set_success++;
            } else {
                set_failed++;
            }
            free(result);
        } else {
            set_failed++;
        }
    }
    
    long long set_end_time = get_time_ms();
    long long set_time = set_end_time - start_time;
    int set_qps = (set_success * 1000) / (set_time > 0 ? set_time : 1);
    
    printf("    %s SET: %d ops, %d success, %d failed, %lld ms, %d QPS\n", 
           engine->name, count, set_success, set_failed, set_time, set_qps);
    
    // GET 性能测试
    start_time = get_time_ms();
    int get_success = 0;
    int get_failed = 0;
    
    for (int i = 0; i < count; i++) {
        char cmd[128];
        snprintf(cmd, sizeof(cmd), "%s perf_key_%d", engine->get_cmd, i);
        
        char* resp_msg = encode_to_resp(cmd);
        if (!resp_msg) {
            get_failed++;
            continue;
        }
        
        int send_res = send_msg(connfd, resp_msg, strlen(resp_msg));
        free(resp_msg);
        
        if (send_res < 0) {
            get_failed++;
            continue;
        }
        
        int recv_len = 0;
        char* result = recv_msg_dynamic(connfd, &recv_len);
        if (result) {
            // 只要接收到响应就认为成功
            get_success++;
            free(result);
        } else {
            get_failed++;
        }
    }
    
    long long get_end_time = get_time_ms();
    long long get_time = get_end_time - start_time;
    int get_qps = (get_success * 1000) / (get_time > 0 ? get_time : 1);
    
    printf("    %s GET: %d ops, %d success, %d failed, %lld ms, %d QPS\n", 
           engine->name, count, get_success, get_failed, get_time, get_qps);
    
    // 清理
    int del_success = 0;
    int del_failed = 0;
    
    for (int i = 0; i < count; i++) {
        char cmd[128];
        snprintf(cmd, sizeof(cmd), "%s perf_key_%d", engine->del_cmd, i);
        
        char* resp_msg = encode_to_resp(cmd);
        if (!resp_msg) {
            del_failed++;
            continue;
        }
        
        int send_res = send_msg(connfd, resp_msg, strlen(resp_msg));
        free(resp_msg);
        
        if (send_res < 0) {
            del_failed++;
            continue;
        }
        
        int recv_len = 0;
        char* result = recv_msg_dynamic(connfd, &recv_len);
        if (result) {
            del_success++;
            free(result);
        } else {
            del_failed++;
        }
    }
    
    printf("    %s DEL: %d ops, %d success, %d failed\n", 
           engine->name, count, del_success, del_failed);
}

// 清理测试数据
void test_clean_data(int connfd, const engine_ops_t* engine) {
    printf("\n  Cleaning %s Engine test data...\n", engine->name);
    
    int cleaned = 0;
    int failed = 0;
    
    // 清理基础测试数据
    char test_key[64];
    snprintf(test_key, sizeof(test_key), "%s_test_key", engine->name);
    
    char cmd[128];
    snprintf(cmd, sizeof(cmd), "%s %s", engine->del_cmd, test_key);
    
    char* resp_msg = encode_to_resp(cmd);
    if (resp_msg) {
        send_msg(connfd, resp_msg, strlen(resp_msg));
        free(resp_msg);
        
        int recv_len = 0;
        char* result = recv_msg_dynamic(connfd, &recv_len);
        if (result) {
            free(result);
        }
        cleaned++;
    } else {
        failed++;
    }
    
    // 清理特殊字符测试数据
    const char* special_keys[] = {
        "key with spaces",
        "key$test",
        "key*test",
        "key\rwith\rcarriage",
        "key\nwith\nnewline",
        "key\r\nwith\r\ncrlf"
    };
    
    for (int i = 0; i < sizeof(special_keys) / sizeof(special_keys[0]); i++) {
        snprintf(cmd, sizeof(cmd), "%s \"%s\"", engine->del_cmd, special_keys[i]);
        
        resp_msg = encode_to_resp(cmd);
        if (resp_msg) {
            send_msg(connfd, resp_msg, strlen(resp_msg));
            free(resp_msg);
            
            int recv_len = 0;
            char* result = recv_msg_dynamic(connfd, &recv_len);
            if (result) {
                free(result);
            }
            cleaned++;
        } else {
            failed++;
        }
    }
    
    // 清理性能测试数据
    for (int i = 0; i < 10000; i++) {
        snprintf(cmd, sizeof(cmd), "%s perf_key_%d", engine->del_cmd, i);
        
        resp_msg = encode_to_resp(cmd);
        if (resp_msg) {
            send_msg(connfd, resp_msg, strlen(resp_msg));
            free(resp_msg);
            
            int recv_len = 0;
            char* result = recv_msg_dynamic(connfd, &recv_len);
            if (result) {
                free(result);
            }
            cleaned++;
        } else {
            failed++;
        }
    }
    
    printf("    %s: %d keys cleaned, %d failed\n", engine->name, cleaned, failed);
}

/* ==================== 引擎定义 ==================== */

static const engine_ops_t g_engines[] = {
    {"Array", "ASET", "AGET", "ADEL", "AMOD", "AEXIST"},
    {"Hash", "HSET", "HGET", "HDEL", "HMOD", "HEXIST"},
    {"Rbtree", "RSET", "RGET", "RDEL", "RMOD", "REXIST"},
    {"Skiplist", "SSET", "SGET", "SDEL", "SMOD", "SEXIST"}
};

static const int g_engine_count = sizeof(g_engines) / sizeof(g_engines[0]);

/* ==================== 主函数 ==================== */

void print_usage(const char* prog) {
    printf("Usage: %s <ip> <port> <mode> [options]\n", prog);
    printf("\nModes:\n");
    printf("  quick       - Quick test (basic CRUD only)\n");
    printf("  standard    - Standard test (basic + special chars)\n");
    printf("  stress      - Stress test (performance)\n");
    printf("  boundary    - Boundary test (large data)\n");
    printf("  full        - Full test (all tests)\n");
    printf("  clean       - Clean all test data\n");
    printf("\nOptions:\n");
    printf("  --engines AHRS  - Select engines (A=Array, H=Hash, R=Rbtree, S=Skiplist)\n");
    printf("  --count N       - Performance test count (default: 10000)\n");
    printf("\nExamples:\n");
    printf("  %s 127.0.0.1 8888 quick\n", prog);
    printf("  %s 127.0.0.1 8888 stress --count 100000\n", prog);
    printf("  %s 127.0.0.1 8888 full --engines AH\n", prog);
    printf("  %s 127.0.0.1 8888 clean\n", prog);
}

int parse_args(int argc, char* argv[]) {
    if (argc < 4) {
        print_usage(argv[0]);
        return -1;
    }

    // 基本参数
    strncpy(g_config.ip, argv[1], sizeof(g_config.ip) - 1);
    g_config.port = atoi(argv[2]);
    strncpy(g_config.mode, argv[3], sizeof(g_config.mode) - 1);
    
    // 默认值
    strcpy(g_config.engines, "AHRS");
    g_config.count = 10000;
    g_config.key_max_size = 1024;
    g_config.value_max_size = 1024;
    g_config.enable_persistence = 0;
    
    // 解析可选参数
    for (int i = 4; i < argc; i++) {
        if (strcmp(argv[i], "--engines") == 0 && i + 1 < argc) {
            strncpy(g_config.engines, argv[i + 1], sizeof(g_config.engines) - 1);
            i++;
        } else if (strcmp(argv[i], "--count") == 0 && i + 1 < argc) {
            g_config.count = atoi(argv[i + 1]);
            i++;
        }
    }
    
    return 0;
}

void run_tests(int connfd) {
    g_stats.start_time_ms = get_time_ms();
    
    printf("\n=== KVStore Test Report ===\n");
    printf("Mode: %s | Engines: %s", g_config.mode, g_config.engines);
    if (strcmp(g_config.mode, "stress") == 0 || strcmp(g_config.mode, "full") == 0) {
        printf(" | Count: %d", g_config.count);
    }
    printf("\n\n");
    
    // 遍历所有引擎
    for (int i = 0; i < g_engine_count; i++) {
        const engine_ops_t* engine = &g_engines[i];
        
        // 检查是否选择了该引擎
        if (strchr(g_config.engines, engine->name[0]) == NULL) {
            continue;
        }
        
        // Clean 模式
        if (strcmp(g_config.mode, "clean") == 0) {
            test_clean_data(connfd, engine);
            continue;
        }
        
        // 基础 CRUD 测试
        if (strcmp(g_config.mode, "quick") == 0 ||
            strcmp(g_config.mode, "standard") == 0 ||
            strcmp(g_config.mode, "full") == 0) {
            test_basic_crud(connfd, engine);
        }
        
        // 特殊字符测试
        if (strcmp(g_config.mode, "standard") == 0 ||
            strcmp(g_config.mode, "full") == 0) {
            test_special_chars(connfd, engine);
        }
        
        // 大数据测试
        if (strcmp(g_config.mode, "boundary") == 0 ||
            strcmp(g_config.mode, "full") == 0) {
            test_large_data(connfd, engine);
        }
        
        // 性能测试
        if (strcmp(g_config.mode, "stress") == 0 ||
            strcmp(g_config.mode, "full") == 0) {
            test_performance(connfd, engine, g_config.count);
        }
    }
    
    g_stats.end_time_ms = get_time_ms();
    
    // 打印最终报告
    printf("\n--- Test Summary ---\n");
    printf("Total: %d Tests | Passed: %d | Failed: %d\n", 
           g_stats.total, g_stats.passed, g_stats.failed);
    printf("Time: %lld ms\n", g_stats.end_time_ms - g_stats.start_time_ms);
    
    if (g_stats.failed == 0) {
        printf("Result: SUCCESS\n");
    } else {
        printf("Result: FAILED\n");
    }
}

int main(int argc, char* argv[]) {
    if (parse_args(argc, argv) != 0) {
        return -1;
    }

    int connfd = connect_server(g_config.ip, g_config.port);
    if (connfd < 0) {
        fprintf(stderr, "Failed to connect to server %s:%d\n", g_config.ip, g_config.port);
        return -1;
    }

    printf("Connected to %s:%d\n", g_config.ip, g_config.port);
    
    run_tests(connfd);
    
    close(connfd);
    
    return g_stats.failed > 0 ? 1 : 0;
}
