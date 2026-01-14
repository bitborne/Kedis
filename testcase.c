// KVStore 自动化测试框架
// 支持 RESP 协议、多引擎测试

#include <netdb.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/time.h>
#include <unistd.h>

#define MAX_MSG 1024
#define TIME_SUB_MS(tv1, tv2) ((tv1.tv_sec - tv2.tv_sec) * 1000 + (tv1.tv_usec - tv2.tv_usec) / 1000)

/* ==================== 基础网络函数 ==================== */

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

int send_msg(int connfd, char* msg, int length) {
    int res = send(connfd, msg, length, 0);
    if (res < 0) {
        perror("send");
        return -1;
    }
    return res;
}

int recv_msg(int connfd, char* result, int length) {
    int res = recv(connfd, result, length, 0);
    if (res < 0) {
        perror("recv");
        return -1;
    }
    return res;
}

/* ==================== RESP 协议编码/解码 ==================== */

char* encode_to_resp(const char* cmd) {
    if (cmd == NULL || strlen(cmd) == 0) {
        return NULL;
    }

    char* cmd_copy = strdup(cmd);
    if (cmd_copy == NULL) {
        return NULL;
    }

    char* args[64];
    int argc = 0;
    char* token = strtok(cmd_copy, " ");

    while (token != NULL && argc < 64) {
        args[argc++] = token;
        token = strtok(NULL, " ");
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
        static char resp_pattern[256];
        int value_len = len - 2;
        snprintf(resp_pattern, sizeof(resp_pattern), "$%d\r\n%.*s\r\n", value_len, value_len, pattern);
        return resp_pattern;
    }

    return pattern;
}

/* ==================== 测试用例函数 ==================== */

void testcase(int connfd, char* msg, char* pattern, char* casename) {
    if (msg == NULL || pattern == NULL || casename == NULL) return;

    char* resp_msg = encode_to_resp(msg);
    if (resp_msg == NULL) {
        printf("[ERROR] %s: Failed to encode command to RESP\n", casename);
        exit(1);
    }

    send_msg(connfd, resp_msg, strlen(resp_msg));
    free(resp_msg);

    char result[MAX_MSG] = {0};
    recv_msg(connfd, result, MAX_MSG);

    const char* resp_pattern = convert_response_to_resp(pattern);

    if (!strcmp(result, resp_pattern)) {
        printf("[PASS] %s\n", casename);
    } else {
        printf("[FAIL] %s: \n==> '%s' != '%s'\n", casename, result, resp_pattern);
        printf("msg is : %s\n", msg);
        exit(1);
    }
}

/* ==================== 多引擎基础操作测试 ==================== */

void test_multi_engine_operations(int connfd) {
    printf("\n--- Testing Multi-Engine Operations ---\n");

    // Array 引擎测试
    printf("\n  Testing Array Engine...\n");
    testcase(connfd, "ASET a_key1 a_value1", "OK\r\n", "ARRAY-SET-1");
    testcase(connfd, "AGET a_key1", "a_value1\r\n", "ARRAY-GET-1");
    testcase(connfd, "AMOD a_key1 a_value_modified", "OK\r\n", "ARRAY-MOD-1");
    testcase(connfd, "AGET a_key1", "a_value_modified\r\n", "ARRAY-GET-2");
    testcase(connfd, "AEXIST a_key1", "YES, Exist\r\n", "ARRAY-EXIST-1");
    testcase(connfd, "AEXIST a_not_exist", "NO, Not Exist\r\n", "ARRAY-EXIST-2");
    testcase(connfd, "ADEL a_key1", "OK\r\n", "ARRAY-DEL-1");
    testcase(connfd, "AEXIST a_key1", "NO, Not Exist\r\n", "ARRAY-EXIST-3");

    // Hash 引擎测试
    printf("\n  Testing Hash Engine...\n");
    testcase(connfd, "HSET h_key1 h_value1", "OK\r\n", "HASH-SET-1");
    testcase(connfd, "HGET h_key1", "h_value1\r\n", "HASH-GET-1");
    testcase(connfd, "HMOD h_key1 h_value_modified", "OK\r\n", "HASH-MOD-1");
    testcase(connfd, "HGET h_key1", "h_value_modified\r\n", "HASH-GET-2");
    testcase(connfd, "HEXIST h_key1", "YES, Exist\r\n", "HASH-EXIST-1");
    testcase(connfd, "HEXIST h_not_exist", "NO, Not Exist\r\n", "HASH-EXIST-2");
    testcase(connfd, "HDEL h_key1", "OK\r\n", "HASH-DEL-1");
    testcase(connfd, "HEXIST h_key1", "NO, Not Exist\r\n", "HASH-EXIST-3");

    // Rbtree 引擎测试
    printf("\n  Testing Rbtree Engine...\n");
    testcase(connfd, "RSET r_key1 r_value1", "OK\r\n", "RBTREE-SET-1");
    testcase(connfd, "RGET r_key1", "r_value1\r\n", "RBTREE-GET-1");
    testcase(connfd, "RMOD r_key1 r_value_modified", "OK\r\n", "RBTREE-MOD-1");
    testcase(connfd, "RGET r_key1", "r_value_modified\r\n", "RBTREE-GET-2");
    testcase(connfd, "REXIST r_key1", "YES, Exist\r\n", "RBTREE-EXIST-1");
    testcase(connfd, "REXIST r_not_exist", "NO, Not Exist\r\n", "RBTREE-EXIST-2");
    testcase(connfd, "RDEL r_key1", "OK\r\n", "RBTREE-DEL-1");
    testcase(connfd, "REXIST r_key1", "NO, Not Exist\r\n", "RBTREE-EXIST-3");

    // Skiplist 引擎测试
    printf("\n  Testing Skiplist Engine...\n");
    testcase(connfd, "SSET s_key1 s_value1", "OK\r\n", "SKIPLIST-SET-1");
    testcase(connfd, "SGET s_key1", "s_value1\r\n", "SKIPLIST-GET-1");
    testcase(connfd, "SMOD s_key1 s_value_modified", "OK\r\n", "SKIPLIST-MOD-1");
    testcase(connfd, "SGET s_key1", "s_value_modified\r\n", "SKIPLIST-GET-2");
    testcase(connfd, "SEXIST s_key1", "YES, Exist\r\n", "SKIPLIST-EXIST-1");
    testcase(connfd, "SEXIST s_not_exist", "NO, Not Exist\r\n", "SKIPLIST-EXIST-2");
    testcase(connfd, "SDEL s_key1", "OK\r\n", "SKIPLIST-DEL-1");
    testcase(connfd, "SEXIST s_key1", "NO, Not Exist\r\n", "SKIPLIST-EXIST-3");
}

/* ==================== 主函数 ==================== */

int main(int argc, char* argv[]) {
    if (argc != 4) {
        printf("Usage: %s <ip> <port> <test_mode>\n", argv[0]);
        printf("\nTest Modes:\n");
        printf("  basic         - Basic multi-engine operations\n");
        return -1;
    }

    char* ip = argv[1];
    unsigned short port = atoi(argv[2]);
    char* mode = argv[3];

    int connfd = connect_server(ip, port);
    if (connfd < 0) {
        fprintf(stderr, "Failed to connect to server\n");
        return -1;
    }

    printf("Connected to %s:%d\n", ip, port);
    printf("Running test mode: %s\n", mode);

    if (strcmp(mode, "basic") == 0) {
        test_multi_engine_operations(connfd);
    } else {
        fprintf(stderr, "Unknown test mode: %s\n", mode);
        close(connfd);
        return -1;
    }

    close(connfd);
    return 0;
}