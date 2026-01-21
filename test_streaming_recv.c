#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

#define TEST_DATA_SIZE (256 * 1024)  // 256KB，超过 IOP_SIZE (128KB)

int main() {
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        perror("socket");
        return 1;
    }

    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = htons(8888);
    addr.sin_addr.s_addr = inet_addr("127.0.0.1");

    if (connect(sockfd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("connect");
        close(sockfd);
        return 1;
    }

    printf("Connected to server\n");

    // 生成 256KB 的随机数据
    char* large_value = malloc(TEST_DATA_SIZE);
    if (!large_value) {
        perror("malloc");
        close(sockfd);
        return 1;
    }

    for (int i = 0; i < TEST_DATA_SIZE; i++) {
        large_value[i] = 'A' + (i % 26);
    }

    // 构造 HSET 命令：*3\r\n$4\r\nHSET\r\n$8\r\ntest_key\r\n$<len>\r\n<data>\r\n
    char cmd[256];
    int cmd_len = snprintf(cmd, sizeof(cmd), "*3\r\n$4\r\nHSET\r\n$8\r\ntest_key\r\n$%d\r\n", TEST_DATA_SIZE);
    send(sockfd, cmd, cmd_len, 0);
    
    // 发送大数据
    send(sockfd, large_value, TEST_DATA_SIZE, 0);
    
    // 发送换行符
    send(sockfd, "\r\n", 2, 0);

    printf("Sent %d bytes of data\n", TEST_DATA_SIZE);

    // 接收响应
    char resp[1024];
    int n = recv(sockfd, resp, sizeof(resp), 0);
    if (n > 0) {
        resp[n] = '\0';
        printf("Received response: %s\n", resp);
    }

    // 构造 HGET 命令：*2\r\n$4\r\nHGET\r\n$8\r\ntest_key\r\n
    char get_cmd[] = "*2\r\n$4\r\nHGET\r\n$8\r\ntest_key\r\n";
    send(sockfd, get_cmd, strlen(get_cmd), 0);

    printf("Sent GET command\n");

    // 接收响应（大数据）
    char* large_resp = malloc(TEST_DATA_SIZE + 1024);
    if (!large_resp) {
        perror("malloc");
        free(large_value);
        close(sockfd);
        return 1;
    }

    int total_received = 0;
    while (1) {
        n = recv(sockfd, large_resp + total_received, TEST_DATA_SIZE + 1024 - total_received, 0);
        if (n <= 0) break;
        total_received += n;
        printf("Received %d bytes (total: %d)\n", n, total_received);
        
        // 检查是否收到完整的响应（应该以 \r\n 结尾）
        if (total_received >= 2 && large_resp[total_received - 2] == '\r' && large_resp[total_received - 1] == '\n') {
            break;
        }
    }

    printf("Total received: %d bytes\n", total_received);

    // 验证数据
    if (total_received >= TEST_DATA_SIZE + 2) {
        // 检查数据是否正确（RESP 格式：$len\r\ndata\r\n）
        char* data_start = strstr(large_resp, "\r\n");
        if (data_start) {
            data_start += 2;  // 跳过 \r\n
            int data_len = total_received - (data_start - large_resp) - 2;  // 减去末尾的 \r\n
            
            if (data_len == TEST_DATA_SIZE) {
                int match = 1;
                for (int i = 0; i < TEST_DATA_SIZE; i++) {
                    if (data_start[i] != large_value[i]) {
                        match = 0;
                        break;
                    }
                }
                
                if (match) {
                    printf("Data verification: SUCCESS\n");
                } else {
                    printf("Data verification: FAILED (data mismatch)\n");
                }
            } else {
                printf("Data verification: FAILED (length mismatch: expected %d, got %d)\n", TEST_DATA_SIZE, data_len);
            }
        } else {
            printf("Data verification: FAILED (invalid RESP format)\n");
        }
    } else {
        printf("Data verification: FAILED (insufficient data)\n");
    }

    free(large_value);
    free(large_resp);
    close(sockfd);

    return 0;
}