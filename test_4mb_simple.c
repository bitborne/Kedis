#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/time.h>

long long get_time_ms() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec * 1000LL + tv.tv_usec / 1000;
}

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

    // 生成 4MB 数据
    size_t data_size = 4 * 1024 * 1024;
    char* data = (char*)malloc(data_size);
    if (!data) {
        perror("malloc");
        close(sockfd);
        return 1;
    }

    // 填充数据
    for (size_t i = 0; i < data_size; i++) {
        data[i] = 'A' + (i % 26);
    }
    data[data_size - 1] = '\0';

    // 构建 RESP 命令: *3\r\n$4\r\nHSET\r\n$8\r\ntest_key\r\n$4194304\r\n<data>\r\n
    // 计算总长度
    size_t cmd_len = 4 + 2 + 4 + 2 + 4 + 2 + 8 + 2 + 8 + 2 + data_size + 2;
    char* cmd = (char*)malloc(cmd_len);
    if (!cmd) {
        perror("malloc");
        free(data);
        close(sockfd);
        return 1;
    }

    char* p = cmd;
    sprintf(p, "*3\r\n"); p += 4;
    sprintf(p, "$4\r\n"); p += 4;
    sprintf(p, "HSET\r\n"); p += 6;
    sprintf(p, "$8\r\n"); p += 4;
    sprintf(p, "test_key\r\n"); p += 10;
    sprintf(p, "$4194304\r\n"); p += 12;
    memcpy(p, data, data_size); p += data_size;
    sprintf(p, "\r\n"); p += 2;

    printf("Sending 4MB data...\n");
    long long start_time = get_time_ms();

    // 发送数据
    size_t sent = 0;
    while (sent < cmd_len) {
        ssize_t n = send(sockfd, cmd + sent, cmd_len - sent, 0);
        if (n < 0) {
            perror("send");
            free(data);
            free(cmd);
            close(sockfd);
            return 1;
        }
        sent += n;
    }

    long long send_time = get_time_ms();
    printf("Sent %zu bytes in %lld ms\n", sent, send_time - start_time);

    // 接收响应
    char resp[1024];
    ssize_t n = recv(sockfd, resp, sizeof(resp) - 1, 0);
    if (n < 0) {
        perror("recv");
    } else {
        resp[n] = '\0';
        printf("Received: %s\n", resp);
    }

    long long recv_time = get_time_ms();
    printf("Total time: %lld ms\n", recv_time - start_time);

    free(data);
    free(cmd);
    close(sockfd);

    return 0;
}
