#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

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

int main() {
    int connfd = connect_server("127.0.0.1", 8888);
    if (connfd < 0) {
        fprintf(stderr, "Failed to connect\n");
        return -1;
    }

    printf("Connected to server\n");

    // 创建一个 20KB 的 value
    int value_size = 20 * 1024;  // 20KB
    char* large_value = (char*)malloc(value_size + 1);
    memset(large_value, 'A', value_size);
    large_value[value_size] = '\0';

    printf("Sending 20KB packet...\n");

    // 构造 RESP 命令: *3\r\n$4\r\nHSET\r\n$3\r\nkey\r\n$<len>\r\n<data>\r\n
    int cmd_len = snprintf(NULL, 0, "*3\r\n$4\r\nHSET\r\n$3\r\nkey\r\n$%d\r\n%s\r\n", value_size, large_value);
    char* cmd = (char*)malloc(cmd_len + 1);
    snprintf(cmd, cmd_len + 1, "*3\r\n$4\r\nHSET\r\n$3\r\nkey\r\n$%d\r\n%s\r\n", value_size, large_value);

    printf("Command size: %d bytes\n", cmd_len);

    // 发送命令
    int sent = send(connfd, cmd, cmd_len, 0);
    printf("Sent %d bytes\n", sent);

    // 接收响应
    char buffer[1024] = {0};
    int recv_len = recv(connfd, buffer, sizeof(buffer) - 1, 0);
    if (recv_len > 0) {
        buffer[recv_len] = '\0';
        printf("Received: %s\n", buffer);
    } else {
        printf("No response received\n");
    }

    free(cmd);
    free(large_value);
    close(connfd);
    return 0;
}