#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#define MB (1024 * 1024)

int main() {
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = htons(8888);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
    
    if (connect(sockfd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("connect");
        return 1;
    }
    
    printf("Connected to server\n");
    
    // 生成 4MB 的 value
    int value_size = 4 * MB;
    char* value = (char*)malloc(value_size);
    if (!value) {
        perror("malloc");
        close(sockfd);
        return 1;
    }
    
    // 填充随机数据
    for (int i = 0; i < value_size; i++) {
        value[i] = 'A' + (i % 26);
    }
    value[value_size - 1] = '\0';
    
    // 构造 RESP 命令: *3\r\n$4\r\nHSET\r\n$4\r\nkey\r\n$<len>\r\n<value>\r\n
    int cmd_len = 4 + 4 + 4 + 4 + 20 + value_size + 2;  // 估算
    char* cmd = (char*)malloc(cmd_len);
    int offset = snprintf(cmd, cmd_len, "*3\r\n$4\r\nHSET\r\n$4\r\nkey\r\n$%d\r\n%s\r\n", value_size, value);
    
    printf("Sending 4MB packet...\n");
    printf("Command size: %d bytes\n", offset);
    
    int sent = send(sockfd, cmd, offset, 0);
    printf("Sent %d bytes\n", sent);
    
    // 接收响应
    char buffer[1024];
    int recv_len = recv(sockfd, buffer, sizeof(buffer), 0);
    if (recv_len > 0) {
        buffer[recv_len] = '\0';
        printf("Received: %.*s\n", recv_len, buffer);
    }
    
    free(value);
    free(cmd);
    close(sockfd);
    
    return 0;
}