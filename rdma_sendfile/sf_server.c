/*
 * Sendfile Server - 使用sendfile发送文件给客户端
 * Usage: ./sf_server <port> <filename>
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/sendfile.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/stat.h>

#define BUFFER_SIZE (64 * 1024)

int main(int argc, char *argv[]) {
    if (argc != 3) {
        fprintf(stderr, "Usage: %s <port> <filename>\n", argv[0]);
        return -1;
    }

    int port = atoi(argv[1]);
    const char *filename = argv[2];

    // 打开文件
    int file_fd = open(filename, O_RDONLY);
    if (file_fd < 0) {
        perror("open file failed");
        return -1;
    }

    // 获取文件大小
    struct stat st;
    if (fstat(file_fd, &st) < 0) {
        perror("fstat failed");
        close(file_fd);
        return -1;
    }
    off_t file_size = st.st_size;
    printf("File: %s, Size: %ld bytes (%.2f MB)\n",
           filename, file_size, file_size / (1024.0 * 1024.0));

    // 创建socket
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        perror("socket failed");
        close(file_fd);
        return -1;
    }

    int opt = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        perror("setsockopt failed");
        close(server_fd);
        close(file_fd);
        return -1;
    }

    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    server_addr.sin_addr.s_addr = INADDR_ANY;

    if (bind(server_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        perror("bind failed");
        close(server_fd);
        close(file_fd);
        return -1;
    }

    if (listen(server_fd, 5) < 0) {
        perror("listen failed");
        close(server_fd);
        close(file_fd);
        return -1;
    }

    printf("Sendfile Server listening on port %d...\n", port);

    while (1) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);

        int client_fd = accept(server_fd, (struct sockaddr *)&client_addr, &client_len);
        if (client_fd < 0) {
            perror("accept failed");
            continue;
        }

        printf("\nClient connected from %s:%d\n",
               inet_ntoa(client_addr.sin_addr), ntohs(client_addr.sin_port));

        // 发送文件大小给客户端
        uint64_t net_file_size = htobe64(file_size);
        if (send(client_fd, &net_file_size, sizeof(net_file_size), 0) != sizeof(net_file_size)) {
            perror("send file size failed");
            close(client_fd);
            continue;
        }

        // 等待客户端确认
        char ack[8];
        if (recv(client_fd, ack, sizeof(ack), 0) <= 0) {
            perror("recv ack failed");
            close(client_fd);
            continue;
        }

        // 使用sendfile发送文件
        off_t offset = 0;
        ssize_t sent_total = 0;

        printf("Sending file...\n");

        while (offset < file_size) {
            ssize_t sent = sendfile(client_fd, file_fd, &offset, file_size - offset);
            if (sent < 0) {
                if (errno == EINTR || errno == EAGAIN) {
                    continue;
                }
                perror("sendfile failed");
                break;
            }
            if (sent == 0) {
                break;
            }
            sent_total += sent;
        }

        printf("Sent %ld bytes to client\n", sent_total);

        // 等待客户端传输完成确认
        char done[8];
        recv(client_fd, done, sizeof(done), 0);

        close(client_fd);
        printf("Client disconnected\n");

        // 重置文件位置以便下次传输
        lseek(file_fd, 0, SEEK_SET);
    }

    close(file_fd);
    close(server_fd);
    return 0;
}
