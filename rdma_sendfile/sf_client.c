/*
 * Sendfile Client - 接收服务端发送的文件并计算传输速度
 * Usage: ./sf_client <server_ip> <port> <output_filename>
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <time.h>

#define BUFFER_SIZE (64 * 1024)

static double get_time_us() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000000.0 + ts.tv_nsec / 1000.0;
}

int main(int argc, char *argv[]) {
    if (argc != 4) {
        fprintf(stderr, "Usage: %s <server_ip> <port> <output_filename>\n", argv[0]);
        return -1;
    }

    const char *server_ip = argv[1];
    int port = atoi(argv[2]);
    const char *output_filename = argv[3];

    // 创建socket
    int sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (sock_fd < 0) {
        perror("socket failed");
        return -1;
    }

    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    if (inet_pton(AF_INET, server_ip, &server_addr.sin_addr) <= 0) {
        fprintf(stderr, "Invalid server IP: %s\n", server_ip);
        close(sock_fd);
        return -1;
    }

    printf("Connecting to %s:%d...\n", server_ip, port);

    if (connect(sock_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        perror("connect failed");
        close(sock_fd);
        return -1;
    }

    printf("Connected to server\n");

    // 接收文件大小
    uint64_t net_file_size;
    if (recv(sock_fd, &net_file_size, sizeof(net_file_size), MSG_WAITALL) != sizeof(net_file_size)) {
        perror("recv file size failed");
        close(sock_fd);
        return -1;
    }
    uint64_t file_size = be64toh(net_file_size);
    printf("File size: %lu bytes (%.2f MB)\n", file_size, file_size / (1024.0 * 1024.0));

    // 创建输出文件
    int out_fd = open(output_filename, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (out_fd < 0) {
        perror("open output file failed");
        close(sock_fd);
        return -1;
    }

    // 发送确认
    send(sock_fd, "READY", 5, 0);

    // 接收文件数据并计算速度
    char buffer[BUFFER_SIZE];
    uint64_t received = 0;
    double start_time = get_time_us();

    printf("Receiving file...\n");

    while (received < file_size) {
        size_t to_read = (file_size - received) < BUFFER_SIZE ? (file_size - received) : BUFFER_SIZE;
        ssize_t n = recv(sock_fd, buffer, to_read, 0);
        if (n < 0) {
            if (errno == EINTR || errno == EAGAIN) {
                continue;
            }
            perror("recv failed");
            break;
        }
        if (n == 0) {
            break;
        }

        ssize_t written = write(out_fd, buffer, n);
        if (written != n) {
            perror("write failed");
            break;
        }
        received += n;
    }

    double end_time = get_time_us();
    double elapsed_us = end_time - start_time;
    double elapsed_sec = elapsed_us / 1000000.0;

    // 发送完成确认
    send(sock_fd, "DONE", 4, 0);

    close(out_fd);
    close(sock_fd);

    // 计算并输出速度
    double speed_mbps = (received * 8.0) / (elapsed_sec * 1000000.0);  // Mbps
    double speed_mb_per_sec = (received / (1024.0 * 1024.0)) / elapsed_sec;  // MB/s

    printf("\n========== Sendfile Transfer Result ==========\n");
    printf("Received: %lu bytes (%.2f MB)\n", received, received / (1024.0 * 1024.0));
    printf("Time: %.3f seconds\n", elapsed_sec);
    printf("Speed: %.2f MB/s (%.2f Mbps)\n", speed_mb_per_sec, speed_mbps);
    printf("==============================================\n");

    return 0;
}
