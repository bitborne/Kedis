/*
 * RDMA Client - 接收服务端发送的文件并计算传输速度
 * Usage: ./rdma_client <server_ip> <port> <output_filename>
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <time.h>
#include <rdma/rdma_cma.h>
#include <infiniband/verbs.h>

#define BUFFER_SIZE (64 * 1024 * 1024)  // 64MB buffer
#define CQ_CAPACITY (128)
#define MAX_SGE (1)
#define MAX_WR (128)

typedef struct {
    struct ibv_pd *pd;
    struct ibv_cq *cq;
    struct ibv_comp_channel *comp_channel;
    struct ibv_qp *qp;
    char *send_buf;
    char *recv_buf;
    struct ibv_mr *send_mr;
    struct ibv_mr *recv_mr;
    int out_fd;
    uint64_t file_size;
    uint64_t received;
    double start_time;
} connection_t;

static double get_time_us() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000000.0 + ts.tv_nsec / 1000.0;
}

static int post_send(connection_t *conn, size_t length) {
    struct ibv_sge sge = {
        .addr = (uintptr_t)conn->send_buf,
        .length = length,
        .lkey = conn->send_mr->lkey
    };

    struct ibv_send_wr wr = {
        .wr_id = (uintptr_t)conn,
        .opcode = IBV_WR_SEND,
        .send_flags = IBV_SEND_SIGNALED,
        .sg_list = &sge,
        .num_sge = 1
    };

    struct ibv_send_wr *bad_wr;
    return ibv_post_send(conn->qp, &wr, &bad_wr);
}

static int post_recv(connection_t *conn, size_t length) {
    struct ibv_sge sge = {
        .addr = (uintptr_t)conn->recv_buf,
        .length = length,
        .lkey = conn->recv_mr->lkey
    };

    struct ibv_recv_wr wr = {
        .wr_id = (uintptr_t)conn,
        .sg_list = &sge,
        .num_sge = 1
    };

    struct ibv_recv_wr *bad_wr;
    return ibv_post_recv(conn->qp, &wr, &bad_wr);
}

static int receive_file(connection_t *conn) {
    struct ibv_wc wc;
    int ne;

    // 接收文件大小
    post_recv(conn, sizeof(uint64_t));
    do {
        ne = ibv_poll_cq(conn->cq, 1, &wc);
    } while (ne == 0);

    if (wc.status != IBV_WC_SUCCESS) {
        fprintf(stderr, "Recv file size failed: %s\n", ibv_wc_status_str(wc.status));
        return -1;
    }

    memcpy(&conn->file_size, conn->recv_buf, sizeof(conn->file_size));
    printf("File size: %lu bytes (%.2f MB)\n",
           conn->file_size, conn->file_size / (1024.0 * 1024.0));

    // 发送就绪确认
    strcpy(conn->send_buf, "READY");
    post_send(conn, 5);
    do {
        ne = ibv_poll_cq(conn->cq, 1, &wc);
    } while (ne == 0);

    printf("Receiving file...\n");
    conn->start_time = get_time_us();

    // 接收文件数据
    while (conn->received < conn->file_size) {
        size_t to_recv = (conn->file_size - conn->received) < BUFFER_SIZE ?
                         (conn->file_size - conn->received) : BUFFER_SIZE;

        post_recv(conn, to_recv);
        do {
            ne = ibv_poll_cq(conn->cq, 1, &wc);
        } while (ne == 0);

        if (wc.status != IBV_WC_SUCCESS) {
            fprintf(stderr, "Recv failed: %s\n", ibv_wc_status_str(wc.status));
            break;
        }

        size_t data_len = wc.byte_len;
        ssize_t written = write(conn->out_fd, conn->recv_buf, data_len);
        if (written != data_len) {
            perror("write failed");
            break;
        }

        conn->received += data_len;
    }

    double end_time = get_time_us();
    double elapsed_us = end_time - conn->start_time;
    double elapsed_sec = elapsed_us / 1000000.0;

    // 发送完成确认
    strcpy(conn->send_buf, "DONE");
    post_send(conn, 4);
    do {
        ne = ibv_poll_cq(conn->cq, 1, &wc);
    } while (ne == 0);

    // 计算并输出速度
    double speed_mbps = (conn->received * 8.0) / (elapsed_sec * 1000000.0);
    double speed_mb_per_sec = (conn->received / (1024.0 * 1024.0)) / elapsed_sec;

    printf("\n========== RDMA Transfer Result ==========\n");
    printf("Received: %lu bytes (%.2f MB)\n", conn->received,
           conn->received / (1024.0 * 1024.0));
    printf("Time: %.3f seconds\n", elapsed_sec);
    printf("Speed: %.2f MB/s (%.2f Mbps)\n", speed_mb_per_sec, speed_mbps);
    printf("==========================================\n");

    return 0;
}

int main(int argc, char *argv[]) {
    if (argc != 4) {
        fprintf(stderr, "Usage: %s <server_ip> <port> <output_filename>\n", argv[0]);
        return -1;
    }

    const char *server_ip = argv[1];
    int port = atoi(argv[2]);
    const char *output_filename = argv[3];

    // 创建输出文件
    int out_fd = open(output_filename, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (out_fd < 0) {
        perror("open output file failed");
        return -1;
    }

    // 创建RDMA事件通道
    struct rdma_event_channel *ec = rdma_create_event_channel();
    if (!ec) {
        perror("rdma_create_event_channel failed");
        close(out_fd);
        return -1;
    }

    struct rdma_cm_id *conn_id = NULL;
    if (rdma_create_id(ec, &conn_id, NULL, RDMA_PS_TCP) < 0) {
        perror("rdma_create_id failed");
        rdma_destroy_event_channel(ec);
        close(out_fd);
        return -1;
    }

    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    if (inet_pton(AF_INET, server_ip, &server_addr.sin_addr) <= 0) {
        fprintf(stderr, "Invalid server IP: %s\n", server_ip);
        rdma_destroy_id(conn_id);
        rdma_destroy_event_channel(ec);
        close(out_fd);
        return -1;
    }

    printf("Connecting to %s:%d...\n", server_ip, port);

    if (rdma_resolve_addr(conn_id, NULL, (struct sockaddr *)&server_addr, 2000) < 0) {
        perror("rdma_resolve_addr failed");
        rdma_destroy_id(conn_id);
        rdma_destroy_event_channel(ec);
        close(out_fd);
        return -1;
    }

    connection_t conn = {0};
    conn.out_fd = out_fd;

    struct rdma_cm_event *event;
    while (rdma_get_cm_event(ec, &event) == 0) {
        if (event->event == RDMA_CM_EVENT_ADDR_RESOLVED) {
            if (rdma_resolve_route(conn_id, 2000) < 0) {
                perror("rdma_resolve_route failed");
                rdma_destroy_id(conn_id);
                rdma_destroy_event_channel(ec);
                close(out_fd);
                return -1;
            }

        } else if (event->event == RDMA_CM_EVENT_ROUTE_RESOLVED) {
            // 创建PD
            conn.pd = ibv_alloc_pd(conn_id->verbs);
            if (!conn.pd) {
                perror("ibv_alloc_pd failed");
                rdma_destroy_id(conn_id);
                rdma_destroy_event_channel(ec);
                close(out_fd);
                return -1;
            }

            // 创建完成通道和CQ
            conn.comp_channel = ibv_create_comp_channel(conn_id->verbs);
            if (!conn.comp_channel) {
                perror("ibv_create_comp_channel failed");
                ibv_dealloc_pd(conn.pd);
                rdma_destroy_id(conn_id);
                rdma_destroy_event_channel(ec);
                close(out_fd);
                return -1;
            }

            conn.cq = ibv_create_cq(conn_id->verbs, CQ_CAPACITY, NULL, conn.comp_channel, 0);
            if (!conn.cq) {
                perror("ibv_create_cq failed");
                ibv_destroy_comp_channel(conn.comp_channel);
                ibv_dealloc_pd(conn.pd);
                rdma_destroy_id(conn_id);
                rdma_destroy_event_channel(ec);
                close(out_fd);
                return -1;
            }

            ibv_req_notify_cq(conn.cq, 0);

            // 创建QP
            struct ibv_qp_init_attr qp_attr = {
                .send_cq = conn.cq,
                .recv_cq = conn.cq,
                .qp_type = IBV_QPT_RC,
                .cap = {
                    .max_send_wr = MAX_WR,
                    .max_recv_wr = MAX_WR,
                    .max_send_sge = MAX_SGE,
                    .max_recv_sge = MAX_SGE
                }
            };

            if (rdma_create_qp(conn_id, conn.pd, &qp_attr) < 0) {
                perror("rdma_create_qp failed");
                ibv_destroy_cq(conn.cq);
                ibv_destroy_comp_channel(conn.comp_channel);
                ibv_dealloc_pd(conn.pd);
                rdma_destroy_id(conn_id);
                rdma_destroy_event_channel(ec);
                close(out_fd);
                return -1;
            }

            conn.qp = conn_id->qp;

            // 分配和注册内存
            conn.send_buf = malloc(8);
            conn.recv_buf = malloc(BUFFER_SIZE);
            if (!conn.send_buf || !conn.recv_buf) {
                perror("malloc failed");
                rdma_destroy_qp(conn_id);
                ibv_destroy_cq(conn.cq);
                ibv_destroy_comp_channel(conn.comp_channel);
                ibv_dealloc_pd(conn.pd);
                rdma_destroy_id(conn_id);
                rdma_destroy_event_channel(ec);
                close(out_fd);
                return -1;
            }

            conn.send_mr = ibv_reg_mr(conn.pd, conn.send_buf, 8, IBV_ACCESS_LOCAL_WRITE);
            conn.recv_mr = ibv_reg_mr(conn.pd, conn.recv_buf, BUFFER_SIZE,
                                      IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
            if (!conn.send_mr || !conn.recv_mr) {
                perror("ibv_reg_mr failed");
                free(conn.send_buf);
                free(conn.recv_buf);
                rdma_destroy_qp(conn_id);
                ibv_destroy_cq(conn.cq);
                ibv_destroy_comp_channel(conn.comp_channel);
                ibv_dealloc_pd(conn.pd);
                rdma_destroy_id(conn_id);
                rdma_destroy_event_channel(ec);
                close(out_fd);
                return -1;
            }

            // 连接服务器
            struct rdma_conn_param conn_param = {0};
            if (rdma_connect(conn_id, &conn_param) < 0) {
                perror("rdma_connect failed");
                ibv_dereg_mr(conn.send_mr);
                ibv_dereg_mr(conn.recv_mr);
                free(conn.send_buf);
                free(conn.recv_buf);
                rdma_destroy_qp(conn_id);
                ibv_destroy_cq(conn.cq);
                ibv_destroy_comp_channel(conn.comp_channel);
                ibv_dealloc_pd(conn.pd);
                rdma_destroy_id(conn_id);
                rdma_destroy_event_channel(ec);
                close(out_fd);
                return -1;
            }

        } else if (event->event == RDMA_CM_EVENT_ESTABLISHED) {
            printf("Connected to server\n");
            rdma_ack_cm_event(event);

            // 接收文件
            receive_file(&conn);

            break;

        } else if (event->event == RDMA_CM_EVENT_DISCONNECTED) {
            rdma_ack_cm_event(event);
            break;
        } else {
            fprintf(stderr, "Unexpected event: %d\n", event->event);
            rdma_ack_cm_event(event);
            break;
        }
        rdma_ack_cm_event(event);
    }

    // 清理资源
    if (conn.send_mr) ibv_dereg_mr(conn.send_mr);
    if (conn.recv_mr) ibv_dereg_mr(conn.recv_mr);
    free(conn.send_buf);
    free(conn.recv_buf);
    rdma_destroy_qp(conn_id);
    if (conn.cq) ibv_destroy_cq(conn.cq);
    if (conn.comp_channel) ibv_destroy_comp_channel(conn.comp_channel);
    if (conn.pd) ibv_dealloc_pd(conn.pd);
    rdma_destroy_id(conn_id);
    rdma_destroy_event_channel(ec);
    close(out_fd);

    return 0;
}
