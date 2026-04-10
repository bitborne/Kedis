/*
 * RDMA Server - 使用RDMA发送文件给客户端
 * Usage: ./rdma_server <port> <filename>
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/stat.h>
#include <pthread.h>
#include <rdma/rdma_cma.h>
#include <infiniband/verbs.h>

#define BUFFER_SIZE (2 * 1024 * 1024)  // 2MB buffer
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
    int file_fd;
    off_t file_size;
    pthread_t cq_thread;
    int client_connected;
} connection_t;


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

static int post_recv(connection_t *conn) {
    struct ibv_sge sge = {
        .addr = (uintptr_t)conn->recv_buf,
        .length = 8,
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

static int send_file(connection_t *conn) {
    struct ibv_wc wc;
    int ne;

    // 先准备好接收"READY"
    post_recv(conn);

    // 发送文件大小给客户端
    memcpy(conn->send_buf, &conn->file_size, sizeof(conn->file_size));
    if (post_send(conn, sizeof(conn->file_size)) < 0) {
        perror("post_send file_size failed");
        return -1;
    }

    // 等待发送完成
    do {
        ne = ibv_poll_cq(conn->cq, 1, &wc);
    } while (ne == 0);

    printf("File size sent: %ld bytes\n", conn->file_size);

    // 等待客户端准备好（"READY"消息）
    do {
        ne = ibv_poll_cq(conn->cq, 1, &wc);
    } while (ne == 0);

    printf("Client ready, sending file data...\n");

    // 发送文件数据
    off_t offset = 0;
    ssize_t sent_total = 0;

    lseek(conn->file_fd, 0, SEEK_SET);

    while (offset < conn->file_size) {
        size_t to_send = (conn->file_size - offset) < BUFFER_SIZE ?
                         (conn->file_size - offset) : BUFFER_SIZE;

        ssize_t n = read(conn->file_fd, conn->send_buf, to_send);
        if (n <= 0) {
            perror("read file failed");
            break;
        }

        if (post_send(conn, n) < 0) {
            perror("post_send failed");
            break;
        }

        // 等待发送完成
        do {
            ne = ibv_poll_cq(conn->cq, 1, &wc);
        } while (ne == 0);

        if (wc.status != IBV_WC_SUCCESS) {
            fprintf(stderr, "Send failed: %s\n", ibv_wc_status_str(wc.status));
            break;
        }

        offset += n;
        sent_total += n;
    }

    printf("Sent %ld bytes to client\n", sent_total);

    // 等待完成确认
    post_recv(conn);
    do {
        ne = ibv_poll_cq(conn->cq, 1, &wc);
    } while (ne == 0);

    return 0;
}

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

    struct stat st;
    if (fstat(file_fd, &st) < 0) {
        perror("fstat failed");
        close(file_fd);
        return -1;
    }

    printf("File: %s, Size: %ld bytes (%.2f MB)\n",
           filename, st.st_size, st.st_size / (1024.0 * 1024.0));

    // 创建RDMA事件通道
    struct rdma_event_channel *ec = rdma_create_event_channel();
    if (!ec) {
        perror("rdma_create_event_channel failed");
        close(file_fd);
        return -1;
    }

    struct rdma_cm_id *listener = NULL;
    if (rdma_create_id(ec, &listener, NULL, RDMA_PS_TCP) < 0) {
        perror("rdma_create_id failed");
        rdma_destroy_event_channel(ec);
        close(file_fd);
        return -1;
    }

    struct sockaddr_in server_addr = {
        .sin_family = AF_INET,
        .sin_port = htons(port),
        .sin_addr.s_addr = INADDR_ANY
    };

    if (rdma_bind_addr(listener, (struct sockaddr *)&server_addr) < 0) {
        perror("rdma_bind_addr failed");
        rdma_destroy_id(listener);
        rdma_destroy_event_channel(ec);
        close(file_fd);
        return -1;
    }

    if (rdma_listen(listener, 5) < 0) {
        perror("rdma_listen failed");
        rdma_destroy_id(listener);
        rdma_destroy_event_channel(ec);
        close(file_fd);
        return -1;
    }

    printf("RDMA Server listening on port %d...\n", port);

    struct rdma_cm_event *event;
    struct rdma_cm_id *conn_id = NULL;
    connection_t conn = {0};
    conn.file_fd = file_fd;
    conn.file_size = st.st_size;

    while (rdma_get_cm_event(ec, &event) == 0) {
        if (event->event == RDMA_CM_EVENT_CONNECT_REQUEST) {
            conn_id = event->id;

            // 创建PD
            conn.pd = ibv_alloc_pd(conn_id->verbs);
            if (!conn.pd) {
                perror("ibv_alloc_pd failed");
                rdma_reject(conn_id, NULL, 0);
                rdma_ack_cm_event(event);
                continue;
            }

            // 创建完成通道和CQ
            conn.comp_channel = ibv_create_comp_channel(conn_id->verbs);
            if (!conn.comp_channel) {
                perror("ibv_create_comp_channel failed");
                ibv_dealloc_pd(conn.pd);
                rdma_reject(conn_id, NULL, 0);
                rdma_ack_cm_event(event);
                continue;
            }

            conn.cq = ibv_create_cq(conn_id->verbs, CQ_CAPACITY, NULL, conn.comp_channel, 0);
            if (!conn.cq) {
                perror("ibv_create_cq failed");
                ibv_destroy_comp_channel(conn.comp_channel);
                ibv_dealloc_pd(conn.pd);
                rdma_reject(conn_id, NULL, 0);
                rdma_ack_cm_event(event);
                continue;
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
                rdma_reject(conn_id, NULL, 0);
                rdma_ack_cm_event(event);
                continue;
            }

            conn.qp = conn_id->qp;

            // 分配和注册内存
            conn.send_buf = malloc(BUFFER_SIZE);
            conn.recv_buf = malloc(8);
            if (!conn.send_buf || !conn.recv_buf) {
                perror("malloc failed");
                rdma_destroy_qp(conn_id);
                ibv_destroy_cq(conn.cq);
                ibv_destroy_comp_channel(conn.comp_channel);
                ibv_dealloc_pd(conn.pd);
                rdma_reject(conn_id, NULL, 0);
                rdma_ack_cm_event(event);
                continue;
            }

            conn.send_mr = ibv_reg_mr(conn.pd, conn.send_buf, BUFFER_SIZE,
                                      IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ);
            conn.recv_mr = ibv_reg_mr(conn.pd, conn.recv_buf, 8, IBV_ACCESS_LOCAL_WRITE);
            if (!conn.send_mr || !conn.recv_mr) {
                perror("ibv_reg_mr failed");
                free(conn.send_buf);
                free(conn.recv_buf);
                rdma_destroy_qp(conn_id);
                ibv_destroy_cq(conn.cq);
                ibv_destroy_comp_channel(conn.comp_channel);
                ibv_dealloc_pd(conn.pd);
                rdma_reject(conn_id, NULL, 0);
                rdma_ack_cm_event(event);
                continue;
            }

            // 接受连接
            struct rdma_conn_param conn_param = {0};
            if (rdma_accept(conn_id, &conn_param) < 0) {
                perror("rdma_accept failed");
                ibv_dereg_mr(conn.send_mr);
                ibv_dereg_mr(conn.recv_mr);
                free(conn.send_buf);
                free(conn.recv_buf);
                rdma_destroy_qp(conn_id);
                ibv_destroy_cq(conn.cq);
                ibv_destroy_comp_channel(conn.comp_channel);
                ibv_dealloc_pd(conn.pd);
                rdma_ack_cm_event(event);
                continue;
            }

            rdma_ack_cm_event(event);

        } else if (event->event == RDMA_CM_EVENT_ESTABLISHED) {
            printf("\nClient connected\n");
            conn.client_connected = 1;
            rdma_ack_cm_event(event);

            // 发送文件
            send_file(&conn);

            printf("Client disconnected\n\n");
            conn.client_connected = 0;

        } else if (event->event == RDMA_CM_EVENT_DISCONNECTED) {
            rdma_ack_cm_event(event);

            // 清理连接资源
            if (conn.send_mr) ibv_dereg_mr(conn.send_mr);
            if (conn.recv_mr) ibv_dereg_mr(conn.recv_mr);
            free(conn.send_buf);
            free(conn.recv_buf);
            rdma_destroy_qp(conn_id);
            if (conn.cq) ibv_destroy_cq(conn.cq);
            if (conn.comp_channel) ibv_destroy_comp_channel(conn.comp_channel);
            if (conn.pd) ibv_dealloc_pd(conn.pd);
            rdma_destroy_id(conn_id);
            conn_id = NULL;

            // 重置连接状态
            memset(&conn, 0, sizeof(conn));
            conn.file_fd = file_fd;
            conn.file_size = st.st_size;

        } else {
            rdma_ack_cm_event(event);
        }
    }

    close(file_fd);
    rdma_destroy_id(listener);
    rdma_destroy_event_channel(ec);

    return 0;
}
