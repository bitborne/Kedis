#include "kvstore.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/sendfile.h>
#include <fcntl.h>
#include <unistd.h>
#include <pthread.h>
#include <errno.h>
#include <sys/stat.h>
#include <stdio.h>
#include <stdlib.h>

extern int kvs_protocol(char* rmsg, int length, char* response);

// Global replication state
replication_state_t replication_info = {
    .is_master = 1,
    .slave_list = NULL,
    .slave_count = 0,
    .slaves = NULL,
    .lock = PTHREAD_MUTEX_INITIALIZER
};

slave_state_t slave_info = {0};

// Helper to write all data to socket
static int send_all(int fd, const char *buf, size_t len) {
    size_t sent = 0;
    while (sent < len) {
        ssize_t n = send(fd, buf + sent, len - sent, 0);
        if (n <= 0) return -1;
        sent += n;
    }
    return 0;
}

// Helper to read exact bytes
static int recv_exact(int fd, void *buf, size_t len) {
    size_t received = 0;
    char *p = (char*)buf;
    while (received < len) {
        ssize_t n = recv(fd, p + received, len - received, 0);
        if (n <= 0) return -1;
        received += n;
    }
    return 0;
}

// --- MASTER SIDE ---

// Called by kvstore.c when "SYNC" is received
int handle_sync_command(int fd) {
    printf("[Master] SYNC command received from fd %d\n", fd);

    // 1. Create Snapshot
    const char *snap_file = "dump-sync.ksf";
    // Note: ksfSave uses data structures, we are inside kvs_protocol which holds global lock.
    // So it is safe to call ksfSave.
    
    if (ksfSave(snap_file) != 0) {
        return -1;
    }

    // 2. Open and get size
    int file_fd = open(snap_file, O_RDONLY);
    if (file_fd < 0) return -1;
    
    struct stat st;
    fstat(file_fd, &st);
    long file_size = st.st_size;

    // 3. Send Size
    char header[64];
    int header_len = snprintf(header, sizeof(header), "SIZE %ld\r\n", file_size);
    if (send_all(fd, header, header_len) < 0) {
        close(file_fd);
        return -1;
    }

    // 4. Send File
    off_t offset = 0;
    ssize_t sent_bytes;
    while (offset < file_size) {
        sent_bytes = sendfile(fd, file_fd, &offset, file_size - offset);
        if (sent_bytes <= 0) {
            close(file_fd);
            return -1;
        }
    }
    close(file_fd);
    printf("[Master] Snapshot sent to slave.\n");

    // 5. Add to slave list
    replication_client_t *slave = malloc(sizeof(replication_client_t));
    slave->fd = fd;
    slave->state = REPL_STATE_CONNECTED;
    
    pthread_mutex_lock(&replication_info.lock);
    slave->next = replication_info.slaves;
    replication_info.slaves = slave;
    replication_info.slave_count++;
    pthread_mutex_unlock(&replication_info.lock);

    return 0;
}

// Called by AOF/Command logic to broadcast updates
void replication_feed_slaves(const char *cmd) {
    if (!replication_info.is_master) return;
    if (cmd == NULL) return;

    pthread_mutex_lock(&replication_info.lock);
    replication_client_t *slave = replication_info.slaves;
    replication_client_t *prev = NULL;
    
    while (slave) {
        char buf[4096];
        int len = snprintf(buf, sizeof(buf), "%s\r\n", cmd);
        
        if (send_all(slave->fd, buf, len) < 0) {
            // Slave disconnected
            printf("[Master] Slave fd %d disconnected.\n", slave->fd);
            close(slave->fd);
            if (prev) prev->next = slave->next;
            else replication_info.slaves = slave->next;
            
            replication_client_t *tmp = slave;
            slave = slave->next;
            free(tmp);
            replication_info.slave_count--;
        } else {
            prev = slave;
            slave = slave->next;
        }
    }
    pthread_mutex_unlock(&replication_info.lock);
}

// --- SLAVE SIDE ---

void *slave_listen_thread(void *arg) {
    int sock = *(int*)arg;
    free(arg);
    
    char buffer[8192]; 
    char response[8192];
    ssize_t n;
    
    printf("[Slave] Listening thread started.\n");
    
    int buf_len = 0;

    while (1) {
        int space_left = sizeof(buffer) - buf_len - 1;
        if (space_left <= 0) {
             buf_len = 0;
             space_left = sizeof(buffer) - 1;
        }

        n = recv(sock, buffer + buf_len, space_left, 0);
        if (n <= 0) break;
        
        buf_len += n;
        buffer[buf_len] = '\0';
        
        // Process lines
        char *ptr = buffer;
        char *eol;
        
        while ((eol = strstr(ptr, "\r\n")) != NULL) {
            *eol = '\0'; // Terminate string
            
            if (strlen(ptr) > 0) {
                // Execute
                // CONTEXT: We must set current_processing_fd to MASTER fd to bypass read-only check.
                current_processing_fd = slave_info.master_fd; 
                
                // kvs_protocol will take global_kvs_lock.
                kvs_protocol(ptr, strlen(ptr), response);
            }
            
            ptr = eol + 2; // Skip \r\n
        }
        
        // Move remaining data to front
        int remaining = buffer + buf_len - ptr;
        if (remaining > 0) {
            memmove(buffer, ptr, remaining);
        }
        buf_len = remaining;
    }
    
    printf("[Slave] Connection to master lost.\n");
    close(sock);
    exit(1);
    return NULL;
}

int init_slave_replication(const char* master_host, int master_port) {
    printf("[Slave] Connecting to Master %s:%d...\n", master_host, master_port);
    
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    
    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = htons(master_port);
    inet_pton(AF_INET, master_host, &addr.sin_addr);
    
    if (connect(fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("Connect failed");
        return -1;
    }
    
    // Set master fd
    slave_info.master_fd = fd;
    
    // Send SYNC
    if (send_all(fd, "SYNC\r\n", 6) < 0) return -1;
    
    // Expect "SIZE <len>\r\n"
    char buf[256];
    int i = 0;
    while (1) {
        if (recv(fd, &buf[i], 1, 0) <= 0) return -1;
        if (buf[i] == '\n') break;
        i++;
        if (i >= sizeof(buf)-1) return -1;
    }
    buf[i+1] = '\0';
    
    long size = 0;
    if (sscanf(buf, "SIZE %ld", &size) != 1) {
        printf("Invalid SYNC response: %s\n", buf);
        return -1;
    }
    
    printf("[Slave] Receiving Snapshot (%ld bytes)...\n", size);
    
    // Save to temp file
    FILE *fp = fopen("slave_dump.ksf", "wb");
    if (!fp) return -1;
    
    char chunk[4096];
    long received = 0;
    while (received < size) {
        int to_read = (size - received > sizeof(chunk)) ? sizeof(chunk) : (size - received);
        int n = recv(fd, chunk, to_read, 0);
        if (n <= 0) break;
        fwrite(chunk, 1, n, fp);
        received += n;
    }
    fclose(fp);
    
    if (received != size) {
        printf("Incomplete snapshot received.\n");
        return -1;
    }
    
    printf("[Slave] Snapshot received. Loading...\n");
    ksfLoad("slave_dump.ksf");
    
    // Set Role
    replication_info.is_master = 0;
    
    // Start listener thread
    int *arg = malloc(sizeof(int));
    *arg = fd;
    pthread_t tid;
    pthread_create(&tid, NULL, slave_listen_thread, arg);
    pthread_detach(tid);
    
    return 0;
}
