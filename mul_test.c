// 自动化测试--> 代替人工连接+发包

// 1. TCP 客包户端建立连接
// 2. 发送 5 种协议
// 3. 接收数据
// 4. 比对预期数据和实际返回数据
#include <netdb.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/time.h>

#define MAX_MSG 1024
#define TIME_SUB_MS(tv1, tv2)  ((tv1.tv_sec - tv2.tv_sec) * 1000 + (tv1.tv_usec - tv2.tv_usec) / 1000)
struct timeval tv_begin, tv_end;


int connect_server(const char *ip, unsigned short port) {
  int connfd = socket(AF_INET, SOCK_STREAM, 0);

  struct sockaddr_in server_addr = {0};
// 填写ip
  server_addr.sin_family = AF_INET;
  server_addr.sin_port = htons(port);
  int ret = inet_pton(AF_INET, ip, &server_addr.sin_addr);
  if (!ret) printf("Not a valid IPv4\n"); 

  if (connect(connfd, (struct sockaddr*)&server_addr, sizeof(struct sockaddr_in)) != 0) {
    perror("connect");
    return -1;
  }
  return connfd;
}


int send_msg(int connfd, char* msg, int length) {
  int res = send(connfd, msg, length, 0);
  if (res < 0) {
    perror("send");
    exit(1);
  }
  return res;
}

int recv_msg(int connfd, char* result, int length) {
  int res = recv(connfd, result, length, 0);
  if (res < 0) {
    perror("recv");
    exit(1);
  }
  return res;
} 

void testcase(int connfd, char* msg, char* pattern, char* casename) {
  if (msg == NULL || pattern == NULL || casename == NULL) return;

  send_msg(connfd, msg, strlen(msg));

  char result[MAX_MSG] = {0};
  recv_msg(connfd, result, MAX_MSG);

  if (!strcmp(result, pattern)) {
    // printf("[PASS] --> %s\n", casename);
  } else {
    printf("[FALSE] --> %s: \n==> '%s' != '%s'\n", casename, result, pattern);
    printf("msg is : %s\n", msg);
    exit(1);
  }

}

void array_testcase_mix(int connfd) {
  testcase(connfd, "ASET Qbb Schatten", "OK\r\n", "SET-Qbb-0");
  testcase(connfd, "AGET Qbb", "Schatten\r\n", "GET-Qbb-0");
  testcase(connfd, "AMOD Qbb Cc", "OK\r\n", "MOD-Qbb-0");
  testcase(connfd, "AMOD Bqq Cc", "Not Exist\r\n", "MOD-Qbb-1");
  testcase(connfd, "ASET Qbb Schatten", "Key has existed\r\n", "SET-Qbb-1");
  testcase(connfd, "AEXIST Qbb", "YES, Exist\r\n", "EXIST-Qbb-0");
  testcase(connfd, "AGET Qbb", "Cc\r\n", "GET-Qbb-1");
  testcase(connfd, "ADEL Qbb", "OK\r\n", "DEL-Qbb-0");
  testcase(connfd, "AEXIST Qbb", "NO, Not Exist\r\n", "EXIST-Qbb-1");
  testcase(connfd, "AGET Qbb", "ERROR / Not Exist\r\n", "GET-Qbb-2");
  testcase(connfd, "ADEL Qbb", "Not Exist\r\n", "DEL-Qbb-1");
  testcase(connfd, "ADEL EveRything", "Not Exist\r\n", "DEL-Qbb-2");

  testcase(connfd, "ASET Qbb", "ERROR\r\n", "SET-ERROR");
  testcase(connfd, "ADEL", "ERROR\r\n", "DEL-ERROR");
  testcase(connfd, "AMOD 1", "ERROR\r\n", "MOD-ERROR");
  testcase(connfd, "AEXIST", "ERROR\r\n", "EXIST-ERROR");
}

void array_testcase_single_1w(int connfd) {
  gettimeofday(&tv_begin, NULL);
  const int cnt = 10000;
  for (int i = 0; i < cnt; i++) {
    char cmd_set[64] = {0};
    snprintf(cmd_set, 64, "ASET Qbb%d Schatten%d", i, i);
    testcase(connfd, cmd_set, "OK\r\n", "ASET-Qbb-0");
  }
  for (int i = 0; i < cnt; i++) {
    char cmd_get[64] = {0};
    char expect_get[64] = {0};
    snprintf(cmd_get, 64, "AGET Qbb%d", i, i);
    snprintf(expect_get, 64, "Schatten%d\r\n", i);
    testcase(connfd, cmd_get, expect_get, "AGET-Qbb-0");

  }
    
  for (int i = 0; i < cnt; i++) {
    char cmd_del[64] = {0};
    snprintf(cmd_del, 64, "ADEL Qbb%d", i);
    testcase(connfd, cmd_del, "OK\r\n", "ADEL-Qbb-0");
  }
  gettimeofday(&tv_end, NULL);
  int time_used = TIME_SUB_MS(tv_end, tv_begin);
  printf("array_single(3w req) --> time_used: %d ms  QPS: %d\n", time_used, 30000 * 1000 / time_used);

}
void rbtree_testcase_single_1w(int connfd) {

  gettimeofday(&tv_begin, NULL);
  const int cnt = 10000;
  for (int i = 0; i < cnt; i++) {
    char cmd_rset[64] = {0};
    snprintf(cmd_rset, 64, "RSET Qbb%d Schatten%d", i, i);
    testcase(connfd, cmd_rset, "OK\r\n", "RSET-Qbb-0");
  }
  for (int i = 0; i < cnt; i++) {
    char cmd_rget[64] = {0};
    char expect_rget[64] = {0};
    snprintf(cmd_rget, 64, "RGET Qbb%d", i, i);
    snprintf(expect_rget, 64, "Schatten%d\r\n", i);
    testcase(connfd, cmd_rget, expect_rget, "RGET-Qbb-0");

  }
    
  for (int i = 0; i < cnt; i++) {
    char cmd_rdel[64] = {0};
    snprintf(cmd_rdel, 64, "RDEL Qbb%d", i);
    testcase(connfd, cmd_rdel, "OK\r\n", "RDEL-Qbb-0");
  }

  gettimeofday(&tv_end, NULL);
  int time_used = TIME_SUB_MS(tv_end, tv_begin);
  printf("rbtree_single(3w req) --> time_used: %d ms  QPS: %d\n", time_used, 30000 * 1000 / time_used);
}



void hash_testcase_single_1w(int connfd) {

  gettimeofday(&tv_begin, NULL);
  const int cnt = 10000;
  for (int i = 0; i < cnt; i++) {
    char cmd_hset[64] = {0};
    snprintf(cmd_hset, 64, "HSET Qbb%d Schatten%d", i, i);
    testcase(connfd, cmd_hset, "OK\r\n", "HSET-Qbb-0");
  }
  for (int i = 0; i < cnt; i++) {
    char cmd_hget[64] = {0};
    char expect_hget[64] = {0};
    snprintf(cmd_hget, 64, "HGET Qbb%d", i, i);
    snprintf(expect_hget, 64, "Schatten%d\r\n", i);
    testcase(connfd, cmd_hget, expect_hget, "HGET-Qbb-0");

  }
    
  for (int i = 0; i < cnt; i++) {
    char cmd_hdel[64] = {0};
    snprintf(cmd_hdel, 64, "HDEL Qbb%d", i);
    testcase(connfd, cmd_hdel, "OK\r\n", "HDEL-Qbb-0");
  }

  gettimeofday(&tv_end, NULL);
  int time_used = TIME_SUB_MS(tv_end, tv_begin);
  printf("hash_single(3w req) --> time_used: %d ms  QPS: %d\n", time_used, 30000 * 1000 / time_used);
}


void rbtree_testcase_mix(int connfd) {
  testcase(connfd, "RSET Qbb Schatten", "OK\r\n", "RSET-Qbb-0");
  testcase(connfd, "RGET Qbb", "Schatten\r\n", "RGET-Qbb-0");
  testcase(connfd, "RMOD Qbb Cc", "OK\r\n", "RMOD-Qbb-0");
  testcase(connfd, "RMOD Bqq Cc", "Not Exist\r\n", "RMOD-Qbb-1");
  testcase(connfd, "RSET Qbb Schatten", "Key has existed\r\n", "RSET-Qbb-1");
  testcase(connfd, "REXIST Qbb", "YES, Exist\r\n", "REXIST-Qbb-0");
  testcase(connfd, "RGET Qbb", "Cc\r\n", "RGET-Qbb-1");
  testcase(connfd, "RDEL Qbb", "OK\r\n", "RDEL-Qbb-0");
  testcase(connfd, "REXIST Qbb", "NO, Not Exist\r\n", "REXIST-Qbb-1");
  testcase(connfd, "RGET Qbb", "ERROR / Not Exist\r\n", "RGET-Qbb-2");
  testcase(connfd, "RDEL Qbb", "Not Exist\r\n", "RDEL-Qbb-1");
  testcase(connfd, "RDEL EveRything", "Not Exist\r\n", "RDEL-Qbb-2");

  testcase(connfd, "RSET Qbb", "ERROR\r\n", "RSET-ERROR");
  testcase(connfd, "RDEL", "ERROR\r\n", "RDEL-ERROR");
  testcase(connfd, "RMOD 1", "ERROR\r\n", "RMOD-ERROR");
  testcase(connfd, "REXIST", "ERROR\r\n", "REXIST-ERROR");
}

void hash_testcase_mix(int connfd) {
  testcase(connfd, "HSET Qbb Schatten", "OK\r\n", "HSET-Qbb-0");
  testcase(connfd, "HGET Qbb", "Schatten\r\n", "HGET-Qbb-0");
  testcase(connfd, "HMOD Qbb Cc", "OK\r\n", "HMOD-Qbb-0");
  testcase(connfd, "HMOD Bqq Cc", "Not Exist\r\n", "HMOD-Qbb-1");
  testcase(connfd, "HSET Qbb Schatten", "Key has existed\r\n", "HSET-Qbb-1");
  testcase(connfd, "HEXIST Qbb", "YES, Exist\r\n", "HEXIST-Qbb-0");
  testcase(connfd, "HGET Qbb", "Cc\r\n", "HGET-Qbb-1");
  testcase(connfd, "HDEL Qbb", "OK\r\n", "HDEL-Qbb-0");
  testcase(connfd, "HEXIST Qbb", "NO, Not Exist\r\n", "HEXIST-Qbb-1");
  testcase(connfd, "HGET Qbb", "ERROR / Not Exist\r\n", "HGET-Qbb-2");
  testcase(connfd, "HDEL Qbb", "Not Exist\r\n", "HDEL-Qbb-1");
  testcase(connfd, "HDEL EveRything", "Not Exist\r\n", "HDEL-Qbb-2");
  testcase(connfd, "HSET Qbb", "ERROR\r\n", "HSET-ERROR");
  testcase(connfd, "HDEL", "ERROR\r\n", "HDEL-ERROR");
  testcase(connfd, "HMOD 1", "ERROR\r\n", "HMOD-ERROR");
  testcase(connfd, "HEXIST", "ERROR\r\n", "HEXIST-ERROR");
}


void array_testcase_1w_mix(int connfd) {

  const int cnt = 10000;

  gettimeofday(&tv_begin, NULL);
  for (int i = 0; i < cnt; i++) {
    array_testcase_mix(connfd);
  }
  gettimeofday(&tv_end, NULL);
  int time_used = TIME_SUB_MS(tv_end, tv_begin);
  printf("array_testcase_1w (160w REQ) --> time_used: %d ms  QPS: %d\n", time_used, 160000 * 1000 / time_used);
  
}

void rbtree_testcase_1w_mix(int connfd) {

  const int cnt = 10000;

  gettimeofday(&tv_begin, NULL);
  for (int i = 0; i < cnt; i++) {
    rbtree_testcase_mix(connfd);
  }
  gettimeofday(&tv_end, NULL);
  int time_used = TIME_SUB_MS(tv_end, tv_begin);
  printf("rbtree_testcase_1w (160w REQ) --> time_used: %d ms  QPS: %d\n", time_used, 160000 * 1000 / time_used);
  
}


void hash_testcase_1w_mix(int connfd) {

  const int cnt = 10000;

  gettimeofday(&tv_begin, NULL);
  for (int i = 0; i < cnt; i++) {
    hash_testcase_mix(connfd);
  }
  gettimeofday(&tv_end, NULL);
  int time_used = TIME_SUB_MS(tv_end, tv_begin);
  printf("hash_testcase_1w (160w REQ) --> time_used: %d ms  QPS: %d\n", time_used, 160000 * 1000 / time_used);
  
}

int main(int argc, char* argv[]) {

  if (argc != 4) {
    perror("arg error\n");
    return -1;
  }

  char* ip = argv[1];
  unsigned short port = atoi(argv[2]);

  int connfd = connect_server(ip, port);
  
  int mode = atoi(argv[3]);
  if (mode == 0) {
    // array_testcase_1w_mix(connfd);
    array_testcase_single_1w(connfd);
  } else if (mode == 1) {
    // rbtree_testcase_1w_mix(connfd);
    rbtree_testcase_single_1w(connfd);
  } else if (mode == 2) {
    hash_testcase_single_1w(connfd);
  } else {
    printf("ARG: ERROR\n");
    return -1;
  }
  return 0;
}
