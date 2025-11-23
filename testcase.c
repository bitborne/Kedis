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
  testcase(connfd, "SET Qbb Schatten", "OK\r\n", "SET-Qbb-0");
  testcase(connfd, "GET Qbb", "Value: Schatten\r\n", "GET-Qbb-0");
  testcase(connfd, "MOD Qbb Cc", "OK\r\n", "MOD-Qbb-0");
  testcase(connfd, "MOD Bqq Cc", "Not Exist\r\n", "MOD-Qbb-1");
  testcase(connfd, "SET Qbb Schatten", "Key has existed\r\n", "SET-Qbb-1");
  testcase(connfd, "EXIST Qbb", "YES, Exist\r\n", "EXIST-Qbb-0");
  testcase(connfd, "GET Qbb", "Value: Cc\r\n", "GET-Qbb-1");
  testcase(connfd, "DEL Qbb", "OK\r\n", "DEL-Qbb-0");
  testcase(connfd, "EXIST Qbb", "NO, Not Exist\r\n", "EXIST-Qbb-1");
  testcase(connfd, "GET Qbb", "ERROR / Not Exist\r\n", "GET-Qbb-2");
  testcase(connfd, "DEL Qbb", "Not Exist\r\n", "DEL-Qbb-1");
  testcase(connfd, "DEL EveRything", "Not Exist\r\n", "DEL-Qbb-2");

  testcase(connfd, "SET Qbb", "ERROR\r\n", "SET-ERROR");
  testcase(connfd, "DEL", "ERROR\r\n", "DEL-ERROR");
  testcase(connfd, "MOD 1", "ERROR\r\n", "MOD-ERROR");
  testcase(connfd, "EXIST", "ERROR\r\n", "EXIST-ERROR");
}

void array_testcase_single_1w(int connfd) {
  gettimeofday(&tv_begin, NULL);
  const int cnt = 10000;
  for (int i = 0; i < cnt; i++) {
    char cmd_set[64] = {0};
    snprintf(cmd_set, 64, "SET Qbb%d Schatten%d", i, i);
    testcase(connfd, cmd_set, "OK\r\n", "SET-Qbb-0");
  }
  for (int i = 0; i < cnt; i++) {
    char cmd_get[64] = {0};
    char expect_get[64] = {0};
    snprintf(cmd_get, 64, "GET Qbb%d", i, i);
    snprintf(expect_get, 64, "Value: Schatten%d\r\n", i);
    testcase(connfd, cmd_get, expect_get, "GET-Qbb-0");

  }

  for (int i = 0; i < cnt; i++) {
    char cmd_del[64] = {0};
    snprintf(cmd_del, 64, "DEL Qbb%d", i);
    testcase(connfd, cmd_del, "OK\r\n", "DEL-Qbb-0");
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
    snprintf(expect_rget, 64, "Value: Schatten%d\r\n", i);
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
    snprintf(expect_hget, 64, "Value: Schatten%d\r\n", i);
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
  testcase(connfd, "RGET Qbb", "Value: Schatten\r\n", "RGET-Qbb-0");
  testcase(connfd, "RMOD Qbb Cc", "OK\r\n", "RMOD-Qbb-0");
  testcase(connfd, "RMOD Bqq Cc", "Not Exist\r\n", "RMOD-Qbb-1");
  testcase(connfd, "RSET Qbb Schatten", "Key has existed\r\n", "RSET-Qbb-1");
  testcase(connfd, "REXIST Qbb", "YES, Exist\r\n", "REXIST-Qbb-0");
  testcase(connfd, "RGET Qbb", "Value: Cc\r\n", "RGET-Qbb-1");
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
  testcase(connfd, "HGET Qbb", "Value: Schatten\r\n", "HGET-Qbb-0");
  testcase(connfd, "HMOD Qbb Cc", "OK\r\n", "HMOD-Qbb-0");
  testcase(connfd, "HMOD Bqq Cc", "Not Exist\r\n", "HMOD-Qbb-1");
  testcase(connfd, "HSET Qbb Schatten", "Key has existed\r\n", "HSET-Qbb-1");
  testcase(connfd, "HEXIST Qbb", "YES, Exist\r\n", "HEXIST-Qbb-0");
  testcase(connfd, "HGET Qbb", "Value: Cc\r\n", "HGET-Qbb-1");
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

void persistence_test_1w(int connfd) {
  const int cnt = 10000;

  gettimeofday(&tv_begin, NULL);


  for (int i = 0; i < cnt; i++) {
    // array * 60000 - 每次循环执行6个操作
    char cmd_set1[64] = {0};
    char cmd_set2[64] = {0};
    char cmd_get[64] = {0};
    char cmd_mod1[64] = {0};
    char cmd_del[64] = {0};
    char expect_Ok[16] = {0};
    char expect_get1[64] = {0};
    char expect_get2[64] = {0};
    snprintf(cmd_set1, 64, "SET Array_k%d Array_v%d", 2 * i, 2 * i);
    snprintf(cmd_set2, 64, "SET Array_k%d Array_v%d", 2 * i + 1, 2 * i + 1);
    snprintf(cmd_get, 64, "GET Array_k%d", i);
    snprintf(cmd_mod1, 64, "MOD Array_k%d Array_MOD_v%d", i, i);
    snprintf(cmd_del, 64, "DEL Array_k%d", i);

    strcpy(expect_Ok, "OK\r\n");
    snprintf(expect_get1, 64, "Value: Array_v%d\r\n", i);
    snprintf(expect_get2, 64, "Value: Array_MOD_v%d\r\n", i);


    testcase(connfd, cmd_set1, expect_Ok, "pers_SET1");
    testcase(connfd, cmd_set2, expect_Ok, "pers_SET2");
    testcase(connfd, cmd_get, expect_get1, "pers_GET1");
    testcase(connfd, cmd_mod1, expect_Ok, "pers_MOD1");
    testcase(connfd, cmd_get, expect_get2, "pers_GET2");
    testcase(connfd, cmd_del, expect_Ok, "pers_DEL");

    // rbtree * 60000 - 每次循环执行6个操作
    char r_cmd_set1[64] = {0};
    char r_cmd_set2[64] = {0};
    char r_cmd_get[64] = {0};
    char r_cmd_mod1[64] = {0};
    char r_cmd_del[64] = {0};
    char r_expect_get1[64] = {0};
    char r_expect_get2[64] = {0};
    snprintf(r_cmd_set1, 64, "RSET R_k%d R_v%d", 2 * i, 2 * i);
    snprintf(r_cmd_set2, 64, "RSET R_k%d R_v%d", 2 * i + 1, 2 * i + 1);
    snprintf(r_cmd_get, 64, "RGET R_k%d", i);
    snprintf(r_cmd_mod1, 64, "RMOD R_k%d R_MOD_v%d", i, i);
    snprintf(r_cmd_del, 64, "RDEL R_k%d", i);

    snprintf(r_expect_get1, 64, "Value: R_v%d\r\n", i);
    snprintf(r_expect_get2, 64, "Value: R_MOD_v%d\r\n", i);

    testcase(connfd, r_cmd_set1, expect_Ok, "pers_RSET1");
    testcase(connfd, r_cmd_set2, expect_Ok, "pers_RSET2");
    testcase(connfd, r_cmd_get, r_expect_get1, "pers_RGET1");
    testcase(connfd, r_cmd_mod1, expect_Ok, "pers_RMOD1");
    testcase(connfd, r_cmd_get, r_expect_get2, "pers_RGET2");
    testcase(connfd, r_cmd_del, expect_Ok, "pers_RDEL");

    // hash * 60000 - 每次循环执行6个操作
    char h_cmd_set1[64] = {0};
    char h_cmd_set2[64] = {0};
    char h_cmd_get[64] = {0};
    char h_cmd_mod1[64] = {0};
    char h_cmd_del[64] = {0};
    char h_expect_get1[64] = {0};
    char h_expect_get2[64] = {0};
    snprintf(h_cmd_set1, 64, "HSET H_k%d H_v%d", 2 * i, 2 * i);
    snprintf(h_cmd_set2, 64, "HSET H_k%d H_v%d", 2 * i + 1, 2 * i + 1);
    snprintf(h_cmd_get, 64, "HGET H_k%d", i);
    snprintf(h_cmd_mod1, 64, "HMOD H_k%d H_MOD_v%d", i, i);
    snprintf(h_cmd_del, 64, "HDEL H_k%d", i);

    snprintf(h_expect_get1, 64, "Value: H_v%d\r\n", i);
    snprintf(h_expect_get2, 64, "Value: H_MOD_v%d\r\n", i);

    testcase(connfd, h_cmd_set1, expect_Ok, "pers_HSET1");
    testcase(connfd, h_cmd_set2, expect_Ok, "pers_HSET2");
    testcase(connfd, h_cmd_get, h_expect_get1, "pers_HGET1");
    testcase(connfd, h_cmd_mod1, expect_Ok, "pers_HMOD1");
    testcase(connfd, h_cmd_get, h_expect_get2, "pers_HGET2");
    testcase(connfd, h_cmd_del, expect_Ok, "pers_HDEL");
  }

  gettimeofday(&tv_end, NULL);
  int time_used = TIME_SUB_MS(tv_end, tv_begin);
  // 总请求数：array(60000) + rbtree(60000) + hash(60000) = 180000
  printf("persistence_test_1w (180000 REQ) --> time_used: %d ms  QPS: %d\n", time_used, 180000 * 1000 / time_used);
}

void persistence_get_all(int connfd) {
  const int cnt = 10000;

  gettimeofday(&tv_begin, NULL);

  // 根据persistence_test_1w函数中的操作，每次循环设置键2*i和2*i+1，然后删除键i
  // 所以最终会剩下键[10000, 19999]，因为键[0, 9999]被删除了
  // 每个后端都会剩下10000个键值对
  for (int i = 10000; i < 20000; i++) {
    // GET array后端的键
    char cmd_get_array[64] = {0};
    char expect_get_array[64] = {0};
    snprintf(cmd_get_array, 64, "GET Array_k%d", i);
    snprintf(expect_get_array, 64, "Value: Array_v%d\r\n", i);
    testcase(connfd, cmd_get_array, expect_get_array, "pers_GET_ARRAY");

    // GET rbtree后端的键
    char cmd_get_rbtree[64] = {0};
    char expect_get_rbtree[64] = {0};
    snprintf(cmd_get_rbtree, 64, "RGET R_k%d", i);
    snprintf(expect_get_rbtree, 64, "Value: R_v%d\r\n", i);
    testcase(connfd, cmd_get_rbtree, expect_get_rbtree, "pers_GET_RBTREE");

    // GET hash后端的键
    char cmd_get_hash[64] = {0};
    char expect_get_hash[64] = {0};
    snprintf(cmd_get_hash, 64, "HGET H_k%d", i);
    snprintf(expect_get_hash, 64, "Value: H_v%d\r\n", i);
    testcase(connfd, cmd_get_hash, expect_get_hash, "pers_GET_HASH");
  }

  gettimeofday(&tv_end, NULL);
  int time_used = TIME_SUB_MS(tv_end, tv_begin);
  // 总请求数：array(10000) + rbtree(10000) + hash(10000) = 30000
  printf("persistence_get_all (30000 REQ) --> time_used: %d ms  QPS: %d\n", time_used, 30000 * 1000 / time_used);
}

void multicmd_testcase(int connfd) {
  // 测试并行命令执行 (使用&)
  testcase(connfd, "SET key1 value1 & SET key2 value2", "OK\r\nOK\r\n", "MULTICMD-SET-AND-0");

  // 测试串行命令执行 (使用&&)
  testcase(connfd, "SET key3 value3 && GET key3", "OK\r\nValue: value3\r\n", "MULTICMD-SET-GET-ANDAND-0");

  // 测试混合命令
  testcase(connfd, "SET key4 value4 && SET key5 value5 & GET key4", "OK\r\nOK\r\nValue: value4\r\n", "MULTICMD-MIXED-0");

  // 测试失败后停止执行 (使用&&)
  testcase(connfd, "DEL nonexistent_key && SET key6 value6", "Not Exist\r\n", "MULTICMD-FAIL-STOP-0");

  // 测试多个并行命令
  testcase(connfd, "SET par_key1 par_val1 & SET par_key2 par_val2 & SET par_key3 par_val3", "OK\r\nOK\r\nOK\r\n", "MULTICMD-PARALLEL-0");

  // 验证并行命令的结果
  testcase(connfd, "GET par_key1", "Value: par_val1\r\n", "MULTICMD-VERIFY-PAR1-0");
  testcase(connfd, "GET par_key2", "Value: par_val2\r\n", "MULTICMD-VERIFY-PAR2-0");
  testcase(connfd, "GET par_key3", "Value: par_val3\r\n", "MULTICMD-VERIFY-PAR3-0");
  printf("passed\n");
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
  } else if (mode == 3) {
    persistence_test_1w(connfd);
  } else if (mode == 4) {
    persistence_get_all(connfd);
  } else if (mode == 5) {
    multicmd_testcase(connfd);
  }


  else {
    printf("ARG: ERROR\n");
    return -1;
  }
  return 0;
}
