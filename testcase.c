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
#include <unistd.h>

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

// base 
void once_testcase_base(int connfd) {
  testcase(connfd, "SET Qbb Schatten", "OK\r\n", "SET-Qbb-0");
  testcase(connfd, "GET Qbb", "Schatten\r\n", "GET-Qbb-0");
  testcase(connfd, "MOD Qbb Cc", "OK\r\n", "MOD-Qbb-0");
  testcase(connfd, "MOD Bqq Cc", "Not Exist\r\n", "MOD-Qbb-1");
  testcase(connfd, "SET Qbb Schatten", "Key has existed\r\n", "SET-Qbb-1");
  testcase(connfd, "EXIST Qbb", "YES, Exist\r\n", "EXIST-Qbb-0");
  testcase(connfd, "GET Qbb", "Cc\r\n", "GET-Qbb-1");
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
void once_testcase_base_1w(int connfd) {

  const int cnt = 10000;

  gettimeofday(&tv_begin, NULL);
  for (int i = 0; i < cnt; i++) {
    once_testcase_base(connfd);
  }
  gettimeofday(&tv_end, NULL);
  int time_used = TIME_SUB_MS(tv_end, tv_begin);
  printf("once_testcase_base_1w (160w REQ) --> time_used: %d ms  QPS: %d\n", time_used, 160000 * 1000 / time_used);

}

void set_testcase_single_1w(int connfd) {
  gettimeofday(&tv_begin, NULL);
  const int cnt = 10000;

  // SET 1w
  for (int i = 0; i < cnt; i++) {
    char cmd_set[64] = {0};
    snprintf(cmd_set, 64, "SET Qbb%d Schatten%d", i, i);
    testcase(connfd, cmd_set, "OK\r\n", "SET-Qbb-0");
  }
  
  gettimeofday(&tv_end, NULL);
  int time_used = TIME_SUB_MS(tv_end, tv_begin);
  printf("array_single(1w req) --> time_used: %d ms  QPS: %d\n", time_used, 10000 * 1000 / time_used);

}
void get_testcase_single_1w(int connfd) {
  gettimeofday(&tv_begin, NULL);
  const int cnt = 10000;

  // GET 1w
  for (int i = 0; i < cnt; i++) {
    char cmd_get[64] = {0};
    char expect_get[64] = {0};
    snprintf(cmd_get, 64, "GET Qbb%d", i, i);
    snprintf(expect_get, 64, "Schatten%d\r\n", i);
    testcase(connfd, cmd_get, expect_get, "GET-Qbb-0");

  }

  gettimeofday(&tv_end, NULL);
  int time_used = TIME_SUB_MS(tv_end, tv_begin);
  printf("array_single(1w req) --> time_used: %d ms  QPS: %d\n", time_used, 10000 * 1000 / time_used);

}
void del_testcase_single_1w(int connfd) {
  gettimeofday(&tv_begin, NULL);
  const int cnt = 10000;


  for (int i = 0; i < cnt; i++) {
    char cmd_del[64] = {0};
    snprintf(cmd_del, 64, "DEL Qbb%d", i);
    testcase(connfd, cmd_del, "OK\r\n", "DEL-Qbb-0");
  }
  gettimeofday(&tv_end, NULL);
  int time_used = TIME_SUB_MS(tv_end, tv_begin);
  printf("array_single(1w req) --> time_used: %d ms  QPS: %d\n", time_used, 10000 * 1000 / time_used);

}

void persistence_set_del_1w(int connfd) {
  const int cnt = 10000;

  gettimeofday(&tv_begin, NULL);

  for (int i = 0; i < cnt; i++) {
    // 60000 - 每次循环执行6个操作
    char cmd_set1[64] = {0};
    char cmd_set2[64] = {0};
    char cmd_get[64] = {0};
    char cmd_mod1[64] = {0};
    char cmd_del[64] = {0};
    char expect_get1[64] = {0};
    char expect_get2[64] = {0};
    snprintf(cmd_set1, 64, "SET persistence_k%d persistence_v%d", 2 * i, 2 * i);
    snprintf(cmd_set2, 64, "SET persistence_k%d persistence_v%d", 2 * i + 1, 2 * i + 1);
    snprintf(cmd_get, 64, "GET persistence_k%d", i);
    snprintf(cmd_mod1, 64, "MOD persistence_k%d persistence_MOD_v%d", i, i);
    snprintf(cmd_del, 64, "DEL persistence_k%d", i);

    snprintf(expect_get1, 64, "persistence_v%d\r\n", i);
    snprintf(expect_get2, 64, "persistence_MOD_v%d\r\n", i);


    testcase(connfd, cmd_set1, "OK\r\n", "pers_SET1");
    testcase(connfd, cmd_set2, "OK\r\n", "pers_SET2");
    testcase(connfd, cmd_get, expect_get1, "pers_GET1");
    testcase(connfd, cmd_mod1, "OK\r\n", "pers_MOD1");
    testcase(connfd, cmd_get, expect_get2, "pers_GET2");
    testcase(connfd, cmd_del, "OK\r\n", "pers_DEL");
  }
 
  gettimeofday(&tv_end, NULL);
  int time_used = TIME_SUB_MS(tv_end, tv_begin);
  printf("persistence_test_1w (60000 REQ) --> time_used: %d ms  QPS: %d\n", time_used, 60000 * 1000 / time_used);
}

void persistence_get_rest(int connfd) {
  const int cnt = 10000;

  gettimeofday(&tv_begin, NULL);
  for (int i = 10000; i < 20000; i++) {
    // GET array后端的键
    char cmd_get_array[64] = {0};
    char expect_get_array[64] = {0};
    snprintf(cmd_get_array, 64, "GET persistence_k%d", i);
    snprintf(expect_get_array, 64, "persistence_v%d\r\n", i);
    testcase(connfd, cmd_get_array, expect_get_array, "pers_GET_ARRAY");
  }


  gettimeofday(&tv_end, NULL);
  int time_used = TIME_SUB_MS(tv_end, tv_begin);
  printf("persistence_get_all (10000 REQ) --> time_used: %d ms  QPS: %d\n", time_used, 10000 * 1000 / time_used);
}

void persistence_del_rest(int connfd) {
  const int cnt = 10000;

  gettimeofday(&tv_begin, NULL);
  for (int i = 10000; i < 20000; i++) {
    // GET array后端的键
    char cmd_del_array[64] = {0};
    snprintf(cmd_del_array, 64, "DEL persistence_k%d", i);
    testcase(connfd, cmd_del_array, "OK\r\n", "pers_DEL_rest");
  }

  gettimeofday(&tv_end, NULL);
  int time_used = TIME_SUB_MS(tv_end, tv_begin);

  printf("persistence_del_rest (10000 REQ) --> time_used: %d ms  QPS: %d\n", time_used, 10000 * 1000 / time_used);
}

void slave_testcase(int connfd_master, int connfd_slave) {
    // 主从复制功能测试
    printf("Starting Master-Slave replication test...\n");

    // 测试1: 在主节点设置键值
    testcase(connfd_master, "SET master_key master_value", "OK\r\n", "MASTER-SET-0");
    printf("Master set key successfully\n");

    // 等待一段时间让命令同步到从节点
    usleep(100000); // 100ms

    // 测试2: 在主节点获取刚设置的键值
    testcase(connfd_master, "GET master_key", "master_value\r\n", "MASTER-GET-0");
    printf("Master get key successfully\n");

    // 测试3: 在从节点获取相同的键值 (应该同步过来)
    testcase(connfd_slave, "GET master_key", "master_value\r\n", "SLAVE-GET-0");
    printf("Slave get replicated key successfully\n");

    // 测试4: 在主节点修改键值
    testcase(connfd_master, "MOD master_key modified_value", "OK\r\n", "MASTER-MOD-0");
    printf("Master modified key successfully\n");

    // 等待同步
    usleep(100000); // 100ms

    // 测试5: 在主节点获取修改后的值
    testcase(connfd_master, "GET master_key", "modified_value\r\n", "MASTER-GET-1");
    printf("Master get modified value successfully\n");

    // 测试6: 在从节点获取修改后的值 (应已同步)
    testcase(connfd_slave, "GET master_key", "modified_value\r\n", "SLAVE-GET-1");
    printf("Slave get modified value successfully\n");

    // 测试7: 在主节点删除键
    testcase(connfd_master, "DEL master_key", "OK\r\n", "MASTER-DEL-0");
    printf("Master delete key successfully\n");

    // 等待同步
    usleep(100000); // 100ms

    // 测试8: 在主节点获取已删除的键
    testcase(connfd_master, "GET master_key", "ERROR / Not Exist\r\n", "MASTER-GET-2");
    printf("Master get deleted key (should not exist) successfully\n");

    // 测试9: 在从节点获取已删除的键 (应已同步删除)
    testcase(connfd_slave, "GET master_key", "ERROR / Not Exist\r\n", "SLAVE-GET-2");
    printf("Slave get deleted key (should not exist) successfully\n");

    // 测试10: 批量主从同步
    for (int i = 0; i < 10; i++) {
        char cmd_set[64] = {0};
        char expected[64] = {0};
        snprintf(cmd_set, 64, "SET batch_key_%d batch_value_%d", i, i);
        testcase(connfd_master, cmd_set, "OK\r\n", "MASTER-BATCH-SET");

        snprintf(expected, 64, "batch_value_%d\r\n", i);
        // 等待同步
        usleep(50000); // 50ms

        snprintf(cmd_set, 64, "GET batch_key_%d", i);
        testcase(connfd_slave, cmd_set, expected, "SLAVE-BATCH-GET");
    }
    printf("Batch replication test passed\n");

    // 测试11: 检查主从节点的EXIST结果
    testcase(connfd_master, "EXIST master_key", "NO, Not Exist\r\n", "MASTER-EXIST-0");
    testcase(connfd_slave, "EXIST master_key", "NO, Not Exist\r\n", "SLAVE-EXIST-0");
    testcase(connfd_master, "EXIST batch_key_5", "YES, Exist\r\n", "MASTER-EXIST-1");
    testcase(connfd_slave, "EXIST batch_key_5", "YES, Exist\r\n", "SLAVE-EXIST-1");

    // 测试12: 在主节点上执行多命令并验证从节点同步
    testcase(connfd_master, "SET multi_key1 val1 & SET multi_key2 val2", "OK\r\nOK\r\n", "MASTER-MULTICMD-SET");
    usleep(100000); // 100ms
    testcase(connfd_slave, "GET multi_key1", "val1\r\n", "SLAVE-GET-MULTI1");
    testcase(connfd_slave, "GET multi_key2", "val2\r\n", "SLAVE-GET-MULTI2");

    printf("All Master-Slave replication tests passed!\n");
}

void multicmd_testcase(int connfd) {
  // 测试并行命令执行 (使用&)
  testcase(connfd, "SET key1 value1 & SET key2 value2", "OK\r\nOK\r\n", "MULTICMD-SET-AND-0");

  // 测试串行命令执行 (使用&&)
  testcase(connfd, "SET key3 value3 && GET key3", "OK\r\nvalue3\r\n", "MULTICMD-SET-GET-ANDAND-0");

  // 测试混合命令
  testcase(connfd, "SET key4 value4 && SET key5 value5 & GET key4", "OK\r\nOK\r\nvalue4\r\n", "MULTICMD-MIXED-0");

  // 测试失败后停止执行 (使用&&)
  testcase(connfd, "DEL nonexistent_key && SET key6 value6", "Not Exist\r\n", "MULTICMD-FAIL-STOP-0");

  // 测试多个并行命令
  testcase(connfd, "SET par_key1 par_val1 & SET par_key2 par_val2 & SET par_key3 par_val3", "OK\r\nOK\r\nOK\r\n", "MULTICMD-PARALLEL-0");

  // 验证并行命令的结果
  testcase(connfd, "GET par_key1", "par_val1\r\n", "MULTICMD-VERIFY-PAR1-0");
  testcase(connfd, "GET par_key2", "par_val2\r\n", "MULTICMD-VERIFY-PAR2-0");
  testcase(connfd, "GET par_key3", "par_val3\r\n", "MULTICMD-VERIFY-PAR3-0");

  // 并行删除
  testcase(connfd, "DEL key1 & DEL key2 & DEL key3 & DEL key4 & DEL key5 & "
                   "DEL par_key1 & DEL par_key2 & DEL par_key3",
                   "OK\r\nOK\r\nOK\r\nOK\r\nOK\r\nOK\r\nOK\r\nOK\r\n",
                    "mul_CLEAN");

  printf("passed\n");
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

int main(int argc, char* argv[]) {

  if (argc != 4 && argc != 6) {
    printf("Arg error\n");
    return -1;
  }

  char* ip = argv[1];
  unsigned short port = atoi(argv[2]);
  char* mode = argv[3];

  int connfd_master = connect_server(ip, port);

  if (argc == 6) {
    if (strcmp(mode, "SM") != 0) {
      perror("arg error\n");
      return -1;
    }
    char* ip_slave = argv[4];
    unsigned short port_slave = atoi(argv[5]);
    int connfd_slave = connect_server(ip_slave, port_slave);

    // SM(主从) test
    slave_testcase(connfd_master, connfd_slave);
  }
  
  else if (!strcmp(mode, "pers1")) {
    persistence_set_del_1w(connfd_master);
  } else if (!strcmp(mode, "pers2")) {
    persistence_get_rest(connfd_master);
  } else if (!strcmp(mode, "pers_clean")) {
    persistence_del_rest(connfd_master);
  }


  // base -> 最简单的测试
  else if (!strcmp(mode, "base")) {
    once_testcase_base_1w(connfd_master);
  }

  else if (!strcmp(mode, "set1w")) {
    set_testcase_single_1w(connfd_master);
  } else if (!strcmp(mode, "get1w")) {
    get_testcase_single_1w(connfd_master);
    
  } else if (!strcmp(mode, "del1w")) {
    del_testcase_single_1w(connfd_master);
  }

  // 多命令并行
  else if (!strcmp(mode, "mul")) {
    multicmd_testcase(connfd_master);
  }

  else if (!strcmp(mode, "A")) array_testcase_single_1w(connfd_master);
  else if (!strcmp(mode, "H")) hash_testcase_single_1w(connfd_master);
  else if (!strcmp(mode, "R")) rbtree_testcase_single_1w(connfd_master);
  else {
    printf("ARG_parse: ERROR\n");
    return -1;
  }
  return 0;
}
