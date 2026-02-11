#ifndef __TESTCASE_H__
#define __TESTCASE_H__

#define MAX_MSG 1024
#define MAX_LARGE_MSG (16 * 1024 * 1024)  // 16MB
#define TIME_SUB_MS(tv1, tv2) ((tv1.tv_sec - tv2.tv_sec) * 1000 + (tv1.tv_usec - tv2.tv_usec) / 1000)

/* ==================== 核心结构体 ==================== */

// 测试配置
typedef struct {
    char ip[32];
    int port;
    char mode[32];            // quick, standard, stress, boundary, full
    char engines[16];         // "AHRS"
    int count;                // 测试数据量
    int key_max_size;         // 最大 Key 大小
    int value_max_size;       // 最大 Value 大小
    int enable_persistence;   // 是否开启持久化测试
} test_config_t;

// 测试统计
typedef struct {
    int total;
    int passed;
    int failed;
    long long start_time_ms;
    long long end_time_ms;
} test_stats_t;

// 引擎操作抽象
typedef struct {
    const char* name;
    const char* set_cmd;
    const char* get_cmd;
    const char* del_cmd;
    const char* mod_cmd;
    const char* exist_cmd;
} engine_ops_t;

// testcase.c
int connect_server(const char *ip, unsigned short port);
int send_msg(int connfd, const char* msg, int length);
char* recv_msg_dynamic(int connfd, int* length);
char* encode_to_resp(const char* cmd);
const char* convert_response_to_resp(const char* pattern);
long long get_time_ms();
char* generate_random_string(int size, const char* charset);
void test_report(const char* test_name, int passed);
void test_basic_crud(int connfd, const engine_ops_t* engine);
void test_special_chars(int connfd, const engine_ops_t* engine);
void test_large_data(int connfd, const engine_ops_t* engine);
void test_performance(int connfd, const engine_ops_t* engine, int count);
void test_clean_data(int connfd, const engine_ops_t* engine);
void test_ksf_persistence(int connfd, const engine_ops_t* engine);
void test_aof_persistence(int connfd, const engine_ops_t* engine);
void test_persistence_load(int connfd, const engine_ops_t* engine, const char* test_type);
void testcase(int connfd, char* msg, char* pattern, char* casename);
void test_basic_crud(int connfd, const engine_ops_t* engine);
void test_special_chars(int connfd, const engine_ops_t* engine);
void test_large_data(int connfd, const engine_ops_t* engine);
void test_performance(int connfd, const engine_ops_t* engine, int count);
void test_clean_data(int connfd, const engine_ops_t* engine);
void print_usage(const char* prog);
int parse_args(int argc, char* argv[]);
void run_tests(int connfd);



// 持久化测试: testcase_persistence.c
void test_ksf_persistence(int connfd, const engine_ops_t* engine);
void test_aof_persistence(int connfd, const engine_ops_t* engine);
void test_ksf_load(int connfd, const engine_ops_t* engine);
void test_aof_load_mod(int connfd, const engine_ops_t* engine);
void test_aof_load_del(int connfd, const engine_ops_t* engine);
void test_persistence_load(int connfd, const engine_ops_t* engine, const char* test_type);
  
#endif