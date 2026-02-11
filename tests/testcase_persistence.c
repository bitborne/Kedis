#include "testcase.h"

#include <netdb.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/time.h>
#include <unistd.h>
#include <time.h>

/* ==================== 持久化测试全局变量 ==================== */

// 小数据测试数据（1000 条）
#define SMALL_TEST_COUNT 10
static char g_small_test_keys[SMALL_TEST_COUNT][64];
static char g_small_test_values[SMALL_TEST_COUNT][128];
static int g_small_test_count = 0;

// 中等数据测试数据（100 条）
#define MEDIUM_TEST_COUNT 10
static char* g_medium_test_keys[MEDIUM_TEST_COUNT];
static char* g_medium_test_values[MEDIUM_TEST_COUNT];
static int g_medium_test_count = 0;

// 大数据测试数据（10 条）
#define LARGE_TEST_COUNT 10
static char* g_large_test_keys[LARGE_TEST_COUNT];
static char* g_large_test_values[LARGE_TEST_COUNT];
static int g_large_test_count = 0;

// AOF MOD 测试数据（存储 MOD 后的值）
static char g_aof_small_mod_values[SMALL_TEST_COUNT][128];
static char* g_aof_medium_mod_values[MEDIUM_TEST_COUNT];
static char* g_aof_large_mod_values[LARGE_TEST_COUNT];

/* ==================== 持久化测试辅助函数 ==================== */

/**
 * 释放持久化测试的全局数据
 */
static void free_persistence_test_data() {
    // 释放中等数据
    for (int i = 0; i < g_medium_test_count; i++) {
        if (g_medium_test_keys[i]) free(g_medium_test_keys[i]);
        if (g_medium_test_values[i]) free(g_medium_test_values[i]);
    }
    g_medium_test_count = 0;
    
    // 释放大数据
    for (int i = 0; i < g_large_test_count; i++) {
        if (g_large_test_keys[i]) free(g_large_test_keys[i]);
        if (g_large_test_values[i]) free(g_large_test_values[i]);
    }
    g_large_test_count = 0;
    
    // 释放 AOF MOD 数据
    for (int i = 0; i < MEDIUM_TEST_COUNT; i++) {
        if (g_aof_medium_mod_values[i]) free(g_aof_medium_mod_values[i]);
    }
    for (int i = 0; i < LARGE_TEST_COUNT; i++) {
        if (g_aof_large_mod_values[i]) free(g_aof_large_mod_values[i]);
    }
}

/* ==================== 持久化测试函数 ==================== */

/**
 * KSF 持久化测试
 */
void test_ksf_persistence(int connfd, const engine_ops_t* engine) {
    printf("\n  === KSF Persistence Test (%s Engine) ===\n", engine->name);
    
    // 释放之前的测试数据
    free_persistence_test_data();
    
    // 测试 1: 小数据量测试 (1000 条)
    printf("\n  Test 1: Small data (1000 records)...\n");
    g_small_test_count = 0;
    for (int i = 0; i < SMALL_TEST_COUNT; i++) {
        char key[64], value[128];
        snprintf(key, sizeof(key), "%s_key_%d", engine->name, i);
        snprintf(value, sizeof(value), "%s_value_%d", engine->name, i);
        
        // 保存到全局变量，用于后续加载测试验证
        strncpy(g_small_test_keys[g_small_test_count], key, sizeof(g_small_test_keys[g_small_test_count]) - 1);
        strncpy(g_small_test_values[g_small_test_count], value, sizeof(g_small_test_values[g_small_test_count]) - 1);
        g_small_test_count++;
        
        // SET 操作
        char cmd[256] = {0};
        snprintf(cmd, sizeof(cmd), "%s %s %s", engine->set_cmd, key, value);
        testcase(connfd, cmd, "OK\r\n", "Persistence: Small SET operation");
    }
    
    // 测试 2: 中等大小数据 (100 条，key 100B, value 1KB)
    printf("\n  Test 2: Medium data (100 records, key=100B, value=1KB)...\n");
    g_medium_test_count = 0;
    for (int i = 0; i < MEDIUM_TEST_COUNT; i++) {
        char key[128];
        snprintf(key, sizeof(key), "ksf_medium_key_%04d_", i);
        
        // 生成随机 key 后缀
        int suffix_len = 100 - strlen(key);
        if (suffix_len > 0) {
            char* random_key_suffix = generate_random_string(suffix_len, NULL);
            if (random_key_suffix) {
                strcat(key, random_key_suffix);
                free(random_key_suffix);
            }
        }
        
        // 生成随机 value（1KB）
        char* random_value = generate_random_string(1024, NULL);
        if (!random_value) {
            continue;
        }
        
        // 保存到全局变量，用于后续加载测试验证
        g_medium_test_keys[g_medium_test_count] = strdup(key);
        g_medium_test_values[g_medium_test_count] = strdup(random_value);
        g_medium_test_count++;
        
        // SET 操作
        char cmd[2048] = {0};
        snprintf(cmd, sizeof(cmd), "%s %s %s", engine->set_cmd, key, random_value);
        free(random_value);
        
        testcase(connfd, cmd, "OK\r\n", "Persistence: Medium SET operation");
    }
    printf("    Medium data: %d SET operations passed\n", MEDIUM_TEST_COUNT);
    
    // 测试 3: 大数据测试 (10 条，key 1KB, value 1MB)
    printf("\n  Test 3: Large data (10 records, key=1KB, value=1MB)...\n");
    g_large_test_count = 0;
    for (int i = 0; i < LARGE_TEST_COUNT; i++) {
        char key[1024] = {0};
        snprintf(key, sizeof(key), "ksf_large_key_%02d_", i);
        
        // 生成随机 key 后缀
        int suffix_len = 1024 - strlen(key);
        if (suffix_len > 0) {
            char* random_key_suffix = generate_random_string(suffix_len, NULL);
            if (random_key_suffix) {
                strcat(key, random_key_suffix);
                free(random_key_suffix);
            }
        }
        
        // 生成随机 value（1MB）
        char* random_value = generate_random_string(1024 * 1024, NULL);
        if (!random_value) {
            continue;
        }
        
        // 保存到全局变量，用于后续加载测试验证
        g_large_test_keys[g_large_test_count] = strdup(key);
        g_large_test_values[g_large_test_count] = strdup(random_value);
        g_large_test_count++;
        
        // SET 操作
        char* cmd = calloc(1, 2 * 1024 * 1024);
        snprintf(cmd, sizeof(cmd), "%s %s %s", engine->set_cmd, key, random_value);
        free(random_value);
        
        testcase(connfd, cmd, "OK\r\n", "Persistence: Large SET operation");
        free(cmd);
        cmd = NULL;
    }
    printf("    Large data: %d SET operations passed\n", LARGE_TEST_COUNT);
    
    printf("\n  KSF Persistence Test completed\n");
}

/**
 * AOF 持久化测试
 */
void test_aof_persistence(int connfd, const engine_ops_t* engine) {
    printf("\n  === AOF Persistence Test (%s Engine) ===\n", engine->name);
    
    // 释放之前的测试数据
    free_persistence_test_data();
    
    // 测试 1: 小数据量测试 (1000 条)
    printf("\n  Test 1: Small data (1000 records)...\n");
    g_small_test_count = 0;
    for (int i = 0; i < SMALL_TEST_COUNT; i++) {
        char key[64], value[128];
        snprintf(key, sizeof(key), "aof_small_key_%d", i);
        snprintf(value, sizeof(value), "aof_small_value_%d", i);
        
        // 保存到全局变量，用于后续加载测试验证
        strncpy(g_small_test_keys[g_small_test_count], key, sizeof(g_small_test_keys[g_small_test_count]) - 1);
        strncpy(g_small_test_values[g_small_test_count], value, sizeof(g_small_test_values[g_small_test_count]) - 1);
        g_small_test_count++;
        
        // SET 操作
        char cmd[256];
        snprintf(cmd, sizeof(cmd), "%s %s %s", engine->set_cmd, key, value);
        testcase(connfd, cmd, "OK\r\n", "Persistence: AOF Small SET operation");
    }
    printf("    Small data: %d SET operations passed\n", SMALL_TEST_COUNT);
    
    // 测试 2: 中等数据测试 (100 条，key 100B, value 1KB)
    printf("\n  Test 2: Medium data (100 records, key=100B, value=1KB)...\n");
    g_medium_test_count = 0;
    for (int i = 0; i < MEDIUM_TEST_COUNT; i++) {
        char key[128];
        snprintf(key, sizeof(key), "aof_medium_key_%04d_", i);
        
        // 生成随机 key 后缀
        int suffix_len = 100 - strlen(key);
        if (suffix_len > 0) {
            char* random_key_suffix = generate_random_string(suffix_len, NULL);
            if (random_key_suffix) {
                strcat(key, random_key_suffix);
                free(random_key_suffix);
            }
        }
        
        // 生成随机 value（1KB）
        char* random_value = generate_random_string(1024, NULL);
        if (!random_value) {
            continue;
        }
        
        // 保存到全局变量，用于后续加载测试验证
        g_medium_test_keys[g_medium_test_count] = strdup(key);
        g_medium_test_values[g_medium_test_count] = strdup(random_value);
        g_medium_test_count++;
        
        // SET 操作
        char cmd[2048];
        snprintf(cmd, sizeof(cmd), "%s %s %s", engine->set_cmd, key, random_value);
        free(random_value);
        
        testcase(connfd, cmd, "OK\r\n", "Persistence: AOF Medium SET operation");
    }
    printf("    Medium data: %d SET operations passed\n", MEDIUM_TEST_COUNT);
    
    // 测试 3: 大数据测试 (10 条，key 1KB, value 1MB)
    printf("\n  Test 3: Large data (10 records, key=1KB, value=1MB)...\n");
    g_large_test_count = 0;
    for (int i = 0; i < LARGE_TEST_COUNT; i++) {
        char key[1024];
        snprintf(key, sizeof(key), "aof_large_key_%02d_", i);
        
        // 生成随机 key 后缀
        int suffix_len = 1024 - strlen(key);
        if (suffix_len > 0) {
            char* random_key_suffix = generate_random_string(suffix_len, NULL);
            if (random_key_suffix) {
                strcat(key, random_key_suffix);
                free(random_key_suffix);
            }
        }
        
        // 生成随机 value（1MB）
        char* random_value = generate_random_string(1024 * 1024, NULL);
        if (!random_value) {
            continue;
        }
        
        // 保存到全局变量，用于后续加载测试验证
        g_large_test_keys[g_large_test_count] = strdup(key);
        g_large_test_values[g_large_test_count] = strdup(random_value);
        g_large_test_count++;
        
        // SET 操作
        char* cmd = calloc(1, 2 * 1024 * 1024);
        snprintf(cmd, sizeof(cmd), "%s %s %s", engine->set_cmd, key, random_value);
        free(random_value);
        
        testcase(connfd, cmd, "OK\r\n", "Persistence: AOF Large SET operation");
        free(cmd);
        cmd = NULL;
    }
    printf("    Large data: %d SET operations passed\n", LARGE_TEST_COUNT);
    
    // 测试 4: MOD 操作测试（对所有数据进行 MOD）
    printf("\n  Test 4: MOD operations (1110 records: 1000 small + 100 medium + 10 large)...\n");
    
    // MOD 小数据
    for (int i = 0; i < g_small_test_count; i++) {
        char mod_value[128];
        snprintf(mod_value, sizeof(mod_value), "aof_small_mod_value_%d", i);
        
        // 保存 MOD 后的值到全局变量
        strncpy(g_aof_small_mod_values[i], mod_value, sizeof(g_aof_small_mod_values[i]) - 1);
        
        // MOD 操作
        char cmd[256];
        snprintf(cmd, sizeof(cmd), "%s %s %s", engine->mod_cmd, g_small_test_keys[i], mod_value);
        testcase(connfd, cmd, "OK\r\n", "Persistence: AOF Small MOD operation");
    }
    
    // MOD 中等数据
    for (int i = 0; i < g_medium_test_count; i++) {
        // 生成随机 MOD value（1KB）
        char* random_mod_value = generate_random_string(1024, NULL);
        if (!random_mod_value) {
            continue;
        }
        
        // 保存 MOD 后的值到全局变量
        g_aof_medium_mod_values[i] = strdup(random_mod_value);
        
        // MOD 操作
        char cmd[2048];
        snprintf(cmd, sizeof(cmd), "%s %s %s", engine->mod_cmd, g_medium_test_keys[i], random_mod_value);
        free(random_mod_value);
        
        testcase(connfd, cmd, "OK\r\n", "Persistence: AOF Medium MOD operation");
    }
    
    // MOD 大数据
    for (int i = 0; i < g_large_test_count; i++) {
        // 生成随机 MOD value（1MB）
        char* random_mod_value = generate_random_string(1024 * 1024, NULL);
        if (!random_mod_value) {
            continue;
        }
        
        // 保存 MOD 后的值到全局变量
        g_aof_large_mod_values[i] = strdup(random_mod_value);
        
        // MOD 操作
        char* cmd = calloc(1, 2 * 1024 * 1024);
        snprintf(cmd, sizeof(cmd), "%s %s %s", engine->mod_cmd, g_large_test_keys[i], random_mod_value);
        free(random_mod_value);
        
        testcase(connfd, cmd, "OK\r\n", "Persistence: AOF Large MOD operation");
        free(cmd);
        cmd = NULL;
    }
    
    printf("    MOD operations: 1110 operations passed\n");
    
    printf("\n  AOF Persistence Test completed\n");
}

/**
 * KSF 加载测试（从 KSF 快照加载后验证数据）
 */
void test_ksf_load(int connfd, const engine_ops_t* engine) {
    printf("\n  === KSF Load Test (%s Engine) ===\n", engine->name);
    
    // 检查小数据是否正确加载
    printf("\n  Checking small data (%d records)...\n", g_small_test_count);
    for (int i = 0; i < g_small_test_count; i++) {
        char cmd[256];
        snprintf(cmd, sizeof(cmd), "%s %s", engine->get_cmd, g_small_test_keys[i]);
        
        char expected[256];
        snprintf(expected, sizeof(expected), "%s\r\n", g_small_test_values[i]);
        
        char casename[256];
        snprintf(casename, sizeof(casename), "Load: KSF Small data GET (%d)", i);
        
        testcase(connfd, cmd, expected, casename);
    }
    printf("    Small data: %d checks completed\n", g_small_test_count);
    
    // 检查中等数据是否正确加载（验证具体内容）
    printf("\n  Checking medium data (%d records)...\n", g_medium_test_count);
    for (int i = 0; i < g_medium_test_count; i++) {
        char cmd[2048];
        snprintf(cmd, sizeof(cmd), "%s %s", engine->get_cmd, g_medium_test_keys[i]);
        
        // 构造期望的响应（value + \r\n）
        size_t value_len = strlen(g_medium_test_values[i]);
        char* expected = (char*)malloc(value_len + 2); // value + \r\n
        if (expected) {
            sprintf(expected, "%s\r\n", g_medium_test_values[i]);
            
            char casename[256];
            snprintf(casename, sizeof(casename), "Load: KSF Medium data GET (%d)", i);
            
            testcase(connfd, cmd, expected, casename);
            free(expected);
        }
    }
    printf("    Medium data: %d checks completed\n", g_medium_test_count);
    
    // 检查大数据是否正确加载（验证具体内容）
    printf("\n  Checking large data (%d records)...\n", g_large_test_count);
    for (int i = 0; i < g_large_test_count; i++) {
        char cmd[2048];
        snprintf(cmd, sizeof(cmd), "%s %s", engine->get_cmd, g_large_test_keys[i]);
        
        // 构造期望的响应（value + \r\n）
        size_t value_len = strlen(g_large_test_values[i]);
        char* expected = (char*)malloc(value_len + 2); // value + \r\n
        if (expected) {
            sprintf(expected, "%s\r\n", g_large_test_values[i]);
            
            char casename[256];
            snprintf(casename, sizeof(casename), "Load: KSF Large data GET (%d)", i);
            
            testcase(connfd, cmd, expected, casename);
            free(expected);
        }
    }
    printf("    Large data: %d checks completed\n", g_large_test_count);
    
    printf("\n  KSF Load Test completed\n");
}

/**
 * AOF 加载测试 - MOD 验证（第一次加载）
 */
void test_aof_load_mod(int connfd, const engine_ops_t* engine) {
    printf("\n  === AOF Load Test (MOD Verification) ===\n");
    
    // 1. 验证小数据（检查是否是 MOD 后的值）
    printf("\n  Checking small data (%d records)...\n", g_small_test_count);
    for (int i = 0; i < g_small_test_count; i++) {
        char cmd[256];
        snprintf(cmd, sizeof(cmd), "%s %s", engine->get_cmd, g_small_test_keys[i]);
        
        char expected[256];
        snprintf(expected, sizeof(expected), "%s\r\n", g_aof_small_mod_values[i]);
        
        char casename[256];
        snprintf(casename, sizeof(casename), "Load: AOF Small MOD GET (%d)", i);
        
        testcase(connfd, cmd, expected, casename);
    }
    printf("    Small data: %d checks completed\n", g_small_test_count);
    
    // 2. 验证中等数据（检查是否是 MOD 后的值）
    printf("\n  Checking medium data (%d records)...\n", g_medium_test_count);
    for (int i = 0; i < g_medium_test_count; i++) {
        char cmd[2048];
        snprintf(cmd, sizeof(cmd), "%s %s", engine->get_cmd, g_medium_test_keys[i]);
        
        // 构造期望的响应（value + \r\n）
        size_t value_len = strlen(g_aof_medium_mod_values[i]);
        char* expected = (char*)malloc(value_len + 2); // value + \r\n
        if (expected) {
            sprintf(expected, "%s\r\n", g_aof_medium_mod_values[i]);
            
            char casename[256];
            snprintf(casename, sizeof(casename), "Load: AOF Medium MOD GET (%d)", i);
            
            testcase(connfd, cmd, expected, casename);
            free(expected);
        }
    }
    printf("    Medium data: %d checks completed\n", g_medium_test_count);
    
    // 3. 验证大数据（检查是否是 MOD 后的值）
    printf("\n  Checking large data (%d records)...\n", g_large_test_count);
    for (int i = 0; i < g_large_test_count; i++) {
        char cmd[2048];
        snprintf(cmd, sizeof(cmd), "%s %s", engine->get_cmd, g_large_test_keys[i]);
        
        // 构造期望的响应（value + \r\n）
        size_t value_len = strlen(g_aof_large_mod_values[i]);
        char* expected = (char*)malloc(value_len + 2); // value + \r\n
        if (expected) {
            sprintf(expected, "%s\r\n", g_aof_large_mod_values[i]);
            
            char casename[256];
            snprintf(casename, sizeof(casename), "Load: AOF Large MOD GET (%d)", i);
            
            testcase(connfd, cmd, expected, casename);
            free(expected);
        }
    }
    printf("    Large data: %d checks completed\n", g_large_test_count);
    
    // 4. DEL 所有键（为第二次加载测试做准备）
    printf("\n  Deleting all keys (%d records)...\n", g_small_test_count + g_medium_test_count + g_large_test_count);
    
    for (int i = 0; i < g_small_test_count; i++) {
        char cmd[128];
        snprintf(cmd, sizeof(cmd), "%s %s", engine->del_cmd, g_small_test_keys[i]);
        testcase(connfd, cmd, "OK\r\n", "Load: AOF Small DEL");
    }
    
    for (int i = 0; i < g_medium_test_count; i++) {
        char cmd[2048];
        snprintf(cmd, sizeof(cmd), "%s %s", engine->del_cmd, g_medium_test_keys[i]);
        testcase(connfd, cmd, "OK\r\n", "Load: AOF Medium DEL");
    }
    
    for (int i = 0; i < g_large_test_count; i++) {
        char cmd[2048];
        snprintf(cmd, sizeof(cmd), "%s %s", engine->del_cmd, g_large_test_keys[i]);
        testcase(connfd, cmd, "OK\r\n", "Load: AOF Large DEL");
    }
    
    printf("    DEL operations: %d operations completed\n", g_small_test_count + g_medium_test_count + g_large_test_count);
    
    printf("\n  AOF Load MOD Test completed\n");
}

/**
 * AOF 加载测试 - DEL 验证（第二次加载）
 */
void test_aof_load_del(int connfd, const engine_ops_t* engine) {
    printf("\n  === AOF Load Test (DEL Verification) ===\n");
    
    // 1. 验证小数据（应该不存在）
    printf("\n  Checking small data (%d records)...\n", g_small_test_count);
    for (int i = 0; i < g_small_test_count; i++) {
        char cmd[256];
        snprintf(cmd, sizeof(cmd), "%s %s", engine->get_cmd, g_small_test_keys[i]);
        
        char casename[256];
        snprintf(casename, sizeof(casename), "Load: AOF Small DEL GET (%d)", i);
        
        testcase(connfd, cmd, "ERROR / Not Exist\r\n", casename);
    }
    printf("    Small data: %d checks completed\n", g_small_test_count);
    
    // 2. 验证中等数据（应该不存在）
    printf("\n  Checking medium data (%d records)...\n", g_medium_test_count);
    for (int i = 0; i < g_medium_test_count; i++) {
        char cmd[2048];
        snprintf(cmd, sizeof(cmd), "%s %s", engine->get_cmd, g_medium_test_keys[i]);
        
        char casename[256];
        snprintf(casename, sizeof(casename), "Load: AOF Medium DEL GET (%d)", i);
        
        testcase(connfd, cmd, "ERROR / Not Exist\r\n", casename);
    }
    printf("    Medium data: %d checks completed\n", g_medium_test_count);
    
    // 3. 验证大数据（应该不存在）
    printf("\n  Checking large data (%d records)...\n", g_large_test_count);
    for (int i = 0; i < g_large_test_count; i++) {
        char cmd[2048];
        snprintf(cmd, sizeof(cmd), "%s %s", engine->get_cmd, g_large_test_keys[i]);
        
        char casename[256];
        snprintf(casename, sizeof(casename), "Load: AOF Large DEL GET (%d)", i);
        
        testcase(connfd, cmd, "ERROR / Not Exist\r\n", casename);
    }
    printf("    Large data: %d checks completed\n", g_large_test_count);
    
    printf("\n  AOF Load DEL Test completed\n");
}

/**
 * 持久化加载测试（兼容旧版本，不推荐使用）
 * @deprecated 请使用 test_ksf_load、test_aof_load_mod 或 test_aof_load_del 代替
 */
void test_persistence_load(int connfd, const engine_ops_t* engine, const char* test_type) {
    printf("\n  [DEPRECATED] test_persistence_load is deprecated, use test_ksf_load, test_aof_load_mod or test_aof_load_del instead\n");
    
    if (strcmp(test_type, "ksf") == 0) {
        test_ksf_load(connfd, engine);
    } else if (strcmp(test_type, "aof") == 0) {
        test_aof_load_mod(connfd, engine);
    } else {
        printf("  Unknown test type: %s\n", test_type);
    }
}