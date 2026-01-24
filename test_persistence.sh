#!/bin/bash

# KVStore 持久化测试脚本
# 功能：自动测试 KSF 和 AOF 持久化功能

set -e

# 配置
SERVER_IP="127.0.0.1"
SERVER_PORT="8888"
TESTCASE_BIN="./testcase"
KVSTORE_BIN="./kvstore"
DATA_DIR="./data"

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 打印函数
print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 检查服务器是否运行
check_server() {
    if pgrep -x "kvstore" > /dev/null; then
        return 0
    else
        return 1
    fi
}

# 启动服务器
start_server() {
    print_info "Starting KVStore server..."
    $KVSTORE_BIN $SERVER_PORT > /dev/null 2>&1 &
    SERVER_PID=$!
    sleep 2
    
    if check_server; then
        print_info "Server started successfully (PID: $SERVER_PID)"
        return 0
    else
        print_error "Failed to start server"
        return 1
    fi
}

# 停止服务器
stop_server() {
    print_info "Stopping KVStore server..."
    if pgrep -x "kvstore" > /dev/null; then
        pkill -x "kvstore"
        sleep 1
        print_info "Server stopped"
    else
        print_warn "Server is not running"
    fi
}

# 清理数据文件
clean_data() {
    print_info "Cleaning data files..."
    rm -f $DATA_DIR/*.ksf $DATA_DIR/*.aof
    print_info "Data files cleaned"
}

# 运行 KSF 持久化测试
test_ksf_persistence() {
    print_info "=== KSF Persistence Test ==="
    
    # 清理旧数据
    clean_data
    
    # 运行 KSF 测试
    print_info "Running KSF persistence test..."
    $TESTCASE_BIN $SERVER_IP $SERVER_PORT ksf
    
    # 停止服务器
    stop_server
    
    # 重新启动服务器（加载 KSF）
    print_info "Restarting server to load KSF snapshot..."
    start_server
    
    # 验证加载
    print_info "Verifying KSF load..."
    $TESTCASE_BIN $SERVER_IP $SERVER_PORT load-ksf
    
    # 清理数据
    clean_data
}

# 运行 AOF 持久化测试
test_aof_persistence() {
    print_info "=== AOF Persistence Test ==="
    
    # 清理旧数据
    clean_data
    
    # 运行 AOF 测试
    print_info "Running AOF persistence test..."
    $TESTCASE_BIN $SERVER_IP $SERVER_PORT aof
    
    # 停止服务器
    stop_server
    
    # 重新启动服务器（加载 AOF）
    print_info "Restarting server to load AOF log..."
    start_server
    
    # 验证加载
    print_info "Verifying AOF load..."
    $TESTCASE_BIN $SERVER_IP $SERVER_PORT load-aof
    
    # 清理数据
    clean_data
}

# 运行完整持久化测试
test_all_persistence() {
    print_info "=== Full Persistence Test ==="
    
    # KSF 测试
    test_ksf_persistence
    
    # AOF 测试
    test_aof_persistence
    
    print_info "=== All Persistence Tests Completed ==="
}

# 主函数
main() {
    echo "======================================"
    echo "  KVStore Persistence Test Script"
    echo "======================================"
    echo ""
    
    # 检查二进制文件
    if [ ! -f "$TESTCASE_BIN" ]; then
        print_error "testcase binary not found: $TESTCASE_BIN"
        print_info "Please run 'make' to build the project"
        exit 1
    fi
    
    if [ ! -f "$KVSTORE_BIN" ]; then
        print_error "kvstore binary not found: $KVSTORE_BIN"
        print_info "Please run 'make' to build the project"
        exit 1
    fi
    
    # 创建数据目录
    mkdir -p "$DATA_DIR"
    
    # 停止现有服务器
    stop_server
    
    # 根据参数选择测试
    case "${1:-all}" in
        "ksf")
            test_ksf_persistence
            ;;
        "aof")
            test_aof_persistence
            ;;
        "all")
            test_all_persistence
            ;;
        *)
            print_error "Invalid test mode: $1"
            echo "Usage: $0 [ksf|aof|all]"
            exit 1
            ;;
    esac
    
    # 停止服务器
    stop_server
    
    print_info "Test script completed"
}

# 运行主函数
main "$@"