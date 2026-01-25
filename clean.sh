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

# 清理数据文件
clean_data() {
    print_info "Cleaning data files..."
    rm -f $DATA_DIR/*.ksf $DATA_DIR/*.aof
    print_info "Data files cleaned"
}

clean_data