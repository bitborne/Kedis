#!/bin/bash

# 获取脚本所在目录，确保在项目根目录运行 python
TEST_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( dirname "$TEST_DIR" )"

cd "$PROJECT_ROOT"

# 先清理历史数据，保证环境纯净
echo "[INFO] Cleaning data files before running all tests..."
bash clean.sh

# 运行 Python 测试启动脚本
echo "[INFO] Starting all KVStore integration tests..."
python3 tests/test_all.py
