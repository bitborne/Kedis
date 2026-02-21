#!/bin/bash

# =============================================================================
# SSET持久化配置测试脚本 - 交互式版本
# 功能: 让用户选择配置方案，执行100万次SSET测试，生成带标签的结果
# 使用方式: 手动执行4次，每次选择不同的配置方案
# =============================================================================

set -e

# --------------------- 配置参数 ---------------------
HOST="127.0.0.1"
PORT="8888"
THREADS=8
CONNECTIONS_PER_THREAD=50
DATA_SIZE=128
KEY_PREFIX="kv_"
TOTAL_KEYS=1000000
KEY_MIN=1
KEY_MAX=1000000

# 计算每个连接需要处理的请求数
REQUESTS_PER_CONN=$((TOTAL_KEYS / (THREADS * CONNECTIONS_PER_THREAD)))

# 输出目录
OUTPUT_DIR="./pers_sset_benchmark_results"
mkdir -p "${OUTPUT_DIR}"

# 使用SSET作为测试命令
TEST_CMD="SSET"

# --------------------- 交互式选择配置 ---------------------

echo "================================================================================"
echo "                    SSET持久化配置测试"
echo "================================================================================"
echo ""
echo "请选择当前测试的持久化配置方案:"
echo ""
echo "  [1] Baseline    - AOF: OFF, RDB: OFF (基准测试)"
echo "  [2] AOF Only    - AOF: ON,  RDB: OFF (仅AOF持久化)"
echo "  [3] RDB Only    - AOF: OFF, RDB: ON  (仅RDB持久化)"
echo "  [4] AOF + RDB   - AOF: ON,  RDB: ON  (双重持久化)"
echo ""
echo "  [0] 退出"
echo ""
echo "================================================================================"

while true; do
    read -p "请输入选项 (0-4): " choice
    
    case $choice in
        1)
            CONFIG_NAME="Baseline_OFF_OFF"
            CONFIG_LABEL="AOF=OFF, RDB=OFF"
            break
            ;;
        2)
            CONFIG_NAME="AOF_ON_RDB_OFF"
            CONFIG_LABEL="AOF=ON, RDB=OFF"
            break
            ;;
        3)
            CONFIG_NAME="AOF_OFF_RDB_ON"
            CONFIG_LABEL="AOF=OFF, RDB=ON"
            break
            ;;
        4)
            CONFIG_NAME="AOF_ON_RDB_ON"
            CONFIG_LABEL="AOF=ON, RDB=ON"
            break
            ;;
        0)
            echo "退出测试"
            exit 0
            ;;
        *)
            echo "无效选项，请重新输入"
            ;;
    esac
done

echo ""
echo "================================================================================"
echo "                    准备执行测试: ${CONFIG_NAME}"
echo "================================================================================"
echo "配置: ${CONFIG_LABEL}"
echo "目标: ${HOST}:${PORT}"
echo "命令: ${TEST_CMD}"
echo "数据量: ${TOTAL_KEYS} 次操作"
echo "数据大小: ${DATA_SIZE} 字节"
echo "并发: ${THREADS} 线程 × ${CONNECTIONS_PER_THREAD} 连接"
echo "================================================================================"
echo ""

# 检查memtier_benchmark
if ! command -v memtier_benchmark &> /dev/null; then
    echo "错误: 未找到 memtier_benchmark 命令"
    exit 1
fi

# 确认开始测试
read -p "确认开始测试? (按Enter继续, Ctrl+C取消)"

echo ""
echo ">>> 开始测试: ${CONFIG_NAME}"
echo ">>> 开始时间: $(date '+%Y-%m-%d %H:%M:%S')"
echo ""

# 记录开始时间
START_TIME=$(date +%s)

# 执行测试
if ! memtier_benchmark \
    -s ${HOST} \
    -p ${PORT} \
    --command="${TEST_CMD} __key__ __data__" \
    --command-ratio=1 \
    --command-key-pattern=P \
    -t ${THREADS} \
    -c ${CONNECTIONS_PER_THREAD} \
    -n ${REQUESTS_PER_CONN} \
    -d ${DATA_SIZE} \
    --key-prefix=${KEY_PREFIX} \
    --key-minimum=${KEY_MIN} \
    --key-maximum=${KEY_MAX} \
    --hide-histogram \
    --hdr-file-prefix="${OUTPUT_DIR}/${CONFIG_NAME}_hdr" \
    --json-out-file="${OUTPUT_DIR}/${CONFIG_NAME}_result.json" \
    --print-percentiles=50,90,95,99,99.9 \
    2>&1 | tee "${OUTPUT_DIR}/${CONFIG_NAME}_output.log"; then
    
    echo "错误: 测试执行失败"
    exit 1
fi

# 记录结束时间
END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

# 生成元数据文件
cat > "${OUTPUT_DIR}/${CONFIG_NAME}_meta.txt" << EOF
CONFIG_NAME: ${CONFIG_NAME}
CONFIG_LABEL: ${CONFIG_LABEL}
AOF: $(echo ${CONFIG_LABEL} | grep -o 'AOF=[^,]*' | cut -d= -f2)
RDB: $(echo ${CONFIG_LABEL} | grep -o 'RDB=[^,]*' | cut -d= -f2)
HOST: ${HOST}
PORT: ${PORT}
THREADS: ${THREADS}
CONNECTIONS: ${CONNECTIONS_PER_THREAD}
DATA_SIZE: ${DATA_SIZE}
TOTAL_KEYS: ${TOTAL_KEYS}
START_TIME: $(date -d @${START_TIME} '+%Y-%m-%d %H:%M:%S')
END_TIME: $(date -d @${END_TIME} '+%Y-%m-%d %H:%M:%S')
DURATION_SECONDS: ${DURATION}
TEST_CMD: ${TEST_CMD}
EOF

echo ""
echo "================================================================================"
echo "                    测试完成"
echo "================================================================================"
echo "配置: ${CONFIG_NAME}"
echo "标签: ${CONFIG_LABEL}"
echo "耗时: ${DURATION} 秒 ($(awk "BEGIN {printf \\"%.2f\\", ${DURATION}/60}") 分钟)"
echo ""
echo "生成文件:"
echo "  - ${OUTPUT_DIR}/${CONFIG_NAME}_result.json"
echo "  - ${OUTPUT_DIR}/${CONFIG_NAME}_output.log"
echo "  - ${OUTPUT_DIR}/${CONFIG_NAME}_meta.txt"
echo "  - ${OUTPUT_DIR}/${CONFIG_NAME}_hdr.txt (延迟直方图)"
echo ""
echo "================================================================================"
echo ""
echo "提示: 请继续执行下一次测试，选择不同的配置方案"
echo "      完成4次测试后，运行图表生成脚本生成对比报告"
echo ""