#!/bin/bash

PORT=$1
MODE=$2

if [ -z "$PORT" ] || [ -z "$MODE" ]; then
    echo "用法: $0 <端口号> <mode>"
    echo ""
    echo "  mode 可选:"
    echo "    none   : 仅测无 mirror 基线"
    echo "    uprobe : 测无 mirror 基线 + uprobe mirror"
    echo "    xdp    : 测无 mirror 基线 + xdp mirror"
    echo "    tc     : 测无 mirror 基线 + tc mirror"
    echo "    all    : 依次测 none -> uprobe -> xdp -> tc（需手动切换）"
    echo ""
    echo "  服务和 mirror 均需手动启动，脚本仅负责提示与跑 benchmark。"
    exit 1
fi

HOST="127.0.0.1"
KEYS=1000000
THREADS=4
CONN=50
REQS_PER_CLIENT=$((KEYS / (THREADS * CONN)))

HGET_CMD="HGET __key__"

run_benchmark() {
    local label=$1
    local cmd=$2
    echo -e "\n========== [$label] =========="
    memtier_benchmark \
        -s ${HOST} \
        -p ${PORT} \
        --command="$cmd" \
        --command-ratio=1 \
        --command-key-pattern=P \
        -t ${THREADS} \
        -c ${CONN} \
        -n ${REQS_PER_CLIENT} \
        --random-data \
        --key-prefix="k" \
        --key-minimum=1 \
        --key-maximum=${KEYS} \
        --hide-histogram
}

run_round() {
    local round_name=$1
    echo ""
    echo ">>> [$round_name] HSET x ${KEYS}"
    run_benchmark "HSET-${round_name}" "HSET __key__ __data__"

    echo ""
    echo ">>> [$round_name] HGET x ${KEYS}"
    run_benchmark "HGET-${round_name}" "$HGET_CMD"

    echo ""
    echo ">>> [$round_name] HDEL x ${KEYS}"
    run_benchmark "HDEL-${round_name}" "HDEL __key__"
}

echo "========================================"
echo "KVStore Mirror QPS 测试"
echo "端口: $PORT | 总请求数: $KEYS | 线程: $THREADS | 连接: $CONN"
echo "每客户端请求数: $REQS_PER_CLIENT"
echo "模式: $MODE"
echo "========================================"

if [ "$MODE" == "none" ]; then
    echo ""
    echo ">>> 请确保 kvstore 正在运行，且无 mirror 启动"
    echo "    按 Enter 开始 [none] 测试..."
    read
    run_round "none"
    echo -e "\n完成！"
    exit 0
fi

# 通用：先测 none 基线
if [ "$MODE" == "all" ] || [ "$MODE" == "uprobe" ] || [ "$MODE" == "xdp" ] || [ "$MODE" == "tc" ]; then
    echo ""
    echo ">>> 阶段 1/4 : [none] 无 mirror 基线"
    echo "    请确保 kvstore 正在运行，且无 mirror 启动"
    echo "    按 Enter 开始..."
    read
    run_round "none"
fi

test_mirror() {
    local m=$1
    echo ""
    echo ">>> 阶段 : [$m] mirror 测试"
    echo "    请手动启动 $m mirror"
    echo "    启动完成后，按 Enter 继续..."
    read
    run_round "$m"
    echo ""
    echo ">>> [$m] 测试结束，请停止 $m mirror"
}

if [ "$MODE" == "all" ]; then
    test_mirror "uprobe"
    test_mirror "xdp"
    test_mirror "tc"
    echo -e "\n全部测试完成！"
elif [ "$MODE" == "uprobe" ]; then
    test_mirror "uprobe"
    echo -e "\n完成！"
elif [ "$MODE" == "xdp" ]; then
    test_mirror "xdp"
    echo -e "\n完成！"
elif [ "$MODE" == "tc" ]; then
    test_mirror "tc"
    echo -e "\n完成！"
else
    echo "未知模式: $MODE"
    exit 1
fi
