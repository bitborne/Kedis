#!/bin/bash
PORT=$1
[ -z "$PORT" ] && echo "用法: $0 <Kedis端口号>" && exit 1

HOST="127.0.0.1"
KEYS=1000000
THREADS=4
CONN=50
REQS_PER_CLIENT=$((KEYS / (THREADS * CONN)))
OUTPUT_DIR="results_save"
mkdir -p "$OUTPUT_DIR"

echo "========== Save 策略 QPS 影响测试 | PORT=$PORT =========="
echo "Keys=$KEYS | Threads=$THREADS | Conn=$CONN | Req/Client=$REQS_PER_CLIENT"
echo ""
echo "strategy,qps" > "$OUTPUT_DIR/summary.csv"

run_test() {
    local strategy=$1
    local desc=$2
    local json_file="$OUTPUT_DIR/${strategy}.json"

    echo ""
    echo "========================================"
    echo "即将测试策略: $strategy"
    echo "说明: $desc"
    echo "========================================"
    echo "请手动修改配置文件、启停服务，确认就绪后按 Enter 继续"
    read -p ""

    memtier_benchmark -s $HOST -p $PORT \
        --command="HSET __key__ __data__" --command-ratio=1 \
        --command-key-pattern=P \
        -t $THREADS -c $CONN -n $REQS_PER_CLIENT \
        --key-prefix="k" --key-minimum=1 --key-maximum=$KEYS \
        --random-data \
        --json-out-file="$json_file" \
        --hide-histogram

    local qps=$(python3 -c "
import json
with open('$json_file') as f: d=json.load(f)
print(d['ALL STATS']['Totals']['Ops/sec'])
")
    echo "$strategy,$qps" >> "$OUTPUT_DIR/summary.csv"
    echo "  $strategy → QPS: $qps"
}

run_test "no_save"    "不开启自动快照保存 (auto-save-enabled no)"
run_test "100k_save"  "10w 条变化强制落盘一次 (auto-save-enabled yes + auto-save-max-changes 100000)"
run_test "10k_save"   "1w 条变化强制落盘一次 (auto-save-enabled yes + auto-save-max-changes 10000)"
run_test "1k_save"    "1k 条变化强制落盘一次 (auto-save-enabled yes + auto-save-max-changes 1000)"

echo ""
echo "完成！结果: $OUTPUT_DIR/summary.csv"
