#!/bin/bash
PORT=$1
[ -z "$PORT" ] && echo "用法: $0 <Redis端口号>" && exit 1

HOST="127.0.0.1"
KEYS=1000000
THREADS=4
CONN=50
REQS_PER_CLIENT=$((KEYS / (THREADS * CONN)))
PIPELINES=(10 20 40 80 160)
LAST_PL="${PIPELINES[${#PIPELINES[@]}-1]}"
OUTPUT_DIR="results_redis"
mkdir -p "$OUTPUT_DIR"

echo "========== Redis (SET/GET/DEL) Pipeline 压测 | PORT=$PORT =========="
echo "Keys=$KEYS | Threads=$THREADS | Conn=$CONN | Req/Client=$REQS_PER_CLIENT"
echo ""
echo "pipeline,command,qps" > "$OUTPUT_DIR/summary.csv"

run_test() {
    local pl=$1
    local cmd_name=$2
    local cmd_str=$3
    local json_file="$OUTPUT_DIR/${cmd_name}_p${pl}.json"

    memtier_benchmark -s $HOST -p $PORT \
        --command="$cmd_str" --command-ratio=1 --pipeline=$pl \
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
    echo "$pl,$cmd_name,$qps" >> "$OUTPUT_DIR/summary.csv"
    echo "  $cmd_name pipeline=$pl → QPS: $qps"
}

echo ""
for PL in "${PIPELINES[@]}"; do
    echo "---------- Pipeline Depth: $PL ----------"
    run_test $PL "SET" "set __key__ __data__"
    run_test $PL "GET" "get __key__"
    run_test $PL "DEL" "del __key__"
    if [ "$PL" != "$LAST_PL" ]; then
        echo ""
        echo "========================================"
        echo "Pipeline=$PL 测试完成"
        echo "请重启你的 Redis 服务，确认就绪后按 Enter 继续"
        echo "========================================"
        read -p ""
    fi
done

echo ""
echo "完成！结果: $OUTPUT_DIR/summary.csv"