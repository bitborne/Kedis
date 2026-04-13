#!/bin/bash

PORT=$1
AOF=$2

if [ -z "$PORT" ] || [ -z "$AOF" ]; then
    echo "用法: $0 <端口号> <aof yes|no>"
    exit 1
fi

HOST="127.0.0.1"
KEYS=1000000
THREADS=4
CONN=50
REQS_PER_CLIENT=$((KEYS / (THREADS * CONN)))


echo "KVStore AOF测试 - 端口:$PORT | AOF=$AOF | 总key数:$KEYS | 每客户端请求数:$REQS_PER_CLIENT"
echo "3秒后开始测试..."
sleep 3

echo -e "\n========== [HSET测试] =========="
memtier_benchmark \
    -s ${HOST} \
    -p ${PORT} \
    --command="HSET __key__ __data__" \
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

echo -e "\n========== [HDEL测试] =========="
memtier_benchmark \
    -s ${HOST} \
    -p ${PORT} \
    --command="HDEL __key__" \
    --command-ratio=1 \
    --command-key-pattern=P \
    -t ${THREADS} \
    -c ${CONN} \
    -n ${REQS_PER_CLIENT} \
    --key-prefix="k" \
    --key-minimum=1 \
    --key-maximum=${KEYS} \
    --hide-histogram

echo -e "\n完成！"
