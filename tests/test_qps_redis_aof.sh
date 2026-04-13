#!/bin/bash

PORT=$1
if [ -z "$PORT" ]; then
    echo "用法: $0 <redis端口号>"
    exit 1
fi

KEYS=1000000
THREADS=4
CONN=50
# 关键修正：计算每个客户端的请求数
REQS_PER_CLIENT=$((KEYS / (THREADS * CONN)))

echo "Redis AOF测试 - 端口:$PORT | 总key数:$KEYS | 每客户端请求数:$REQS_PER_CLIENT"

test_aof() {
    local aof=$1
    echo -e "\n========== AOF=$aof =========="
    
    redis-cli -p $PORT CONFIG SET appendonly $aof >/dev/null
    redis-cli -p $PORT FLUSHALL >/dev/null
    echo "AOF已设置，数据已清空，3秒后开始..."
    sleep 3
    
    echo "[SET测试]"
    memtier_benchmark -p $PORT -t $THREADS -c $CONN -n $REQS_PER_CLIENT \
        --command="set __key__ __data__" --key-prefix="k" --key-maximum=$KEYS \
        --random-data --hide-histogram
    
    echo "[DEL测试]"
    memtier_benchmark -p $PORT -t $THREADS -c $CONN -n $REQS_PER_CLIENT \
        --command="del __key__" --key-prefix="k" --key-maximum=$KEYS \
        --hide-histogram
}

test_aof no
test_aof yes

redis-cli -p $PORT CONFIG SET appendonly no >/dev/null
echo -e "\n完成！"