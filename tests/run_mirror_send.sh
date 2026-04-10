#!/bin/bash
# save as: tests/run_mirror_send.sh
# usage: chmod +x tests/run_mirror_send.sh && ./tests/run_mirror_send.sh

set -e

# ---------- 自动识别项目根目录 ----------
# 获取脚本所在目录的绝对路径（即 tests/ 目录）
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
# 项目根目录 = tests/ 的上一级目录
PROJECT_ROOT="$( dirname "$SCRIPT_DIR" )"

echo "[*] 项目根目录: $PROJECT_ROOT"
echo "[*] 脚本所在目录: $SCRIPT_DIR"

# 检查必要的子目录是否存在
if [[ ! -d "$PROJECT_ROOT/mirror/src" ]]; then
    echo "[ERROR] 未找到 mirror/src 目录: $PROJECT_ROOT/mirror/src"
    exit 1
fi
if [[ ! -d "$PROJECT_ROOT/tests" ]]; then
    echo "[ERROR] 未找到 tests 目录: $PROJECT_ROOT/tests"
    exit 1
fi

# ---------- 定义路径 ----------
XDP_MIRROR="$PROJECT_ROOT/mirror/src/xdp_mirror"
CONFORMANCE_PY="$PROJECT_ROOT/tests/conformance.py"

# 检查文件是否存在且可执行
if [[ ! -x "$XDP_MIRROR" ]]; then
    echo "[ERROR] xdp_mirror 不存在或不可执行: $XDP_MIRROR"
    exit 1
fi
if [[ ! -f "$CONFORMANCE_PY" ]]; then
    echo "[ERROR] conformance.py 不存在: $CONFORMANCE_PY"
    exit 1
fi

# ---------- 启动测试 ----------
echo "[*] 正在启动 mirror (后台运行)..."

# 在后台启动 xdp_mirror，并记录 PID
sudo "$XDP_MIRROR" lo 172.20.10.2 8888 &
XDP_PID=$!

# 设置退出时自动清理（防止 xdp_mirror 残留）
cleanup() {
    echo "[*] 正在停止 xdp_mirror (PID: $XDP_PID)..."
    sudo kill $XDP_PID 2>/dev/null || true
    wait $XDP_PID 2>/dev/null || true
}
trap cleanup EXIT

# 等待 XDP 程序加载到网卡（通常需要几百毫秒到 1 秒）
sleep 2

echo "[*] 正在运行 conformance.py 并自动输入数据..."

# 向 conformance.py 依次输入: 1, 3, 8888, 回车（注意第二个选项从1改为3，原脚本写的是1但注释说3，这里按注释使用3）
# 使用 printf 确保最后一个空行（回车）也被发送
printf "1\n3\n8888\nq\n" | uv run python3 "$CONFORMANCE_PY"

echo "[*] 测试完成，脚本结束"