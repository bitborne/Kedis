#!/bin/bash

# =============================================================================
# Kedis 完整性能测试套件 - 每个测试点独立 kvstore 实例
#
# 特性:
#   1. 每个测试点都启动全新的 kvstore（确保公平性）
#   2. 每次测试后自动停止 kvstore 并清理数据
#   3. 详细进度打印
#
# 使用方法:
#   ./run_full_benchmark_suite.sh [选项]
# =============================================================================

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
PURPLE='\033[0;35m'
NC='\033[0m'

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }
log_highlight() { echo -e "${CYAN}$1${NC}"; }
log_phase() { echo -e "${PURPLE}$1${NC}"; }

# 默认配置
QUICK_MODE=false
RUN_ENGINE=true
RUN_MIXED=true
REPORT_DIR="./full_benchmark_report"

# 解析参数
while [[ $# -gt 0 ]]; do
    case $1 in
        --quick)
            QUICK_MODE=true
            shift
            ;;
        --engine)
            RUN_ENGINE=true
            RUN_MIXED=false
            shift
            ;;
        --mixed)
            RUN_ENGINE=false
            RUN_MIXED=true
            shift
            ;;
        --all)
            RUN_ENGINE=true
            RUN_MIXED=true
            shift
            ;;
        --help)
            echo "Kedis 完整性能测试套件"
            echo ""
            echo "用法: $0 [选项]"
            echo ""
            echo "选项:"
            echo "  --quick    快速模式 (缩短测试时间)"
            echo "  --engine   仅运行引擎对比测试"
            echo "  --mixed    仅运行混合负载测试"
            echo "  --all      运行所有测试 (默认)"
            echo "  --help     显示此帮助"
            echo ""
            echo "示例:"
            echo "  $0 --engine           # 仅引擎测试"
            echo "  $0 --engine --quick   # 快速引擎测试"
            exit 0
            ;;
        *)
            log_error "未知选项: $1"
            echo "使用 --help 查看帮助"
            exit 1
            ;;
    esac
done

# 检查依赖
check_dependencies() {
    local deps_ok=true

    for cmd in memtier_benchmark jq python3 bc nc; do
        if ! command -v $cmd &> /dev/null; then
            log_error "未找到依赖: $cmd"
            deps_ok=false
        fi
    done

    if ! $deps_ok; then
        echo ""
        echo "请安装缺失的依赖:"
        echo "  memtier_benchmark: https://github.com/RedisLabs/memtier_benchmark"
        echo "  jq: sudo pacman -S jq 或 sudo apt-get install jq"
        echo "  bc: sudo pacman -S bc 或 sudo apt-get install bc"
        echo "  nc: sudo pacman -S openbsd-netcat 或 sudo apt-get install netcat"
        exit 1
    fi

    if ! python3 -c "import matplotlib" 2>/dev/null; then
        log_error "未找到 Python matplotlib"
        echo "请安装: pip3 install matplotlib numpy"
        exit 1
    fi

    # 检查 kvstore 可执行文件
    if [[ ! -x "../kvstore" ]]; then
        log_error "未找到 kvstore 可执行文件"
        echo "请先编译: cd .. && make"
        exit 1
    fi

    # 检查 clean.sh
    if [[ ! -x "../clean.sh" ]]; then
        log_warn "未找到 clean.sh 脚本"
    fi
}

# 启动 kvstore（轻量版，用于单个测试点）
start_kvstore_for_test() {
    # 检查是否已有 kvstore 在运行，如果有则停止
    if pgrep -x "kvstore" > /dev/null; then
        pkill -x "kvstore" 2>/dev/null || true
        sleep 1
    fi

    # 启动 kvstore
    cd ..
    ./kvstore > /dev/null 2>&1 &
    local pid=$!
    cd - > /dev/null

    # 等待 kvstore 就绪（最多10秒）
    local retries=0
    while ! nc -z 127.0.0.1 8888 2>/dev/null; do
        sleep 0.5
        retries=$((retries + 1))
        if [[ $retries -gt 20 ]]; then
            log_error "kvstore 启动超时"
            return 1
        fi
    done

    echo $pid
}

# 停止 kvstore 并清理（轻量版）
stop_kvstore_and_clean() {
    local pid=$1

    # 停止 kvstore
    if [[ -n "$pid" ]] && kill -0 $pid 2>/dev/null; then
        kill $pid 2>/dev/null || true
        wait $pid 2>/dev/null || true
    fi

    # 确保没有残留进程
    pkill -x "kvstore" 2>/dev/null || true
    sleep 0.5

    # 运行 clean.sh 清理数据
    if [[ -x "../clean.sh" ]]; then
        cd ..
        ./clean.sh > /dev/null 2>&1 || true
        cd - > /dev/null
    fi
}

# 显示测试进度
print_progress() {
    local current=$1
    local total=$2
    local label=$3
    local percent=$((current * 100 / total))
    local bar_length=40
    local filled=$((percent * bar_length / 100))
    local empty=$((bar_length - filled))

    printf "\r["
    printf "%${filled}s" | tr ' ' '█'
    printf "%${empty}s" | tr ' ' '░'
    printf "] %3d%% (%d/%d) %s" "$percent" "$current" "$total" "$label"
}

# 运行单个 memtier 测试（带 kvstore 生命周期管理）
run_single_test() {
    local test_name=$1
    shift
    local memtier_args=("$@")

    # 启动 kvstore
    local kvs_pid=$(start_kvstore_for_test)
    if [[ -z "$kvs_pid" ]]; then
        log_error "启动 kvstore 失败"
        return 1
    fi

    # 运行 memtier 测试
    memtier_benchmark "${memtier_args[@]}" > /dev/null 2>&1 || true

    # 停止 kvstore 并清理
    stop_kvstore_and_clean "$kvs_pid"
}

# 运行引擎对比测试
run_engine_benchmark() {
    log_phase "═══════════════════════════════════════════════════════════════"
    log_phase "  阶段 1: 引擎性能对比测试"
    log_phase "═══════════════════════════════════════════════════════════════"
    echo ""

    local engines=("A" "R" "H" "S")
    local engine_names=("Array" "RBTree" "Hash" "SkipList")
    local data_sizes=(128)
    local key_spaces=(50000)
    local key_labels=("Medium_50K")

    if ! $QUICK_MODE; then
        data_sizes=(128 512 1024)
        key_spaces=(10000 50000)
        key_labels=("Small_10K" "Medium_50K")
    fi

    local total_tests=$((${#engines[@]} * ${#data_sizes[@]} * ${#key_spaces[@]} * 2))  # ×2 for SET+GET
    local current=0
    local start_time=$(date +%s)

    log_info "总测试点数: $total_tests"
    log_info "引擎: ${engine_names[*]}"
    log_info "数据大小: ${data_sizes[*]} B"
    log_info "键空间: ${key_labels[*]}"
    echo ""
    log_warn "每个测试点使用独立的 kvstore 实例"
    echo ""

    # 创建输出目录
    mkdir -p ./engine_benchmark_results

    for op in "SET" "GET"; do
        log_phase "▶ ${op} 操作测试"
        echo ""

        for i in "${!engines[@]}"; do
            local engine="${engines[$i]}"
            local engine_name="${engine_names[$i]}"

            log_highlight "  测试引擎: ${engine_name} (${engine})"

            for size in "${data_sizes[@]}"; do
                for k in "${!key_spaces[@]}"; do
                    local key_space="${key_spaces[$k]}"
                    local key_label="${key_labels[$k]}"

                    current=$((current + 1))
                    local config_name="${op}_${engine}_${size}B_${key_label}"

                    print_progress $current $total_tests "${engine} ${op} ${size}B"
                    echo ""

                    if [[ "$op" == "SET" ]]; then
                        # SET 测试 - 新 kvstore 实例
                        run_single_test "${config_name}" \
                            -s 127.0.0.1 -p 8888 \
                            --command="${engine}SET __key__ __data__" \
                            --command-ratio=1 --command-key-pattern=R \
                            -t 8 -c 50 --test-time=30 \
                            -d ${size} \
                            --key-prefix="${engine}_" \
                            --key-minimum=1 --key-maximum=${key_space} \
                            --hide-histogram \
                            --json-out-file="./engine_benchmark_results/${config_name}_result.json"

                    else
                        # GET 测试 - 需要预热，然后新 kvstore 实例
                        # 先在一个实例中预热
                        local kvs_pid=$(start_kvstore_for_test)

                        # 预热写入
                        memtier_benchmark \
                            -s 127.0.0.1 -p 8888 \
                            --command="${engine}SET __key__ __data__" \
                            --command-ratio=1 --command-key-pattern=P \
                            -t 4 -c 10 --test-time=5 \
                            -d ${size} \
                            --key-prefix="warm_${engine}_" \
                            --key-minimum=1 --key-maximum=${key_space} \
                            > /dev/null 2>&1 || true

                        # GET 测试
                        memtier_benchmark \
                            -s 127.0.0.1 -p 8888 \
                            --command="${engine}GET __key__" \
                            --command-ratio=1 --command-key-pattern=R \
                            -t 8 -c 50 --test-time=30 \
                            --key-prefix="warm_${engine}_" \
                            --key-minimum=1 --key-maximum=${key_space} \
                            --hide-histogram \
                            --json-out-file="./engine_benchmark_results/${config_name}_result.json" \
                            > /dev/null 2>&1 || true

                        # 停止并清理
                        stop_kvstore_and_clean "$kvs_pid"
                    fi
                done
            done
        done
    done

    echo ""
    local end_time=$(date +%s)
    log_success "引擎测试完成，耗时 $((end_time - start_time)) 秒"

    # 生成图表
    log_info "生成引擎对比图表..."
    python3 gen_engine_charts.py ./engine_benchmark_results 2>/dev/null || log_warn "图表生成失败"
}

# 运行混合负载测试
run_mixed_benchmark() {
    log_phase "═══════════════════════════════════════════════════════════════"
    log_phase "  阶段 2: 混合负载性能测试"
    log_phase "═══════════════════════════════════════════════════════════════"
    echo ""

    local engines=("A" "R" "H" "S")
    local engine_names=("Array" "RBTree" "Hash" "SkipList")

    # 场景配置: "场景名称|SET比例|GET比例|数据大小"
    if $QUICK_MODE; then
        local scenarios=(
            "Balanced_55|50|50|128"
            "Read_Heavy|0|100|128"
        )
    else
        local scenarios=(
            "Write_Heavy|100|0|128"
            "Write_Read_82|80|20|128"
            "Balanced_55|50|50|128"
            "Read_Cache_28|20|80|128"
            "Read_Heavy|0|100|128"
        )
    fi

    local key_space=50000
    local test_time=30
    local threads=8
    local connections=50

    local total_tests=$((${#engines[@]} * ${#scenarios[@]}))
    local current=0
    local start_time=$(date +%s)

    log_info "总测试点数: $total_tests"
    log_info "引擎: ${engine_names[*]}"
    log_info "场景: $(echo "${scenarios[@]}" | tr '|' ' ')"
    log_info "键空间: ${key_space}, 测试时间: ${test_time}s"
    echo ""
    log_warn "每个测试点使用独立的 kvstore 实例"
    echo ""

    # 创建输出目录
    mkdir -p ./mixed_workload_results

    for scenario_config in "${scenarios[@]}"; do
        IFS='|' read -r scenario_name set_ratio get_ratio data_size <<< "$scenario_config"

        log_phase "▶ 场景: ${scenario_name} (SET:${set_ratio}% GET:${get_ratio}%)"
        echo ""

        for i in "${!engines[@]}"; do
            local engine="${engines[$i]}"
            local engine_name="${engine_names[$i]}"

            current=$((current + 1))
            local config_name="${engine}_${scenario_name}"

            print_progress $current $total_tests "${engine} ${scenario_name}"
            echo ""

            # 启动 kvstore
            local kvs_pid=$(start_kvstore_for_test)

            # 预热数据（如果需要读取）
            if [[ "$get_ratio" -gt 0 ]]; then
                memtier_benchmark \
                    -s 127.0.0.1 -p 8888 \
                    --command="${engine}SET __key__ __data__" \
                    --command-ratio=1 --command-key-pattern=P \
                    -t 4 -c 10 --test-time=5 \
                    -d ${data_size} \
                    --key-prefix="warm_${engine}_" \
                    --key-minimum=1 --key-maximum=${key_space} \
                    > /dev/null 2>&1 || true
            fi

            if [[ "$set_ratio" == "100" ]]; then
                # 纯写入
                memtier_benchmark \
                    -s 127.0.0.1 -p 8888 \
                    --command="${engine}SET __key__ __data__" \
                    --command-ratio=1 --command-key-pattern=R \
                    -t ${threads} -c ${connections} --test-time=${test_time} \
                    -d ${data_size} \
                    --key-prefix="${engine}_" \
                    --key-minimum=1 --key-maximum=${key_space} \
                    --hide-histogram \
                    --json-out-file="./mixed_workload_results/${config_name}_result.json" \
                    > /dev/null 2>&1 || true

            elif [[ "$get_ratio" == "100" ]]; then
                # 纯读取
                memtier_benchmark \
                    -s 127.0.0.1 -p 8888 \
                    --command="${engine}GET __key__" \
                    --command-ratio=1 --command-key-pattern=R \
                    -t ${threads} -c ${connections} --test-time=${test_time} \
                    --key-prefix="warm_${engine}_" \
                    --key-minimum=1 --key-maximum=${key_space} \
                    --hide-histogram \
                    --json-out-file="./mixed_workload_results/${config_name}_result.json" \
                    > /dev/null 2>&1 || true

            else
                # 混合读写 - 使用两个 command 参数分别指定 SET 和 GET
                memtier_benchmark \
                    -s 127.0.0.1 -p 8888 \
                    -t ${threads} -c ${connections} --test-time=${test_time} \
                    --command="${engine}SET __key__ __data__" \
                    --command-ratio=${set_ratio} \
                    --command-key-pattern=R \
                    --command="${engine}GET __key__" \
                    --command-ratio=${get_ratio} \
                    --command-key-pattern=R \
                    -d ${data_size} \
                    --key-prefix="mix_${engine}_" \
                    --key-minimum=1 --key-maximum=${key_space} \
                    --hide-histogram \
                    --json-out-file="./mixed_workload_results/${config_name}_result.json" \
                    > /dev/null 2>&1 || true
            fi

            # 停止 kvstore 并清理
            stop_kvstore_and_clean "$kvs_pid"
        done
    done

    echo ""
    local end_time=$(date +%s)
    log_success "混合负载测试完成，耗时 $((end_time - start_time)) 秒"

    # 生成图表
    log_info "生成混合负载图表..."
    python3 gen_mixed_charts.py ./mixed_workload_results 2>/dev/null || log_warn "图表生成失败"
}

# 生成综合报告
generate_report() {
    log_phase "═══════════════════════════════════════════════════════════════"
    log_phase "  生成测试报告"
    log_phase "═══════════════════════════════════════════════════════════════"
    echo ""

    mkdir -p "${REPORT_DIR}"

    if [[ -d "./engine_benchmark_results/charts" ]]; then
        cp -r ./engine_benchmark_results/charts "${REPORT_DIR}/engine_charts"
        log_success "已复制引擎对比图表"
    fi

    if [[ -d "./mixed_workload_results/charts" ]]; then
        cp -r ./mixed_workload_results/charts "${REPORT_DIR}/mixed_charts"
        log_success "已复制混合负载图表"
    fi

    # 生成索引文件
    cat > "${REPORT_DIR}/index.html" << 'EOF'
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Kedis 性能测试报告</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 40px; background: #f5f5f5; }
        h1 { color: #333; border-bottom: 3px solid #3498db; padding-bottom: 10px; }
        h2 { color: #555; margin-top: 30px; }
        .section { background: white; padding: 20px; margin: 20px 0; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        .chart-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(400px, 1fr)); gap: 20px; }
        .chart-item { text-align: center; }
        .chart-item img { max-width: 100%; border: 1px solid #ddd; border-radius: 4px; }
        .chart-item p { color: #666; margin-top: 10px; }
        .timestamp { color: #999; font-size: 0.9em; }
        .note { background: #fff3cd; border-left: 4px solid #ffc107; padding: 15px; margin: 20px 0; }
    </style>
</head>
<body>
    <h1>Kedis 性能测试报告</h1>
    <p class="timestamp">生成时间: TIMESTAMP</p>

    <div class="note">
        <strong>测试方法说明:</strong> 每个测试点都使用独立的 kvstore 实例，
        测试前启动全新进程，测试后停止并清理数据，确保测试公平性。
    </div>

    <div class="section">
        <h2>引擎对比测试</h2>
        <p>对比四种存储引擎（Array、RBTree、Hash、SkipList）在不同数据大小和键空间下的 SET/GET 性能。</p>
        <div class="chart-grid">
ENGINE_CHARTS
        </div>
    </div>

    <div class="section">
        <h2>混合负载测试</h2>
        <p>模拟真实业务场景下的读写混合负载表现。</p>
        <div class="chart-grid">
MIXED_CHARTS
        </div>
    </div>
</body>
</html>
EOF

    # 替换时间戳
    sed -i "s/TIMESTAMP/$(date '+%Y-%m-%d %H:%M:%S')/g" "${REPORT_DIR}/index.html"

    # 生成引擎图表列表
    local engine_charts=""
    if [[ -d "${REPORT_DIR}/engine_charts" ]]; then
        for img in "${REPORT_DIR}/engine_charts"/*.png; do
            [[ -f "$img" ]] || continue
            local basename=$(basename "$img")
            engine_charts+="            <div class=\"chart-item\"><img src=\"engine_charts/${basename}\" alt=\"${basename}\"><p>${basename%.png}</p></div>\n"
        done
    fi

    # 生成混合负载图表列表
    local mixed_charts=""
    if [[ -d "${REPORT_DIR}/mixed_charts" ]]; then
        for img in "${REPORT_DIR}/mixed_charts"/*.png; do
            [[ -f "$img" ]] || continue
            local basename=$(basename "$img")
            mixed_charts+="            <div class=\"chart-item\"><img src=\"mixed_charts/${basename}\" alt=\"${basename}\"><p>${basename%.png}</p></div>\n"
        done
    fi

    # 替换图表占位符
    sed -i "s|ENGINE_CHARTS|${engine_charts}|g" "${REPORT_DIR}/index.html"
    sed -i "s|MIXED_CHARTS|${mixed_charts}|g" "${REPORT_DIR}/index.html"

    log_success "报告已生成: ${REPORT_DIR}/"
    log_info "可用浏览器打开: ${REPORT_DIR}/index.html"
}

# 主函数
main() {
    local start_time=$(date +%s)

    check_dependencies

    echo ""
    log_highlight "╔════════════════════════════════════════════════════════════════╗"
    log_highlight "║              Kedis 完整性能测试套件                            ║"
    log_highlight "║         （每个测试点使用独立 kvstore 实例）                    ║"
    log_highlight "╚════════════════════════════════════════════════════════════════╝"
    echo ""

    if $QUICK_MODE; then
        log_warn "快速模式已启用"
        echo ""
    fi

    log_info "测试计划:"
    if $RUN_ENGINE; then
        echo "  [✓] 引擎对比测试 (每个测试点重启 kvstore)"
    fi
    if $RUN_MIXED; then
        echo "  [✓] 混合负载测试 (每个测试点重启 kvstore)"
    fi
    echo ""

    read -p "按 Enter 开始测试，或 Ctrl+C 取消..."
    echo ""

    # 运行测试
    if $RUN_ENGINE; then
        run_engine_benchmark
    fi

    if $RUN_MIXED; then
        run_mixed_benchmark
    fi

    # 生成报告
    generate_report

    # 计算耗时
    local end_time=$(date +%s)
    local total_duration=$((end_time - start_time))

    echo ""
    log_highlight "╔════════════════════════════════════════════════════════════════╗"
    log_highlight "║                     测试套件执行完毕                           ║"
    log_highlight "╚════════════════════════════════════════════════════════════════╝"
    echo ""
    log_success "总耗时: $((total_duration / 60)) 分 $((total_duration % 60)) 秒"
    echo ""
    echo "报告位置: ${REPORT_DIR}/"
    echo ""
}

main "$@"
