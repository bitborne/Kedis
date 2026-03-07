#!/usr/bin/env python3
"""
Kedis Performance Benchmark Tool
================================
简化版：单次测试，保留JSON记录，同一commit覆盖

Usage:
    python3 benchmark.py -e H -o SET
    python3 benchmark.py -e S -o GET -d 256
"""

import argparse
import json
import os
import subprocess
import sys
import time
import shutil
from datetime import datetime

# =============================================================================
# 用户可修改的常量配置
# =============================================================================

CPU_CORES = 18          # CPU 核心数
CONNECTIONS = 50        # 每线程连接数
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8888
TEST_DURATION = 30      # GET测试持续时间（秒）
SET_TOTAL_KEYS = 1_000_000  # SET测试固定插入key数量
DEFAULT_DATA_SIZE = 128         # value 大小（bytes）
DEFAULT_KEY_MAX = 1_000_000     # key 数量上限
KEY_PREFIX = "perf_"

ENGINE_PREFIX = {
    "A": "A",   # Array
    "R": "R",   # RBTree
    "H": "H",   # Hash
    "S": "S",   # SkipList
}

# =============================================================================

def log(msg: str, level="INFO"):
    print(f"[{level}] {msg}")

def get_git_info() -> dict:
    """获取当前 git commit 信息"""
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    info = {"hash": "unknown", "message": ""}
    
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, cwd=root_dir
        )
        if result.returncode == 0:
            info["hash"] = result.stdout.strip()
        
        result = subprocess.run(
            ["git", "log", "-1", "--format=%s"],
            capture_output=True, text=True, cwd=root_dir
        )
        if result.returncode == 0:
            info["message"] = result.stdout.strip()
    except:
        pass
    
    return info

def check_memtier() -> bool:
    return shutil.which("memtier_benchmark") is not None

def wait_for_server(host: str, port: int, timeout: int = 30) -> bool:
    import socket
    start = time.time()
    while time.time() - start < timeout:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(1)
            if sock.connect_ex((host, port)) == 0:
                sock.close()
                return True
            sock.close()
        except:
            pass
        time.sleep(0.5)
    return False

def parse_result(json_path: str) -> dict:
    """解析 memtier_benchmark JSON 结果，正确提取 p99"""
    try:
        with open(json_path, "r") as f:
            data = json.load(f)
        
        # 找到命令对应的统计数据（如 "Hsets", "Ssets", "Hgets" 等）
        all_stats = data.get("ALL STATS", {})
        
        # 排除 Runtime，找到第一个命令统计
        cmd_stats = None
        for key in all_stats:
            if key != "Runtime":
                cmd_stats = all_stats[key]
                break
        
        if not cmd_stats:
            return {"error": "No command stats found"}
        
        # 提取基本指标
        result = {
            "ops_sec": float(cmd_stats.get("Ops/sec", 0)),
            "latency_avg": float(cmd_stats.get("Latency", 0)),
            "count": int(cmd_stats.get("Count", 0)),
        }
        
        # 提取百分位数 - 注意键名是 p50.00, p99.00, p99.90
        percentiles = cmd_stats.get("Percentile Latencies", {})
        result["latency_p50"] = float(percentiles.get("p50.00", percentiles.get("p50", 0)))
        result["latency_p99"] = float(percentiles.get("p99.00", percentiles.get("p99", 0)))
        result["latency_p999"] = float(percentiles.get("p99.90", percentiles.get("p99.9", percentiles.get("p999", 0))))
        
        return result
        
    except Exception as e:
        return {"error": str(e)}

def run_memtier(
    engine: str,
    operation: str,
    host: str,
    port: int,
    data_size: int,
    key_max: int,
    duration: int,
    output_dir: str,
    tag: str
) -> dict:
    """运行单次 memtier_benchmark"""
    prefix = ENGINE_PREFIX.get(engine, engine)
    cmd_prefix = f"{prefix}{operation}"
    
    cmd = [
        "memtier_benchmark",
        "-s", host,
        "-p", str(port),
        "-t", str(CPU_CORES),
        "-c", str(CONNECTIONS),
        "--test-time", str(duration),
        "--data-size", str(data_size),
        "--key-minimum", "1",
        "--key-maximum", str(key_max),
        "--key-prefix", KEY_PREFIX,
        "--command", f"{cmd_prefix} __key__ __data__" if operation == "SET" else f"{cmd_prefix} __key__",
        "--command-ratio", "1",
        "--command-key-pattern", "R" if operation == "GET" else "P",
        "--json-out-file", os.path.join(output_dir, f"{tag}.json"),
        "--print-percentiles", "50,90,95,99,99.9",
        "--hide-histogram",
    ]
    
    log(f"Running {tag}: {cmd_prefix}...")
    
    result_file = os.path.join(output_dir, f"{tag}.json")
    log_file = os.path.join(output_dir, f"{tag}.log")
    
    try:
        with open(log_file, "w") as f:
            f.write(f"Command: {' '.join(cmd)}\n")
            f.write(f"Time: {datetime.now().isoformat()}\n\n")
            f.flush()
            
            result = subprocess.run(cmd, stdout=f, stderr=subprocess.STDOUT, text=True)
        
        if result.returncode != 0:
            log(f"memtier failed with code {result.returncode}", "ERROR")
            return {"error": f"exit code {result.returncode}"}
        
        return parse_result(result_file)
        
    except Exception as e:
        log(f"Exception: {e}", "ERROR")
        return {"error": str(e)}

def run_fixed_set(engine: str, host: str, port: int, data_size: int, output_dir: str, tag: str) -> dict:
    """SET 测试：固定插入 100万 key，使用 -n 指定请求数"""
    prefix = ENGINE_PREFIX.get(engine, engine)
    cmd_prefix = f"{prefix}SET"
    
    # 计算每连接请求数
    total_conns = CPU_CORES * CONNECTIONS
    requests_per_conn = SET_TOTAL_KEYS // total_conns
    
    log(f"Fixed SET test: inserting {SET_TOTAL_KEYS} keys ({requests_per_conn} per connection)...")
    
    cmd = [
        "memtier_benchmark",
        "-s", host,
        "-p", str(port),
        "-t", str(CPU_CORES),
        "-c", str(CONNECTIONS),
        "-n", str(requests_per_conn),  # 每连接请求数
        "--data-size", str(data_size),
        "--key-minimum", "1",
        "--key-maximum", str(SET_TOTAL_KEYS),
        "--key-prefix", KEY_PREFIX,
        "--command", f"{cmd_prefix} __key__ __data__",
        "--command-ratio", "1",
        "--command-key-pattern", "P",  # 顺序写入
        "--json-out-file", os.path.join(output_dir, f"{tag}.json"),
        "--print-percentiles", "50,90,95,99,99.9",
        "--hide-histogram",
    ]
    
    result_file = os.path.join(output_dir, f"{tag}.json")
    log_file = os.path.join(output_dir, f"{tag}.log")
    
    try:
        with open(log_file, "w") as f:
            f.write(f"Command: {' '.join(cmd)}\n")
            f.write(f"Time: {datetime.now().isoformat()}\n")
            f.write(f"Fixed keys: {SET_TOTAL_KEYS}, per conn: {requests_per_conn}\n\n")
            f.flush()
            
            result = subprocess.run(cmd, stdout=f, stderr=subprocess.STDOUT, text=True)
        
        if result.returncode != 0:
            log(f"memtier failed with code {result.returncode}", "ERROR")
            return {"error": f"exit code {result.returncode}"}
        
        return parse_result(result_file)
        
    except Exception as e:
        log(f"Exception: {e}", "ERROR")
        return {"error": str(e)}

def run_prepopulate(engine: str, host: str, port: int, data_size: int, key_max: int) -> bool:
    """GET 测试前准备：写入全部数据"""
    log(f"Prepopulating: writing {key_max} keys...")
    
    tmp_dir = "/tmp/kedis_prepop"
    os.makedirs(tmp_dir, exist_ok=True)
    
    # 给足够时间写入
    duration = max(TEST_DURATION * 2, 60)
    
    result = run_memtier(
        engine=engine,
        operation="SET",
        host=host,
        port=port,
        data_size=data_size,
        key_max=key_max,
        duration=duration,
        output_dir=tmp_dir,
        tag="prepop"
    )
    
    if "error" in result:
        log("Prepopulate failed", "ERROR")
        return False
    
    written = int(result.get("ops_sec", 0) * duration)
    log(f"Prepopulate complete: ~{written} keys written")
    time.sleep(1)
    return True

def main():
    parser = argparse.ArgumentParser(description="Kedis Benchmark (Single Run)")
    parser.add_argument("-e", "--engine", choices=["A", "R", "H", "S"], required=True)
    parser.add_argument("-o", "--operation", choices=["SET", "GET"], required=True)
    parser.add_argument("-d", "--data-size", type=int, default=DEFAULT_DATA_SIZE)
    parser.add_argument("--key-max", type=int, default=DEFAULT_KEY_MAX)
    parser.add_argument("--no-warmup", action="store_true", help="GET测试跳过数据准备")
    args = parser.parse_args()
    
    if not check_memtier():
        log("memtier_benchmark not found", "ERROR")
        sys.exit(1)
    
    # Git 信息
    git_info = get_git_info()
    log(f"Git: {git_info['hash']} - {git_info['message'][:50]}...")
    
    # 创建输出目录（以 git hash 命名，同一 commit 覆盖）
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(script_dir, f"{git_info['hash']}_benchmark_results")
    os.makedirs(output_dir, exist_ok=True)
    
    # 等待服务器
    log(f"Connecting to {DEFAULT_HOST}:{DEFAULT_PORT}...")
    if not wait_for_server(DEFAULT_HOST, DEFAULT_PORT):
        log("Server not ready", "ERROR")
        sys.exit(1)
    
    # 执行测试
    log(f"\nBenchmark: {args.engine} engine, {args.operation} operation")
    log(f"Config: {CPU_CORES} threads, {CONNECTIONS} conn/thread")
    
    if args.operation == "SET":
        # SET：固定插入100万key，不预热
        result = run_fixed_set(
            engine=args.engine,
            host=DEFAULT_HOST,
            port=DEFAULT_PORT,
            data_size=args.data_size,
            output_dir=output_dir,
            tag=f"{args.engine}_{args.operation}"
        )
    else:  # GET
        # GET：先写入数据，再测试
        if not args.no_warmup:
            if not run_prepopulate(args.engine, DEFAULT_HOST, DEFAULT_PORT, args.data_size, args.key_max):
                sys.exit(1)
        
        log(f"\nGET test duration: {TEST_DURATION}s")
        result = run_memtier(
            engine=args.engine,
            operation="GET",
            host=DEFAULT_HOST,
            port=DEFAULT_PORT,
            data_size=args.data_size,
            key_max=args.key_max,
            duration=TEST_DURATION,
            output_dir=output_dir,
            tag=f"{args.engine}_{args.operation}"
        )
    
    # 显示结果
    if "error" in result:
        log(f"Test failed: {result['error']}", "ERROR")
        sys.exit(1)
    
    log(f"\n{'='*50}")
    log(f"QPS:        {result['ops_sec']:,.0f} ops/sec")
    log(f"Latency Avg: {result['latency_avg']:.2f} ms")
    log(f"Latency P50: {result['latency_p50']:.2f} ms")
    log(f"Latency P99: {result['latency_p99']:.2f} ms")
    log(f"Latency P99.9: {result['latency_p999']:.2f} ms")
    log(f"Total Ops:  {result['count']:,}")
    log(f"{'='*50}")
    
    # 保存汇总
    summary = {
        "timestamp": datetime.now().isoformat(),
        "git_hash": git_info['hash'],
        "git_message": git_info['message'],
        "engine": args.engine,
        "operation": args.operation,
        "config": {
            "host": DEFAULT_HOST,
            "port": DEFAULT_PORT,
            "threads": CPU_CORES,
            "connections": CONNECTIONS,
            "duration": TEST_DURATION,
            "data_size": args.data_size,
            "key_max": args.key_max,
        },
        "result": result,
    }
    
    summary_file = os.path.join(output_dir, f"{args.engine}_{args.operation}_summary.json")
    with open(summary_file, "w") as f:
        json.dump(summary, f, indent=2)
    
    log(f"\nResults saved to: {output_dir}")
    log(f"  - JSON: {args.engine}_{args.operation}.json")
    log(f"  - Summary: {args.engine}_{args.operation}_summary.json")

if __name__ == "__main__":
    main()
