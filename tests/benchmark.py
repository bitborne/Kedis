import socket
import time
import random
import threading
import json
import argparse
import os
import string
import sys
import math

# 引用现有的 RESP 客户端逻辑
from resp_client import RawRedisClient

class BenchmarkClient(RawRedisClient):
    """极致性能压测客户端：计时期间零解析、零预处理"""
    
    def send_only(self, raw_data):
        """仅执行发送操作"""
        self.sock.sendall(raw_data)

    def wait_for_first_byte(self):
        """阻塞直到接收到第一个响应字节，用于立即停止计时"""
        return self.reader.read(1)

    def consume_rest(self, first_byte):
        """在计时结束后，快速清理 Socket 缓冲区以准备下一个请求"""
        if not first_byte: return
        
        # 读取当前行的剩余部分 (\r\n)
        line_rest = self.reader.readline()
        
        # 如果是 Bulk String ($), 需要读完后续的数据块
        if first_byte == b'$':
            try:
                length = int(line_rest[:-2])
                if length != -1:
                    self.reader.read(length + 2) # 数据内容 + \r\n
            except: pass
        # 如果是 Array (*), 递归清理每个元素
        elif first_byte == b'*':
            try:
                count = int(line_rest[:-2])
                for _ in range(count):
                    fb = self.reader.read(1)
                    self.consume_rest(fb)
            except: pass

class PerformanceTester:
    def __init__(self, args):
        self.args = args
        self.results_lock = threading.Lock()
        
        # 统计数据
        self.latencies = {"TOTAL": []}
        self.op_counts = {"TOTAL": 0}
        
        # 混合比例解析
        self.ratios = self._parse_ratio(args.ratio)
        
        self.start_time = 0
        self.stop_event = threading.Event()

    def _parse_ratio(self, ratio_str):
        """解析 set:1,get:10 格式"""
        if not ratio_str: return None
        pairs = ratio_str.split(',')
        ratios = []
        for p in pairs:
            cmd, weight = p.split(':')
            ratios.extend([cmd.upper()] * int(weight))
        return ratios

    def generate_payload(self, size):
        return ''.join(random.choices(string.ascii_letters + string.digits, k=size))

    def get_key(self, is_hit_path=True):
        """科学的 Key 生成逻辑"""
        key_range = self.args.key_maximum - self.args.key_minimum
        
        if is_hit_path:
            # 热区：[min, max]
            key_id = random.randint(self.args.key_minimum, self.args.key_maximum)
        else:
            # 冷区：[max + 1, max + range*2] 确保绝对不命中
            key_id = random.randint(self.args.key_maximum + 1, self.args.key_maximum + 1 + key_range * 2)
            
        return f"{self.args.key_prefix}{key_id}"

    def worker(self, thread_id):
        clients = []
        try:
            for _ in range(self.args.clients):
                client = BenchmarkClient(self.args.server, self.args.port)
                client.connect()
                clients.append(client)
        except Exception as e:
            print(f"[Thread {thread_id}] 连接失败: {e}")
            return

        base_payload = self.generate_payload(self.args.max_size)
        local_stats = {} # { "CMD": [latencies] }

        while not self.stop_event.is_set():
            if self.args.test_time > 0 and (time.time() - self.start_time) > self.args.test_time:
                break
            
            for client in clients:
                # 1. 确定操作类型
                if self.ratios:
                    cmd = random.choice(self.ratios)
                else:
                    cmd = self.args.command.split()[0].upper()

                # 2. 命中率控制
                is_read = cmd in ["GET", "EXIST", "AGET", "HGET", "RGET", "SGET"]
                is_hit = random.random() < self.args.hit_rate if is_read else True
                key = self.get_key(is_hit)
                
                # 3. 负载大小控制
                p_size = random.randint(self.args.min_size, self.args.max_size) if self.args.random_size else self.args.data_size
                payload = base_payload[:p_size]

                # 4. 【数据预处理 - 计时开始前】
                if self.ratios:
                    actual_args = [cmd, key]
                    if "SET" in cmd or "MOD" in cmd: actual_args.append(payload)
                else:
                    cmd_template = self.args.command.split()
                    actual_args = [p.replace("__key__", key).replace("__data__", payload).replace("__value__", payload) for p in cmd_template]
                
                raw_msg = client.encode_array(*actual_args)
                tag = f"{actual_args[0].upper()}_{'HIT' if is_hit else 'MISS'}" if is_read else actual_args[0].upper()

                # 5. 【核心计时 - 仅包含物理发送与首字节到达】
                start = time.perf_counter()
                try:
                    client.send_only(raw_msg)
                    first_byte = client.wait_for_first_byte()
                    end = time.perf_counter()
                    
                    # 6. 【响应清理 - 计时结束后】
                    client.consume_rest(first_byte)
                    
                    lat = (end - start) * 1000
                    if tag not in local_stats: local_stats[tag] = []
                    local_stats[tag].append(lat)
                except Exception:
                    continue
                
                if self.stop_event.is_set(): break

        for client in clients: client.close()

        with self.results_lock:
            for tag, lats in local_stats.items():
                if tag not in self.latencies: 
                    self.latencies[tag] = []
                    self.op_counts[tag] = 0
                self.latencies[tag].extend(lats)
                self.op_counts[tag] += len(lats)
                self.latencies["TOTAL"].extend(lats)
                self.op_counts["TOTAL"] += len(lats)

    def populate(self):
        """预热阶段：全量填充 4 个引擎的热区数据并验证"""
        engines = ['A', 'R', 'H', 'S']
        count = self.args.key_maximum - self.args.key_minimum + 1
        
        print(f"\n{'='*20} DATA POPULATE PHASE {'='*20}")
        print(f"热区范围: [{self.args.key_minimum} - {self.args.key_maximum}] (共 {count} 个键)")
        
        def run_parallel_task(task_name, action_fn):
            print(f"  -> {task_name}...", end="", flush=True)
            threads = []
            num_workers = 10 # 使用 10 线程并发预热
            chunk = count // num_workers
            if chunk == 0: chunk = count
            
            for i in range(num_workers):
                s = self.args.key_minimum + i * chunk
                if s > self.args.key_maximum: break
                e = s + chunk - 1 if i < num_workers - 1 else self.args.key_maximum
                t = threading.Thread(target=action_fn, args=(s, e))
                t.start()
                threads.append(t)
            for t in threads: t.join()
            print(" [DONE]")

        # 1. SET 阶段
        print("\n[Step 1/2] 正在向 4 个引擎填充数据...")
        for eng in engines:
            def set_action(start, end):
                c = BenchmarkClient(self.args.server, self.args.port).connect()
                p = self.generate_payload(self.args.data_size)
                cmd = f"{eng}SET"
                for i in range(start, end + 1):
                    # 使用标准 execute 进行同步预热
                    c.execute(cmd, f"{self.args.key_prefix}{i}", p)
                c.close()
            run_parallel_task(f"填充 {eng} 引擎", set_action)

        # 2. GET 验证阶段
        print("\n[Step 2/2] 正在执行全量 GET 验证...")
        for eng in engines:
            def get_action(start, end):
                c = BenchmarkClient(self.args.server, self.args.port).connect()
                cmd = f"{eng}GET"
                for i in range(start, end + 1):
                    resp = c.execute(cmd, f"{self.args.key_prefix}{i}")
                    # 验证响应，确保热区 Key 确实存在
                    if not resp or resp[0] != 'bulk_string' or resp[1] is None:
                        print(f"\n[CRITICAL] 预热验证失败! 引擎: {eng}, Key: {self.args.key_prefix}{i}, 响应: {resp}")
                c.close()
            run_parallel_task(f"验证 {eng} 引擎", get_action)

        print(f"\n{'='*60}")
        print("预热完成！热区已全部就绪。")
        if self.args.ratio and "SET" in self.args.ratio.upper():
            print("注意: 由于已完成预热，正式压测中的 SET 指令将触发 'Key has existed' 逻辑。")
            print("建议: 若要测试真实写入/更新性能，请将 ratio 中的 'set' 改为 'mod'。")
        print(f"{'='*60}\n")
        
        input(">>> 请检查服务器状态，确认无误后按 [Enter] 键正式开始性能测试...")

    def run(self):
        if self.args.populate:
            self.populate()

        print(f"=== KVStore Hybrid Benchmark Started ===")
        print(f"Target: {self.args.server}:{self.args.port} | Threads: {self.args.threads}")
        if self.args.ratio:
            print(f"Ratio: {self.args.ratio} | Hit Rate: {self.args.hit_rate*100}%")
        else:
            print(f"Command: {self.args.command}")
        
        self.start_time = time.time()
        threads = [threading.Thread(target=self.worker, args=(i,)) for i in range(self.args.threads)]
        for t in threads: t.start()

        try:
            for t in threads: t.join()
        except KeyboardInterrupt:
            self.stop_event.set()
            for t in threads: t.join()

        self.report(time.time() - self.start_time)

    def report(self, duration):
        if self.op_counts["TOTAL"] == 0:
            print("No data collected.")
            return

        report = {
            "summary": {
                "qps": round(self.op_counts["TOTAL"] / duration, 2),
                "duration": round(duration, 2),
                "total_ops": self.op_counts["TOTAL"]
            },
            "details": {}
        }

        print("\n" + "="*60)
        print(f"{'Phase':<15} | {'QPS':<10} | {'Avg(ms)':<10} | {'p99(ms)':<10}")
        print("-" * 60)

        for tag in sorted(self.latencies.keys()):
            lats = self.latencies[tag]
            if not lats: continue
            lats.sort()
            
            avg = sum(lats) / len(lats)
            p99 = lats[int(len(lats) * 0.99)]
            cmd_qps = len(lats) / duration
            
            # 修正：理论极限 QPS = (单连接在当前延迟下的潜力) * 总并发连接数
            total_concurrency = self.args.threads * self.args.clients
            theoretical_qps = (1000 / avg) * total_concurrency if avg > 0 else 0
            
            print(f"{tag:<15} | {cmd_qps:<10.2f} | {avg:<10.4f} | {p99:<10.4f}")
            
            report["details"][tag] = {
                "actual_qps": round(cmd_qps, 2),
                "theoretical_max_qps": round(theoretical_qps, 2),
                "avg_lat_ms": round(avg, 4),
                "p99_lat_ms": round(p99, 4),
                "count": len(lats)
            }

        print("="*60)
        filename = f"report_hybrid_{int(time.time())}.json"
        with open(filename, 'w') as f:
            json.dump(report, f, indent=2)
        print(f"详细报告已保存至: {filename}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--threads", type=int, default=4)
    parser.add_argument("-c", "--clients", type=int, default=10)
    parser.add_argument("-s", "--server", default="127.0.0.1")
    parser.add_argument("-p", "--port", type=int, default=8888)
    parser.add_argument("--command", default="SET __key__ __data__")
    parser.add_argument("--ratio", help="指令混合比例, 如 'set:1,get:9'")
    parser.add_argument("--hit-rate", type=float, default=0.8, help="读取命中率 (0.0-1.0)")
    parser.add_argument("--key-prefix", default="kv_")
    parser.add_argument("--key-minimum", type=int, default=1)
    parser.add_argument("--key-maximum", type=int, default=100000)
    parser.add_argument("--data-size", type=int, default=128)
    parser.add_argument("--random-size", action="store_true")
    parser.add_argument("--min-size", type=int, default=10)
    parser.add_argument("--max-size", type=int, default=1024)
    parser.add_argument("--test-time", type=int, default=10)
    parser.add_argument("--populate", action="store_true", help="是否在压测前预先填充热区数据")
    
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    tester = PerformanceTester(parser.parse_args())
    tester.run()