#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
jemalloc vs kmem 内存分配器测试脚本

测试流程:
    1. SSET 插入 1,000,000 个 key
    2. SGET 读取全部 key  
    3. SDEL 删除全部 key

前置条件:
    - 服务器已通过 valgrind massif 启动
    - 数据目录已清理 (./clean.sh)

使用方法:
    # 1. 先启动服务器（在另一个终端）
    # 2. 运行测试脚本
    python3 tests/1000K_mem_test.py --host 127.0.0.1 --port 8888

    # 可选参数
    python3 tests/1000K_mem_test.py --host 127.0.0.1 --port 8888 --count 1000000 --value-size 128 --batch-report 10000
"""

import argparse
import sys
import os
import time

# 确保能导入 base.py
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from base import KVRedis


class MemoryTest1000K:
    """100万 key 内存测试 - 基于 redis-py"""
    
    def __init__(self, host='127.0.0.1', port=8888, count=1000000, 
                 value_size=128, batch_report=10000):
        self.host = host
        self.port = port
        self.count = count
        self.value_size = value_size
        self.batch_report = batch_report
        self.client = None
        
        # 测试统计
        self.stats = {
            'set_count': 0,
            'set_errors': 0,
            'set_start_time': 0,
            'set_end_time': 0,
            'get_count': 0,
            'get_errors': 0,
            'get_misses': 0,
            'get_start_time': 0,
            'get_end_time': 0,
            'del_count': 0,
            'del_errors': 0,
            'del_start_time': 0,
            'del_end_time': 0,
        }
        
    def connect(self):
        """连接服务器 - 使用 redis-py 客户端"""
        print(f"[*] 连接到 {self.host}:{self.port}")
        # 使用 base.py 中的 KVRedis 类
        # decode_responses=False 保持字节流返回，与 RESP 协议一致
        self.client = KVRedis(
            host=self.host, 
            port=self.port, 
            decode_responses=False,
            lib_name=None,      # 禁用 lib 名称发送
            lib_version=None,   # 禁用 lib 版本发送
            socket_connect_timeout=10,
            socket_timeout=30,
            health_check_interval=0  # 禁用健康检查以提高性能
        )
        
        # # 测试连接
        # try:
        #     self.client.ping()
        #     print("[+] 连接成功")
        # except Exception as e:
        #     print(f"[!] 连接测试失败: {e}")
        #     raise
        
    def close(self):
        """关闭连接"""
        if self.client:
            self.client.close()
            print("[+] 连接已关闭")
            
    def phase_set(self):
        """SET 阶段: 插入所有 key"""
        print(f"\n{'='*60}")
        print(f"[PHASE 1/3] SET - 插入 {self.count} 个 key")
        print(f"{'='*60}")
        
        value = 'V' * self.value_size
        self.stats['set_start_time'] = time.time()
        
        for i in range(self.count):
            key = f"key_{i:08d}"
            try:
                # 使用 redis-py 的 execute_command 发送 SSET 命令
                resp = self.client.execute_command('SSET', key, value)
                self.stats['set_count'] += 1
                
                if i > 0 and i % self.batch_report == 0:
                    elapsed = time.time() - self.stats['set_start_time']
                    ops = i / elapsed
                    progress = (i / self.count) * 100
                    print(f"  [{i}/{self.count}] {progress:5.1f}% | "
                          f"{ops:,.0f} ops/sec | 已用 {elapsed:.1f}s")
                          
            except Exception as e:
                self.stats['set_errors'] += 1
                if self.stats['set_errors'] <= 5:
                    print(f"  [!] SET {key} 失败: {e}")
                    
        self.stats['set_end_time'] = time.time()
        
        elapsed = self.stats['set_end_time'] - self.stats['set_start_time']
        ops = self.stats['set_count'] / elapsed
        print(f"[+] SET 完成: {self.stats['set_count']} 成功, "
              f"{self.stats['set_errors']} 失败")
        print(f"    耗时: {elapsed:.2f}s, 平均: {ops:,.0f} ops/sec")
        
        # 停顿一下，让内存稳定
        print("[*] 等待 2 秒让内存稳定...")
        time.sleep(2)
        
    def phase_get(self):
        """GET 阶段: 读取所有 key"""
        print(f"\n{'='*60}")
        print(f"[PHASE 2/3] GET - 读取 {self.count} 个 key")
        print(f"{'='*60}")
        
        self.stats['get_start_time'] = time.time()
        
        for i in range(self.count):
            key = f"key_{i:08d}"
            try:
                # 使用 redis-py 的 execute_command 发送 SGET 命令
                resp = self.client.execute_command('SGET', key)
                self.stats['get_count'] += 1
                
                # redis-py 返回的是字节或字符串，None 表示 key 不存在
                if resp is None or resp == b'' or resp == '':
                    self.stats['get_misses'] += 1
                    
                if i > 0 and i % self.batch_report == 0:
                    elapsed = time.time() - self.stats['get_start_time']
                    ops = i / elapsed
                    progress = (i / self.count) * 100
                    print(f"  [{i}/{self.count}] {progress:5.1f}% | "
                          f"{ops:,.0f} ops/sec | 已用 {elapsed:.1f}s")
                          
            except Exception as e:
                self.stats['get_errors'] += 1
                if self.stats['get_errors'] <= 5:
                    print(f"  [!] GET {key} 失败: {e}")
                    
        self.stats['get_end_time'] = time.time()
        
        elapsed = self.stats['get_end_time'] - self.stats['get_start_time']
        ops = self.stats['get_count'] / elapsed
        print(f"[+] GET 完成: {self.stats['get_count']} 成功, "
              f"{self.stats['get_errors']} 失败, {self.stats['get_misses']} 未命中")
        print(f"    耗时: {elapsed:.2f}s, 平均: {ops:,.0f} ops/sec")
        
        # 停顿一下
        print("[*] 等待 2 秒让内存稳定...")
        time.sleep(2)
        
    def phase_del(self):
        """DEL 阶段: 删除所有 key"""
        print(f"\n{'='*60}")
        print(f"[PHASE 3/3] DEL - 删除 {self.count} 个 key")
        print(f"{'='*60}")
        
        self.stats['del_start_time'] = time.time()
        
        for i in range(self.count):
            key = f"key_{i:08d}"
            try:
                # 使用 redis-py 的 execute_command 发送 SDEL 命令
                resp = self.client.execute_command('SDEL', key)
                self.stats['del_count'] += 1
                
                if i > 0 and i % self.batch_report == 0:
                    elapsed = time.time() - self.stats['del_start_time']
                    ops = i / elapsed
                    progress = (i / self.count) * 100
                    print(f"  [{i}/{self.count}] {progress:5.1f}% | "
                          f"{ops:,.0f} ops/sec | 已用 {elapsed:.1f}s")
                          
            except Exception as e:
                self.stats['del_errors'] += 1
                if self.stats['del_errors'] <= 5:
                    print(f"  [!] DEL {key} 失败: {e}")
                    
        self.stats['del_end_time'] = time.time()
        
        elapsed = self.stats['del_end_time'] - self.stats['del_start_time']
        ops = self.stats['del_count'] / elapsed
        print(f"[+] DEL 完成: {self.stats['del_count']} 成功, "
              f"{self.stats['del_errors']} 失败")
        print(f"    耗时: {elapsed:.2f}s, 平均: {ops:,.0f} ops/sec")
        
        # 停顿让内存稳定
        print("[*] 等待 5 秒让内存稳定（massif 需要收集最终状态）...")
        time.sleep(5)
        
    def print_summary(self):
        """打印测试摘要"""
        print(f"\n{'='*60}")
        print("测试摘要")
        print(f"{'='*60}")
        
        total_time = (self.stats['set_end_time'] - self.stats['set_start_time'] +
                     self.stats['get_end_time'] - self.stats['get_start_time'] +
                     self.stats['del_end_time'] - self.stats['del_start_time'])
        
        print(f"总操作数: {self.count * 3:,} (SET/GET/DEL)")
        print(f"总耗时: {total_time:.2f}s")
        print(f"")
        print(f"SET: {self.stats['set_count']:,} ops, "
              f"{self.stats['set_end_time']-self.stats['set_start_time']:.2f}s")
        print(f"GET: {self.stats['get_count']:,} ops, "
              f"{self.stats['get_end_time']-self.stats['get_start_time']:.2f}s")
        print(f"DEL: {self.stats['del_count']:,} ops, "
              f"{self.stats['del_end_time']-self.stats['del_start_time']:.2f}s")
        print(f"")
        print("[IMPORTANT] 现在可以停止 valgrind 服务器 (Ctrl+C)")
        
    def run(self):
        """运行完整测试"""
        try:
            self.connect()
            self.phase_set()
            self.phase_get()
            self.phase_del()
            self.print_summary()
        finally:
            self.close()


def main():
    parser = argparse.ArgumentParser(
        description='100万 key 内存分配器测试（基于 redis-py）',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
    # 基础用法（测试 100 万 key）
    python3 tests/1000K_mem_test.py
    
    # 自定义参数
    python3 tests/1000K_mem_test.py --count 500000 --value-size 256 --batch-report 5000
    
    # 小数据量测试（用于验证流程）
    python3 tests/1000K_mem_test.py --count 1000 --batch-report 100
        """
    )
    parser.add_argument('--host', default='127.0.0.1', help='服务器地址 (默认: 127.0.0.1)')
    parser.add_argument('--port', type=int, default=8888, help='服务器端口 (默认: 8888)')
    parser.add_argument('--count', type=int, default=1000000, help='key 数量 (默认: 1000000)')
    parser.add_argument('--value-size', type=int, default=128, help='value 大小字节 (默认: 128)')
    parser.add_argument('--batch-report', type=int, default=10000, 
                       help='每多少条输出进度 (默认: 10000)')
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("1000K Memory Allocator Test (redis-py client)")
    print("=" * 60)
    print(f"目标: {args.count:,} keys")
    print(f"Value大小: {args.value_size} bytes")
    print(f"预估总数据量: {args.count * (len(str(args.count)) + 8 + args.value_size) / 1024 / 1024:.1f} MB")
    print("=" * 60)
    
    test = MemoryTest1000K(
        host=args.host,
        port=args.port,
        count=args.count,
        value_size=args.value_size,
        batch_report=args.batch_report
    )
    test.run()


if __name__ == '__main__':
    main()
