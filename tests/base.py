import unittest
import redis
import json
import os
import subprocess
import time
import socket
import signal
from typing import Any, List

class KVRedis(redis.Redis):
    """支持自定义命令拼接的客户端"""
    def _engine_cmd(self, engine: str, cmd: str, *args):
        return self.execute_command(f"{engine.upper()}{cmd.upper()}", *args)

class KVServerBase(unittest.TestCase):
    """服务器管理基类，提供启动、强杀、清理和客户端获取等核心功能"""
    host = '127.0.0.1'
    port = 8888
    server_proc = None
    # 自动定位项目根目录 (假设 base.py 在 tests/ 目录下)
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    @classmethod
    def _cleanup_files(cls):
        """调用 clean.sh 清理数据文件，确保测试环境纯净"""
        clean_script = os.path.join(cls.root_dir, "clean.sh")
        if os.path.exists(clean_script):
            subprocess.run(["bash", clean_script], cwd=cls.root_dir, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    def _start_server(self, config_rel_path: str):
        """启动服务并等待端口就绪"""
        config_path = os.path.join(self.root_dir, config_rel_path)
        executable = os.path.join(self.root_dir, "kvstore")
        
        if not os.path.exists(executable):
            raise FileNotFoundError(f"kvstore binary not found at {executable}. Please run make first.")

        # 启动进程，屏蔽输出以保持测试控制台整洁
        self.server_proc = subprocess.Popen(
            [executable, config_path],
            cwd=self.root_dir,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        
        # 轮询探测端口直到成功连接或超时
        for _ in range(50):
            try:
                with socket.create_connection((self.host, self.port), timeout=0.1):
                    time.sleep(0.3) # 预留时间让引擎完成初始化
                    return
            except (ConnectionRefusedError, socket.timeout):
                time.sleep(0.1)
        
        if self.server_proc.poll() is not None:
            raise RuntimeError(f"Server exited immediately with code {self.server_proc.returncode}")
        raise RuntimeError(f"Server failed to bind to {self.port} within timeout")

    def _crash_server(self):
        """强制强杀进程 (SIGKILL)，用于测试持久化恢复能力"""
        if self.server_proc:
            try:
                os.kill(self.server_proc.pid, signal.SIGKILL)
                self.server_proc.wait()
            except ProcessLookupError:
                pass
            self.server_proc = None
            time.sleep(0.5)

    def _stop_server(self):
        """优雅关闭服务"""
        if self.server_proc:
            self.server_proc.terminate()
            try:
                self.server_proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                self._crash_server()
            self.server_proc = None

    def _get_client(self):
        """获取配置了 hiredis 的 Redis 客户端"""
        return KVRedis(host=self.host, port=self.port, decode_responses=False)

    @staticmethod
    def load_test_pairs():
        """从 default.json 加载测试数据集"""
        json_path = os.path.join(os.path.dirname(__file__), 'default.json')
        if not os.path.exists(json_path):
            return []
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return data.get('tests', [])

    def _eval_expr(self, expr: str) -> Any:
        """解析 JSON 中的 Python 表达式"""
        try:
            return eval(expr, {"__builtins__": {}}, {"range": range, "chr": chr})
        except:
            return expr

    def _to_bytes(self, v: Any) -> Any:
        """标准化为字节流，保留 None 语义"""
        if v is None: return None
        if isinstance(v, bytes): return v
        return str(v).encode('utf-8')
