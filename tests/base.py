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

    def _start_server(self, config_rel_path: str, wait_ready: bool = True):
        """启动服务并等待端口就绪
        
        Args:
            config_rel_path: 配置文件相对路径
            wait_ready: 是否等待服务完全就绪（发送PING命令确认）
        """
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
        
        # 第一阶段：轮询探测端口直到成功连接或超时
        port_ready = False
        for _ in range(100):  # 增加重试次数
            try:
                with socket.create_connection((self.host, self.port), timeout=0.1):
                    port_ready = True
                    break
            except (ConnectionRefusedError, socket.timeout, OSError):
                # 检查进程是否已崩溃
                if self.server_proc.poll() is not None:
                    raise RuntimeError(f"Server exited immediately with code {self.server_proc.returncode}")
                time.sleep(0.05)
        
        if not port_ready:
            self._stop_server()
            raise RuntimeError(f"Server failed to bind to {self.port} within timeout")
        
        # 第二阶段：等待服务完全就绪（可选）
        if wait_ready:
            # 尝试发送 EXIST 命令确认服务可以处理请求
            for _ in range(50):
                try:
                    client = self._get_client()
                    # 使用支持的命令测试服务就绪
                    client._engine_cmd('H', 'EXIST', b'ready_check')
                    # 额外等待一小段时间确保引擎初始化完成
                    time.sleep(0.2)
                    return
                except Exception:
                    if self.server_proc.poll() is not None:
                        raise RuntimeError(f"Server exited during startup")
                    time.sleep(0.05)
            
            self._stop_server()
            raise RuntimeError(f"Server failed to respond to commands within timeout")

    def _crash_server(self):
        """强制强杀进程 (SIGKILL)，用于测试持久化恢复能力
        
        注意：SIGKILL 会导致进程无法优雅退出，可能导致AOF数据丢失。
        先用 SIGTERM 尝试优雅退出，超时后再用 SIGKILL。
        """
        if self.server_proc:
            try:
                # 先尝试优雅退出（SIGTERM）
                os.kill(self.server_proc.pid, signal.SIGTERM)
                try:
                    self.server_proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    # 超时后强制 kill
                    os.kill(self.server_proc.pid, signal.SIGKILL)
                    self.server_proc.wait()
            except ProcessLookupError:
                pass  # 进程已经不存在
            except Exception as e:
                print(f"Warning: Error during server crash: {e}")
            self.server_proc = None
            # 增加等待时间确保端口释放
            time.sleep(0.8)

    def _stop_server(self):
        """优雅关闭服务"""
        if self.server_proc:
            # 发送 SIGTERM 让服务优雅退出
            self.server_proc.terminate()
            try:
                # 等待服务完全退出
                self.server_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                print("Warning: Server did not terminate gracefully, forcing kill...")
                try:
                    self.server_proc.kill()
                    self.server_proc.wait(timeout=5)
                except Exception as e:
                    print(f"Warning: Error during force kill: {e}")
            self.server_proc = None
            # 增加等待时间确保资源释放
            time.sleep(0.5)

    def _get_client(self):
        """获取配置了 hiredis 的 Redis 客户端"""
        return KVRedis(host=self.host, port=self.port, decode_responses=False, lib_name=None, lib_version=None)

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
