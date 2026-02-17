import unittest
import redis
import json
import os
import sys
import subprocess
import time
import socket
import signal
from typing import Any, List

class KVRedis(redis.Redis):
    """支持自定义命令拼接的客户端"""
    def _engine_cmd(self, engine: str, cmd: str, *args):
        # 统一拼接引擎前缀和命令，如 ASET, SGET
        return self.execute_command(f"{engine.upper()}{cmd.upper()}", *args)

class KVServerBase(unittest.TestCase):
    """服务器管理基类"""
    host = '127.0.0.1'
    port = 8888
    server_proc = None
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

        self.server_proc = subprocess.Popen(
            [executable, config_path],
            cwd=self.root_dir,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        
        # 探测端口直到就绪
        for _ in range(50):
            try:
                with socket.create_connection((self.host, self.port), timeout=0.1):
                    time.sleep(0.3)
                    return
            except (ConnectionRefusedError, socket.timeout):
                time.sleep(0.1)
        
        if self.server_proc.poll() is not None:
            raise RuntimeError(f"Server exited immediately with code {self.server_proc.returncode}")
        raise RuntimeError(f"Server failed to bind to {self.port} within timeout")

    def _crash_server(self):
        """强制强杀进程 (SIGKILL)，模拟突然断电或崩溃场景"""
        if self.server_proc:
            try:
                os.kill(self.server_proc.pid, signal.SIGKILL)
                self.server_proc.wait()
            except ProcessLookupError:
                pass
            self.server_proc = None
            time.sleep(0.5)

    def _stop_server(self):
        """正常退出服务"""
        if self.server_proc:
            self.server_proc.terminate()
            try:
                self.server_proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                self._crash_server()
            self.server_proc = None

    def _get_client(self):
        """配置了 hiredis 的 Redis 客户端，关闭自动解码以校验原始字节"""
        return KVRedis(host=self.host, port=self.port, decode_responses=False)

    @staticmethod
    def load_test_pairs():
        json_path = os.path.join(os.path.dirname(__file__), 'default.json')
        if not os.path.exists(json_path):
            return []
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return data.get('tests', [])

    def _eval_expr(self, expr: str) -> Any:
        try:
            return eval(expr, {"__builtins__": {}}, {"range": range, "chr": chr})
        except:
            return expr

    def _to_bytes(self, v: Any) -> Any:
        if v is None: return None
        if isinstance(v, bytes): return v
        return str(v).encode('utf-8')

class TestKVBasic(KVServerBase):
    """
    基础功能单元测试
    【修复】：恢复了完整的 7 步验证流程，确保每个操作在四个引擎上都准确无误。
    """
    @classmethod
    def setUpClass(cls):
        cls._cleanup_files()
        cls.manager = TestKVBasic()
        cls.manager._start_server("tests/config_init.conf")
        cls.test_pairs = cls.load_test_pairs()

    @classmethod
    def tearDownClass(cls):
        cls.manager._stop_server()

    def test_kv_engines_flow(self):
        client = self._get_client()
        engines = ['A', 'H', 'R', 'S']
        ok_resps = [True, b'OK']
        
        for pair in self.test_pairs:
            name = pair['name']
            key = self._to_bytes(self._eval_expr(pair['key_expr']))
            val = self._to_bytes(self._eval_expr(pair['value_expr']))
            mod_val = self._to_bytes(self._eval_expr(pair['mod_value_expr']))

            for engine in engines:
                with self.subTest(pair=name, engine=engine):
                    # 1. SET: 写入初始值
                    self.assertIn(client._engine_cmd(engine, 'SET', key, val), ok_resps)
                    # 2. EXIST: 验证存在
                    self.assertEqual(client._engine_cmd(engine, 'EXIST', key), 1)
                    # 3. GET: 验证数据一致性
                    self.assertEqual(client._engine_cmd(engine, 'GET', key), val)
                    # 4. MOD: 验证修改操作
                    self.assertIn(client._engine_cmd(engine, 'MOD', key, mod_val), ok_resps)
                    # 5. GET: 验证修改后的值
                    self.assertEqual(client._engine_cmd(engine, 'GET', key), mod_val)
                    # 6. DEL: 验证删除
                    self.assertIn(client._engine_cmd(engine, 'DEL', key), ok_resps)
                    # 7. EXIST: 验证已删除
                    self.assertEqual(client._engine_cmd(engine, 'EXIST', key), 0)

class TestKVPersistence(KVServerBase):
    """
    持久化可靠性测试
    【修复】：对所有测试对在全部四个引擎上执行持久化验证。
    """
    def tearDown(self):
        self._stop_server()

    def test_snapshot_recovery(self):
        """验证 Snapshot 持久化: 全引擎全量写入 -> BGSAVE -> Crash -> 重启验证"""
        self._cleanup_files()
        self._start_server("tests/config_snapshot.conf")
        client = self._get_client()
        pairs = self.load_test_pairs()
        engines = ['A', 'H', 'R', 'S']

        # 1. 全量引擎写入
        for pair in pairs:
            key = self._to_bytes(self._eval_expr(pair['key_expr']))
            val = self._to_bytes(self._eval_expr(pair['value_expr']))
            for engine in engines:
                client._engine_cmd(engine, 'SET', key, val)

        # 2. 触发快照保存
        resp = client.execute_command('BGSAVE')
        self.assertIn(resp, [True, b'OK', b'Background saving started'])
        
        # 3. 预留足够时间完成持久化 IO (包含 10MB 的大 Key)
        time.sleep(5) 
        
        # 4. 强杀模拟崩溃
        self._crash_server()

        # 5. 重启加载快照
        self._start_server("tests/config_snapshot.conf")
        client = self._get_client()
        
        # 6. 【修复】：遍历全引擎全数据进行验证
        for pair in pairs:
            p_name = pair['name']
            key = self._to_bytes(self._eval_expr(pair['key_expr']))
            val = self._to_bytes(self._eval_expr(pair['value_expr']))
            for engine in engines:
                with self.subTest(pair=p_name, engine=engine):
                    try:
                        recovered = client._engine_cmd(engine, 'GET', key)
                        self.assertEqual(recovered, val, f"Snapshot recovery failed for {p_name} on {engine}")
                    except redis.exceptions.ResponseError as e:
                        self.fail(f"Snapshot recovery error (Key missing) for {p_name} on {engine}: {e}")

    def test_aof_recovery(self):
        """验证 AOF 持久化: 全引擎写入 -> Crash (无手动保存) -> AOF重放验证"""
        self._cleanup_files()
        self._start_server("tests/config_aof.conf")
        client = self._get_client()
        pairs = self.load_test_pairs()
        engines = ['A', 'H', 'R', 'S']

        # 1. 全引擎连续操作
        for pair in pairs:
            key = self._to_bytes(self._eval_expr(pair['key_expr']))
            val = self._to_bytes(self._eval_expr(pair['value_expr']))
            mod_val = self._to_bytes(self._eval_expr(pair['mod_value_expr']))
            for engine in engines:
                client._engine_cmd(engine, 'SET', key, val)
                client._engine_cmd(engine, 'MOD', key, mod_val)

        # 2. 模拟系统崩溃前，稍微延长等待，确保 AOF 后台刷盘策略执行完毕
        time.sleep(3) 
        self._crash_server()

        # 3. 重启触发 AOF 重放
        self._start_server("tests/config_aof.conf")
        client = self._get_client()
        
        # 4. 【修复】：全引擎验证 AOF 重放后的最终状态
        for pair in pairs:
            p_name = pair['name']
            key = self._to_bytes(self._eval_expr(pair['key_expr']))
            mod_val = self._to_bytes(self._eval_expr(pair['mod_value_expr']))
            for engine in engines:
                with self.subTest(pair=p_name, engine=engine):
                    try:
                        recovered = client._engine_cmd(engine, 'GET', key)
                        self.assertEqual(recovered, mod_val, f"AOF recovery value mismatch for {p_name} on {engine}")
                    except redis.exceptions.ResponseError as e:
                        # 重点修复：捕获 ResponseError 以报告是哪个 Key/引擎丢失了
                        self.fail(f"AOF recovery failed (Key missing or Error) for {p_name} on {engine}: {e}")

if __name__ == '__main__':
    unittest.main(verbosity=2)
