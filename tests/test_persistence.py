import unittest
import redis
import time
from base import KVServerBase

class TestKVPersistence(KVServerBase):
    """
    持久化可靠性测试
    验证 Snapshot (KSF) 和 AOF 在崩溃场景下的数据完整性
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

        # 1. 全量引擎写入 (192个 Key)
        for pair in pairs:
            key = self._to_bytes(self._eval_expr(pair['key_expr']))
            val = self._to_bytes(self._eval_expr(pair['value_expr']))
            for engine in engines:
                client._engine_cmd(engine, 'SET', key, val)

        # 2. 触发快照保存 (KSF 使用 io_uring 异步写入)
        resp = client.execute_command('BGSAVE')
        self.assertIn(resp, [True, b'OK', b'Background saving started'])
        
        # 3. 增加等待时间，确保大文件 (含10MB数据) 完全落盘
        time.sleep(5) 
        
        # 4. 强杀模拟崩溃
        self._crash_server()

        # 5. 重启加载快照
        self._start_server("tests/config_snapshot.conf")
        client = self._get_client()
        
        # 6. 验证全引擎全数据进行验证
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

        # 1. 全引擎连续操作: SET -> MOD
        for pair in pairs:
            key = self._to_bytes(self._eval_expr(pair['key_expr']))
            val = self._to_bytes(self._eval_expr(pair['value_expr']))
            mod_val = self._to_bytes(self._eval_expr(pair['mod_value_expr']))
            for engine in engines:
                client._engine_cmd(engine, 'SET', key, val)
                client._engine_cmd(engine, 'MOD', key, mod_val)

        # 2. 等待 AOF 刷盘策略 (通常是每秒 fsync) 执行完毕
        time.sleep(3) 
        self._crash_server()

        # 3. 重启触发 AOF 重放
        self._start_server("tests/config_aof.conf")
        client = self._get_client()
        
        # 4. 全引擎验证 AOF 重放后的最终状态
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
                        self.fail(f"AOF recovery failed (Key missing or Error) for {p_name} on {engine}: {e}")

if __name__ == '__main__':
    unittest.main(verbosity=2)
