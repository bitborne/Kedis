import unittest
import redis
import time
from base import KVServerBase

class TestKVAof(KVServerBase):
    """
    AOF 持久化可靠性测试
    验证 AOF 在崩溃场景下的数据完整性和指令重放能力
    """
    def tearDown(self):
        self._stop_server()

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
