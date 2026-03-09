import unittest
import redis
import time
from base import KVServerBase

class TestKVAof(KVServerBase):
    """
    AOF 持久化可靠性测试
    通过多次重启和崩溃，完整覆盖 SET -> MOD -> DEL 的 AOF 记录与重放。
    """
    def tearDown(self):
        self._stop_server()

    def _verify_state(self, phase, client, pairs, engines, expected_value_type):
        """
        【重构】将重复的验证逻辑封装成辅助函数，提高代码复用性
        :param phase: 当前测试阶段 (e.g., "after-set", "after-mod")
        :param expected_value_type: 'val' (初始), 'mod_val' (修改后), 'non_existent' (删除后)
        """
        for pair in pairs:
            p_name = pair['name']
            key = self._to_bytes(self._eval_expr(pair['key_expr']))
            
            for engine in engines:
                with self.subTest(phase=phase, pair=p_name, engine=engine):
                    try:
                        if expected_value_type == 'non_existent':
                            existence = client._engine_cmd(engine, 'EXIST', key)
                            self.assertEqual(existence, 0, f"Key should not exist after DEL in phase '{phase}'")
                        else:
                            expected_val = self._to_bytes(self._eval_expr(pair[expected_value_type]))
                            recovered = client._engine_cmd(engine, 'GET', key)
                            self.assertEqual(recovered, expected_val, f"Value mismatch in phase '{phase}'")
                    except redis.exceptions.ResponseError as e:
                        self.fail(f"AOF recovery failed (Key missing or Error) in phase '{phase}' for {p_name} on {engine}: {e}")

    def test_aof_full_lifecycle_recovery(self):
        """
        验证 AOF 持久化: SET -> Crash -> Verify -> MOD -> Crash -> Verify -> DEL -> Crash -> Verify
        这个测试更完整地模拟了服务的生命周期，确保 AOF 在每个环节都能正确工作。
        """
        self._cleanup_files()
        pairs = self.load_test_pairs()
        engines = ['A', 'H', 'R', 'S']
        
        # === 阶段 1: SET 并验证 ===
        self._start_server("tests/config_aof.conf")
        client = self._get_client()
        for pair in pairs:
            key = self._to_bytes(self._eval_expr(pair['key_expr']))
            val = self._to_bytes(self._eval_expr(pair['value_expr']))
            for engine in engines:
                client._engine_cmd(engine, 'SET', key, val)
        # 确保AOF数据落盘：发送EXIST触发before_sleep刷新缓冲区，然后等待fsync
        client._engine_cmd('H', 'EXIST', b'trigger_flush')
        time.sleep(1.5)  # 等待后台fsync线程刷盘
        self._crash_server()

        self._start_server("tests/config_aof.conf")
        time.sleep(3)
        client = self._get_client()
        self._verify_state("after-set", client, pairs, engines, 'value_expr')
        
        # === 阶段 2: MOD 并验证 ===
        for pair in pairs:
            key = self._to_bytes(self._eval_expr(pair['key_expr']))
            mod_val = self._to_bytes(self._eval_expr(pair['mod_value_expr']))
            for engine in engines:
                client._engine_cmd(engine, 'MOD', key, mod_val)
        # 确保AOF数据落盘：发送EXIST触发before_sleep刷新缓冲区，然后等待fsync
        client._engine_cmd('A', 'EXIST', b'trigger_flush')
        time.sleep(1.5)  # 等待后台fsync线程刷盘
        self._crash_server()

        self._start_server("tests/config_aof.conf")
        time.sleep(3)
        client = self._get_client()
        self._verify_state("after-mod", client, pairs, engines, 'mod_value_expr')

        # === 阶段 3: DEL 并验证 ===
        for pair in pairs:
            key = self._to_bytes(self._eval_expr(pair['key_expr']))
            for engine in engines:
                client._engine_cmd(engine, 'DEL', key)
        # 确保AOF数据落盘：发送EXIST触发before_sleep刷新缓冲区，然后等待fsync
        client._engine_cmd('A', 'EXIST', b'trigger_flush')
        time.sleep(1.5)  # 等待后台fsync线程刷盘
        self._crash_server()
        
        self._start_server("tests/config_aof.conf")
        time.sleep(3)
        client = self._get_client()
        self._verify_state("after-del", client, pairs, engines, 'non_existent')

if __name__ == '__main__':
    unittest.main(verbosity=2)
