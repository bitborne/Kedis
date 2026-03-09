import unittest
import redis
import time
from base import KVServerBase

class TestKVSnapshot(KVServerBase):
    """
    快照持久化可靠性测试
    通过多次重启和崩溃，完整覆盖 SET -> MOD -> DEL 的快照记录与恢复。
    """
    def tearDown(self):
        self._stop_server()

    def _verify_state(self, phase, client, pairs, engines, expected_value_type):
        """
        将重复的验证逻辑封装成辅助函数，提高代码复用性
        :param phase: 当前测试阶段 (e.g., "after-set", "after-mod")
        :param expected_value_type: 'value_expr' (初始), 'mod_value_expr' (修改后), 'non_existent' (删除后)
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
                        self.fail(f"Snapshot recovery failed (Key missing or Error) in phase '{phase}' for {p_name} on {engine}: {e}")

    def test_snapshot_full_lifecycle_recovery(self):
        """
        验证快照持久化: SET -> BGSAVE -> Crash -> Verify -> MOD -> BGSAVE -> Crash -> Verify -> DEL -> BGSAVE -> Crash -> Verify
        """
        self._cleanup_files()
        pairs = self.load_test_pairs()
        engines = ['A', 'H', 'R', 'S']
        
        # === 阶段 1: SET 并验证 ===
        self._start_server("tests/config_snapshot.conf")
        client = self._get_client()
        for pair in pairs:
            key = self._to_bytes(self._eval_expr(pair['key_expr']))
            val = self._to_bytes(self._eval_expr(pair['value_expr']))
            for engine in engines:
                client._engine_cmd(engine, 'SET', key, val)
        
        resp = client.execute_command('BGSAVE')
        self.assertIn(resp, [True, b'OK', b'Background saving started'])
        # 等待BGSAVE完成（后台进程写入文件需要时间）
        time.sleep(2.5)
        # 发送读命令触发事件循环，确保文件写入完成
        client._engine_cmd('H', 'EXIST', b'check_save')
        time.sleep(1)
        self._crash_server()

        self._start_server("tests/config_snapshot.conf")
        # 等待服务完全启动和KSF文件加载
        time.sleep(1.5)
        client = self._get_client()
        self._verify_state("after-set", client, pairs, engines, 'value_expr')
        
        # === 阶段 2: MOD 并验证 ===
        for pair in pairs:
            key = self._to_bytes(self._eval_expr(pair['key_expr']))
            mod_val = self._to_bytes(self._eval_expr(pair['mod_value_expr']))
            for engine in engines:
                client._engine_cmd(engine, 'MOD', key, mod_val)
        
        resp = client.execute_command('BGSAVE')
        self.assertIn(resp, [True, b'OK', b'Background saving started'])
        # 等待BGSAVE完成（后台进程写入文件需要时间）
        time.sleep(2.5)
        # 发送读命令触发事件循环，确保文件写入完成
        client._engine_cmd('H', 'EXIST', b'check_save')
        time.sleep(1)
        self._crash_server()

        self._start_server("tests/config_snapshot.conf")
        # 等待服务完全启动和KSF文件加载
        time.sleep(1.5)
        client = self._get_client()
        self._verify_state("after-mod", client, pairs, engines, 'mod_value_expr')

        # === 阶段 3: DEL 并验证 ===
        for pair in pairs:
            key = self._to_bytes(self._eval_expr(pair['key_expr']))
            for engine in engines:
                client._engine_cmd(engine, 'DEL', key)
        
        resp = client.execute_command('BGSAVE')
        self.assertIn(resp, [True, b'OK', b'Background saving started'])
        # 等待BGSAVE完成（后台进程写入文件需要时间）
        time.sleep(2.5)
        # 发送读命令触发事件循环，确保文件写入完成
        client._engine_cmd('H', 'EXIST', b'check_save')
        time.sleep(1)
        self._crash_server()
        
        self._start_server("tests/config_snapshot.conf")
        # 等待服务完全启动和KSF文件加载
        time.sleep(1.5)
        client = self._get_client()
        self._verify_state("after-del", client, pairs, engines, 'non_existent')

if __name__ == '__main__':
    unittest.main(verbosity=2)
