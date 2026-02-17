import unittest
from base import KVServerBase

class TestKVBasic(KVServerBase):
    """
    基础功能单元测试
    验证 A, H, R, S 四大引擎在正常运行状态下的命令执行准确性
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
        """执行完整生命周期验证: SET -> EXIST -> GET -> MOD -> GET -> DEL -> EXIST"""
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
                    # 1. SET
                    self.assertIn(client._engine_cmd(engine, 'SET', key, val), ok_resps)
                    # 2. EXIST
                    self.assertEqual(client._engine_cmd(engine, 'EXIST', key), 1)
                    # 3. GET
                    self.assertEqual(client._engine_cmd(engine, 'GET', key), val)
                    # 4. MOD
                    self.assertIn(client._engine_cmd(engine, 'MOD', key, mod_val), ok_resps)
                    # 5. GET (Verified Modification)
                    self.assertEqual(client._engine_cmd(engine, 'GET', key), mod_val)
                    # 6. DEL
                    self.assertIn(client._engine_cmd(engine, 'DEL', key), ok_resps)
                    # 7. EXIST (Verified Deletion)
                    self.assertEqual(client._engine_cmd(engine, 'EXIST', key), 0)

if __name__ == '__main__':
    unittest.main(verbosity=2)
