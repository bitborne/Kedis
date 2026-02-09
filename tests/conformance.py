from resp_client import RawRedisClient, eval_input
from dataclasses import dataclass, asdict, field
from typing import List, Dict, Any, Optional
import json
import os


# 引擎配置
ENGINES = ['A', 'R', 'S', 'H']
COMMANDS = ['SET', 'GET', 'MOD', 'DEL', 'EXIST']


@dataclass
class TestPair:
    """测试对定义"""
    name: str
    key_expr: str
    value_expr: str
    engine_cmds: Dict[str, Dict[str, str]] = field(default_factory=dict)
    
    def get_cmd_for_engine(self, engine: str, phase: str) -> str:
        default_cmd = 'SET' if phase == 'set' else 'GET'
        cmd = self.engine_cmds.get(engine, {}).get(phase, default_cmd)
        return f"{engine}{cmd}"


def format_value(v: Any, max_len: int = 30) -> str:
    """格式化显示值"""
    s = repr(v)
    if len(s) > max_len:
        return s[:max_len] + f"...({len(v) if hasattr(v, '__len__') else '?'})"
    return s


def to_redis_str(v: Any) -> str:
    """统一转换为字符串"""
    if isinstance(v, bytes):
        return v.decode('utf-8', errors='replace')
    return str(v)


class ConformanceTest:
    """多引擎一致性测试框架"""
    
    def __init__(self, host: str = '172.20.10.2'):
        self.host = host
        self.test_pairs: List[TestPair] = []
        self.work_dir = os.getcwd()
        print(f"工作目录: {self.work_dir}")
        print(f"引擎: {', '.join(ENGINES)}")
    
    def add_pair(self, name: str, key_expr: str, value_expr: str,
                 engine_cmds: Optional[Dict[str, Dict[str, str]]] = None) -> None:
        self.test_pairs.append(TestPair(
            name=name, key_expr=key_expr, value_expr=value_expr,
            engine_cmds=engine_cmds or {}
        ))
    
    def load_default_tests(self) -> None:
        self.test_pairs = [
            TestPair('small_string', '"k1"', '"v1"'),
            TestPair('large_string', '"large_key"', '"x" * 10000'),
            TestPair('unicode', '"中文key"', '"中文value 🎉"'),
            TestPair('number_value', '"num_key"', '12345'),
            TestPair('special_chars', '"spec:key"', '"val\\r\\nwith\\nlines"'),
            TestPair('huge_key', '"k" * 1000', '"huge_key_test"'),
        ]
        print(f"已加载 {len(self.test_pairs)} 个默认测试对")
    
    def save_to_json(self, filename: str) -> None:
        """保存测试对到 JSON 文件"""
        if not self.test_pairs:
            print("没有测试对可保存")
            return
        
        data = {
            'engines': ENGINES,
            'commands': COMMANDS,
            'count': len(self.test_pairs),
            'tests': [asdict(p) for p in self.test_pairs]
        }
        
        # 自动添加 .json 后缀
        if not filename.endswith('.json'):
            filename += '.json'
        
        filepath = os.path.join(self.work_dir, filename)
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            print(f"已保存 {len(self.test_pairs)} 个测试对到: {filepath}")
        except Exception as e:
            print(f"保存失败: {e}")
    
    def load_from_json(self, filename: str) -> bool:
        """从 JSON 文件加载测试对"""
        # 自动添加 .json 后缀
        if not filename.endswith('.json'):
            filename += '.json'
        
        filepath = os.path.join(self.work_dir, filename)
        
        if not os.path.exists(filepath):
            print(f"文件不存在: {filepath}")
            return False
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            self.test_pairs = []
            for t in data.get('tests', []):
                self.test_pairs.append(TestPair(
                    name=t['name'],
                    key_expr=t['key_expr'],
                    value_expr=t['value_expr'],
                    engine_cmds=t.get('engine_cmds', {})
                ))
            
            print(f"已从 {filepath} 加载 {len(self.test_pairs)} 个测试对")
            return True
            
        except Exception as e:
            print(f"加载失败: {e}")
            return False
    
    def _eval_expr(self, expr: str) -> Any:
        try:
            return eval(expr, {"__builtins__": {}}, {})
        except Exception as e:
            print(f"表达式错误 '{expr}': {e}")
            return expr
    
    def _execute_single_engine(self, pair: TestPair, engine: str,
                              phase: str, port: int) -> tuple[bool, Optional[str], Any]:
        """执行单引擎，返回(成功, 错误信息, 原始响应)"""
        key = to_redis_str(self._eval_expr(pair.key_expr))
        value = to_redis_str(self._eval_expr(pair.value_expr))
        cmd = pair.get_cmd_for_engine(engine, phase)
        
        client = RawRedisClient(self.host, port)
        try:
            client.connect()
            
            if phase == 'set':
                resp = client.execute(cmd, key, value)
                if resp is None:
                    return False, "无响应", None
                if resp[0] == 'error':
                    return False, resp[1], resp
                return resp[0] in ('simple_string', 'integer'), None, resp
            else:
                resp = client.execute(cmd, key)
                if resp is None:
                    return False, "无响应", None
                if resp[0] == 'error':
                    return False, resp[1], resp
                if resp[0] == 'bulk_string':
                    got = resp[1] if resp[1] is not None else ""
                    if got == value:
                        return True, None, resp
                    return False, f"值不匹配", resp
                return False, f"意外响应:{resp[0]}", resp
        except Exception as e:
            return False, str(e), None
        finally:
            client.close()
    
    def run_phase(self, phase: str, port: int) -> None:
        """运行阶段，只显示失败"""
        print(f"\n{'='*50}")
        print(f"阶段: {phase.upper()} | {self.host}:{port}")
        print(f"{'='*50}")
        
        fail_count = 0
        
        for pair in self.test_pairs:
            key = to_redis_str(self._eval_expr(pair.key_expr))
            
            for engine in ENGINES:
                success, error, resp = self._execute_single_engine(pair, engine, phase, port)
                
                if not success:
                    fail_count += 1
                    cmd = pair.get_cmd_for_engine(engine, phase)
                    print(f"[FAIL] {pair.name:15} | {engine}引擎 | {cmd:8} | {error}")
                    print(f"       key={format_value(key)}")
                    if resp:
                        print(f"       响应: {resp}")
            
        if fail_count == 0:
            print("全部通过")
        else:
            print(f"\n失败: {fail_count}/{len(self.test_pairs) * len(ENGINES)}")
    
    def interactive_mode(self) -> None:
        """交互模式"""
        print("="*50)
        print("多引擎一致性测试")
        print("="*50)
        
        print("\n1. 加载默认测试集")
        print("2. 从 JSON 文件加载")
        print("3. 手动添加测试对")
        
        choice = input("\n选择 (1/2/3): ").strip()
        
        if choice == '2':
            filename = input("文件名: ").strip()
            if not self.load_from_json(filename):
                print("加载失败，加载默认测试集")
                self.load_default_tests()
        elif choice == '3':
            self._manual_add_pairs()
        else:
            self.load_default_tests()
        
        self._show_test_pairs()
        
        while True:
            print(f"\n{'='*50}")
            print("主菜单:")
            print("  [1] SET阶段 (所有引擎写入)")
            print("  [2] GET阶段 (所有引擎读取)")
            print("  [3] 查看测试对")
            print("  [4] 添加测试对")
            print("  [5] 保存测试对到 JSON")
            print("  [q] 退出")
            
            cmd = input("\n选择: ").strip().lower()
            
            if cmd == '1':
                port = int(input("端口号: "))
                self.run_phase('set', port)
            elif cmd == '2':
                port = int(input("端口号: "))
                self.run_phase('get', port)
            elif cmd == '3':
                self._show_test_pairs()
            elif cmd == '4':
                self._manual_add_one_pair()
            elif cmd == '5':
                filename = input("保存文件名: ").strip()
                if filename:
                    self.save_to_json(filename)
                else:
                    print("文件名不能为空")
            elif cmd == 'q':
                print("退出")
                break
    
    def _manual_add_pairs(self) -> None:
      print("\n添加测试对 (空名称结束)")
      while True:
          name = input("名称: ").strip()
          if not name:
              break
        
          # 验证 key 表达式
          while True:
              key_expr = input("key: ").strip()
              try:
                  test_key = eval(key_expr, {"__builtins__": {}}, {})
                  break  # 成功则退出循环
              except Exception as e:
                  print(f"  [错误] {e}, 请重新输入")
          
          # 验证 value 表达式
          while True:
              value_expr = input("value: ").strip()
              try:
                  test_val = eval(value_expr, {"__builtins__": {}}, {})
                  break  # 成功则退出循环
              except Exception as e:
                  print(f"  [错误] {e}, 请重新输入")
          
          self.add_pair(name, key_expr, value_expr)
          print(f"已添加: {name}")
    
    def _manual_add_one_pair(self) -> None:
        name = input("名称: ").strip()
        if not name:
            return
        
        # 验证 key 表达式
        while True:
            key_expr = input("key: ").strip()
            try:
                test_key = eval(key_expr, {"__builtins__": {}}, {})
                break
            except Exception as e:
                print(f"  [错误] {e}, 请重新输入")
        
        # 验证 value 表达式
        while True:
            value_expr = input("value: ").strip()
            try:
                test_val = eval(value_expr, {"__builtins__": {}}, {})
                break
            except Exception as e:
                print(f"  [错误] {e}, 请重新输入")
        
        self.add_pair(name, key_expr, value_expr)
        print(f"已添加: {name}")
    
    def _show_test_pairs(self) -> None:
        print(f"\n共 {len(self.test_pairs)} 个测试对:")
        for i, p in enumerate(self.test_pairs, 1):
            print(f"  {i}. {p.name}: {p.key_expr} = {p.value_expr}")


if __name__ == "__main__":
    test = ConformanceTest()
    test.interactive_mode()