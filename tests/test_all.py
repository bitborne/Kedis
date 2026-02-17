import unittest
import os
import sys

def run_all_tests():
    # 获取测试目录路径
    test_dir = os.path.dirname(os.path.abspath(__file__))
    
    # 将测试目录加入 Python 路径，确保 base.py 能被正确导入
    if test_dir not in sys.path:
        sys.path.insert(0, test_dir)
        
    print(f"正在扫描并执行测试用例: {test_dir}")
    print("=" * 60)

    # 自动发现所有 test_*.py 文件
    loader = unittest.TestLoader()
    suite = loader.discover(start_dir=test_dir, pattern='test_*.py')

    # 运行测试
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    # 根据测试结果退出
    if not result.wasSuccessful():
        sys.exit(1)

if __name__ == '__main__':
    run_all_tests()
