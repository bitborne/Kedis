#!/usr/bin/env python3
"""
Kedis Test Suite - 统一测试入口

Usage:
    # 运行所有功能测试 (默认: 所有基本功能 + 三种 mirror 类型增量复制)
    sudo python3 test_all.py
    
    # 只运行所有基本功能测试 (不含增量复制)
    sudo python3 test_all.py --basic-only
    
    # 运行指定 mirror 类型的增量复制测试
    MIRROR_TYPE=uprobe sudo -E python3 test_all.py test_incre_repl
    MIRROR_TYPE=xdp sudo -E python3 test_all.py test_incre_repl
    
    # 运行所有 mirror 类型的增量复制测试
    sudo python3 test_all.py --all-mirrors
    
    # 运行单个测试模块
    sudo python3 test_all.py test_basic
    sudo python3 test_all.py test_aof
"""

import unittest
import os
import sys
import subprocess

# 所有 mirror 类型
MIRROR_TYPES = ['mirror', 'xdp', 'uprobe']


def run_single_test(test_file, mirror_type=None):
    """运行单个测试文件"""
    test_dir = os.path.dirname(os.path.abspath(__file__))
    
    if test_dir not in sys.path:
        sys.path.insert(0, test_dir)
    
    # 设置环境变量
    if mirror_type:
        os.environ['MIRROR_TYPE'] = mirror_type
    
    # 使用 unittest 自动发现
    loader = unittest.TestLoader()
    suite = loader.discover(start_dir=test_dir, pattern=test_file)
    
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    return result.wasSuccessful()


def run_basic_tests():
    """运行基本功能测试 (排除 test_incre_repl)"""
    test_dir = os.path.dirname(os.path.abspath(__file__))
    
    if test_dir not in sys.path:
        sys.path.insert(0, test_dir)
    
    loader = unittest.TestLoader()
    
    # 发现并运行所有测试，但排除 test_incre_repl
    all_suite = loader.discover(start_dir=test_dir, pattern='test_*.py')
    
    # 过滤掉 incre_repl 测试
    filtered_suite = unittest.TestSuite()
    for suite in all_suite:
        for test_group in suite:
            for test_case in test_group:
                test_class = test_case.__class__.__name__
                test_name = str(test_case)
                # 排除包含 incre_repl 的测试
                if 'IncreRepl' not in test_class and 'incre_repl' not in test_name:
                    filtered_suite.addTest(test_case)
    
    if filtered_suite.countTestCases() == 0:
        print("没有基本功能测试可运行")
        return True
    
    print(f"\n{'='*70}")
    print(f"运行基本功能测试 ({filtered_suite.countTestCases()} 个测试用例)")
    print('='*70)
    
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(filtered_suite)
    
    return result.wasSuccessful()


def run_incre_repl_all_mirrors():
    """运行所有 mirror 类型的增量复制测试"""
    all_passed = True
    
    for mt in MIRROR_TYPES:
        print(f"\n{'='*70}")
        print(f"增量复制测试 - Mirror 类型: {mt.upper()}")
        print('='*70)
        
        # 清除环境变量，让 test_incre_repl 使用自己的逻辑
        if 'MIRROR_TYPE' in os.environ:
            del os.environ['MIRROR_TYPE']
        
        # 通过参数传递 mirror 类型
        test_dir = os.path.dirname(os.path.abspath(__file__))
        cmd = [
            sys.executable,
            os.path.join(test_dir, 'test_incre_repl.py'),
            '--mirror-type', mt
        ]
        
        result = subprocess.run(cmd, cwd=test_dir)
        if result.returncode != 0:
            all_passed = False
    
    return all_passed


def run_all_tests():
    """运行所有测试：基本功能 + 三种 mirror 增量复制"""
    print("\n" + "="*70)
    print("KEDIS 完整测试套件")
    print("="*70)
    
    all_passed = True
    
    # 1. 运行基本功能测试
    if not run_basic_tests():
        all_passed = False
    
    # 2. 运行所有 mirror 类型的增量复制测试
    if not run_incre_repl_all_mirrors():
        all_passed = False
    
    # 最终结果
    print(f"\n{'='*70}")
    if all_passed:
        print("✓ 所有测试通过!")
    else:
        print("✗ 部分测试失败!")
    print('='*70 + "\n")
    
    return all_passed


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Kedis Test Suite')
    parser.add_argument('test', nargs='?', help='指定测试模块 (如 test_basic, test_incre_repl)')
    parser.add_argument('--all-mirrors', '-a', action='store_true', help='只运行所有 mirror 类型的增量复制测试')
    parser.add_argument('--basic-only', '-b', action='store_true', help='只运行基本功能测试')
    args = parser.parse_args()
    
    # 检查权限
    if os.geteuid() != 0:
        print("警告: 部分测试需要 sudo 权限")
    
    if args.basic_only:
        # 只运行基本功能测试
        success = run_basic_tests()
        sys.exit(0 if success else 1)
    
    elif args.all_mirrors:
        # 只运行所有 mirror 类型的增量复制测试
        success = run_incre_repl_all_mirrors()
        sys.exit(0 if success else 1)
    
    elif args.test:
        # 运行指定测试
        pattern = f'{args.test}.py' if not args.test.endswith('.py') else args.test
        
        if 'incre_repl' in pattern:
            # 增量复制测试，检查环境变量
            mirror_type = os.environ.get('MIRROR_TYPE')
            if mirror_type:
                success = run_single_test(pattern, mirror_type)
            else:
                # 没有指定 mirror 类型，运行所有三种
                success = run_incre_repl_all_mirrors()
        else:
            success = run_single_test(pattern)
        
        sys.exit(0 if success else 1)
    
    else:
        # 默认：运行所有测试 (基本功能 + 三种 mirror 类型)
        success = run_all_tests()
        sys.exit(0 if success else 1)
