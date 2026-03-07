#!/usr/bin/env python3
# bulk_import.py
import sys
import os

# 生成 128 字节随机数据（如果不需要随机，可改成固定字符串）
DATA = os.urandom(128)  # 128字节随机二进制数据
# 或者固定数据：DATA = b"X" * 128

def generate_resp():
    for i in range(1, 1000001):  # 1 到 1000000
        key = f"key_{i}"
        # RESP 协议格式：
        # *3           -> 3个参数
        # $3\r\nSET    -> 命令是SET，长度3
        # ${len(key)}\r\n{key}  -> key名
        # $128\r\n{data}        -> value数据，长度128
        header = f"*3\r\n$4\r\nSSET\r\n${len(key)}\r\n{key}\r\n$128\r\n".encode()
        
        sys.stdout.buffer.write(header)
        sys.stdout.buffer.write(DATA)
        sys.stdout.buffer.write(b"\r\n")

if __name__ == "__main__":
    generate_resp()