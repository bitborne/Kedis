import socket
import struct

class RawRedisClient:
    """优化后的原始 RESP 协议客户端，使用带缓冲的读取以提升性能"""
    
    def __init__(self, host='localhost', port=6379):
        self.host = host
        self.port = port
        self.sock = None
        self.reader = None # 缓冲读取器
    
    def connect(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((self.host, self.port))
        # 核心优化：使用 makefile 创建一个带缓冲的二进制文件对象
        # 默认缓冲区通常为 8KB，能极大减少 recv 系统调用
        self.reader = self.sock.makefile('rb')
        return self
    
    def close(self):
        if self.reader:
            self.reader.close()
            self.reader = None
        if self.sock:
            self.sock.close()
            self.sock = None
    
    def encode_bulk_string(self, s):
        """编码 Bulk String"""
        if isinstance(s, bytes):
            data = s
        else:
            data = str(s).encode('utf-8')
        length = len(data)
        return f"${length}\r\n".encode('utf-8') + data + b"\r\n"
    
    def encode_array(self, *args):
        """编码 RESP 数组"""
        result = f"*{len(args)}\r\n".encode('utf-8')
        for arg in args:
            result += self.encode_bulk_string(arg)
        return result
    
    def send_raw(self, data_bytes):
        """发送原始字节"""
        self.sock.sendall(data_bytes)
    
    def recv_response(self):
        """高效接收并解析 RESP 响应"""
        if not self.reader:
            return None
            
        # 1. 使用 readline 一次性读取类型和长度/内容行，不再使用 recv(1)
        line = self.reader.readline()
        if not line:
            return None
        
        resp_type = chr(line[0])
        # 去掉前面的类型字符和末尾的 \r\n
        content = line[1:-2].decode('utf-8', errors='replace')
        
        if resp_type == '+':  # Simple String
            return ("simple_string", content)
        elif resp_type == '-':  # Error
            return ("error", content)
        elif resp_type == ':':  # Integer
            return ("integer", int(content))
        elif resp_type == '$':  # Bulk String
            length = int(content)
            if length == -1:
                return ("bulk_string", None)
            # 2. 已知长度，直接从缓冲区读取固定字节
            data = self.reader.read(length)
            self.reader.read(2)  # 跳过末尾的 \r\n
            return ("bulk_string", data.decode('utf-8', errors='replace'))
        elif resp_type == '*':  # Array
            count = int(content)
            if count == -1:
                return ("array", None)
            return ("array", [self.recv_response() for _ in range(count)])
        
        return ("unknown", line)
    
    def execute(self, *args):
        """执行命令"""
        data = self.encode_array(*args)
        self.send_raw(data)
        # 移除调试打印，避免在高性能测试中产生额外开销
        return self.recv_response()

def eval_input(prompt):
    user_input = input(prompt)
    if not user_input: return None
    try:
        return eval(user_input, {"__builtins__": {}}, {})
    except:
        return user_input

if __name__ == "__main__":
    # 保持原有示例逻辑
    port = int(input("输入端口号: "))
    client = RawRedisClient('127.0.0.1', port)
    client.connect()
    try:
        while True:
            cmd = input("\n命令 (或quit退出): ").strip()
            if cmd.lower() in ('quit', 'q', 'exit'): break
            key = eval_input("key: ")
            if key is None: continue
            value = eval_input("value (回车跳过): ")
            args = [cmd, key] if value is None else [cmd, key, value]
            print(f"响应: {client.execute(*args)}")
    finally:
        client.close()
