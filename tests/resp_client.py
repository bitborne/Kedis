import socket
import struct


class RawRedisClient:
    """原始 RESP 协议客户端，精确控制每个字节"""
    
    def __init__(self, host='localhost', port=6379):
        self.host = host
        self.port = port
        self.sock = None
    
    def connect(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((self.host, self.port))
        return self
    
    def close(self):
        if self.sock:
            self.sock.close()
            self.sock = None
    
    def encode_bulk_string(self, s):
        """编码 Bulk String，支持 str/bytes/int/float"""
        # 统一转成字符串处理
        if isinstance(s, bytes):
            data = s
        else:
            # int, float 等先转 str
            data = str(s).encode('utf-8')
        
        length = len(data)
        return f"${length}\r\n".encode('utf-8') + data + b"\r\n"
    
    def encode_array(self, *args):
        """编码 RESP 数组: *数量\r\n$长度\r\n内容\r\n..."""
        # *数量\r\n
        result = f"*{len(args)}\r\n".encode('utf-8')
        
        # 每个参数作为 Bulk String
        for arg in args:
            result += self.encode_bulk_string(arg)
        
        return result
    
    def send_raw(self, data_bytes):
        """发送原始字节，确保完全发送"""
        total_sent = 0
        while total_sent < len(data_bytes):
            sent = self.sock.send(data_bytes[total_sent:])
            if sent == 0:
                raise ConnectionError("Socket connection broken")
            total_sent += sent
        return total_sent
    
    def recv_response(self):
        """接收并解析 RESP 响应（简单实现）"""
        # 先读取第一个字节判断类型
        type_byte = self.sock.recv(1)
        if not type_byte:
            return None
        
        # 读取到 \r\n 结束的第一行
        line = type_byte
        while not line.endswith(b'\r\n'):
            chunk = self.sock.recv(1)
            if not chunk:
                break
            line += chunk
        
        resp_type = chr(line[0])
        content = line[1:-2].decode('utf-8')  # 去掉类型和\r\n
        
        if resp_type == '+':  # Simple String
            return ("simple_string", content)
        elif resp_type == '-':  # Error
            return ("error", content)
        elif resp_type == ':':  # Integer
            return ("integer", int(content))
        elif resp_type == '$':  # Bulk String
            length = int(content)
            if length == -1:
                return ("bulk_string", None)  # null
            # 读取 length + 2 (\r\n)
            data = b''
            while len(data) < length + 2:
                data += self.sock.recv(length + 2 - len(data))
            return ("bulk_string", data[:-2].decode('utf-8'))
        elif resp_type == '*':  # Array
            count = int(content)
            if count == -1:
                return ("array", None)
            elements = []
            for _ in range(count):
                elements.append(self.recv_response())
            return ("array", elements)
        
        return ("unknown", line)
    
    def execute(self, *args):
        """执行命令，args 为命令和参数"""
        # 构造 RESP 数组
        data = self.encode_array(*args)
        
        # 打印即将发送的原始字节（调试用）
        print("Sending raw bytes:")
        print(repr(data))
        # print("Hex dump:")
        # print(data.hex(' '))
        
        # 发送
        self.send_raw(data)
        
        # 接收响应
        return self.recv_response()

def eval_input(prompt):

  user_input = input(prompt)
  if not user_input:
      return None  # 空输入表示None

  # 安全地执行简单的字符串乘法
  # 只允许字符串和数字的*运算
  try:
      # 尝试eval，但限制环境
      result = eval(user_input, {"__builtins__": {}}, {})
      return result
  except:
      # 如果eval失败，就当普通字符串返回
      return user_input
# ============ 使用示例 ============

if __name__ == "__main__":
    port = int(input("输入端口号: "))
    client = RawRedisClient('172.20.10.2', port)
    client.connect()
    
    try:
        while True:
            # 输入命令
            cmd = input("\n命令 (或quit退出): ").strip()
            if cmd.lower() in ('quit', 'q', 'exit'):
                break
            
            # 输入key，支持运算
            key = eval_input("key (支持如 'a'*3): ")
            if key is None:
                print("key不能为空")
                continue
            
            # 输入value，支持运算，空表示只有2个参数
            value = eval_input("value (回车跳过，支持如 'b'*4): ")
            
            # 构造参数列表
            if value is None:
                args = [cmd, key]
                print(f"执行: {cmd} {key!r} (2参数)")
            else:
                args = [cmd, key, value]
                print(f"执行: {cmd} {key!r} {value!r} (3参数)")
            
            # 执行并打印结果
            result = client.execute(*args)
            print(f"响应: {result}")
            
    finally:
        client.close()
        print("已断开连接")