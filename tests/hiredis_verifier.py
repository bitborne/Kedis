import redis

r = redis.Redis(host='localhost', port=8888, decode_responses=False)

# 获取实际连接
pool = r.connection_pool
conn = pool.make_connection()
conn.connect()  # 真正建立连接

# 查看解析器
parser = conn._parser
print(f"当前解析器: {type(parser).__name__}")
print(f"所属模块: {type(parser).__module__}")

# 关闭连接
conn.disconnect()