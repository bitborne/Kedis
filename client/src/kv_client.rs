use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;
use percent_encoding::{utf8_percent_encode, AsciiSet, NON_ALPHANUMERIC};

/// 定义需要编码的字符集（除了字母数字外的大部分字符）
pub const ENCODE_SET: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~');

/// KV存储客户端结构体
pub struct KVClient {
    pub host: String,
    pub port: u16,
}

impl KVClient {
    /// 创建新的KV客户端
    pub fn new(host: String, port: u16) -> Self {
        Self { host, port }
    }

    /// 连接到KV服务器
    fn connect(&self) -> Result<TcpStream, Box<dyn std::error::Error>> {
        let address = format!("{}:{}", self.host, self.port);
        let stream = TcpStream::connect(address)?;
        stream.set_read_timeout(Some(Duration::from_secs(5)))?;
        stream.set_write_timeout(Some(Duration::from_secs(5)))?;
        Ok(stream)
    }

    /// 发送命令到服务器并返回响应
    pub fn send_command(&self, command: &str) -> Result<String, Box<dyn std::error::Error>> {
        let mut stream = self.connect()?;

        // 发送命令
        stream.write_all(command.as_bytes())?;

        // 读取响应
        let mut response = String::new();
        stream.read_to_string(&mut response)?;

        Ok(response)
    }

    /// 发送命令到服务器并返回响应，使用已建立的连接（用于交互模式）
    pub fn send_command_with_connection(&self, command: &str, stream: &mut TcpStream) -> Result<String, Box<dyn std::error::Error>> {
        stream.write_all(command.as_bytes())?;
        stream.flush()?; // 确保命令被发送

        // 读取响应 - 使用缓冲区而不是read_to_string，因为服务器可能不会立即发送完整的响应
        let mut response = Vec::new();
        let mut buffer = [0u8; 1024];  // 1KB缓冲区
        let start_time = std::time::Instant::now();
        let timeout = std::time::Duration::from_secs(5); // 5秒超时

        loop {
            // 检查是否超时
            if start_time.elapsed() > timeout {
                return Err("读取响应超时".into());
            }

            match stream.read(&mut buffer) {
                Ok(0) => break, // 连接已关闭
                Ok(n) => {
                    response.extend_from_slice(&buffer[..n]);
                    // 如果响应包含换行符，说明可能已经收到了完整的响应
                    if buffer[..n].contains(&b'\n') {
                        break;
                    }
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    // 没有数据可读，继续循环，但稍作延时
                    std::thread::sleep(std::time::Duration::from_millis(10));
                    continue;
                }
                Err(e) => return Err(e.into()),
            }
        }

        let response_str = String::from_utf8_lossy(&response);
        Ok(response_str.to_string())
    }

    /// 设置键值对 - 使用URL编码处理特殊字符
    pub fn set(&self, key: &str, value: &str) -> Result<String, Box<dyn std::error::Error>> {
        // 只对值进行编码以处理特殊字符和空格
        let encoded_value = utf8_percent_encode(value, ENCODE_SET).to_string();
        let command = format!("SET {} {}\n", key, encoded_value);
        self.send_command(&command)
    }
/*
    /// 获取键的值
    pub fn get(&self, key: &str) -> Result<String, Box<dyn std::error::Error>> {
        let command = format!("GET {}\n", key);
        let response = self.send_command(&command)?;

        // 直接尝试解码响应，无论是错误信息还是值，都会被正确处理
        if let Ok(decoded_value) = percent_decode_str(&response).decode_utf8() {
            return Ok(decoded_value.to_string().trim_end_matches("\r\n").to_string());
        }

        // 如果解码失败，返回原始响应
        Ok(response)
    }

    /// 删除键
    pub fn del(&self, key: &str) -> Result<String, Box<dyn std::error::Error>> {
        let command = format!("DEL {}\n", key);
        self.send_command(&command)
    }

    /// 修改键的值 - 使用URL编码处理特殊字符
    pub fn mod_key(&self, key: &str, value: &str) -> Result<String, Box<dyn std::error::Error>> {
        // 只对值进行编码以处理特殊字符和空格
        let encoded_value = utf8_percent_encode(value, ENCODE_SET).to_string();
        let command = format!("MOD {} {}\n", key, encoded_value);
        self.send_command(&command)
    }

    /// 检查键是否存在
    pub fn exists(&self, key: &str) -> Result<String, Box<dyn std::error::Error>> {
        let command = format!("EXIST {}\n", key);
        self.send_command(&command)
    }

    /// 多命令并行执行 - 使用 & 操作符
    pub fn multi_cmd_parallel(&self, commands: Vec<&str>) -> Result<String, Box<dyn std::error::Error>> {
        let command_str = commands.join(" & ");
        self.send_command(&format!("{}\n", command_str))
    }

    /// 多命令顺序执行 - 使用 && 操作符
    pub fn multi_cmd_sequential(&self, commands: Vec<&str>) -> Result<String, Box<dyn std::error::Error>> {
        let command_str = commands.join(" && ");
        self.send_command(&format!("{}\n", command_str))
    }

    /// 从文件读取内容并插入作为博客
    pub fn insert_blog_from_file(&self, file_path: &str) -> Result<String, Box<dyn std::error::Error>> {
        let content = std::fs::read_to_string(file_path)?;
        let title = std::path::Path::new(file_path)
            .file_stem()
            .ok_or("无法从文件路径获取文件名")?
            .to_str()
            .ok_or("文件名包含无效字符")?
            .to_string();

        self.set(&title, &content)
    }
    */
}

