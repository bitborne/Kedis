use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

/// KV存储客户端结构体
pub struct KVClient {
    host: String,
    port: u16,
}

impl KVClient {
    /// 创建新的KV客户端
    pub fn new(host: String, port: u16) -> Self {
        Self { host, port }
    }

    /// 连接到KV服务器
    fn connect(&self) -> Result<TcpStream, Box<dyn std::error::Error>> {
        let address = format!("{}:{}", self.host, self.port);
        let mut stream = TcpStream::connect(address)?;
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

    /// 设置键值对
    pub fn set(&self, key: &str, value: &str) -> Result<String, Box<dyn std::error::Error>> {
        let command = format!("SET {} {}\n", key, value);
        self.send_command(&command)
    }

    /// 获取键的值
    pub fn get(&self, key: &str) -> Result<String, Box<dyn std::error::Error>> {
        let command = format!("GET {}\n", key);
        self.send_command(&command)
    }

    /// 删除键
    pub fn del(&self, key: &str) -> Result<String, Box<dyn std::error::Error>> {
        let command = format!("DEL {}\n", key);
        self.send_command(&command)
    }

    /// 修改键的值
    pub fn mod_key(&self, key: &str, value: &str) -> Result<String, Box<dyn std::error::Error>> {
        let command = format!("MOD {} {}\n", key, value);
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
}