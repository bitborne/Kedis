use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

/// 所有支持的命令列表
const VALID_COMMANDS: &[&str] = &[
    // 基本命令
    "SET", "GET", "DEL", "MOD", "EXIST",
    // Array 引擎
    "ASET", "AGET", "ADEL", "AMOD", "AEXIST",
    // Hash 引擎
    "HSET", "HGET", "HDEL", "HMOD", "HEXIST",
    // RBTRee 引擎
    "RSET", "RGET", "RDEL", "RMOD", "REXIST",
    // Skiplist 引擎
    "SSET", "SGET", "SDEL", "SMOD", "SEXIST",
    // 管理命令
    "SAVE", "BGSAVE", "SYNC",
];

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
    pub fn connect(&self) -> Result<TcpStream, Box<dyn std::error::Error>> {
        let address = format!("{}:{}", self.host, self.port);
        let stream = TcpStream::connect(address)?;
        stream.set_read_timeout(Some(Duration::from_secs(5)))?;
        stream.set_write_timeout(Some(Duration::from_secs(5)))?;
        Ok(stream)
    }

    /// 发送RESP命令并返回响应
    pub fn send_resp_command(&self, resp_data: &str) -> Result<String, Box<dyn std::error::Error>> {
        let mut stream = self.connect()?;
        stream.write_all(resp_data.as_bytes())?;
        stream.flush()?;

        // 读取响应
        let mut response = Vec::new();
        let mut buffer = [0u8; 1024];
        let start_time = std::time::Instant::now();
        let timeout = std::time::Duration::from_secs(5);

        loop {
            if start_time.elapsed() > timeout {
                return Err("读取响应超时".into());
            }

            match stream.read(&mut buffer) {
                Ok(0) => break,
                Ok(n) => {
                    response.extend_from_slice(&buffer[..n]);
                    if buffer[..n].contains(&b'\n') {
                        break;
                    }
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(std::time::Duration::from_millis(10));
                    continue;
                }
                Err(e) => return Err(e.into()),
            }
        }

        let response_str = String::from_utf8_lossy(&response);
        Ok(response_str.to_string())
    }

    /// 使用已有连接发送RESP命令
    pub fn send_resp_command_with_connection(&self, resp_data: &str, stream: &mut TcpStream) -> Result<String, Box<dyn std::error::Error>> {
        stream.write_all(resp_data.as_bytes())?;
        stream.flush()?;

        let mut response = Vec::new();
        let mut buffer = [0u8; 1024];
        let start_time = std::time::Instant::now();
        let timeout = std::time::Duration::from_secs(5);

        loop {
            if start_time.elapsed() > timeout {
                return Err("读取响应超时".into());
            }

            match stream.read(&mut buffer) {
                Ok(0) => break,
                Ok(n) => {
                    response.extend_from_slice(&buffer[..n]);
                    if buffer[..n].contains(&b'\n') {
                        break;
                    }
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(std::time::Duration::from_millis(10));
                    continue;
                }
                Err(e) => return Err(e.into()),
            }
        }

        let response_str = String::from_utf8_lossy(&response);
        Ok(response_str.to_string())
    }

    /// 将命令字符串编码为RESP格式
    pub fn encode_to_resp(&self, cmd: &str) -> Result<String, Box<dyn std::error::Error>> {
        let mut args: Vec<String> = Vec::new();
        let mut chars = cmd.chars().peekable();
        let mut current_arg = String::new();
        let mut in_quotes = false;
        let mut escape = false;

        while let Some(c) = chars.next() {
            if escape {
                match c {
                    'r' => current_arg.push('\r'),
                    'n' => current_arg.push('\n'),
                    't' => current_arg.push('\t'),
                    '\\' => current_arg.push('\\'),
                    '"' => current_arg.push('"'),
                    _ => {
                        current_arg.push('\\');
                        current_arg.push(c);
                    }
                }
                escape = false;
            } else if c == '\\' {
                escape = true;
            } else if c == '"' {
                in_quotes = !in_quotes;
            } else if c == ' ' && !in_quotes {
                if !current_arg.is_empty() {
                    args.push(current_arg.clone());
                    current_arg.clear();
                }
            } else {
                current_arg.push(c);
            }
        }

        if !current_arg.is_empty() {
            args.push(current_arg);
        }

        // 验证命令是否有效
        if !args.is_empty() {
            let cmd_upper = args[0].to_uppercase();
            if !VALID_COMMANDS.contains(&cmd_upper.as_str()) {
                return Err(format!("无效的命令: {} (支持的命令: {:?})", args[0], VALID_COMMANDS).into());
            }
            args[0] = cmd_upper;
        }

        // 构建RESP协议
        let mut resp = format!("*{}\r\n", args.len());
        for arg in &args {
            resp.push_str(&format!("${}\r\n{}\r\n", arg.len(), arg));
        }

        Ok(resp)
    }

    /// 解析多命令输入
    /// 输入格式：<cmd1> <arg1> <arg2> <cmd2> <arg1> <cmd3> <arg1>
    /// 返回：Vec<String> 每个元素是一个完整的命令字符串
    pub fn parse_multi_command(&self, input: &str) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let mut commands: Vec<String> = Vec::new();
        let mut current_cmd = String::new();
        let mut chars = input.chars().peekable();
        let mut in_quotes = false;
        let mut escape = false;

        while let Some(c) = chars.next() {
            if escape {
                current_cmd.push('\\');
                current_cmd.push(c);
                escape = false;
            } else if c == '\\' {
                escape = true;
            } else if c == '"' {
                in_quotes = !in_quotes;
                current_cmd.push(c);
            } else if c == ' ' && !in_quotes {
                // 空格分隔参数
                if !current_cmd.is_empty() {
                    current_cmd.push(' ');
                }
            } else if c == ' ' && in_quotes {
                // 引号内的空格
                current_cmd.push(c);
            } else {
                current_cmd.push(c);
            }

            // 检测命令边界：如果当前字符是空格且不在引号内，检查下一个token是否是命令
            if c == ' ' && !in_quotes && !escape {
                // 跳过后续的空格
                let mut peek_chars = chars.clone();
                while let Some(&next_c) = peek_chars.peek() {
                    if next_c == ' ' {
                        peek_chars.next();
                    } else {
                        break;
                    }
                }

                // 提取下一个token
                let mut next_token = String::new();
                let mut token_in_quotes = false;
                let mut token_escape = false;

                while let Some(&next_c) = peek_chars.peek() {
                    if token_escape {
                        next_token.push(next_c);
                        token_escape = false;
                        peek_chars.next();
                    } else if next_c == '\\' {
                        token_escape = true;
                        peek_chars.next();
                    } else if next_c == '"' {
                        token_in_quotes = !token_in_quotes;
                        next_token.push(next_c);
                        peek_chars.next();
                    } else if next_c == ' ' && !token_in_quotes {
                        break;
                    } else {
                        next_token.push(next_c);
                        peek_chars.next();
                    }
                }

                // 检查下一个token是否是有效命令
                if !next_token.is_empty() {
                    let next_token_upper = next_token.to_uppercase();
                    if VALID_COMMANDS.contains(&next_token_upper.as_str()) {
                        // 找到命令边界，保存当前命令
                        if !current_cmd.trim().is_empty() {
                            commands.push(current_cmd.trim().to_string());
                            current_cmd.clear();
                        }
                    }
                }
            }
        }

        // 添加最后一个命令
        if !current_cmd.trim().is_empty() {
            commands.push(current_cmd.trim().to_string());
        }

        Ok(commands)
    }

    /// 验证命令是否有效
    pub fn validate_command(&self, cmd: &str) -> Result<(), Box<dyn std::error::Error>> {
        let args: Vec<&str> = cmd.split_whitespace().collect();
        if args.is_empty() {
            return Err("命令为空".into());
        }

        let cmd_upper = args[0].to_uppercase();
        if !VALID_COMMANDS.contains(&cmd_upper.as_str()) {
            return Err(format!("无效的命令: {} (支持的命令: {:?})", args[0], VALID_COMMANDS).into());
        }

        Ok(())
    }

    /// 执行多命令（顺序执行）
    pub fn execute_multi_commands(&self, input: &str) -> Result<String, Box<dyn std::error::Error>> {
        let commands = self.parse_multi_command(input)?;

        if commands.is_empty() {
            return Ok("ERROR: No commands found".to_string());
        }

        let mut results = Vec::new();

        for cmd in &commands {
            // 验证命令
            if let Err(e) = self.validate_command(cmd) {
                results.push(format!("ERROR: {}", e));
                continue;
            }

            let resp_data = self.encode_to_resp(cmd)?;
            let response = self.send_resp_command(&resp_data)?;
            results.push(response);
        }

        Ok(results.join("\n"))
    }

    /// 使用已有连接执行多命令
    pub fn execute_multi_commands_with_connection(&self, input: &str, stream: &mut TcpStream) -> Result<String, Box<dyn std::error::Error>> {
        let commands = self.parse_multi_command(input)?;

        if commands.is_empty() {
            return Ok("ERROR: No commands found".to_string());
        }

        let mut results = Vec::new();

        for cmd in &commands {
            // 验证命令
            if let Err(e) = self.validate_command(cmd) {
                results.push(format!("ERROR: {}", e));
                continue;
            }


            // println!("CMD_RAW: {cmd}");
            let resp_data = self.encode_to_resp(cmd)?;
            // println!("RESP: {resp_data}");
            let response = self.send_resp_command_with_connection(&resp_data, stream)?;
            results.push(response);
        }

        Ok(results.join("\n"))
    }

    /// 获取所有支持的命令列表
    pub fn get_valid_commands() -> Vec<&'static str> {
        VALID_COMMANDS.to_vec()
    }
}
