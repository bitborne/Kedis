mod kv_client;

use clap::Parser;
use kv_client::KVClient;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::time::Duration;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// 服务器主机地址
    #[arg(short = 'H', long, default_value = "127.0.0.1")]
    host: String,

    /// 服务器端口
    #[arg(short = 'P', long, default_value_t = 8888)]
    port: u16,

    /// 要执行的命令
    #[arg(value_parser)]
    command: Option<String>,

    /// 从文件读取命令
    #[arg(short = 'f', long)]
    file: Option<String>,

    /// 博客文件路径，用于插入博客
    #[arg(short = 'b', long)]
    blog: Option<String>,

    /// 启用交互模式
    #[arg(short = 'i', long)]
    interactive: bool,

    /// 选择使用的引擎 (array, hash, rbtree, skiplist)
    #[arg(short = 'e', long, default_value = "array")]
    engine: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let client = KVClient::new(cli.host, cli.port, cli.engine);

    // 根据参数选择执行模式
    // 如果提供了命令、文件或博客参数，则非交互模式
    // 否则进入交互模式（默认）
    if cli.command.is_some() || cli.file.is_some() || cli.blog.is_some() {
        // 非交互模式
        match (&cli.command, &cli.file, &cli.blog) {
            (Some(cmd), _, _) => {
                // 执行单个命令
                let result = client.send_resp_command(cmd)?;
                println!("{}", result);
            }
            (_, Some(file_path), _) => {
                // 从文件读取并执行命令
                let mut file = std::fs::File::open(file_path)?;
                let mut commands = String::new();
                file.read_to_string(&mut commands)?;
                let result = client.send_resp_command(&commands)?;
                println!("{}", result);
            }
            (_, _, Some(blog_path)) => {
                // 从文件插入博客
                let result = client.insert_blog_from_file(blog_path)?;
                println!("博客插入结果: {}", result);
            }
            _ => {
                eprintln!("请提供命令或使用 -h 查看帮助");
                std::process::exit(1);
            }
        }
    } else {
        // 默认为交互模式
        run_interactive_mode(client)?;
    }

    Ok(())
}

/// 运行交互模式
fn run_interactive_mode(client: KVClient) -> Result<(), Box<dyn std::error::Error>> {
    // 建立与服务器的持久连接
    let address = format!("{}:{}", client.host, client.port);
    let mut stream = TcpStream::connect(address)?;
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    stream.set_write_timeout(Some(Duration::from_secs(5)))?;

    println!("已连接到KV存储服务器 {}:{} (使用 {} 引擎)", client.host, client.port, client.engine);
    println!("输入命令或使用以下特殊命令:");
    println!("  quit/exit - 退出交互模式");
    println!("  blog <file_path> - 插入博客文件");
    println!("支持引号包裹的 key/value 以包含特殊字符:");
    println!("  例如: SET \"key with spaces\" \"value with spaces\"");
    println!("       SET \"key\\nwith\\nnewlines\" \"value\\ttabs\"");
    println!("       GET \"key with spaces\"");
    println!("      blog /path/to/blog.md");
    println!("");

    let stdin = std::io::stdin();
    let mut reader = BufReader::new(stdin.lock());
    let mut input = String::new();

    loop {
        print!("kv_client> ");
        std::io::stdout().flush()?;

        input.clear();
        reader.read_line(&mut input)?;

        // 移除换行符
        let input = input.trim();

        // 检查退出命令
        if input == "quit" || input == "exit" {
            break;
        }

        // 检查是否是插入博客命令
        if input.starts_with("blog ") {
            let parts: Vec<&str> = input.splitn(2, ' ').collect();
            if parts.len() == 2 {
                let file_path = parts[1];

                // 扩展路径中的 ~ 为用户的主目录
                let expanded_path = if file_path.starts_with("~/") {
                    let home_dir = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
                    format!("{}/{}", home_dir, &file_path[2..])
                } else {
                    file_path.to_string()
                };

                // 读取博客文件内容
                match std::fs::read_to_string(&expanded_path) {
                    Ok(content) => {
                        match std::path::Path::new(&expanded_path)
                            .file_stem()
                            .ok_or("无法从文件路径获取文件名")
                            .and_then(|os_str| os_str.to_str().ok_or("文件名包含无效字符")) {

                            Ok(title_str) => {
                                // 使用 RESP 协议发送 SET 命令
                                let command = format!("SET \"{}\" \"{}\"", title_str, content.replace('"', "\\\""));
                                match client.send_resp_command_with_connection(&command, &mut stream) {
                                    Ok(response) => {
                                        println!("博客插入结果: {}", response);
                                    }
                                    Err(e) => {
                                        eprintln!("服务器错误: {}", e);
                                        // 尝试重新建立连接
                                        let address = format!("{}:{}", client.host, client.port);
                                        match TcpStream::connect(address) {
                                            Ok(new_stream) => {
                                                stream = new_stream;
                                                if let Err(_) = stream.set_read_timeout(Some(Duration::from_secs(5))) {
                                                    eprintln!("警告: 设置读取超时失败");
                                                }
                                                if let Err(_) = stream.set_write_timeout(Some(Duration::from_secs(5))) {
                                                    eprintln!("警告: 设置写入超时失败");
                                                }
                                            },
                                            Err(e) => {
                                                eprintln!("重新连接失败: {}", e);
                                            }
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                eprintln!("文件名错误: {}", e);
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("路径错误: {}", e);
                    }
                }

                continue;
            }
        }

        // 使用 RESP 协议发送命令
        let response = client.send_resp_command_with_connection(input, &mut stream)?;

        // 直接尝试解码响应，无论是错误信息还是值，都会被正确处理
        if let Ok(decoded_response) = percent_encoding::percent_decode_str(&response).decode_utf8() {
            println!("{}", decoded_response);
        } else {
            // 如果解码失败，输出原始响应
            println!("{}", response);
        }
    }

    println!("Bye!");
    Ok(())
}