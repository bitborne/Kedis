mod kv_client;

use clap::Parser;
use kv_client::KVClient;
use std::io::{self, Read};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// 服务器主机地址
    #[arg(short, long, default_value = "127.0.0.1")]
    host: String,

    /// 服务器端口
    #[arg(short, long, default_value_t = 8080)]
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
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let client = KVClient::new(cli.host, cli.port);

    match (&cli.command, &cli.file, &cli.blog) {
        (Some(cmd), _, _) => {
            // 执行单个命令
            let result = client.send_command(cmd)?;
            println!("{}", result);
        }
        (_, Some(file_path), _) => {
            // 从文件读取并执行命令
            let mut file = std::fs::File::open(file_path)?;
            let mut commands = String::new();
            file.read_to_string(&mut commands)?;
            let result = client.send_command(&commands)?;
            println!("{}", result);
        }
        (_, _, Some(blog_path)) => {
            // 从文件插入博客
            let result = client.insert_blog_from_file(blog_path)?;
            println!("博客插入结果: {}", result);
        }
        _ => {
            // 交互模式
            eprintln!("请提供命令或使用 -h 查看帮助");
            std::process::exit(1);
        }
    }

    Ok(())
}