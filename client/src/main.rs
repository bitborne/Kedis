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

    /// 启用交互模式
    #[arg(short = 'i', long)]
    interactive: bool,

    /// 列出所有支持的命令
    #[arg(short = 'l', long)]
    list_commands: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let client = KVClient::new(cli.host, cli.port);

    // 列出所有支持的命令
    if cli.list_commands {
        println!("支持的命令列表:");
        for cmd in KVClient::get_valid_commands() {
            println!("  - {}", cmd);
        }
        println!("\n基本命令: SET, GET, DEL, MOD, EXIST");
        println!("Array引擎: ASET, AGET, ADEL, AMOD, AEXIST");
        println!("Hash引擎: HSET, HGET, HDEL, HMOD, HEXIST");
        println!("RBTRee引擎: RSET, RGET, RDEL, RMOD, REXIST");
        println!("Skiplist引擎: SSET, SGET, SDEL, SMOD, SEXIST");
        println!("管理命令: SAVE, BGSAVE, SYNC");
        return Ok(());
    }

    // 根据参数选择执行模式
    if cli.command.is_some() || cli.file.is_some() {
        // 非交互模式
        match (&cli.command, &cli.file) {
            (Some(cmd), _) => {
                // 执行命令（支持多命令）
                let result = client.execute_multi_commands(cmd)?;
                println!("{}", result);
            }
            (_, Some(file_path)) => {
                // 从文件读取并执行命令
                let mut file = std::fs::File::open(file_path)?;
                let mut commands = String::new();
                file.read_to_string(&mut commands)?;
                let result = client.execute_multi_commands(&commands)?;
                println!("{}", result);
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

    println!("已连接到KV存储服务器 {}:{}", client.host, client.port);
    println!("输入命令或使用以下特殊命令:");
    println!("  quit/exit - 退出交互模式");
    println!("  help - 显示帮助信息");
    println!("支持多命令格式: <cmd1> <arg1> <arg2> <cmd2> <arg1> <cmd3> <arg1>");
    println!("例如: SET key1 value1 GET key1 SET key2 value2 GET key2");
    println!("支持引号包裹的 key/value 以包含特殊字符:");
    println!("  例如: SET \"key with spaces\" \"value with spaces\"");
    println!("       SET \"key\\nwith\\nnewlines\" \"value\\ttabs\"");
    println!("       GET \"key with spaces\"");
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

        // 显示帮助
        if input == "help" {
            println!("支持的命令:");
            for cmd in KVClient::get_valid_commands() {
                println!("  - {}", cmd);
            }
            println!("\n特殊命令:");
            println!("  quit/exit - 退出");
            println!("  help - 显示此帮助");
            continue;
        }

        

        // 执行命令（支持多命令）
        let response = client.execute_multi_commands_with_connection(input, &mut stream)?;

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


