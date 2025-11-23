use kv_client::KVClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = KVClient::new("127.0.0.1".to_string(), 8080);
    
    println!("测试多命令并行执行:");
    let commands = vec![
        "SET key1 value1",
        "SET key2 value2",
        "GET key1"
    ];
    
    match client.multi_cmd_parallel(commands) {
        Ok(result) => println!("并行执行结果: {}", result),
        Err(e) => eprintln!("并行执行失败: {}", e),
    }
    
    println!("\n测试多命令顺序执行:");
    let commands_seq = vec![
        "SET key3 value3",
        "GET key3"
    ];
    
    match client.multi_cmd_sequential(commands_seq) {
        Ok(result) => println!("顺序执行结果: {}", result),
        Err(e) => eprintln!("顺序执行失败: {}", e),
    }
    
    // 测试复杂多命令场景
    println!("\n测试复杂多命令场景:");
    let complex_commands = vec![
        "SET blog1 '# First Blog\nContent here...'",
        "SET blog2 '# Second Blog\nMore content...'", 
        "GET blog1"
    ];
    
    match client.multi_cmd_parallel(complex_commands) {
        Ok(result) => println!("复杂命令执行结果: {}", result),
        Err(e) => eprintln!("复杂命令执行失败: {}", e),
    }
    
    Ok(())
}