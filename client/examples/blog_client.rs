use kv_client::KVClient;
use std::fs;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = KVClient::new("127.0.0.1".to_string(), 8080);
    
    // 创建一个示例博客内容
    let blog_content = r#"# 我的第一篇博客

欢迎来到我的博客！

## 章节一
这是博客的第一章节内容。

## 章节二
这是博客的第二章节内容。

- 列表项1
- 列表项2

```rust
fn main() {
    println!("Hello, blog!");
}
```
"#;
    
    // 将博客内容保存到临时文件
    fs::write("sample_blog.md", blog_content)?;
    
    // 从文件插入博客
    match client.insert_blog_from_file("sample_blog.md") {
        Ok(result) => println!("博客插入成功: {}", result),
        Err(e) => eprintln!("博客插入失败: {}", e),
    }
    
    // 测试获取博客内容
    match client.get("sample_blog") {
        Ok(result) => println!("获取博客内容: {}", result),
        Err(e) => eprintln!("获取博客内容失败: {}", e),
    }
    
    // 清理临时文件
    std::fs::remove_file("sample_blog.md")?;
    
    Ok(())
}