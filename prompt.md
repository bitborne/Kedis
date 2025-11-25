提示词：KV 存储 v2 —— 把持久化全部掀桌子重写  
（仅 fork + 缓冲区，无线程，不兼容旧 .dat，一步到位）

---

角色  
你是「刚学完 Redis 源码的学生」，手头只有一个单进程 KV 仓库（含基础网络、哈希表、fork）。  
**任务：把持久化层整个扔掉，换成最简 KSF + 最简 AOF**，其余代码不动。

---

一、先看清现有代码（必须逐文件确认）
persist.c
kvstore.c
kvstore.c
看清楚目录下的.dat和.log两个文件, 分别对应我想实现的类redis的".rdb"和".aof"
二、重写范围（不允许新增线程，只用 fork）

1. AOF 部分（改写为文本 RESP）
   1.1 新增全局变量  
   ```
   char aof_buf[256*1024];     // 256 KB 静态缓冲
   int  aof_len;               
   ```
   1.2 写命令入口 `call()` 最后把命令按 RESP 追加到 `aof_buf`，**不写盘**。  
   1.3 启动时 `fork()` 一个**子进程** `aof_fsync_process()`：  
       - 死循环 `sleep(1); write(aof_fd, aof_buf, aof_len); fsync(aof_fd);`  
       - 父进程在 fork 前把 `aof_buf + aof_len` 通过 `pipe` 送给子进程，**以后每秒通过 pipe 传一次缓冲区快照**（无锁，父进程只在 pipe 里写）。  
   1.4 不提供 AOF 重写，**文件会越来越大**，先接受。  
   1.5 单测：随机写 1 万次，kill -9 → 重启丢数据 ≤ 1 秒。

2. KSF 部分（全新二进制快照，代替 .dat）
   2.1 格式**最简**，只用「长度+正文」：  
       ```
       [klen  VLQ]  [vlen  VLQ]  [key]  [value]   （一条 KV 连续）
       ```
       无 chunk、无 crc、无压缩，**顺序写，单遍完成**。  
   2.2 文件名 `dump-YYYYmmdd-HHMMSS.ksf`，临时文件 `temp-<pid>.ksf`，写完 `rename()`。  
   2.3 提供两个命令：  
       - `SAVE` → 主线程直接 `ksfSave()`，**阻塞**所有客户端直到写完。  
       - `BGSAVE` → `fork()` → 子进程调 `ksfSave()` → 父进程立即返回 `+OK`。  
   2.4 定时 save（放在 `serverCron()`）：  
       配置写死 `save 300 100`（5 分钟 & 100 次写）→ 满足就 `fork()` 一次 BGSAVE。  
   2.5 开机加载：  
       - 若 `appendonly.aof` 存在 → 逐行重放（老代码 RESP 解析器留着）。  
       - 再加载最新 `dump-*.ksf`（按文件名时间戳排序）→ 覆盖/追加到内存。  
       - 若两者都无 → 空库启动。

3. 混合模式（固定）
   运行期 **AOF 子进程每秒 fsync + KSF 每 5 分钟 BGSAVE** 同时生效，  
   配置项**本次全部写死**，不允许用户改，减少交互。

---

三、源码修改点（直接给你行级指引）

1. `kvstore.h`  
   新增 extern  
   ```
   extern char aof_buf[];
   extern int  aof_len;
   int ksfSave(const char *filename);
   int ksfSaveBackground(void);
   ```

2. `persist.c, kvstore.c`

  有关原先的持久化存储方案全部改写

3. `aof.c`
   - `appendToAofBuffer()` → 把 RESP 串追加到 `aof_buf`  
   - `aof_fsync_process()` → 子进程死循环 1 s fsync  
   - 启动时 `pipe()` + `fork()` 一次，父进程只写 pipe。

4. `ksf.c` 新建（100 行左右）
   ```
   ksfWriteOneKv(int fd, const char *k, size_t klen, const char *v, size_t vlen)
   {
       uint8_t vlq[16];
       write(fd, vlq, encode_vlq(klen, vlq));
       write(fd, vlq, encode_vlq(vlen, vlq));
       write(fd, k, klen);
       write(fd, v, vlen);
   }
   ksfSave() { fd=open("temp-xxx.ksf"); 遍历哈希表调 ksfWriteOneKv(); rename(); }
   ```

5. `Makefile`  
   把旧 `.dat` 读写文件直接移除，只加 `ksf.o`。

---

四、交付标准（单测脚本一步到位）

② kill -9 → 重启 → 丢数据 ≤ 1 秒（AOF 1s 窗口）  
③ `strace -f` 主线程从未 `fsync`  
④ `BGSAVE` 期间 `ps` 可见子进程，退出码 0  
⑤ 老 `.dat` 文件**不再识别**，直接忽略（不报错）

---

五、红线

- 不允许引入 pthread 或任何第三方库  
- 不允许在父线程做 fsync / 大块 write  
- 不允许保留旧 .dat 解析代码  
- 配置全部写死（save 300 100，aof 1s

--
