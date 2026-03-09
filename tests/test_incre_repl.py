import unittest
import redis
import time
import subprocess
import os
import signal
import socket
import sys
import argparse
from base import KVServerBase, KVRedis

class TestInreRepl(KVServerBase):
    """
    网络内请求复制 (In-Network Request Replication) 测试
    验证通过 BPF mirror 工具实现的 Master -> Slave 实时数据同步
    
    支持三种 mirror 类型:
    - mirror: TC ingress hook (参数: <ifname> <slave_ip> <slave_port>)
    - xdp_mirror: XDP (参数: <ifname> <slave_ip> <slave_port>)
    - uprobe_mirror: uprobe (参数: <kvstore_path> <pid> <slave_ip> <slave_port>)
    """
    
    slave_proc = None
    mirror_proc = None
    slave_port = 9999
    mirror_type = "mirror"  # 默认使用 TC mirror
    mirror_ifname = "lo"     # 默认网卡

    def tearDown(self):
        """清理所有进程"""
        # 停止 BPF mirror 工具 (使用 sudo kill)
        if self.mirror_proc:
            subprocess.run(["sudo", "kill", str(self.mirror_proc.pid)], 
                          stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
            try:
                self.mirror_proc.wait(timeout=2)
            except:
                pass
            self.mirror_proc = None
        
        # 停止 Slave 服务
        if self.slave_proc:
            try:
                os.kill(self.slave_proc.pid, signal.SIGTERM)
                self.slave_proc.wait(timeout=2)
            except:
                if self.slave_proc:
                    try:
                        os.kill(self.slave_proc.pid, signal.SIGKILL)
                    except:
                        pass
            self.slave_proc = None
            
        # 停止 Master 服务 (基类逻辑)
        self._stop_server()

    def _wait_for_port(self, port: int, timeout: int = 50):
        """辅助函数：等待指定端口就绪"""
        for _ in range(timeout):
            try:
                with socket.create_connection((self.host, port), timeout=0.1):
                    time.sleep(0.3)
                    return
            except (ConnectionRefusedError, socket.timeout):
                time.sleep(0.1)
        raise RuntimeError(f"Port {port} failed to bind within timeout")

    def _get_pid_by_port(self, port: int) -> int:
        """通过端口查找进程 PID"""
        try:
            # 使用 lsof 或 ss 查找
            result = subprocess.run(
                ["lsof", "-i", f":{port}", "-t"],
                capture_output=True, text=True
            )
            if result.returncode == 0:
                pids = result.stdout.strip().split('\n')
                for pid_str in pids:
                    if pid_str.isdigit():
                        return int(pid_str)
        except:
            pass
        
        # 备选：使用 ss 和 ps
        try:
            result = subprocess.run(
                ["ss", "-tlnp"],
                capture_output=True, text=True
            )
            for line in result.stdout.split('\n'):
                if f":{port}" in line and "kvstore" in line:
                    # 解析 pid
                    parts = line.split()
                    for part in parts:
                        if "pid=" in part:
                            pid_str = part.split("pid=")[1].rstrip(")")
                            return int(pid_str)
        except:
            pass
        
        return 0

    def _verify_state(self, phase, client, pairs, engines, expected_value_type):
        """
        将重复的验证逻辑封装成辅助函数，提高代码复用性
        :param phase: 当前测试阶段 (e.g., "after-set", "after-mod")
        :param expected_value_type: 'value_expr' (初始), 'mod_value_expr' (修改后), 'non_existent' (删除后)
        """
        for pair in pairs:
            p_name = pair['name']
            key = self._to_bytes(self._eval_expr(pair['key_expr']))
            
            for engine in engines:
                with self.subTest(phase=phase, pair=p_name, engine=engine):
                    try:
                        if expected_value_type == 'non_existent':
                            existence = client._engine_cmd(engine, 'EXIST', key)
                            self.assertEqual(existence, 0, f"Key should not exist after DEL in phase '{phase}'")
                        else:
                            expected_val = self._to_bytes(self._eval_expr(pair[expected_value_type]))
                            recovered = client._engine_cmd(engine, 'GET', key)
                            self.assertEqual(recovered, expected_val, f"Value mismatch in phase '{phase}'")
                    except redis.exceptions.ResponseError as e:
                        self.fail(f"Snapshot recovery failed (Key missing or Error) in phase '{phase}' for {p_name} on {engine}: {e}")

    def test_in_network_replication_flow(self):
        """验证镜像流程: 8888(SET) -> 9999(验证) -> 8888(MOD) -> 9999(验证) -> 8888(DEL) -> 9999(验证)"""
        
        # 1. 环境清理
        self._cleanup_files()

        # 2. 启动 Master 服务 (8888, 使用 kvstore.conf)
        self._start_server("kvstore.conf")
        master_client = self._get_client()

        # 3. 启动 Slave 服务 (9999, 使用 tests/config_slave.conf)
        config_slave = os.path.join(self.root_dir, "tests/config_slave.conf")
        executable = os.path.join(self.root_dir, "kvstore")
        
        self.slave_proc = subprocess.Popen(
            [executable, config_slave],
            cwd=self.root_dir,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        self._wait_for_port(self.slave_port)
        slave_client = KVRedis(host=self.host, port=self.slave_port, decode_responses=False)

        # 4. 启动 Mirror 工具
        if self.mirror_type == "uprobe":
            # uprobe_mirror 需要 PID
            master_pid = self._get_pid_by_port(8888)
            if not master_pid:
                self.fail("Failed to get Master PID for uprobe mirror")
            
            mirror_bin = os.path.join(self.root_dir, "mirror/src/uprobe_mirror")
            if not os.path.exists(mirror_bin):
                self.fail(f"uprobe_mirror tool not found at {mirror_bin}. Please compile it first.")
            
            mirror_cmd = ["sudo", mirror_bin, executable, str(master_pid), "127.0.0.1", str(self.slave_port)]
            mirror_name = f"uprobe_mirror (PID={master_pid})"
            
        elif self.mirror_type == "xdp":
            # xdp_mirror 参数与 mirror 相同
            mirror_bin = os.path.join(self.root_dir, "mirror/src/xdp_mirror")
            if not os.path.exists(mirror_bin):
                self.fail(f"xdp_mirror tool not found at {mirror_bin}. Please compile it first.")
            
            mirror_cmd = ["sudo", mirror_bin, self.mirror_ifname, "127.0.0.1", str(self.slave_port)]
            mirror_name = f"xdp_mirror ({self.mirror_ifname})"
            
        else:  # 默认 TC mirror
            mirror_bin = os.path.join(self.root_dir, "mirror/src/mirror")
            if not os.path.exists(mirror_bin):
                self.fail(f"Mirror tool not found at {mirror_bin}. Please compile it first.")
            
            mirror_cmd = ["sudo", mirror_bin, self.mirror_ifname, "127.0.0.1", str(self.slave_port)]
            mirror_name = f"mirror ({self.mirror_ifname})"        
        
        
        self.mirror_proc = subprocess.Popen(
            mirror_cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        time.sleep(2) # 等待 BPF 程序挂载并生效

        # 5. 开始数据驱动测试
        pairs = self.load_test_pairs()
        engines = ['A', 'H', 'R', 'S']
        ok_resps = [True, b'OK']

        # --- STEP 1: 批量 SET ---
        
        for pair in pairs:
            key = self._to_bytes(self._eval_expr(pair['key_expr']))
            val = self._to_bytes(self._eval_expr(pair['value_expr']))
            for engine in engines:
                master_client._engine_cmd(engine, 'SET', key, val)
        
        # 等待镜像同步 (包含 10MB 大 Key，需要较长等待时间)
        
        # 等待镜像同步完成，发送命令触发缓冲区刷新
        time.sleep(2)
        master_client._engine_cmd('H', 'EXIST', b'check_sync')
        time.sleep(1)
        self._verify_state("after-set", slave_client, pairs, engines, 'value_expr')
        

        # --- STEP 2: 批量 MOD ---
        
        for pair in pairs:
            key = self._to_bytes(self._eval_expr(pair['key_expr']))
            mod_val = self._to_bytes(self._eval_expr(pair['mod_value_expr']))
            for engine in engines:
                master_client._engine_cmd(engine, 'MOD', key, mod_val)
        
        # 等待镜像同步
        
        time.sleep(2)
        master_client._engine_cmd('H', 'EXIST', b'check_sync')
        time.sleep(1)
        self._verify_state("after-mod", slave_client, pairs, engines, 'mod_value_expr')
        

        # --- STEP 3: 批量 DEL ---
        
        for pair in pairs:
            key = self._to_bytes(self._eval_expr(pair['key_expr']))
            for engine in engines:
                master_client._engine_cmd(engine, 'DEL', key)
        
        # 等待镜像同步
        time.sleep(0.5)
        master_client._engine_cmd('H', 'EXIST', b'check_sync')
        time.sleep(0.5)
        self._verify_state("after-del", slave_client, pairs, engines, 'non_existent')
        
        
        
        
        

def parse_args():
    """解析命令行参数（支持 unittest 参数和自定义参数）"""
    # 先尝试从环境变量读取
    mirror_type = os.environ.get('MIRROR_TYPE', 'mirror')
    mirror_iface = os.environ.get('MIRROR_IFACE', 'lo')
    
    # 解析命令行参数（如果有）
    parser = argparse.ArgumentParser(description='Incremental Replication Test', add_help=False)
    parser.add_argument('--mirror-type', '-t', choices=['mirror', 'xdp', 'uprobe'], default=mirror_type)
    parser.add_argument('--interface', '-i', default=mirror_iface)
    args, remaining = parser.parse_known_args()
    
    return args, remaining

if __name__ == '__main__':
    # 解析参数
    args, remaining = parse_args()
    
    # 设置 mirror 类型到测试类
    TestInreRepl.mirror_type = args.mirror_type
    TestInreRepl.mirror_ifname = args.interface

    iface_info = f" (interface: {args.interface})" if args.mirror_type in ['mirror', 'xdp'] else ""
        
    # 运行 unittest（传递剩余参数）
    unittest.main(argv=[''] + remaining, verbosity=2)
