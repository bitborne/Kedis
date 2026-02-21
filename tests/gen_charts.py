#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SSET持久化配置对比图表生成脚本 - 单图版本
根据实际memtier_benchmark JSON结构解析数据
每个指标生成单独的图表，字体清晰不重叠

使用方法:
    python3 generate_charts_separate.py <结果目录>
    
    示例:
    python3 generate_charts_separate.py ./pers_sset_benchmark_results
"""

import json
import os
import sys
from datetime import datetime

try:
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import numpy as np
    plt.rcParams['font.sans-serif'] = ['DejaVu Sans', 'Arial', 'Helvetica', 'SimHei']
    plt.rcParams['axes.unicode_minus'] = False
    plt.rcParams['font.family'] = 'sans-serif'
except ImportError as e:
    print(f"错误: 缺少必要的Python库 - {e}")
    print("请安装: pip3 install matplotlib numpy")
    sys.exit(1)

# 标准配置顺序
STANDARD_CONFIGS = [
    "Baseline_OFF_OFF",
    "AOF_ON_RDB_OFF", 
    "AOF_OFF_RDB_ON",
    "AOF_ON_RDB_ON"
]

CONFIG_LABELS = {
    "Baseline_OFF_OFF": "Baseline\n(AOF=OFF, RDB=OFF)",
    "AOF_ON_RDB_OFF": "AOF Only\n(AOF=ON, RDB=OFF)",
    "AOF_OFF_RDB_ON": "RDB Only\n(AOF=OFF, RDB=ON)",
    "AOF_ON_RDB_ON": "AOF + RDB\n(AOF=ON, RDB=ON)"
}

CONFIG_SHORT = {
    "Baseline_OFF_OFF": "Baseline",
    "AOF_ON_RDB_OFF": "AOF Only",
    "AOF_OFF_RDB_ON": "RDB Only", 
    "AOF_ON_RDB_ON": "AOF + RDB"
}

# 配色
colors = ['#2ecc71', '#3498db', '#f39c12', '#e74c3c']  # 绿、蓝、橙、红

def parse_json(filepath):
    """解析memtier_benchmark JSON文件"""
    try:
        with open(filepath, 'r') as f:
            data = json.load(f)
        
        all_stats = data.get('ALL STATS', {})
        ssets = all_stats.get('Ssets', {})
        
        if not ssets:
            print(f"  警告: 未找到 Ssets 数据")
            return None
        
        # 从 Percentile Latencies 获取百分位数据
        percentiles = ssets.get('Percentile Latencies', {})
        
        result = {
            'ops_sec': float(ssets.get('Ops/sec', 0)),
            'latency_avg': float(ssets.get('Average Latency', ssets.get('Latency', 0))),
            'p50': float(percentiles.get('p50.00', 0)),
            'p90': float(percentiles.get('p90.00', 0)),
            'p95': float(percentiles.get('p95.00', 0)),
            'p99': float(percentiles.get('p99.00', 0)),
            'p999': float(percentiles.get('p99.90', 0)),
            'count': int(ssets.get('Count', 0)),
        }
        
        # 验证数据
        if result['p50'] == 0:
            print(f"  警告: P50为0，可能未正确解析百分位数据")
            print(f"  可用键: {list(percentiles.keys())}")
        
        return result
        
    except Exception as e:
        print(f"  错误: 解析失败 - {e}")
        return None

def format_number(num):
    if num >= 1000000:
        return f"{num/1000000:.2f}M"
    elif num >= 1000:
        return f"{num/1000:.1f}K"
    return f"{num:.0f}"

def create_single_chart(output_dir, configs, data_key, title, ylabel, filename, unit=""):
    """创建单个指标的图表"""
    
    values = [configs[c][data_key] for c in configs.keys()]
    labels = [CONFIG_LABELS[c] for c in configs.keys()]
    bar_colors = [colors[STANDARD_CONFIGS.index(c)] for c in configs.keys()]
    
    fig, ax = plt.subplots(figsize=(10, 6), facecolor='white')
    ax.set_facecolor('#fafafa')
    
    bars = ax.bar(range(len(values)), values, color=bar_colors, 
                   edgecolor='white', linewidth=2, width=0.6)
    
    # 设置X轴标签
    ax.set_xticks(range(len(labels)))
    ax.set_xticklabels(labels, fontsize=11, rotation=0, ha='center')
    
    # 设置标题和标签
    ax.set_title(title, fontsize=16, fontweight='bold', pad=20)
    ax.set_ylabel(ylabel, fontsize=13, fontweight='bold')
    
    # 添加网格
    ax.grid(axis='y', alpha=0.3, linestyle='--', color='gray')
    ax.set_axisbelow(True)
    
    # 隐藏上右边框
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    
    # 在柱子上添加数值标签
    for i, (bar, val) in enumerate(zip(bars, values)):
        height = bar.get_height()
        if unit == "":
            text = format_number(val)
        else:
            text = f"{val:.2f}{unit}"
        
        ax.text(bar.get_x() + bar.get_width()/2., height + height*0.02,
                text, ha='center', va='bottom', fontsize=12, fontweight='bold',
                color=colors[STANDARD_CONFIGS.index(list(configs.keys())[i])])
    
    # 调整Y轴范围，留出标签空间
    ax.set_ylim(0, max(values) * 1.15)
    
    plt.tight_layout()
    
    # 保存
    filepath = os.path.join(output_dir, filename)
    plt.savefig(filepath, dpi=150, bbox_inches='tight', facecolor='white')
    plt.close()
    
    print(f"  ✓ {filepath}")
    return filepath

def create_latency_percentiles_chart(output_dir, configs):
    """创建延迟百分位数分组柱状图"""
    
    fig, ax = plt.subplots(figsize=(12, 7), facecolor='white')
    ax.set_facecolor('#fafafa')
    
    x = np.arange(len(configs))
    width = 0.15
    
    p50 = [configs[c]['p50'] for c in configs.keys()]
    p90 = [configs[c]['p90'] for c in configs.keys()]
    p95 = [configs[c]['p95'] for c in configs.keys()]
    p99 = [configs[c]['p99'] for c in configs.keys()]
    p999 = [configs[c]['p999'] for c in configs.keys()]
    
    # 使用不同颜色
    colors_pct = ['#1abc9c', '#27ae60', '#f39c12', '#e74c3c', '#9b59b6']
    
    bars1 = ax.bar(x - 2*width, p50, width, label='P50', color=colors_pct[0], edgecolor='white')
    bars2 = ax.bar(x - width, p90, width, label='P90', color=colors_pct[1], edgecolor='white')
    bars3 = ax.bar(x, p95, width, label='P95', color=colors_pct[2], edgecolor='white')
    bars4 = ax.bar(x + width, p99, width, label='P99', color=colors_pct[3], edgecolor='white')
    bars5 = ax.bar(x + 2*width, p999, width, label='P99.9', color=colors_pct[4], edgecolor='white')
    
    # 添加数值标签（新增代码）
    def add_labels(bars, values):
        for bar, val in zip(bars, values):
            height = bar.get_height()
            if val > 0:
                ax.text(bar.get_x() + bar.get_width()/2., height + height*0.02,
                       f'{val:.1f}', ha='center', va='bottom', fontsize=8)

    add_labels(bars1, p50)
    add_labels(bars2, p90)
    add_labels(bars3, p95)
    add_labels(bars4, p99)
    add_labels(bars5, p999)
    
    # 调整Y轴范围，给标签留出空间
    ax.set_ylim(0, max(max(p999), max(p99)) * 1.15)
    # 添加数值标签（新增代码）end

    # 设置标签
    labels = [CONFIG_SHORT[c] for c in configs.keys()]
    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=12)
    ax.set_ylabel('Latency (ms)', fontsize=13, fontweight='bold')
    ax.set_title('Latency Percentiles Comparison', fontsize=16, fontweight='bold', pad=20)
    
    # 图例
    ax.legend(loc='upper left', fontsize=11, framealpha=0.9)
    
    # 网格
    ax.grid(axis='y', alpha=0.3, linestyle='--')
    ax.set_axisbelow(True)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    
    # 确保Y轴从0开始
    ax.set_ylim(bottom=0)
    
    plt.tight_layout()
    
    filepath = os.path.join(output_dir, '04_latency_percentiles.png')
    plt.savefig(filepath, dpi=150, bbox_inches='tight', facecolor='white')
    plt.close()
    
    print(f"  ✓ {filepath}")
    return filepath

def create_overhead_chart(output_dir, configs):
    """创建性能损耗图"""
    
    if 'Baseline_OFF_OFF' not in configs:
        print("  ! 跳过性能损耗图（无Baseline数据）")
        return None
    
    baseline_qps = configs['Baseline_OFF_OFF']['ops_sec']
    
    # 只包含有损耗的配置
    overhead_configs = {k: v for k, v in configs.items() if k != 'Baseline_OFF_OFF'}
    
    if not overhead_configs:
        return None
    
    overhead_values = []
    for c in overhead_configs.keys():
        qps = overhead_configs[c]['ops_sec']
        loss = ((baseline_qps - qps) / baseline_qps) * 100
        overhead_values.append(loss)
    
    labels = [CONFIG_SHORT[c] for c in overhead_configs.keys()]
    bar_colors = [colors[STANDARD_CONFIGS.index(c)] for c in overhead_configs.keys()]
    
    fig, ax = plt.subplots(figsize=(10, 6), facecolor='white')
    ax.set_facecolor('#fafafa')
    
    bars = ax.barh(range(len(overhead_values)), overhead_values, 
                   color=bar_colors, edgecolor='white', linewidth=2, height=0.5)
    
    ax.set_yticks(range(len(labels)))
    ax.set_yticklabels(labels, fontsize=12)
    ax.set_xlabel('Performance Loss (%)', fontsize=13, fontweight='bold')
    ax.set_title('Performance Overhead vs Baseline', fontsize=16, fontweight='bold', pad=20)
    
    ax.grid(axis='x', alpha=0.3, linestyle='--')
    ax.set_axisbelow(True)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    
    # 添加数值标签
    for i, (bar, val) in enumerate(zip(bars, overhead_values)):
        width = bar.get_width()
        ax.text(width + 1, bar.get_y() + bar.get_height()/2.,
                f'{val:.1f}%', ha='left', va='center', fontsize=12, fontweight='bold')
    
    # 调整X轴范围
    ax.set_xlim(0, max(overhead_values) * 1.2)
    
    plt.tight_layout()
    
    filepath = os.path.join(output_dir, '05_overhead.png')
    plt.savefig(filepath, dpi=150, bbox_inches='tight', facecolor='white')
    plt.close()
    
    print(f"  ✓ {filepath}")
    return filepath

def create_summary_table(output_dir, configs):
    """创建汇总表格图"""
    
    fig, ax = plt.subplots(figsize=(14, 6), facecolor='white')
    ax.axis('off')
    ax.set_facecolor('white')
    
    # 准备数据
    headers = ['Configuration', 'QPS (ops/sec)', 'Avg Latency', 'P50 (ms)', 'P90 (ms)', 
               'P95 (ms)', 'P99 (ms)', 'P99.9 (ms)']
    
    table_data = []
    for cfg_name in configs.keys():
        c = configs[cfg_name]
        table_data.append([
            CONFIG_SHORT[cfg_name],
            f"{c['ops_sec']:,.0f}",
            f"{c['latency_avg']:.2f} ms",
            f"{c['p50']:.2f}",
            f"{c['p90']:.2f}",
            f"{c['p95']:.2f}",
            f"{c['p99']:.2f}",
            f"{c['p999']:.2f}"
        ])
    
    # 创建表格
    table = ax.table(
        cellText=table_data,
        colLabels=headers,
        cellLoc='center',
        loc='center',
        colWidths=[0.15, 0.15, 0.14, 0.1, 0.1, 0.1, 0.1, 0.12]
    )
    
    table.auto_set_font_size(False)
    table.set_fontsize(11)
    table.scale(1, 2.2)
    
    # 表头样式
    for i in range(len(headers)):
        cell = table[(0, i)]
        cell.set_facecolor('#2c3e50')
        cell.set_text_props(weight='bold', color='white', fontsize=12)
    
    # 数据行样式
    for i in range(1, len(table_data) + 1):
        for j in range(len(headers)):
            cell = table[(i, j)]
            if i % 2 == 0:
                cell.set_facecolor('#ecf0f1')
            else:
                cell.set_facecolor('white')
            
            # 高亮QPS列
            if j == 1:
                cell.set_text_props(weight='bold', color='#2980b9')
    
    ax.set_title('Performance Summary Table', fontsize=16, fontweight='bold', pad=20)
    
    plt.tight_layout()
    
    filepath = os.path.join(output_dir, '06_summary_table.png')
    plt.savefig(filepath, dpi=150, bbox_inches='tight', facecolor='white')
    plt.close()
    
    print(f"  ✓ {filepath}")
    return filepath

def main():
    if len(sys.argv) < 2:
        print("用法: python3 generate_charts_separate.py <结果目录>")
        print("示例: python3 generate_charts_separate.py ./pers_sset_benchmark_results")
        sys.exit(1)
    
    result_dir = sys.argv[1]
    
    if not os.path.isdir(result_dir):
        print(f"错误: 目录不存在: {result_dir}")
        sys.exit(1)
    
    # 创建输出目录
    charts_dir = os.path.join(result_dir, 'charts')
    os.makedirs(charts_dir, exist_ok=True)
    
    print("=" * 60)
    print("SSET持久化配置对比图表生成")
    print("=" * 60)
    print(f"\n扫描目录: {result_dir}\n")
    
    # 扫描结果
    configs = {}
    for cfg_name in STANDARD_CONFIGS:
        json_file = os.path.join(result_dir, f"{cfg_name}_result.json")
        if os.path.exists(json_file):
            print(f"✓ 找到: {cfg_name}")
            data = parse_json(json_file)
            if data:
                configs[cfg_name] = data
                print(f"  QPS: {data['ops_sec']:.0f}, P50: {data['p50']:.2f}ms, P99: {data['p99']:.2f}ms")
        else:
            print(f"✗ 未找到: {cfg_name}_result.json")
    
    if not configs:
        print("\n错误: 未找到任何有效结果文件")
        sys.exit(1)
    
    print(f"\n共找到 {len(configs)} 个配置，开始生成图表...\n")
    print("生成图表:")
    
    # 生成各个图表
    create_single_chart(charts_dir, configs, 'ops_sec', 
                       'Throughput (QPS)', 'Operations Per Second', 
                       '01_throughput_qps.png')
    
    create_single_chart(charts_dir, configs, 'latency_avg',
                       'Average Latency', 'Latency (ms)',
                       '02_average_latency.png', ' ms')
    
    create_single_chart(charts_dir, configs, 'p99',
                       'P99 Latency (Tail Latency)', 'Latency (ms)',
                       '03_p99_latency.png', ' ms')
    
    create_latency_percentiles_chart(charts_dir, configs)
    
    create_overhead_chart(charts_dir, configs)
    
    create_summary_table(charts_dir, configs)
    
    print(f"\n" + "=" * 60)
    print("图表生成完成!")
    print("=" * 60)
    print(f"\n输出目录: {charts_dir}/")
    print("  01_throughput_qps.png      - QPS对比")
    print("  02_average_latency.png     - 平均延迟对比")
    print("  03_p99_latency.png         - P99延迟对比")
    print("  04_latency_percentiles.png - 延迟百分位数分组")
    print("  05_overhead.png            - 性能损耗分析")
    print("  06_summary_table.png       - 数据汇总表格")
    print()

if __name__ == '__main__':
    main()