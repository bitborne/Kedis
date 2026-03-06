#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
jemalloc vs kmem 内存分配器对比图表生成

使用方法:
    python3 tests/gen_mem_allocator_charts.py heap_kmem.csv heap_jemalloc.csv
    
输出:
    - allocator_comparison.png: 对比折线图
    - allocator_comparison_detail.png: 细节放大图
    - allocator_comparison_summary.png: 汇总统计图
"""

import sys
import os
import matplotlib
matplotlib.use('Agg')  # 无图形界面环境
import matplotlib.pyplot as plt
import numpy as np

# 设置中文字体支持
plt.rcParams['font.sans-serif'] = ['DejaVu Sans', 'Arial', 'Helvetica']
plt.rcParams['axes.unicode_minus'] = False


def load_csv(filepath):
    """加载 CSV 文件"""
    if not os.path.exists(filepath):
        print(f"[!] 文件不存在: {filepath}")
        sys.exit(1)
        
    times = []
    mems = []
    
    with open(filepath) as f:
        header = f.readline().strip()
        if header != 'time,mem_heap_B':
            print(f"[!] 未知的 CSV 格式: {header}")
            print("[*] 尝试解析...")
            
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                # 支持两种格式: "time=123,mem_heap_B=456" 或 "123,456"
                if '=' in line:
                    parts = line.split(',')
                    t = int(parts[0].split('=')[1])
                    m = int(parts[1].split('=')[1])
                else:
                    t, m = line.split(',')
                    t = int(t)
                    m = int(m)
                times.append(t / 1000)  # 转换为秒
                mems.append(m / 1024 / 1024)  # 转换为 MB
            except Exception as e:
                print(f"[!] 解析行失败: {line} - {e}")
                continue
                
    return times, mems


def create_comparison_chart(kmem_file, je_file, output_dir='.'):
    """创建对比图表"""
    
    print("[*] 加载数据...")
    kmem_times, kmem_mems = load_csv(kmem_file)
    je_times, je_mems = load_csv(je_file)
    
    kmem_peak = max(kmem_mems) if kmem_mems else 0
    je_peak = max(je_mems) if je_mems else 0
    
    kmem_avg = np.mean(kmem_mems) if kmem_mems else 0
    je_avg = np.mean(je_mems) if je_mems else 0
    
    print(f"[+] kmem: {len(kmem_times)} 数据点, 峰值: {kmem_peak:.2f} MB")
    print(f"[+] jemalloc: {len(je_times)} 数据点, 峰值: {je_peak:.2f} MB")
    
    # 图 1: 完整对比
    print("[*] 生成对比图...")
    fig, ax = plt.subplots(figsize=(14, 7), facecolor='white')
    ax.set_facecolor('#fafafa')
    
    ax.plot(kmem_times, kmem_mems, label='kmem (self-developed)',
            linewidth=2, color='#2ecc71', alpha=0.9)
    ax.plot(je_times, je_mems, label='jemalloc',
            linewidth=2, color='#3498db', alpha=0.9)
    
    # 标记峰值
    kmem_peak_idx = kmem_mems.index(kmem_peak)
    je_peak_idx = je_mems.index(je_peak)
    
    ax.scatter([kmem_times[kmem_peak_idx]], [kmem_peak], 
              color='#2ecc71', s=100, zorder=5, marker='o')
    ax.scatter([je_times[je_peak_idx]], [je_peak], 
              color='#3498db', s=100, zorder=5, marker='o')
    
    # 峰值标签
    ax.annotate(f'Peak: {kmem_peak:.1f} MB',
               xy=(kmem_times[kmem_peak_idx], kmem_peak),
               xytext=(10, 20), textcoords='offset points',
               fontsize=11, color='#2ecc71', fontweight='bold',
               arrowprops=dict(arrowstyle='->', color='#2ecc71', alpha=0.7))
    
    ax.annotate(f'Peak: {je_peak:.1f} MB',
               xy=(je_times[je_peak_idx], je_peak),
               xytext=(10, -30), textcoords='offset points',
               fontsize=11, color='#3498db', fontweight='bold',
               arrowprops=dict(arrowstyle='->', color='#3498db', alpha=0.7))
    
    ax.set_xlabel('Time (seconds)', fontsize=12, fontweight='bold')
    ax.set_ylabel('Heap Memory (MB)', fontsize=12, fontweight='bold')
    ax.set_title('Memory Allocator Comparison: kmem vs jemalloc\n'
                '(1M keys: SET → GET → DEL)', 
                fontsize=14, fontweight='bold')
    ax.legend(loc='upper right', fontsize=11, framealpha=0.9)
    ax.grid(True, alpha=0.3, linestyle='--')
    ax.set_axisbelow(True)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    
    plt.tight_layout()
    output_path = os.path.join(output_dir, 'allocator_comparison.png')
    plt.savefig(output_path, dpi=150, bbox_inches='tight', facecolor='white')
    plt.close()
    print(f"[+] 已保存: {output_path}")
    
    # 图 2: 细节放大图（显示 SET 阶段）
    print("[*] 生成细节图...")
    fig, ax = plt.subplots(figsize=(14, 7), facecolor='white')
    ax.set_facecolor('#fafafa')
    
    # 只显示前 50% 的时间（通常是 SET 和 GET 阶段）
    kmem_mid = len(kmem_times) // 2
    je_mid = len(je_times) // 2
    
    ax.plot(kmem_times[:kmem_mid], kmem_mems[:kmem_mid], 
           label='kmem', linewidth=2, color='#2ecc71', alpha=0.9)
    ax.plot(je_times[:je_mid], je_mems[:je_mid], 
           label='jemalloc', linewidth=2, color='#3498db', alpha=0.9)
    
    ax.set_xlabel('Time (seconds)', fontsize=12, fontweight='bold')
    ax.set_ylabel('Heap Memory (MB)', fontsize=12, fontweight='bold')
    ax.set_title('Memory Allocator Comparison - Detail View\n'
                '(SET & GET phases)', 
                fontsize=14, fontweight='bold')
    ax.legend(loc='lower right', fontsize=11)
    ax.grid(True, alpha=0.3, linestyle='--')
    ax.set_axisbelow(True)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    
    plt.tight_layout()
    output_path = os.path.join(output_dir, 'allocator_comparison_detail.png')
    plt.savefig(output_path, dpi=150, bbox_inches='tight', facecolor='white')
    plt.close()
    print(f"[+] 已保存: {output_path}")
    
    # 图 3: 汇总统计
    print("[*] 生成汇总图...")
    fig, axes = plt.subplots(1, 2, figsize=(14, 6), facecolor='white')
    
    # 左图：峰值对比柱状图
    ax1 = axes[0]
    ax1.set_facecolor('#fafafa')
    
    allocators = ['kmem\n(self-developed)', 'jemalloc']
    peaks = [kmem_peak, je_peak]
    colors = ['#2ecc71', '#3498db']
    
    bars = ax1.bar(allocators, peaks, color=colors, edgecolor='white', linewidth=2)
    
    # 添加数值标签
    for bar, peak in zip(bars, peaks):
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2., height + height*0.02,
                f'{peak:.2f} MB',
                ha='center', va='bottom', fontsize=12, fontweight='bold')
    
    # 计算节省比例
    if je_peak > 0:
        saving = ((je_peak - kmem_peak) / je_peak) * 100
        ax1.text(0.5, max(peaks) * 0.5, f'Memory Saving:\n{saving:.1f}%',
                ha='center', va='center', fontsize=14, fontweight='bold',
                bbox=dict(boxstyle='round', facecolor='#f1c40f', alpha=0.8))
    
    ax1.set_ylabel('Peak Memory (MB)', fontsize=12, fontweight='bold')
    ax1.set_title('Peak Memory Usage Comparison', fontsize=13, fontweight='bold')
    ax1.grid(axis='y', alpha=0.3, linestyle='--')
    ax1.spines['top'].set_visible(False)
    ax1.spines['right'].set_visible(False)
    
    # 右图：内存使用特征对比（替代不合理的归一化进度图）
    ax2 = axes[1]
    ax2.set_facecolor('#fafafa')
    
    # 计算内存使用特征指标
    kmem_final = kmem_mems[-1] if kmem_mems else 0
    je_final = je_mems[-1] if je_mems else 0
    
    # 内存归还率 = (峰值 - 最终) / 峰值 * 100%
    kmem_return_rate = ((kmem_peak - kmem_final) / kmem_peak * 100) if kmem_peak > 0 else 0
    je_return_rate = ((je_peak - je_final) / je_peak * 100) if je_peak > 0 else 0
    
    # 内存波动率（标准差/平均值）
    kmem_std = np.std(kmem_mems) if kmem_mems else 0
    je_std = np.std(je_mems) if je_mems else 0
    kmem_volatility = (kmem_std / kmem_avg * 100) if kmem_avg > 0 else 0
    je_volatility = (je_std / je_avg * 100) if je_avg > 0 else 0
    
    # 绘制分组柱状图对比关键指标
    categories = ['Peak\n(MB)', 'Final\n(MB)', 'Return\nRate(%)', 'Volatility\n(%)']
    kmem_values = [kmem_peak, kmem_final, kmem_return_rate, kmem_volatility]
    je_values = [je_peak, je_final, je_return_rate, je_volatility]
    
    x = np.arange(len(categories))
    width = 0.35
    
    bars1 = ax2.bar(x - width/2, kmem_values, width, label='kmem', 
                   color='#2ecc71', edgecolor='white', linewidth=1.5)
    bars2 = ax2.bar(x + width/2, je_values, width, label='jemalloc', 
                   color='#3498db', edgecolor='white', linewidth=1.5)
    
    # 添加数值标签
    for bars in [bars1, bars2]:
        for bar in bars:
            height = bar.get_height()
            if height > 0:
                ax2.text(bar.get_x() + bar.get_width()/2., height + height*0.02,
                        f'{height:.1f}',
                        ha='center', va='bottom', fontsize=9, fontweight='bold')
    
    ax2.set_ylabel('Value', fontsize=12, fontweight='bold')
    ax2.set_title('Memory Usage Characteristics', fontsize=13, fontweight='bold')
    ax2.set_xticks(x)
    ax2.set_xticklabels(categories, fontsize=10)
    ax2.legend(loc='upper right', fontsize=10)
    ax2.grid(axis='y', alpha=0.3, linestyle='--')
    ax2.set_axisbelow(True)
    ax2.spines['top'].set_visible(False)
    ax2.spines['right'].set_visible(False)
    
    plt.tight_layout()
    output_path = os.path.join(output_dir, 'allocator_comparison_summary.png')
    plt.savefig(output_path, dpi=150, bbox_inches='tight', facecolor='white')
    plt.close()
    print(f"[+] 已保存: {output_path}")
    
    # 输出统计摘要
    print("\n" + "="*60)
    print("内存分配器对比摘要")
    print("="*60)
    print(f"{'Metric':<25} {'kmem':>15} {'jemalloc':>15}")
    print("-"*60)
    print(f"{'Peak Memory':<25} {kmem_peak:>13.2f}MB {je_peak:>13.2f}MB")
    print(f"{'Average Memory':<25} {kmem_avg:>13.2f}MB {je_avg:>13.2f}MB")
    print(f"{'Data Points':<25} {len(kmem_times):>15} {len(je_times):>15}")
    if je_peak > 0:
        saving = ((je_peak - kmem_peak) / je_peak) * 100
        print(f"{'Memory Saving':<25} {saving:>14.1f}% {'':>15}")
    print("="*60)
    
    return {
        'kmem_peak': kmem_peak,
        'jemalloc_peak': je_peak,
        'kmem_avg': kmem_avg,
        'jemalloc_avg': je_avg
    }


def main():
    if len(sys.argv) < 3:
        print("用法: python3 gen_mem_allocator_charts.py <kmem_csv> <jemalloc_csv> [output_dir]")
        print("示例:")
        print("  python3 gen_mem_allocator_charts.py heap_kmem.csv heap_jemalloc.csv")
        print("  python3 gen_mem_allocator_charts.py heap_kmem.csv heap_jemalloc.csv ./charts")
        sys.exit(1)
        
    kmem_file = sys.argv[1]
    je_file = sys.argv[2]
    output_dir = sys.argv[3] if len(sys.argv) > 3 else '.'
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        
    print("="*60)
    print("jemalloc vs kmem 内存分配器对比图表生成")
    print("="*60)
    print(f"kmem 数据: {kmem_file}")
    print(f"jemalloc 数据: {je_file}")
    print(f"输出目录: {output_dir}")
    print("="*60 + "\n")
    
    stats = create_comparison_chart(kmem_file, je_file, output_dir)
    
    print("\n" + "="*60)
    print("图表生成完成!")
    print("="*60)
    print(f"输出文件:")
    print(f"  {output_dir}/allocator_comparison.png         - 完整对比图")
    print(f"  {output_dir}/allocator_comparison_detail.png  - 细节放大图")
    print(f"  {output_dir}/allocator_comparison_summary.png - 汇总统计图")
    print("="*60)


if __name__ == '__main__':
    main()
