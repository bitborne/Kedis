#!/usr/bin/env python3
# tests/plot_benchmark.py
# 用法: cd tests && python plot_benchmark.py

import csv
import os
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.ticker import FuncFormatter


def load_csv(path):
    """读取CSV为 {command: {pipeline: qps}}"""
    data = {}
    with open(path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            pipeline = int(row['pipeline'])
            cmd = row['command']
            qps = float(row['qps'])
            data.setdefault(cmd, {})[pipeline] = qps
    return data


def format_si(val, pos):
    """Y轴刻度格式化: 400K / 1.2M"""
    if val >= 1e6:
        return f'{val/1e6:.1f}M'
    if val >= 1e3:
        return f'{val/1e3:.0f}K'
    return f'{val:.0f}'


def plot_grouped_bar(ax, data, title, palette):
    commands = sorted(data.keys())
    pipelines = sorted(next(iter(data.values())).keys())

    x = np.arange(len(pipelines))
    width = 0.8 / len(commands)

    for i, cmd in enumerate(commands):
        values = [data[cmd][p] for p in pipelines]
        offset = width * (i - (len(commands) - 1) / 2)
        bars = ax.bar(
            x + offset, values, width,
            label=cmd, color=palette[cmd],
            edgecolor='white', linewidth=0.5
        )

        # 柱顶标注数值
        for bar in bars:
            h = bar.get_height()
            ax.annotate(
                format_si(h, None),
                xy=(bar.get_x() + bar.get_width() / 2, h),
                xytext=(0, 2), textcoords='offset points',
                ha='center', va='bottom', fontsize=8
            )

    ax.set_xlabel('Pipeline', fontsize=11)
    ax.set_ylabel('QPS', fontsize=11)
    ax.set_title(title, fontsize=13, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(pipelines)
    ax.legend(frameon=False)
    ax.grid(axis='y', alpha=0.3, linestyle='--')
    ax.set_axisbelow(True)

    # 关键：修复Y轴刻度为专业格式
    ax.yaxis.set_major_formatter(FuncFormatter(format_si))


# ---------------- 路径处理 ----------------
script_dir = os.path.dirname(os.path.abspath(__file__))

# 优先尝试与脚本同目录下的 results_xxx/
redis_path = os.path.join(script_dir, 'results_redis', 'summary.csv')
kedis_path = os.path.join(script_dir, 'results_kedis', 'summary.csv')

# 回退：如果目录结构是 results_xxx 与 tests 同级
if not os.path.exists(redis_path):
    redis_path = os.path.join(script_dir, '..', 'results_redis', 'summary.csv')
    kedis_path = os.path.join(script_dir, '..', 'results_kedis', 'summary.csv')

redis_data = load_csv(redis_path)
kedis_data = load_csv(kedis_path)

# ---------------- 绘图 ----------------
fig, axes = plt.subplots(1, 2, figsize=(13, 5.5))

palette = {
    'HSET': '#E74C3C', 'HGET': '#3498DB', 'HDEL': '#2ECC71',
    'SET':  '#E74C3C', 'GET':  '#3498DB', 'DEL':  '#2ECC71'
}

plot_grouped_bar(axes[0], redis_data, 'Redis', palette)
plot_grouped_bar(axes[1], kedis_data, 'Kedis', palette)

plt.tight_layout()
out_path = os.path.join(script_dir, 'benchmark_comparison.png')
plt.savefig(out_path, dpi=150, bbox_inches='tight')
print(f'图表已保存: {out_path}')