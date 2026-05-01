#!/usr/bin/env python3

import csv
import os
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter


def fmt_si(val, pos):
    if val >= 1e6:
        return f'{val/1e6:.1f}M'
    return f'{val/1e3:.0f}K'


script_dir = os.path.dirname(os.path.abspath(__file__))
csv_path = os.path.join(script_dir, 'results_aof/summary.csv')

data = {}
with open(csv_path, 'r') as f:
    for row in csv.DictReader(f):
        sys = row['system']
        aof = row['aof']
        op = row['operation']
        qps = float(row['qps'])
        if sys not in data:
            data[sys] = {}
        if aof not in data[sys]:
            data[sys][aof] = {}
        data[sys][aof][op] = qps

fig, axes = plt.subplots(1, 2, figsize=(10, 5))

# 以 Kedis 数据为准，计算统一的 Y 轴上限
kedis_ops = ['HSET', 'HDEL']
kedis_qps_no = [data.get('kedis', {}).get('no', {}).get(op, 0) for op in kedis_ops]
kedis_qps_yes = [data.get('kedis', {}).get('yes', {}).get(op, 0) for op in kedis_ops]
ymax_kedis = max(max(kedis_qps_no or [0]), max(kedis_qps_yes or [0]))
ylim_max = ymax_kedis * 1.25

# ---------- 左图：Redis ----------
ax = axes[0]
ops = ['SET', 'DEL']
x = range(len(ops))
width = 0.35
qps_no = [data.get('redis', {}).get('no', {}).get(op, 0) for op in ops]
qps_yes = [data.get('redis', {}).get('yes', {}).get(op, 0) for op in ops]

bars1 = ax.bar(
    [i - width / 2 for i in x], qps_no, width,
    label='AOF OFF', color='#2ECC71', edgecolor='white', linewidth=0.8
)
bars2 = ax.bar(
    [i + width / 2 for i in x], qps_yes, width,
    label='AOF ON', color='#E74C3C', edgecolor='white', linewidth=0.8
)

for bars in [bars1, bars2]:
    for bar in bars:
        h = bar.get_height()
        ax.annotate(
            fmt_si(h, None),
            xy=(bar.get_x() + bar.get_width() / 2, h),
            xytext=(0, 4),
            textcoords='offset points',
            ha='center', va='bottom', fontsize=9, fontweight='bold'
        )

ax.set_ylabel('Throughput', fontsize=12)
ax.set_title('Redis', fontsize=13, fontweight='bold', pad=15)
ax.set_xticks(x)
ax.set_xticklabels(ops, fontsize=11)
ax.yaxis.set_major_formatter(FuncFormatter(fmt_si))
ax.legend(fontsize=10, frameon=True, fancybox=True)
ax.grid(axis='y', alpha=0.3, linestyle='--')
ax.set_axisbelow(True)
ax.set_ylim(0, ylim_max)
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)

# ---------- 右图：Kedis ----------
ax = axes[1]
ops = ['HSET', 'HDEL']
qps_no = kedis_qps_no
qps_yes = kedis_qps_yes

bars1 = ax.bar(
    [i - width / 2 for i in x], qps_no, width,
    label='AOF OFF', color='#2ECC71', edgecolor='white', linewidth=0.8
)
bars2 = ax.bar(
    [i + width / 2 for i in x], qps_yes, width,
    label='AOF ON', color='#E74C3C', edgecolor='white', linewidth=0.8
)

for bars in [bars1, bars2]:
    for bar in bars:
        h = bar.get_height()
        ax.annotate(
            fmt_si(h, None),
            xy=(bar.get_x() + bar.get_width() / 2, h),
            xytext=(0, 4),
            textcoords='offset points',
            ha='center', va='bottom', fontsize=9, fontweight='bold'
        )

ax.set_ylabel('Throughput', fontsize=12)
ax.set_title('Kedis', fontsize=13, fontweight='bold', pad=15)
ax.set_xticks(x)
ax.set_xticklabels(ops, fontsize=11)
ax.yaxis.set_major_formatter(FuncFormatter(fmt_si))
ax.legend(fontsize=10, frameon=True, fancybox=True)
ax.grid(axis='y', alpha=0.3, linestyle='--')
ax.set_axisbelow(True)
ax.set_ylim(0, ylim_max)
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)

fig.suptitle('QPS Impact by AOF Strategy', fontsize=14, fontweight='bold', y=1.02)
plt.tight_layout()
out = os.path.join(script_dir, 'aof_qps.png')
plt.savefig(out, dpi=150, bbox_inches='tight')
print(f'已保存: {out}')
