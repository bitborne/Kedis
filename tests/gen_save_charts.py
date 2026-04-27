#!/usr/bin/env python3
# tests/plot_save_strategy.py

import csv
import os
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter


def fmt_si(val, pos):
    if val >= 1e6:
        return f'{val/1e6:.1f}M'
    return f'{val/1e3:.0f}K'


script_dir = os.path.dirname(os.path.abspath(__file__))
csv_path = os.path.join(script_dir, 'results_save/summary.csv')

strategies, qps_vals = [], []
with open(csv_path, 'r') as f:
    for row in csv.DictReader(f):
        strategies.append(row['strategy'])
        qps_vals.append(float(row['qps']))

fig, ax = plt.subplots(figsize=(7, 4.5))

colors = ['#2ECC71', '#F1C40F', '#E74C3C']
bars = ax.bar(strategies, qps_vals, color=colors, edgecolor='white', width=0.55)

for bar in bars:
    h = bar.get_height()
    ax.annotate(
        fmt_si(h, None),
        xy=(bar.get_x() + bar.get_width() / 2, h),
        xytext=(0, 5), textcoords='offset points',   # 偏移加大到5
        ha='center', va='bottom', fontsize=11, fontweight='bold'
    )

ax.set_ylabel('Throughput', fontsize=11)
ax.set_title('QPS Impact by Save Strategy', fontsize=12, fontweight='bold', pad=15)  # 标题下移一点
ax.yaxis.set_major_formatter(FuncFormatter(fmt_si))
ax.set_xticklabels(['No Save', '100K Save', '10K Save'], fontsize=10)
ax.grid(axis='y', alpha=0.3, linestyle='--')
ax.set_axisbelow(True)

# 关键：Y轴上限留足空间，避免柱顶文字被截断
ax.set_ylim(0, max(qps_vals) * 1.15)

plt.tight_layout()
out = os.path.join(script_dir, 'save_strategy.png')
plt.savefig(out, dpi=150, bbox_inches='tight')
print(f'已保存: {out}')