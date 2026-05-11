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
csv_path = os.path.join(script_dir, 'results_mirror/summary.csv')

# 按 mode 分组，收集 HSET / HGET / HDEL 数据
from collections import defaultdict
data = defaultdict(dict)
with open(csv_path, 'r') as f:
    for row in csv.DictReader(f):
        data[row['mode']][row['operation']] = float(row['qps'])

modes = list(data.keys())
operations = ['HSET', 'HGET', 'HDEL']
x = range(len(modes))
width = 0.25

fig, ax = plt.subplots(figsize=(11, 5))

colors = ['#2ECC71', '#3498DB', '#E74C3C']
for i, op in enumerate(operations):
    qps_vals = [data[mode].get(op, 0) for mode in modes]
    bars = ax.bar(
        [j + i * width for j in x],
        qps_vals,
        width,
        label=op,
        color=colors[i],
        edgecolor='white',
        linewidth=0.8
    )
    for bar in bars:
        h = bar.get_height()
        ax.annotate(
            fmt_si(h, None),
            xy=(bar.get_x() + bar.get_width() / 2, h),
            xytext=(0, 4),
            textcoords='offset points',
            ha='center',
            va='bottom',
            fontsize=8,
            fontweight='bold'
        )

ax.set_ylabel('Throughput', fontsize=12)
ax.set_title('QPS Impact by Mirror Mode', fontsize=14, fontweight='bold', pad=18)
ax.set_xticks([j + width for j in x])
ax.set_xticklabels(modes, fontsize=11)
ax.yaxis.set_major_formatter(FuncFormatter(fmt_si))
ax.legend(fontsize=10, frameon=True, fancybox=True, shadow=False)
ax.grid(axis='y', alpha=0.3, linestyle='--')
ax.set_axisbelow(True)

all_vals = [v for mode_data in data.values() for v in mode_data.values()]
ax.set_ylim(0, max(all_vals) * 1.18)

# 去掉顶部和右侧边框
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)

plt.tight_layout()
out = os.path.join(script_dir, 'mirror_qps.png')
plt.savefig(out, dpi=150, bbox_inches='tight')
print(f'已保存: {out}')
