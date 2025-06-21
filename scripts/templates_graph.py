import os
import re
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator
from datetime import datetime
import glob
import seaborn as sns

plt.style.use('seaborn-v0_8-whitegrid')

# 🎨 Colores
COLOR_PRIMARY = '#5B9BD5'
COLOR_MEAN = '#E76F51'
GRADIENT = sns.color_palette("Blues", 5)
PALETTE = sns.color_palette("pastel")

ORDER = ['spark-master'] + [f'spark-worker-{i}' for i in range(1, 6)]


def clean_memory(mem_str):
    try:
        used, total = mem_str.split(' / ')
        def to_gb(val):
            if 'GiB' in val:
                return float(val.replace('GiB', '').strip())
            elif 'MiB' in val:
                return float(val.replace('MiB', '').strip()) / 1024
            else:
                return 0
        return pd.Series([to_gb(used), to_gb(total)])
    except:
        return pd.Series([None, None])


def load_and_clean_metrics(file_path):
    df = pd.read_csv(file_path)
    df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y%m%d_%H%M%S')
    df['timestamp'] = (df['timestamp'] - df['timestamp'].min()).dt.total_seconds() / 60  # en minutos
    df['cpu_perc'] = df['cpu_perc'].str.replace('%', '').astype(float)
    df[['mem_used_GB', 'mem_total_GB']] = df['mem_usage'].apply(clean_memory)
    df['name_clean'] = df['name'].str.replace('-', '_').str.replace('spark_', '')
    return df


def plot_cpu_memory_time(df, out_dir, run_label):
    fig, ax = plt.subplots(figsize=(10, 5))
    subset_names = ['spark-master'] + [w for w in ORDER if 'worker' in w][:2]
    for name in subset_names:
        if name in df['name'].unique():
            grp = df[df['name'] == name]
            ax.plot(grp['timestamp'], grp['cpu_perc'], label=name)
    ax.set_xlabel('Tiempo (minutos)')
    ax.set_ylabel('% CPU')
    ax.set_title(f'Uso de CPU - {run_label}')
    ax.legend(loc='upper right', fontsize='small', frameon=True)
    ax.grid(axis='x')
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    plt.xticks(rotation=0)
    plt.tight_layout()
    plt.savefig(os.path.join(out_dir, f'{run_label}_cpu_time.png'), dpi=300)
    plt.close()

    fig, ax = plt.subplots(figsize=(10, 5))
    for name in subset_names:
        if name in df['name'].unique():
            grp = df[df['name'] == name]
            ax.plot(grp['timestamp'], grp['mem_used_GB'], label=name)
    ax.set_xlabel('Tiempo (minutos)')
    ax.set_ylabel('Memoria usada (GB)')
    ax.set_title(f'Uso de Memoria - {run_label}')
    ax.legend(loc='upper right', fontsize='small', frameon=True)
    ax.grid(axis='x')
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    plt.xticks(rotation=0)
    plt.tight_layout()
    plt.savefig(os.path.join(out_dir, f'{run_label}_mem_time.png'), dpi=300)
    plt.close()


def plot_boxplots(df, out_dir, run_label):
    for metric, ylabel in [('cpu_perc', '% CPU'), ('mem_used_GB', 'Memoria usada (GB)')]:
        fig, ax = plt.subplots(figsize=(6, 5))
        data = [grp[metric].dropna() for _, grp in df.groupby('name')]
        labels = df['name'].unique().tolist()
        bp = ax.boxplot(data, patch_artist=True, labels=labels)
        for i, patch in enumerate(bp['boxes']):
            patch.set_facecolor(COLOR_PRIMARY)
        for i, grp in enumerate(df.groupby('name')):
            mean_val = grp[1][metric].mean()
            ax.plot(i + 1, mean_val, marker='D', color=COLOR_MEAN)
        ax.set_title(f'Boxplot {ylabel} - {run_label}')
        ax.set_ylabel(ylabel)
        ax.grid(axis='x')
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        plt.xticks(rotation=0)
        plt.tight_layout()
        plt.savefig(os.path.join(out_dir, f'{run_label}_{metric}_boxplot.png'), dpi=600)
        plt.close()


def plot_bar_means(df, out_dir, run_label):
    for metric, ylabel in [('cpu_perc', '% CPU'), ('mem_used_GB', 'Memoria usada (GB)')]:
        means = df.groupby('name')[metric].mean().reindex(ORDER)
        fig, ax = plt.subplots(figsize=(8, 5))
        bars = ax.bar(means.index, means.values, color=COLOR_PRIMARY)
        for i, v in enumerate(means.values):
            ax.text(i, v + 0.01 * v, f"{v:.1f}", ha='center', va='bottom', fontsize=9)
        ax.set_title(f'Media de {ylabel} - {run_label}')
        ax.set_ylabel(ylabel)
        ax.set_xlabel('Contenedor')
        ax.grid(axis='x')
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        plt.xticks(rotation=0)
        plt.tight_layout()
        plt.savefig(os.path.join(out_dir, f'{run_label}_{metric}_bar.png'), dpi=600)
        plt.close()


def save_stats(df, out_dir, run_label):
    stats = df.groupby('name').agg({
        'cpu_perc': ['mean', 'std', 'min', 'max', 'median', lambda x: x.quantile(0.25), lambda x: x.quantile(0.75)],
        'mem_used_GB': ['mean', 'std', 'min', 'max', 'median', lambda x: x.quantile(0.25), lambda x: x.quantile(0.75)]
    })
    stats.columns = ['_'.join(col).replace('<lambda>', q) for col, q in zip(stats.columns, ['q1', 'q3']*2*7)]
    stats.reset_index(inplace=True)
    stats['name_clean'] = stats['name'].str.replace('-', '_').str.replace('spark_', '')
    stats.to_csv(os.path.join(out_dir, f'{run_label}_stats.csv'), index=False)


def plot_comparison_by_worker_grouped(df_all_runs, out_dir, tag):
    df_all_runs['run_num'] = df_all_runs['run'].str.extract(r'(\d+)').astype(int)

    for metric, ylabel in [('cpu_perc', '% CPU'), ('mem_used_GB', 'Memoria usada (GB)')]:
        grouped = df_all_runs.groupby(['name', 'run_num'])[metric].mean().unstack().reindex(ORDER)

        fig, ax = plt.subplots(figsize=(10, 6))
        x = range(len(grouped.index))
        total_runs = grouped.shape[1]
        width = 0.13

        for i in range(total_runs):
            ax.bar(
                [pos + (i - 2) * width for pos in x],
                grouped.iloc[:, i],
                width=width,
                label=f'Run {i + 1}',
                color=GRADIENT[i]
            )

        ax.set_xticks(x)
        ax.set_xticklabels(grouped.index, rotation=0)
        ax.set_title(f'Media de {ylabel} por contenedor (5 runs)')
        ax.set_ylabel(ylabel)
        ax.set_xlabel('Contenedor')
        ax.grid(axis='x')
        ax.legend(title="Run", frameon=True)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        plt.tight_layout()
        plt.savefig(os.path.join(out_dir, f'{tag}_{metric}_grouped_bar.png'), dpi=300)
        plt.close()
