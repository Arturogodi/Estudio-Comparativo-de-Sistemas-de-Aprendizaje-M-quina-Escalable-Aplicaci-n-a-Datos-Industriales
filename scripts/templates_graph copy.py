import os
import re
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator
from datetime import datetime
import glob

plt.style.use('default')

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
    df['cpu_perc'] = df['cpu_perc'].str.replace('%', '').astype(float)
    df[['mem_used_GB', 'mem_total_GB']] = df['mem_usage'].apply(clean_memory)
    return df

def plot_cpu_memory_time(df, out_dir, run_label):
    fig, ax = plt.subplots(figsize=(10, 5))
    for name, grp in df.groupby('name'):
        ax.plot(grp['timestamp'], grp['cpu_perc'], label=name)
    ax.set_xlabel('Tiempo')
    ax.set_ylabel('% CPU')
    ax.set_title(f'Uso de CPU - {run_label}')
    ax.legend(loc='upper right', fontsize='small')
    ax.grid(axis='y')
    plt.xticks(rotation=0)
    plt.tight_layout()
    plt.savefig(os.path.join(out_dir, f'{run_label}_cpu_time.png'))
    plt.close()

    fig, ax = plt.subplots(figsize=(10, 5))
    for name, grp in df.groupby('name'):
        ax.plot(grp['timestamp'], grp['mem_used_GB'], label=name)
    ax.set_xlabel('Tiempo')
    ax.set_ylabel('Memoria usada (GB)')
    ax.set_title(f'Uso de Memoria - {run_label}')
    ax.legend(loc='upper right', fontsize='small')
    ax.grid(axis='y')
    plt.xticks(rotation=0)
    plt.tight_layout()
    plt.savefig(os.path.join(out_dir, f'{run_label}_mem_time.png'))
    plt.close()

def plot_boxplots(df, out_dir, run_label):
    for metric, ylabel in [('cpu_perc', '% CPU'), ('mem_used_GB', 'Memoria usada (GB)')]:
        fig, ax = plt.subplots(figsize=(6, 5))
        data = [grp[metric].dropna() for _, grp in df.groupby('name')]
        labels = df['name'].unique()
        bp = ax.boxplot(data, patch_artist=True, labels=labels)
        for patch in bp['boxes']:
            patch.set_facecolor('#cce5ff')
        for i, grp in enumerate(df.groupby('name')):
            mean_val = grp[1][metric].mean()
            ax.plot(i + 1, mean_val, marker='D', color='red')
        ax.set_title(f'Boxplot {ylabel} - {run_label}')
        ax.set_ylabel(ylabel)
        ax.grid(axis='y')
        plt.xticks(rotation=0)
        plt.tight_layout()
        plt.savefig(os.path.join(out_dir, f'{run_label}_{metric}_boxplot.png'))
        plt.close()

def plot_bar_means(df, out_dir, run_label):
    for metric, ylabel in [('cpu_perc', '% CPU'), ('mem_used_GB', 'Memoria usada (GB)')]:
        means = df.groupby('name')[metric].mean()
        fig, ax = plt.subplots(figsize=(8, 5))
        ax.bar(means.index, means.values)
        ax.set_title(f'Media de {ylabel} - {run_label}')
        ax.set_ylabel(ylabel)
        ax.set_xlabel('Contenedor')
        ax.grid(axis='y')
        plt.xticks(rotation=0)
        plt.tight_layout()
        plt.savefig(os.path.join(out_dir, f'{run_label}_{metric}_bar.png'))
        plt.close()

def save_stats(df, out_dir, run_label):
    stats = df.groupby('name').agg({
        'cpu_perc': ['mean', 'std', 'min', 'max'],
        'mem_used_GB': ['mean', 'std', 'min', 'max']
    })
    stats.columns = ['_'.join(col) for col in stats.columns]
    stats.to_csv(os.path.join(out_dir, f'{run_label}_stats.csv'))

def process_run(file_path, script_tag, run_id):
    out_dir = os.path.join('data', 'images', script_tag)
    os.makedirs(out_dir, exist_ok=True)
    df = load_and_clean_metrics(file_path)
    run_label = f'run{run_id}'
    plot_cpu_memory_time(df, out_dir, run_label)
    plot_boxplots(df, out_dir, run_label)
    plot_bar_means(df, out_dir, run_label)
    save_stats(df, out_dir, run_label)
    print(f"Procesado: {file_path} -> {out_dir}")

def batch_process_all_runs(metrics_dir='data/metrics'):
    mapping = {
        'Normalize_Dataset__': '00_Normalize',
        'Normalize_Reviews_Dataset__': '00_Normalize',
        'EDA_Meta_Dataset__': '01_EDA',
        'EDA_Reviews_Dataset__': '01_EDA',
        'ETL_Join_Meta_+_Reviews__': '02_ETL',
        'Preprocesamiento_para_modelos__': '03_Model',
        'KMeans_Clustering_Amazon23__': '04_KMeans'
    }
