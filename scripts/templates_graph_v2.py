import os
import re
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime

plt.style.use('seaborn-v0_8-whitegrid')

# 🎨 Colores
COLOR_PRIMARY = '#5B9BD5'
COLOR_MEAN = '#E76F51'
PALETTE = sns.color_palette("pastel")

# Orden de contenedores para gráficas
ORDER = ['ray-head'] + [f'ray-worker-{i}' for i in range(1, 6)] + ['ray-client']


def clean_memory(mem_str):
    """Convierte la cadena de memoria a GB"""
    try:
        used, total = mem_str.split(' / ')
        def to_gb(val):
            if 'GiB' in val:
                return float(val.replace('GiB', '').strip())
            elif 'MiB' in val:
                return float(val.replace('MiB', '').strip()) / 1024
            elif 'B' in val:
                return float(val.replace('B', '').strip()) / (1024 * 1024 * 1024)
            return 0
        return pd.Series([to_gb(used), to_gb(total)])
    except:
        return pd.Series([None, None])


def load_and_clean_metrics(file_path):
    """Carga y limpia los datos del archivo CSV"""
    try:
        df = pd.read_csv(file_path)
        
        # Convertir timestamp a minutos
        df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y%m%d_%H%M%S')
        df['timestamp'] = (df['timestamp'] - df['timestamp'].min()).dt.total_seconds() / 60
        
        # Limpiar y convertir valores
        df['cpu_perc'] = df['cpu_perc'].str.replace('%', '').astype(float)
        df[['mem_used_GB', 'mem_total_GB']] = df['mem_usage'].apply(clean_memory)
        df['mem_perc'] = df['mem_perc'].str.replace('%', '').astype(float)
        
        # Limpiar nombres
        df['name_clean'] = df['name'].str.replace('-', '_')
        
        # Validar datos
        if df.empty:
            raise ValueError("El DataFrame está vacío")
            
        required_columns = ['timestamp', 'name', 'cpu_perc', 'mem_used_GB', 'mem_perc']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Columnas faltantes: {', '.join(missing_columns)}")
            
        return df
        
    except Exception as e:
        raise ValueError(f"Error al procesar el archivo {file_path}: {str(e)}")


def plot_cpu_memory_time(df, out_dir, run_label):
    """Genera gráficas de CPU y memoria en el tiempo"""
    try:
        # Validar datos
        if df.empty:
            print(f"Advertencia: No hay datos para {run_label}")
            return
            
        # Crear directorio si no existe
        os.makedirs(out_dir, exist_ok=True)
        
        # Configurar tamaño de figura
        fig, axes = plt.subplots(2, 1, figsize=(12, 8), sharex=True)
        
        # Gráfica de CPU
        ax = axes[0]
        for name in ORDER:
            if name in df['name'].unique():
                grp = df[df['name'] == name]
                if not grp.empty:
                    ax.plot(grp['timestamp'], grp['cpu_perc'], label=name)
        
        ax.set_ylabel('% CPU')
        ax.set_title(f'Uso de CPU - {run_label}')
        ax.grid(True, linestyle='--', alpha=0.7)
        ax.legend(loc='upper right', fontsize='small', framealpha=0.8)
        
        # Gráfica de Memoria
        ax = axes[1]
        for name in ORDER:
            if name in df['name'].unique():
                grp = df[df['name'] == name]
                if not grp.empty:
                    ax.plot(grp['timestamp'], grp['mem_used_GB'], label=name)
        
        ax.set_xlabel('Tiempo (minutos)')
        ax.set_ylabel('Memoria usada (GB)')
        ax.set_title(f'Uso de Memoria - {run_label}')
        ax.grid(True, linestyle='--', alpha=0.7)
        ax.legend(loc='upper right', fontsize='small', framealpha=0.8)
        
        plt.tight_layout()
        plt.savefig(os.path.join(out_dir, f'{run_label}_cpu_mem_time.png'), dpi=300)
        plt.close()
        
    except Exception as e:
        print(f"Error al generar gráficas de CPU/Memoria para {run_label}: {str(e)}")


def plot_boxplots(df, out_dir, run_label):
    """Genera boxplots de CPU y memoria"""
    try:
        if df.empty:
            print(f"Advertencia: No hay datos para {run_label}")
            return
            
        os.makedirs(out_dir, exist_ok=True)
        
        for metric, ylabel in [('cpu_perc', '% CPU'), ('mem_used_GB', 'Memoria usada (GB)')]:
            fig, ax = plt.subplots(figsize=(10, 6))
            
            # Preparar datos
            data = []
            labels = []
            for name in ORDER:
                if name in df['name'].unique():
                    grp = df[df['name'] == name][metric].dropna()
                    if not grp.empty:
                        data.append(grp)
                        labels.append(name)
            
            if not data:
                print(f"Advertencia: No hay datos válidos para {metric} en {run_label}")
                continue
                
            # Crear boxplot
            bp = ax.boxplot(data, patch_artist=True, labels=labels)
            
            # Estilizar
            for patch in bp['boxes']:
                patch.set_facecolor(COLOR_PRIMARY)
            
            # Agregar medias
            for i, d in enumerate(data):
                mean_val = d.mean()
                ax.plot(i + 1, mean_val, marker='D', color=COLOR_MEAN)
            
            ax.set_title(f'Boxplot {ylabel} - {run_label}')
            ax.set_ylabel(ylabel)
            ax.grid(True, linestyle='--', alpha=0.7)
            plt.xticks(rotation=45)
            plt.tight_layout()
            
            plt.savefig(os.path.join(out_dir, f'{run_label}_{metric}_boxplot.png'), dpi=300)
            plt.close()
            
    except Exception as e:
        print(f"Error al generar boxplots para {run_label}: {str(e)}")


def plot_bar_means(df, out_dir, run_label):
    """Genera gráficas de barras con medias"""
    try:
        if df.empty:
            print(f"Advertencia: No hay datos para {run_label}")
            return
            
        os.makedirs(out_dir, exist_ok=True)
        
        for metric, ylabel in [('cpu_perc', '% CPU'), ('mem_used_GB', 'Memoria usada (GB)')]:
            means = df.groupby('name')[metric].mean().reindex(ORDER)
            
            if means.isnull().all():
                print(f"Advertencia: No hay datos válidos para {metric} en {run_label}")
                continue
                
            fig, ax = plt.subplots(figsize=(10, 6))
            bars = ax.bar(means.index, means.values, color=COLOR_PRIMARY)
            
            # Agregar etiquetas
            for i, v in enumerate(means.values):
                if pd.notna(v) and not np.isinf(v):
                    ax.text(i, v + 0.02 * v, f"{v:.1f}", 
                          ha='center', va='bottom', fontsize=9, color='black')
            
            ax.set_title(f'Media de {ylabel} - {run_label}')
            ax.set_ylabel(ylabel)
            ax.set_xlabel('Contenedor')
            ax.grid(True, axis='y', linestyle='--', alpha=0.7)
            plt.xticks(rotation=45)
            plt.tight_layout()
            
            plt.savefig(os.path.join(out_dir, f'{run_label}_{metric}_bar.png'), dpi=300)
            plt.close()
            
    except Exception as e:
        print(f"Error al generar gráficas de barras para {run_label}: {str(e)}")


def save_stats(df, out_dir, run_label):
    """Guarda estadísticas en CSV"""
    try:
        if df.empty:
            print(f"Advertencia: No hay datos para {run_label}")
            return
            
        os.makedirs(out_dir, exist_ok=True)
        
        # Calcular estadísticas
        stats = df.groupby('name').agg({
            'cpu_perc': ['mean', 'std', 'min', 'max', 'median', lambda x: x.quantile(0.25), lambda x: x.quantile(0.75)],
            'mem_used_GB': ['mean', 'std', 'min', 'max', 'median', lambda x: x.quantile(0.25), lambda x: x.quantile(0.75)]
        })
        
        # Renombrar columnas
        stats.columns = ['_'.join(col).replace('<lambda>', q) for col, q in zip(stats.columns, ['q1', 'q3']*2*7)]
        
        # Limpiar y guardar
        stats.reset_index(inplace=True)
        stats['name_clean'] = stats['name'].str.replace('-', '_')
        stats.to_csv(os.path.join(out_dir, f'{run_label}_stats.csv'), index=False)
        
    except Exception as e:
        print(f"Error al guardar estadísticas para {run_label}: {str(e)}")


def plot_comparison_by_worker_grouped(df_all_runs, out_dir, tag):
    """Genera gráficas comparativas agrupadas por worker"""
    try:
        if df_all_runs.empty:
            print(f"Advertencia: No hay datos para la comparación")
            return
            
        df_all_runs['run_num'] = df_all_runs['run'].str.extract(r'(\d+)').astype(int)

        for metric, ylabel in [('cpu_perc', '% CPU'), ('mem_used_GB', 'Memoria usada (GB)')]:
            # Agrupar y calcular medias
            grouped = df_all_runs.groupby(['name', 'run_num'])[metric].mean().unstack().reindex(ORDER)
            
            if grouped.empty:
                print(f"Advertencia: No hay datos válidos para {metric}")
                continue
                
            # Crear figura
            fig, ax = plt.subplots(figsize=(12, 8))
            
            # Configurar ancho de barras
            x = range(len(grouped.index))
            total_runs = grouped.shape[1]
            width = 0.8 / total_runs
            
            # Dibujar barras
            for i in range(total_runs):
                ax.bar(
                    [pos + (i - total_runs/2 + 0.5) * width for pos in x],
                    grouped.iloc[:, i],
                    width=width,
                    label=f'Run {i+1}',
                    alpha=0.8
                )
            
            # Configurar gráfica
            ax.set_title(f'Comparación de {ylabel} - {tag}')
            ax.set_ylabel(ylabel)
            ax.set_xlabel('Contenedor')
            ax.grid(True, axis='y', linestyle='--', alpha=0.7)
            ax.legend(loc='upper right', fontsize='small')
            plt.xticks(x, grouped.index, rotation=45)
            plt.tight_layout()
            
            # Guardar
            plt.savefig(os.path.join(out_dir, f'{tag}_{metric}_comparison.png'), dpi=300)
            plt.close()
            
    except Exception as e:
        print(f"Error al generar gráfica de comparación: {str(e)}")
