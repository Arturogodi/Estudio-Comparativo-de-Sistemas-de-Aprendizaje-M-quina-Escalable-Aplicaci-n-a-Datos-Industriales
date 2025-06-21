import os
import pandas as pd
from templates_graph import (
    load_and_clean_metrics,
    plot_cpu_memory_time,
    plot_boxplots,
    plot_bar_means,
    save_stats,
    plot_comparison_by_worker_grouped
)

# Directorio base de salida
base_dir = "data/images/07_AMZN_SPARK"
os.makedirs(base_dir, exist_ok=True)

# Crear subcarpeta para read_filter_write spark
rfw_dir = os.path.join(base_dir, "read_filter_write_spark")
os.makedirs(rfw_dir, exist_ok=True)

# Directorio de salida para los gráficos
out_dir = rfw_dir

# Generar lista de archivos a procesar
archivos = []
metrics_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "metrics"))

# Para cada run (1-5)
for run in range(1, 6):
    # Construir el nombre del archivo
    filename = f"Read_Filter__amzn23_model_data.parquet___run{run}.csv"
    # Crear la ruta completa
    path = os.path.join(metrics_dir, filename)
    # Crear la etiqueta usando el primer run
    run_label = f"run{run}_ReadFilter_Spark"
    # Añadir a la lista si existe el archivo
    if os.path.exists(path):
        archivos.append((path, run_label))
    else:
        print(f"Advertencia: Archivo no encontrado: {filename}")

# Ordenar archivos por nombre para consistencia
archivos.sort(key=lambda x: x[1])

# Lista para almacenar todos los DataFrames
all_dfs = []

# Procesamiento de cada archivo
for path, run_label in archivos:
    if not os.path.isfile(path):
        print(f"Archivo no encontrado: {path}")
        continue
    try:
        df = load_and_clean_metrics(path)
        # Añadir columna 'run'
        df['run'] = run_label.split('_')[0]
        
        # Verificar si las columnas necesarias existen
        required_columns = ['name', 'timestamp', 'cpu_perc', 'mem_usage']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            print(f"Advertencia: Columnas faltantes en {run_label}: {', '.join(missing_columns)}")
            continue
            
        # Verificar si hay valores None en columnas importantes
        if df['timestamp'].isnull().any():
            print(f"Advertencia: Valores None en timestamp en {run_label}")
            continue
            
        if df['cpu_perc'].isnull().any():
            print(f"Advertencia: Valores None en cpu_perc en {run_label}")
            continue
            
        if df['mem_usage'].isnull().any():
            print(f"Advertencia: Valores None en mem_usage en {run_label}")
            continue
            
        # Generar gráficas
        plot_cpu_memory_time(df, out_dir, run_label)
        plot_boxplots(df, out_dir, run_label)
        plot_bar_means(df, out_dir, run_label)
        save_stats(df, out_dir, run_label)
        
        # Añadir a la lista de DataFrames para la comparación final
        all_dfs.append(df)
        print(f"Procesado exitosamente: {run_label}")
        
    except Exception as e:
        print(f"ERROR procesando {run_label}: {str(e)}")
        continue

# Crear gráfica comparativa agrupada
if all_dfs:
    df_concat = pd.concat(all_dfs, ignore_index=True)
    plot_comparison_by_worker_grouped(df_concat, base_dir, "07_AMZN_SPARK")
