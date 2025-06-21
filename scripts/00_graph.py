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
base_dir = "data/images/00_AMZN23"
os.makedirs(base_dir, exist_ok=True)

# Lista para almacenar todos los DataFrames
all_dfs = []

# Lista de tipos y sus prefijos
tipos = {
    "meta": "Normalize_Dataset__meta_",
    "review": "Normalize_Reviews_Dataset__reviews_"
}

# Generar lista de archivos a procesar
archivos = []
metrics_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "metrics"))

# Para cada tipo (meta y review)
for tipo, prefijo in tipos.items():
    # Para cada categoría
    for cat in [
        "Books",
        "Clothing_Shoes_and_Jewelry",
        "Electronics",
        "Home_and_Kitchen",
        "Toys_and_Games"
    ]:
        # Para cada run (1-5)
        for run in range(1, 6):
            filename = f"{prefijo}{cat}.parquet__run{run}.csv"
            path = os.path.join(metrics_dir, filename)
            run_label = f"run{run}_{tipo}_{cat}"
            if os.path.exists(path):
                archivos.append((path, run_label))
            else:
                print(f"Advertencia: Archivo no encontrado: {filename}")

# Ordenar archivos por nombre para consistencia
archivos.sort(key=lambda x: x[1])

# Procesamiento manual
for path, run_label in archivos:
    try:
        df = load_and_clean_metrics(path)
        df['run'] = run_label.split('_')[0]  # Añadir columna 'run'
        
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
            
        # Extraer la categoría y tipo del run_label
        parts = run_label.split('_')
        run = parts[0]
        tipo = parts[1]
        cat = parts[2]
        
        # Crear la ruta de salida usando el nombre completo de la categoría
        out_subdir = os.path.join(base_dir, tipo, cat)
        os.makedirs(out_subdir, exist_ok=True)
        
        # Procesar gráficos y estadísticas
        plot_cpu_memory_time(df, out_subdir, run_label)
        plot_boxplots(df, out_subdir, run_label)
        plot_bar_means(df, out_subdir, run_label)
        save_stats(df, out_subdir, run_label)
        
        # Añadir a la lista de DataFrames para la comparación final
        all_dfs.append(df)
        print(f"Procesado exitosamente: {run_label}")
        
    except Exception as e:
        print(f"ERROR procesando {run_label}: {str(e)}")
        continue

# Crear gráfica comparativa agrupada
if all_dfs:
    df_concat = pd.concat(all_dfs, ignore_index=True)
    plot_comparison_by_worker_grouped(df_concat, base_dir, "00_AMZN23")