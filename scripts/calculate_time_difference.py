import pandas as pd
import datetime
import numpy as np

# Leer el archivo CSV
file_path = 'data\metrics\ETL_Join_Meta_+_Reviews____run1.csv'
df = pd.read_csv(file_path)

# Convertir la columna timestamp a datetime
df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y%m%d_%H%M%S')

# Ordenar por timestamp
df = df.sort_values('timestamp')

# Calcular las diferencias entre timestamps consecutivos
df['time_diff'] = df['timestamp'].diff()

# Convertir diferencias a segundos
df['time_diff_seconds'] = df['time_diff'].dt.total_seconds()

# Identificar todos los saltos temporales
large_jumps = df[df['time_diff_seconds'].notna()].copy()

# Mostrar todos los saltos temporales con su duración
print("\nTodos los saltos temporales:")
print(large_jumps[['timestamp', 'time_diff_seconds']])

# Identificar los runs basándonos en los saltos
runs = []
start_idx = 0
for idx, row in large_jumps.iterrows():
    end_idx = df.index.get_loc(idx)
    runs.append({
        'start': df.iloc[start_idx]['timestamp'],
        'end': df.iloc[end_idx-1]['timestamp'],
        'duration': df.iloc[end_idx-1]['timestamp'] - df.iloc[start_idx]['timestamp']
    })
    start_idx = end_idx

# Agregar el último run
runs.append({
    'start': df.iloc[start_idx]['timestamp'],
    'end': df.iloc[-1]['timestamp'],
    'duration': df.iloc[-1]['timestamp'] - df.iloc[start_idx]['timestamp']
})

# Mostrar el resumen completo de todos los runs
print("\nResumen completo de todos los runs:")
for i, run in enumerate(runs):
    print(f"\nRun {i+1}:")
    print(f"Inicio: {run['start']}")
    print(f"Fin: {run['end']}")
    print(f"Duración: {run['duration']}")
    print(f"Duración (segundos): {run['duration'].total_seconds():.2f}")
    print(f"Duración (minutos): {run['duration'].total_seconds()/60:.2f}")

# Identificar los runs basándonos en los saltos
runs = []
start_idx = 0
for idx, row in large_jumps.iterrows():
    end_idx = df.index.get_loc(idx)
    runs.append({
        'start': df.iloc[start_idx]['timestamp'],
        'end': df.iloc[end_idx-1]['timestamp'],
        'duration': df.iloc[end_idx-1]['timestamp'] - df.iloc[start_idx]['timestamp']
    })
    start_idx = end_idx

# Agregar el último run
runs.append({
    'start': df.iloc[start_idx]['timestamp'],
    'end': df.iloc[-1]['timestamp'],
    'duration': df.iloc[-1]['timestamp'] - df.iloc[start_idx]['timestamp']
})

print("\nDetalles de los runs encontrados:")
for i, run in enumerate(runs):
    print(f"\nRun {i+1}:")
    print(f"Inicio: {run['start']}")
    print(f"Fin: {run['end']}")
    print(f"Duración: {run['duration']}")
    print(f"Duración (segundos): {run['duration'].total_seconds():.2f}")
    print(f"Duración (minutos): {run['duration'].total_seconds()/60:.2f}")

# Resumen de saltos temporales significativos
print("\nSaltos temporales significativos:")
print(large_jumps[['timestamp', 'time_diff_seconds']])
