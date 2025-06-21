import pandas as pd
from pathlib import Path
import re

# Función para formatear valores numéricos según APA
def format_apa(value):
    if pd.isna(value):
        return "-"
    if isinstance(value, (int, float)):
        return f"{value:.2f}"
    return str(value)

def generate_apa_table(df, tipo, dataset, run):
    # Filtrar el DataFrame para el tipo, dataset y run específicos
    df_filtered = df[(df['tipo'] == tipo) & (df['dataset'] == dataset) & (df['run'] == run)]
    
    if df_filtered.empty:
        return None
    
    # Crear la tabla APA
    table = []
    
    # Encabezado
    table.append(f"Table {tipo}_{dataset}_{run}")
    table.append(f"Descriptive Statistics for {tipo.replace('_', ' ').title()} on {dataset.replace('_', ' ').title()} Dataset (Run {run})")
    table.append("\n")
    
    # Columnas
    columns = ['name', 'cpu_perc_mean', 'cpu_perc_std', 'cpu_perc_median', 
              'mem_used_GB_mean', 'mem_used_GB_std', 'mem_used_GB_median']
    
    # Encabezados de columna
    headers = ['Container', 'CPU Mean (%)', 'CPU Std (%)', 'CPU Median (%)', 
              'Mem Mean (GB)', 'Mem Std (GB)', 'Mem Median (GB)']
    
    # Línea de encabezados
    header_line = "| " + " | ".join(headers) + " |"
    table.append(header_line)
    
    # Línea de separación
    sep_line = "| " + " | ".join(["-" * len(h) for h in headers]) + " |"
    table.append(sep_line)
    
    # Filas de datos
    for _, row in df_filtered.iterrows():
        values = [format_apa(row[col]) for col in columns]
        line = "| " + " | ".join(values) + " |"
        table.append(line)
    
    # Notas
    table.append("\n")
    table.append("Note. CPU = Central Processing Unit; Mem = Memory.")
    
    return "\n".join(table)

# Leer el archivo combinado
df = pd.read_csv(Path("data/images/all_stats_summary.csv"))

# Crear el directorio para las tablas si no existe
output_dir = Path("data/images/apa_tables")
output_dir.mkdir(exist_ok=True)

# Generar tablas individuales para cada combinación
groups = df.groupby(['tipo', 'dataset', 'run'])

for (tipo, dataset, run), group in groups:
    # Generar la tabla APA
    apa_table = generate_apa_table(df, tipo, dataset, run)
    
    if apa_table:
        # Crear el nombre del archivo
        filename = f"{tipo}_{dataset}_{run}_apa_table.txt"
        filepath = output_dir / filename
        
        # Guardar la tabla
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(apa_table)
        
        print(f"Generated APA table: {filename}")
