import pandas as pd
from pathlib import Path
import re

def format_value(value, format_type='latex'):
    if pd.isna(value):
        return "-" if format_type == 'latex' else "-"
    if isinstance(value, (int, float)):
        # Si es un número entero (int) o un float que representa un entero
        if isinstance(value, int) or (isinstance(value, float) and value.is_integer()):
            return f"{int(value)}"
        # Si es un número decimal
        return f"{value:.2f}"
    return str(value)

def generate_latex_table_comparison(df, dataset, tipo, proceso):
    table = []
    
    # Encabezado
    table.append("\\begin{table}[H]")
    table.append("\\centering")
    table.append(f"\\caption{{Métricas por Run y Contenedor en {dataset} ({tipo}) - Proceso {proceso}}}")
    table.append("\\label{tab:metrics-by-run}")
    table.append("\\begin{tabular}{|l|l|r|r|r|r|r|r|}")
    table.append("\\hline")
    table.append("Run & Contenedor & CPU Media (%) & CPU Desv. Estándar (%) & CPU Mediana (%) & Mem. Media (GB) & Mem. Desv. Estándar (GB) & Mem. Mediana (GB) \\ \\hline")
    table.append("\\hline")
    
    # Encabezados
    headers = ['Run', 'Contenedor', 'CPU Media (%)', 'CPU Desv. Estándar (%)', 'CPU Mediana (%)', 
              'Mem. Media (GB)', 'Mem. Desv. Estándar (GB)', 'Mem. Mediana (GB)']
    columns = ['run', 'name', 'cpu_perc_mean', 'cpu_perc_std', 'cpu_perc_median',
              'mem_used_GB_mean', 'mem_used_GB_std', 'mem_used_GB_median']
    
    # Línea de encabezados
    header_line = " & ".join(headers) + " \\\\ \\hline"
    table.append(header_line)
    
    # Filas de datos
    for _, row in df.iterrows():
        values = [format_value(row[col], 'latex') for col in columns]
        line = " & ".join(values) + " \\\\ \\hline"
        table.append(line)
    
    # Notas
    table.append("\\end{tabular}")
    table.append("\\end{table}")
    table.append("\\smallskip")
    table.append("\\textit{Nota. CPU = Unidad Central de Procesamiento; Mem. = Memoria.}")
    
    return "\n".join(table)

def generate_markdown_table_comparison(df, dataset, tipo, proceso):
    table = []
    
    # Encabezado
    table.append(f"# Métricas por Run y Contenedor en {dataset} ({tipo}) - Proceso {proceso}")
    table.append("\n")
    
    # Encabezados
    headers = ['Run', 'Contenedor', 'CPU Media (%)', 'CPU Desv. Estándar (%)', 'CPU Mediana (%)', 
              'Mem. Media (GB)', 'Mem. Desv. Estándar (GB)', 'Mem. Mediana (GB)']
    columns = ['run', 'name', 'cpu_perc_mean', 'cpu_perc_std', 'cpu_perc_median',
              'mem_used_GB_mean', 'mem_used_GB_std', 'mem_used_GB_median']
    
    # Líneas de la tabla
    table.append("| " + " | ".join(headers) + " |")
    table.append("| " + " | ".join(["-" * len(h) for h in headers]) + " |")
    
    # Filas de datos
    for _, row in df.iterrows():
        values = [format_value(row[col], 'markdown') for col in columns]
        line = "| " + " | ".join(values) + " |"
        table.append(line)
    
    # Notas
    table.append("\n")
    table.append("*Nota.* CPU = Unidad Central de Procesamiento; Mem. = Memoria.")
    
    return "\n".join(table)

def generate_html_table_comparison(df, dataset, tipo, proceso):
    table = []
    
    # Encabezado
    table.append(f"<div class='table-container'>")
    table.append(f"<h3>Métricas por Run y Contenedor en {dataset} ({tipo}) - Proceso {proceso}</h3>")
    
    # Tabla HTML
    table.append("<table class='stats-table'>")
    
    # Encabezados
    headers = ['Run', 'Contenedor', 'CPU Media (%)', 'CPU Desv. Estándar (%)', 'CPU Mediana (%)', 
              'Mem. Media (GB)', 'Mem. Desv. Estándar (GB)', 'Mem. Mediana (GB)']
    columns = ['run', 'name', 'cpu_perc_mean', 'cpu_perc_std', 'cpu_perc_median',
              'mem_used_GB_mean', 'mem_used_GB_std', 'mem_used_GB_median']
    
    table.append("<thead>")
    table.append("<tr>")
    for header in headers:
        table.append(f"<th>{header}</th>")
    table.append("</tr>")
    table.append("</thead>")
    
    # Filas de datos
    table.append("<tbody>")
    for _, row in df.iterrows():
        table.append("<tr>")
        for col in columns:
            value = format_value(row[col], 'html')
            table.append(f"<td>{value}</td>")
        table.append("</tr>")
    table.append("</tbody>")
    
    table.append("</table>")
    table.append("<p class='note'>Nota: CPU = Unidad Central de Procesamiento; Mem. = Memoria.</p>")
    table.append("</div>")
    
    return "\n".join(table)

# Leer el archivo combinado
df = pd.read_csv(Path("data/images/all_stats_summary.csv"))

# Extraer el número de run del nombre del archivo
# El patrón es "runX_" donde X es el número de run
pattern = r'run(\d+)_'

def extract_run_number(filename):
    match = re.search(pattern, filename)
    if match:
        return int(match.group(1))
    return None

# Crear una nueva columna 'run_number' usando el número extraído del archivo
# y eliminar la columna 'run' que tiene 'stats'
df['run_number'] = df['archivo'].apply(extract_run_number)
df = df.drop('run', axis=1)

# Crear el directorio para las nuevas tablas
output_dir = Path("data/images/formatted_tables_v2")
output_dir.mkdir(exist_ok=True)

# Agrupar por dataset y tipo de operación
for (dataset, tipo, proceso), group in df.groupby(['dataset', 'tipo', 'proceso']):
    # Filtrar las columnas que necesitamos
    df_filtered = group[['run_number', 'name', 'cpu_perc_mean', 'cpu_perc_std', 'cpu_perc_median',
                        'mem_used_GB_mean', 'mem_used_GB_std', 'mem_used_GB_median']]
    
    if not df_filtered.empty:
        # Obtener los contenedores únicos
        containers = df_filtered['name'].unique()
        
        # Obtener los runs únicos
        runs = sorted(df_filtered['run_number'].unique())
        
        # Crear un DataFrame vacío para las métricas promedio
        metrics = pd.DataFrame(columns=['run', 'name', 'cpu_perc_mean', 'cpu_perc_std', 
                                     'cpu_perc_median', 'mem_used_GB_mean', 
                                     'mem_used_GB_std', 'mem_used_GB_median'])
        
        # Para cada run y contenedor, calcular las métricas promedio
        metrics_list = []
        for run in runs:
            for container in containers:
                run_container_df = df_filtered[(df_filtered['run_number'] == run) & 
                                             (df_filtered['name'] == container)]
                if not run_container_df.empty:
                    # Calcular las métricas promedio
                    row = {
                        'run': run,
                        'name': container,
                        'cpu_perc_mean': run_container_df['cpu_perc_mean'].mean(),
                        'cpu_perc_std': run_container_df['cpu_perc_std'].mean(),
                        'cpu_perc_median': run_container_df['cpu_perc_median'].mean(),
                        'mem_used_GB_mean': run_container_df['mem_used_GB_mean'].mean(),
                        'mem_used_GB_std': run_container_df['mem_used_GB_std'].mean(),
                        'mem_used_GB_median': run_container_df['mem_used_GB_median'].mean()
                    }
                    metrics_list.append(pd.DataFrame([row]))
        
        # Concatenar todos los DataFrames
        metrics = pd.concat(metrics_list, ignore_index=True)
        
        # Ordenar por run y contenedor
        metrics = metrics.sort_values(['run', 'name'])
        
        # Generar tablas en diferentes formatos
        formats = {
            'latex': generate_latex_table_comparison,
            'markdown': generate_markdown_table_comparison,
            'html': generate_html_table_comparison
        }
        
        for fmt, func in formats.items():
            # Generar tabla comparativa
            table_content = func(metrics, dataset, tipo, proceso)
            
            # Crear el nombre del archivo
            filename = f"{dataset}_{tipo}_{proceso}_metrics.{fmt}"
            filename = f"{dataset}_{tipo}_metrics.{fmt}"
            filepath = output_dir / filename
            
            # Guardar la tabla
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(table_content)
            
            print(f"Generated {fmt} metrics table: {filename}")
