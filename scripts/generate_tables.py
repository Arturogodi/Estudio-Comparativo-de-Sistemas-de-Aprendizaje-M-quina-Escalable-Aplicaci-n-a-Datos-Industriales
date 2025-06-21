import pandas as pd
from pathlib import Path
import re

# Función para formatear valores numéricos
def format_value(value, format_type='latex'):
    if pd.isna(value):
        return "-" if format_type == 'latex' else "-"
    if isinstance(value, (int, float)):
        return f"{value:.2f}"
    return str(value)

def generate_latex_table(df, tipo, dataset, run):
    table = []
    
    # Encabezado
    table.append(f"\\begin{{table}}[ht]")
    table.append("\\centering")
    table.append(f"\\caption{{Descriptive Statistics for {tipo.replace('_', ' ').title()} on {dataset.replace('_', ' ').title()} Dataset (Run {run})}}")
    table.append(f"\\label{{tab:{tipo}_{dataset}_{run}}}")
    table.append("\\begin{tabular}{|l|c|c|c|c|c|c|}")
    table.append("\\hline")
    
    # Encabezados
    headers = ['Container', 'CPU Mean (%)', 'CPU Std (%)', 'CPU Median (%)', 
              'Mem Mean (GB)', 'Mem Std (GB)', 'Mem Median (GB)']
    columns = ['name', 'cpu_perc_mean', 'cpu_perc_std', 'cpu_perc_median',
              'mem_used_GB_mean', 'mem_used_GB_std', 'mem_used_GB_median']
    
    # Línea de encabezados
    header_line = " & ".join(headers) + " \\\\\hline"
    table.append(header_line)
    
    # Filas de datos
    for _, row in df.iterrows():
        values = [format_value(row[col], 'latex') for col in columns]
        line = " & ".join(values) + " \\\\\hline"
        table.append(line)
    
    # Notas
    table.append("\\end{tabular}")
    table.append("\\end{table}")
    table.append("\\smallskip")
    table.append("\\textit{Note. CPU = Central Processing Unit; Mem = Memory.}")
    
    return "\\n".join(table)

def generate_markdown_table(df, tipo, dataset, run):
    table = []
    
    # Encabezado
    table.append(f"# Descriptive Statistics for {tipo.replace('_', ' ').title()} on {dataset.replace('_', ' ').title()} Dataset (Run {run})")
    table.append("\n")
    
    # Encabezados
    headers = ['Container', 'CPU Mean (%)', 'CPU Std (%)', 'CPU Median (%)', 
              'Mem Mean (GB)', 'Mem Std (GB)', 'Mem Median (GB)']
    columns = ['name', 'cpu_perc_mean', 'cpu_perc_std', 'cpu_perc_median',
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
    table.append("*Note.* CPU = Central Processing Unit; Mem = Memory.")
    
    return "\n".join(table)

def generate_html_table(df, tipo, dataset, run):
    table = []
    
    # Encabezado
    table.append(f"<div class='table-container'>")
    table.append(f"<h3>Descriptive Statistics for {tipo.replace('_', ' ').title()} on {dataset.replace('_', ' ').title()} Dataset (Run {run})</h3>")
    
    # Tabla HTML
    table.append("<table class='stats-table'>")
    
    # Encabezados
    headers = ['Container', 'CPU Mean (%)', 'CPU Std (%)', 'CPU Median (%)', 
              'Mem Mean (GB)', 'Mem Std (GB)', 'Mem Median (GB)']
    columns = ['name', 'cpu_perc_mean', 'cpu_perc_std', 'cpu_perc_median',
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
    table.append("<p class='note'>Note: CPU = Central Processing Unit; Mem = Memory.</p>")
    table.append("</div>")
    
    return "\n".join(table)

# Leer el archivo combinado
df = pd.read_csv(Path("data/images/all_stats_summary.csv"))

# Crear el directorio para las tablas si no existe
output_dir = Path("data/images/formatted_tables")
output_dir.mkdir(exist_ok=True)

# Leer el archivo combinado
df = pd.read_csv(Path("data/images/all_stats_summary.csv"))

# Crear el directorio para las tablas si no existe
output_dir = Path("data/images/formatted_tables")
output_dir.mkdir(exist_ok=True)

# Generar tablas individuales para cada combinación
groups = df.groupby(['tipo', 'dataset', 'run'])

for (tipo, dataset, run), group in groups:
    # Filtrar el DataFrame para el tipo, dataset y run específicos
    df_filtered = group[['name', 'cpu_perc_mean', 'cpu_perc_std', 'cpu_perc_median',
                        'mem_used_GB_mean', 'mem_used_GB_std', 'mem_used_GB_median']]
    
    if not df_filtered.empty:
        # Generar tablas en diferentes formatos
        formats = {
            'latex': generate_latex_table,
            'markdown': generate_markdown_table,
            'html': generate_html_table
        }
        
        for fmt, func in formats.items():
            table_content = func(df_filtered, tipo, dataset, run)
            
            # Crear el nombre del archivo
            filename = f"{tipo}_{dataset}_{run}_table.{fmt}"
            filepath = output_dir / filename
            
            # Guardar la tabla
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(table_content)
            
            print(f"Generated {fmt} table: {filename}")
