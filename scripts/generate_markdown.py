import pandas as pd

def generate_markdown():
    # Leer el archivo CSV
    df = pd.read_csv("data/images/file_analysis.csv")
    
    # Crear tablas separadas por capa
    bronze_meta = df[(df['capa'] == 'bronze') & (df['fichero'].str.contains('meta_'))]
    bronze_reviews = df[(df['capa'] == 'bronze') & (df['fichero'].str.contains('reviews_'))]
    gold = df[df['capa'] == 'gold']
    
    # Generar el contenido del archivo Markdown
    markdown = """# Análisis de Archivos por Capa

## Capa Bronze - Meta Data

| Fichero | Peso (GB) | Extension | Número de Filas | Número de Columnas |
|---------|-----------|-----------|-----------------|-------------------|
"""
    
    # Agregar filas para bronze meta
    for _, row in bronze_meta.iterrows():
        markdown += f"| {row['fichero']} | {row['peso_gb']} | {row['extension']} | {row['num_filas']} | {row['num_columnas']} |\n"
    
    markdown += "\n## Capa Bronze - Reviews\n\n| Fichero | Peso (GB) | Extension | Número de Filas | Número de Columnas |\n|---------|-----------|-----------|-----------------|-------------------|\n"
    
    # Agregar filas para bronze reviews
    for _, row in bronze_reviews.iterrows():
        markdown += f"| {row['fichero']} | {row['peso_gb']} | {row['extension']} | {row['num_filas']} | {row['num_columnas']} |\n"
    
    markdown += "\n## Capa Gold\n\n| Fichero | Peso (GB) | Extension | Número de Filas | Número de Columnas |\n|---------|-----------|-----------|-----------------|-------------------|\n"
    
    # Agregar filas para gold
    for _, row in gold.iterrows():
        markdown += f"| {row['fichero']} | {row['peso_gb']} | {row['extension']} | {row['num_filas']} | {row['num_columnas']} |\n"
    
    # Guardar el archivo Markdown
    output_file = "data/images/file_analysis.md"
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(markdown)
    
    print(f"Archivo Markdown generado: {output_file}")

if __name__ == "__main__":
    generate_markdown()
