from pyspark.sql import SparkSession
import os
import pandas as pd
from pathlib import Path

# Configuración de Spark
def create_spark_session():
    return SparkSession.builder \
        .appName("Spark Benchmark") \
        .master("spark://spark-master:7077") \
        .config("spark.executor.memory", "9g") \
        .config("spark.executor.cores", "2") \
        .config("spark.cores.max", "10") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.sql.debug.maxToStringFields", "100") \
        .getOrCreate()

def analyze_files():
    # Crear sesión de Spark
    spark = create_spark_session()

    # Directorios a analizar
    directories = [
        "data/bronze",
        "data/silver",
        "data/gold"
    ]

    # Lista para almacenar resultados
    results = []

    # Para cada directorio
    for directory in directories:
        # Obtener todos los archivos
        for root, _, files in os.walk(directory):
            # Agrupar archivos por nombre base (sin el número de partición)
            file_groups = {}
            for file in files:
                if not file.endswith(('.parquet', '.csv')):
                    continue

                # Extraer el nombre base del archivo (sin el número de partición)
                base_name = file.split('.')[0].split('-')[0]
                if base_name not in file_groups:
                    file_groups[base_name] = []
                file_groups[base_name].append(file)

            # Procesar cada grupo de archivos
            for base_name, file_list in file_groups.items():
                # Si hay múltiples archivos, leer el directorio completo
                if len(file_list) > 1:
                    file_path = root
                else:
                    file_path = os.path.join(root, file_list[0])

                try:
                    # Leer archivo según su extensión
                    if file_path.endswith('.parquet') or all(f.endswith('.parquet') for f in file_list):
                        df = spark.read.parquet(file_path)
                        extension = '.parquet'
                    else:
                        df = spark.read.csv(file_path, header=True, inferSchema=True)
                        extension = '.csv'

                    # Obtener estadísticas
                    num_rows = df.count()
                    num_cols = len(df.columns)

                    # Calcular tamaño total del grupo de archivos
                    total_size = sum(os.path.getsize(os.path.join(root, f)) for f in file_list)
                    file_size_gb = total_size / (1024 * 1024 * 1024)

                    # Agregar a resultados
                    results.append({
                        'fichero': base_name,
                        'peso_gb': file_size_gb,
                        'extension': extension,
                        'num_filas': num_rows,
                        'num_columnas': num_cols
                    })

                    print(f"Procesado: {file_path}")

                except Exception as e:
                    print(f"Error procesando {file_path}: {str(e)}")
                    continue

    # Convertir resultados a DataFrame
    results_df = pd.DataFrame(results)

    # Guardar en CSV
    output_path = "data/images/file_analysis.csv"
    results_df.to_csv(output_path, index=False)
    print(f"\nResultados guardados en: {output_path}")
    print("\nResumen de archivos:")
    print(results_df[['fichero', 'peso_gb', 'num_filas', 'num_columnas']].to_string(index=False))

if __name__ == "__main__":
    analyze_files()
