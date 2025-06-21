from pyspark.sql import SparkSession
from pyspark.sql.functions import col, size
import os
from pathlib import Path
import json

def get_file_size_gb(file_path):
    """Convierte el tamaño del archivo a GB"""
    size_bytes = os.path.getsize(file_path)
    return size_bytes / (1024 * 1024 * 1024)

def analyze_directory(spark, directory):
    """Analiza un directorio y sus subdirectorios"""
    results = []
    
    # Buscar archivos parquet y csv
    parquet_files = Path(directory).rglob('*.parquet')
    csv_files = Path(directory).rglob('*.csv')
    
    # Analizar archivos parquet
    for file_path in parquet_files:
        try:
            # Cargar el archivo sin cachear
            df = spark.read.parquet(str(file_path))
            
            # Obtener número de columnas
            num_columns = len(df.columns)
            
            # Cachear el DataFrame para evitar múltiples lecturas
            df.cache()
            
            # Obtener número de filas
            num_rows = df.count()
            
            results.append({
                'size_gb': get_file_size_gb(file_path),
                'num_columns': num_columns,
                'num_rows': num_rows,
                'format': 'parquet',
                'filename': file_path.name,
                'directory': str(file_path.parent)
            })
            
            # Liberar memoria
            df.unpersist()
            
        except Exception as e:
            print(f"Error al procesar {file_path}: {str(e)}")
    
    # Analizar archivos csv
    for file_path in csv_files:
        try:
            # Cargar el archivo sin cachear
            df = spark.read.csv(str(file_path), inferSchema=True, header=True)
            
            # Obtener número de columnas
            num_columns = len(df.columns)
            
            # Cachear el DataFrame para evitar múltiples lecturas
            df.cache()
            
            # Obtener número de filas
            num_rows = df.count()
            
            results.append({
                'size_gb': get_file_size_gb(file_path),
                'num_columns': num_columns,
                'num_rows': num_rows,
                'format': 'csv',
                'filename': file_path.name,
                'directory': str(file_path.parent)
            })
            
            # Liberar memoria
            df.unpersist()
            
        except Exception as e:
            print(f"Error al procesar {file_path}: {str(e)}")
    
    return results

def main():
    # Crear sesión Spark
    spark = SparkSession.builder \
        .appName("Spark Benchmark") \
        .master("spark://spark-master:7077") \
        .config("spark.executor.memory", "9g") \
        .config("spark.executor.cores", "2") \
        .config("spark.cores.max", "10") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.sql.debug.maxToStringFields", "100") \
        .getOrCreate()
    
    try:
        # Directorios a analizar
        directories = ['data/gold', 'data/bronze', 'data/silver']
        
        # Lista para almacenar todos los resultados
        all_results = []
        
        # Recorrer todos los directorios
        for directory in directories:
            print(f"\nAnalizando {directory}...")
            results = analyze_directory(spark, directory)
            all_results.extend(results)
        
        # Crear DataFrame con los resultados
        df_results = spark.createDataFrame(all_results)
        
        # Guardar en CSV
        output_path = 'data/images/files_analysis.csv'
        df_results.write.csv(output_path, header=True, mode='overwrite')
        print(f"\nArchivo de análisis generado en: {output_path}")
        
    finally:
        # Detener la sesión Spark
        spark.stop()

if __name__ == "__main__":
    main()
