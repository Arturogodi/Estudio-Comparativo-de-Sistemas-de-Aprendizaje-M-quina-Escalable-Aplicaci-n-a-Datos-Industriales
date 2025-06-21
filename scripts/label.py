import pandas as pd
import os
from pathlib import Path
from pyspark.sql import SparkSession
import sys

def get_parquet_files():
    """Obtiene la lista de archivos parquet disponibles"""
    base_dir = Path("../data/gold/amazon2023")
    parquet_dir = base_dir / "amzn23_model_data.parquet"
    
    if not parquet_dir.exists():
        print(f"[!] Directorio no encontrado: {parquet_dir}")
        return []
    
    # Obtener todos los archivos parquet en el directorio y subdirectorios
    parquet_files = list(parquet_dir.rglob("*.parquet"))
    return parquet_files

def inspect_parquet_spark(spark, file_path):
    """Inspecciona un archivo parquet usando Spark"""
    print(f"\n[i] Inspeccionando archivo: {file_path}")
    
    # Crear DataFrame de Spark
    df = spark.read.parquet(str(file_path))
    
    # Mostrar información del esquema
    print("\n[i] Esquema del dataset:")
    df.printSchema()
    
    # Mostrar las primeras filas
    print("\n[i] Primeras filas del dataset:")
    df.show(5)
    
    # Convertir a pandas para mostrar más detalles
    print("\n[i] Columnas del dataset:")
    print(list(df.columns))
    
    # Mostrar estadísticas básicas
    print("\n[i] Estadísticas básicas:")
    df.describe().show()

def main():
    # Inicializar Spark
    spark = SparkSession.builder \
        .appName("Label") \
        .master("spark://spark-master:7077") \
        .config("spark.executor.memory", "9g") \
        .config("spark.executor.cores", "2") \
        .config("spark.cores.max", "10") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.sql.debug.maxToStringFields", "100") \
        .getOrCreate()
    
    try:
        # Crear el archivo de salida en el directorio scripts con extensión .md
        output_path = Path(__file__).parent / "label_inspection_output.md"
        with open(output_path, 'w') as f:
            # Redirigir la salida estándar al archivo
            original_stdout = sys.stdout
            sys.stdout = f
            
            # Obtener lista de archivos disponibles
            files = get_parquet_files()
            
            if not files:
                print("[!] No se encontraron archivos parquet disponibles")
                return
                
            print(f"[i] Se encontraron {len(files)} archivos parquet disponibles:")
            for i, file in enumerate(files, 1):
                print(f"{i}. {file}")
            
            # Inspeccionar el primer archivo como ejemplo
            if files:
                inspect_parquet_spark(spark, files[0])
            
            # Restaurar la salida estándar
            sys.stdout = original_stdout
            print(f"\n[i] Resultados guardados en: {output_path}")
            
    finally:
        # Detener la sesión de Spark
        spark.stop()

if __name__ == "__main__":
    main()
