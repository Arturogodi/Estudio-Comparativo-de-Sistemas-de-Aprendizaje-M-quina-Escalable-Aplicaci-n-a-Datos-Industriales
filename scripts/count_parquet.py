from pyspark.sql import SparkSession

def count_parquet():
    # Crear sesión de Spark
    spark = SparkSession.builder \
        .appName("CountParquetRows") \
        .getOrCreate()
    
    try:
        # Leer el archivo Parquet
        df = spark.read.parquet("data/gold/amazon2023/joint_dataset.parquet")
        
        # Contar las filas
        row_count = df.count()
        
        print(f"\nNúmero total de filas en el archivo Parquet: {row_count}")
        
        # Mostrar información adicional
        print("\nEstructura del DataFrame:")
        df.printSchema()
        
        print("\nPrimeras 5 filas:")
        df.show(5)
        
    except Exception as e:
        print(f"Error al procesar el archivo Parquet: {str(e)}")
    
    finally:
        # Detener la sesión de Spark
        spark.stop()

if __name__ == "__main__":
    count_parquet()
