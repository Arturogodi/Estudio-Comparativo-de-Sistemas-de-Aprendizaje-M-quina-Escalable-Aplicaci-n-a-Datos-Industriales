from pyspark.sql import SparkSession
from typing import Optional

def create_spark_session(app_name: Optional[str] = None) -> SparkSession:
    """
    Crea una sesión Spark configurada con optimizaciones para rendimiento y memoria.
    
    Args:
        app_name (Optional[str]): Nombre de la aplicación. Si no se especifica, se usa "Spark Benchmark"
        
    Returns:
        SparkSession: Sesión Spark configurada y optimizada
    """
    # Crear el builder base
    builder = SparkSession.builder
    
    # Configuración básica
    builder = builder.appName(app_name if app_name else "Spark Benchmark")
    builder = builder.master("spark://spark-master:7077")
    
    # Configuración de memoria y CPU
    builder = builder.config("spark.executor.memory", "9g")  # 9GB de memoria por executor
    builder = builder.config("spark.executor.cores", "2")     # 2 cores por executor
    builder = builder.config("spark.cores.max", "10")         # Máximo de 10 cores en total
    
    # Optimizaciones de SQL y DataFrame
    builder = builder.config("spark.sql.execution.arrow.pyspark.enabled", "true")  # Habilita Arrow para mejor rendimiento
    builder = builder.config("spark.sql.debug.maxToStringFields", "100")          # Aumenta límite de campos en toString
    builder = builder.config("spark.sql.shuffle.partitions", "200")               # Particiones por defecto para shuffle
    builder = builder.config("spark.sql.adaptive.enabled", "true")               # Habilita optimización adaptativa
    builder = builder.config("spark.sql.autoBroadcastJoinThreshold", "10485760")  # Umbral para broadcast joins (10MB)
    
    # Optimizaciones de shuffle
    builder = builder.config("spark.shuffle.compress", "true")                    # Comprime datos durante shuffle
    builder = builder.config("spark.shuffle.spill.compress", "true")             # Comprime datos cuando se derraman
    builder = builder.config("spark.io.compression.codec", "lz4")                # Usa LZ4 para compresión
    
    # Optimizaciones de ejecución
    builder = builder.config("spark.speculation", "true")                        # Habilita especulación para tareas lentas
    builder = builder.config("spark.speculation.interval", "100")                # Intervalo para comprobar tareas lentas
    builder = builder.config("spark.speculation.multiplier", "1.5")              # Factor para identificar tareas lentas
    
    # Optimizaciones de almacenamiento
    builder = builder.config("spark.storage.memoryFraction", "0.6")              # 60% de memoria para almacenamiento
    builder = builder.config("spark.storage.replication", "1")                   # Factor de replicación para almacenamiento
    
    # Optimizaciones de networking
    builder = builder.config("spark.network.timeout", "600s")                    # Timeout para conexiones
    builder = builder.config("spark.rpc.message.maxSize", "512")                 # Tamaño máximo de mensajes RPC
    
    # Configuración de logging
    builder = builder.config("spark.ui.showConsoleProgress", "true")             # Muestra progreso en consola
    builder = builder.config("spark.eventLog.enabled", "true")                  # Habilita logging de eventos
    
    return builder.getOrCreate()