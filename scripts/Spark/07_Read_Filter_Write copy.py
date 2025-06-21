import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from template import (
    create_spark_session,
    measure_time_with_run_id,
    redirect_output,
    write_state,
    clear_state,
    get_run_id
)

INPUT_PATH = "data/gold/amazon2023/amzn23_model_data.parquet"
OUTPUT_PATH = "data/gold/amazon2023/amzn23_price_category_counts.parquet"

def process_data(spark):
    print("[i] Iniciando procesamiento de datos...")
    
    # Leer el dataset
    df = spark.read.parquet(INPUT_PATH)
    print(f"[i] Dataset cargado con {df.count()} filas")
    
    # Filtrar productos con precio válido
    filtered = df.filter(col("price") > 0)
    print(f"[i] Filtrado: {filtered.count()} filas con precio > 0")
    
    # Agrupar por main_category y contar
    grouped = filtered.groupBy("main_category").count()
    print(f"[i] Resultado: {grouped.count()} categorías")
    
    # Escribir el resultado
    grouped.write.mode("overwrite").parquet(OUTPUT_PATH)
    print(f"[✓] Resultado guardado en: {OUTPUT_PATH}")
    
    return grouped.count()

def main():
    script_name = os.path.splitext(os.path.basename(__file__))[0]
    redirect_output(script_name)

    run_base = "Read_Filter_Write"
    run_id = get_run_id(run_base)
    fake_input = f"{run_base}__run{run_id}.parquet"

    write_state("Read Filter Write", input_file=fake_input)

    spark = create_spark_session()
    total_start = time.time()

    measure_time_with_run_id(
        operation_name="Read Filter Write",
        input_path=INPUT_PATH,
        func=lambda: process_data(spark),
        output_path=OUTPUT_PATH,
        script_name=script_name,
        log_output=False,
        run_id=run_id
    )

    total_duration = time.time() - total_start
    report_path = f"data/report/{script_name}/{script_name}_report.md"
    os.makedirs(os.path.dirname(report_path), exist_ok=True)
    with open(report_path, "a", encoding="utf-8") as f:
        f.write(f"\n## Tiempo total del script\n- {total_duration:.2f} segundos\n")

    spark.stop()
    clear_state()

if __name__ == "__main__":
    main()
