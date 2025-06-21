import os
import time
from datetime import datetime
from pyspark.sql.functions import col, expr
from template import write_state, clear_state, measure_time, create_spark_session


def normalize_reviews_images(df):
    # Asumimos que images es un array de structs. Si es string, usar from_json.
    for i in range(3):  # Hasta 3 imágenes por fila
        df = df.withColumn(f"images_{i}_attachment_type", expr(f"images[{i}].attachment_type"))
        df = df.withColumn(f"images_{i}_large_image_url", expr(f"images[{i}].large_image_url"))
        df = df.withColumn(f"images_{i}_medium_image_url", expr(f"images[{i}].medium_image_url"))
        df = df.withColumn(f"images_{i}_small_image_url", expr(f"images[{i}].small_image_url"))
    return df.drop("images")


def normalize_reviews_dataset(spark, input_path, output_path, script_name):
    def execute():
        df = spark.read.parquet(input_path)

        print(f"[i] Columnas originales: {df.columns}")
        if "images" not in df.columns:
            print("[!] Columna 'images' no encontrada. Se omite normalización.")
            df.write.mode("overwrite").parquet(output_path)
            return df

        df = normalize_reviews_images(df)

        print(f"[✓] Escribiendo salida en: {output_path}")
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df.write.mode("overwrite").parquet(output_path)
        return df

    return measure_time("Normalize Reviews Dataset", input_path, execute, output_path=output_path, script_name=script_name, log_output=True)


def main():
    script_name = os.path.splitext(os.path.basename(__file__))[0]
    spark = create_spark_session()
    total_start = time.time()

    files = [
        "reviews_Books.parquet",
        "reviews_Clothing_Shoes_and_Jewelry.parquet",
        "reviews_Electronics.parquet",
        "reviews_Home_and_Kitchen.parquet",
        "reviews_Toys_and_Games.parquet"
    ]

    for file in files:
        input_path = f"/data/bronze/amazon2023/{file}"
        output_path = f"/data/silver/amazon2023/{file.replace('.parquet', '_normalized.parquet')}"
        normalize_reviews_dataset(spark, input_path, output_path, script_name)

    spark.stop()

    total_duration = time.time() - total_start
    report_path = os.path.join("data/report", script_name, f"{script_name}_report.md")
    with open(report_path, "a", encoding="utf-8") as f:
        f.write(f"\n## Tiempo total del script\n- {total_duration:.2f} segundos\n")


if __name__ == "__main__":
    main()
