import sys
import os
import json
import time
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import size, from_json
from pyspark.sql.types import StringType, MapType
from template import write_state, clear_state, measure_time, create_spark_session


def normalize_column(df, col_name):
    col_type = df.schema[col_name].dataType.typeName()
    if col_type == 'array':
        df = df.withColumn(f'{col_name}_length', size(df[col_name]))
        df = df.withColumn(f'{col_name}_as_list', df[col_name])
    elif col_type == 'struct':
        if col_name == 'images':
            df = df.withColumn('images_hi_res_url', df['images']['hi_res'][0])
            df = df.withColumn('images_large_url', df['images']['large'][0])
            df = df.withColumn('images_thumb_url', df['images']['thumb'][0])
            df = df.withColumn('images_variant', df['images']['variant'][0])
        elif col_name == 'videos':
            df = df.withColumn('videos_title', df['videos']['title'][0])
            df = df.withColumn('videos_url', df['videos']['url'][0])
            df = df.withColumn('videos_user_id', df['videos']['user_id'][0])
    return df


def normalize_dataset(spark, input_path: str, output_path: str, script_name: str):
    def execute():
        df = spark.read.parquet(input_path)

        # Renombrar columnas con espacios
        rename_dict = {c: c.replace(' ', '_') for c in df.columns if ' ' in c}
        for old_name, new_name in rename_dict.items():
            df = df.withColumnRenamed(old_name, new_name)

        for col_name in df.columns:
            df = normalize_column(df, col_name)

        # Normalizar columnas específicas
        if 'categories' in df.columns:
            df = df.withColumn('categories_length', size(df['categories']))
            df = df.withColumn('categories_as_list', df['categories'])

        if 'details' in df.columns:
            df = df.withColumn('details_json', from_json('details', MapType(StringType(), StringType())))
            fields = ['Publisher', 'Language', 'Hardcover', 'ISBN 10', 'ISBN 13', 'Item Weight', 'Dimensions']
            for field in fields:
                key = field.lower().replace(" ", "_")
                df = df.withColumn(f'details_{key}_exists', df['details_json'][field].isNotNull())
                df = df.withColumn(f'details_{key}_value', df['details_json'][field])

        df = df.drop(*[c for c in ['details', 'details_json'] if c in df.columns])

        # Guardar resultado
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df.write.mode("overwrite").parquet(output_path)
        return df

    return measure_time("Normalize Dataset", input_path, execute, output_path=output_path, script_name=script_name, log_output=True)


def main():
    script_name = os.path.splitext(os.path.basename(__file__))[0]
    spark = create_spark_session()
    total_start = time.time()  # Inicia temporizador total

    files = [
        "meta_Books.parquet",
        "meta_Clothing_Shoes_and_Jewelry.parquet",
        "meta_Electronics.parquet",
        "meta_Home_and_Kitchen.parquet",
        "meta_Toys_and_Games.parquet"
    ]

    for file in files:
        input_path = f"/data/bronze/amazon2023/{file}"
        output_path = f"/data/silver/amazon2023/{file.replace('.parquet', '_normalized.parquet')}"
        normalize_dataset(spark, input_path, output_path, script_name=script_name)

    spark.stop()

    # Calcula y guarda tiempo total
    total_duration = time.time() - total_start
    report_path = os.path.join("data/report", script_name, f"{script_name}_report.md")
    with open(report_path, "a", encoding="utf-8") as f:
        f.write(f"\n## Tiempo total del script\n- {total_duration:.2f} segundos\n")



if __name__ == "__main__":
    main()
