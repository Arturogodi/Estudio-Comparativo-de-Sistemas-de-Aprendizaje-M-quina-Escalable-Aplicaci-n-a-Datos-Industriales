import os
import time
from datetime import datetime
from pyspark.sql.functions import col, isnan, countDistinct, count, when
from pyspark.sql.types import ArrayType, StructType, NumericType, StringType

from template import (
    create_spark_session,
    measure_time,
    redirect_output,
)


def analyze_dataframe(df, file_name):
    print(f"\nAnalizando archivo: {file_name}")
    print(f"Total de filas: {df.count()}")
    print(f"Total de columnas: {len(df.columns)}\n")

    # Clasificación por tipo
    array_fields = [f.name for f in df.schema.fields if isinstance(f.dataType, ArrayType)]
    struct_fields = [f.name for f in df.schema.fields if isinstance(f.dataType, StructType)]
    numeric_fields = [f.name for f in df.schema.fields if isinstance(f.dataType, NumericType)]
    string_fields = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]

    print(f"Columnas numéricas: {numeric_fields}")
    print(f"Columnas string: {string_fields}")
    print(f"Columnas array: {array_fields}")
    print(f"Columnas struct: {struct_fields}")

    analysis_cols = numeric_fields + string_fields
    print("\nResumen de columnas clave:")
    for c in analysis_cols:
        nulls = df.filter(col(c).isNull()).count()
        nans = df.filter(isnan(c) if isinstance(df.schema[c].dataType, NumericType) else col(c) == "NaN").count()
        uniques = df.select(countDistinct(c)).collect()[0][0]
        print(f" - {c}: nulls={nulls}, NaNs={nans}, únicos={uniques}")


def eda_file(spark, input_path, script_name):
    def execute():
        df = spark.read.parquet(input_path)
        analyze_dataframe(df, input_path)
        return df

    return measure_time(
        operation_name="EDA Meta Dataset",
        input_path=input_path,
        func=execute,
        script_name=script_name,
        log_output=False  # Ya redirigido globalmente
    )


def main():
    script_name = os.path.splitext(os.path.basename(__file__))[0]

    # Redirige salida estándar y errores solo una vez
    redirect_output(script_name)

    spark = create_spark_session()
    total_start = time.time()

    silver_dir = "/data/silver/amazon2023"
    files = [
        f for f in os.listdir(silver_dir)
        if f.startswith("meta_") and f.endswith("_normalized.parquet")
    ]

    for file in files:
        input_path = os.path.join(silver_dir, file)
        eda_file(spark, input_path, script_name)

    spark.stop()
    total_duration = time.time() - total_start

    report_path = os.path.join("data/report", script_name, f"{script_name}_report.md")
    with open(report_path, "a", encoding="utf-8") as f:
        f.write(f"\n## Tiempo total del script\n- {total_duration:.2f} segundos\n")


if __name__ == "__main__":
    main()
