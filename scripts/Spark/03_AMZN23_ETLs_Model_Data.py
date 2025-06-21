import os
import time
from pyspark.sql.functions import col, year, month, avg
from template import (
    create_spark_session,
    measure_time_with_run_id,
    redirect_output,
    write_state,
    clear_state,
    get_run_id
)

INPUT_PATH = "data/gold/amazon2023/joint_dataset.parquet"
OUTPUT_PATH = "data/gold/amazon2023/amzn23_model_data.parquet"

CATEGORICAL_NULL_FILL = "unknown"
NUM_COLS_TO_IMPUTE = [
    "price", "details_hardcover_value", "details_item_weight_value",
    "details_dimensions_width", "details_dimensions_depth", "details_dimensions_height"
]

def preprocess_for_model(spark):
    df = spark.read.parquet(INPUT_PATH)

    df = df.withColumn("review_year", year("timestamp")) \
           .withColumn("review_month", month("timestamp")) \
           .withColumn("has_valid_price", col("price").isNotNull().cast("int"))

    df = df.fillna({
        "details_publisher_value": CATEGORICAL_NULL_FILL,
        "details_language_value": CATEGORICAL_NULL_FILL,
        "main_category": CATEGORICAL_NULL_FILL
    })

    for col_name in NUM_COLS_TO_IMPUTE:
        mean_value = df.select(avg(col_name)).first()[0]
        df = df.fillna({col_name: mean_value})

    df = df.drop("title", "timestamp", "parent_asin")
    df = df.withColumn("verified_purchase", col("verified_purchase").cast("int"))

    ordered_cols = sorted(df.columns)
    df = df.select(ordered_cols)

    df.write.mode("overwrite").parquet(OUTPUT_PATH)
    print(f"[✓] Dataset final para modelos guardado en: {OUTPUT_PATH}")


def main():
    script_name = os.path.splitext(os.path.basename(__file__))[0]
    redirect_output(script_name)

    run_base = "Preprocesamiento_para_modelos"
    run_id = get_run_id(run_base)
    fake_input = f"{run_base}__run{run_id}.parquet"

    write_state("Preprocesamiento para modelos", input_file=fake_input)

    spark = create_spark_session()
    start_time = time.time()

    measure_time_with_run_id(
        operation_name="Preprocesamiento para modelos",
        input_path=f"data/gold/amazon2023/{fake_input}",
        func=lambda: preprocess_for_model(spark),
        output_path=OUTPUT_PATH,
        script_name=script_name,
        log_output=False,
        run_id=run_id
    )

    total_duration = time.time() - start_time
    report_path = f"data/report/{script_name}/{script_name}_report.md"
    with open(report_path, "a", encoding="utf-8") as f:
        f.write(f"\n## Tiempo total del script\n- {total_duration:.2f} segundos\n")

    spark.stop()
    clear_state()

if __name__ == "__main__":
    main()
