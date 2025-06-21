import os
import time
from pyspark.sql import functions as F
from template import (
    create_spark_session,
    measure_time_with_run_id,  # función nueva agregada por ti
    redirect_output,
    write_state,
    clear_state,
    get_run_id
)

# Categorías a procesar
CATEGORIES = [
    "Books",
    "Clothing_Shoes_and_Jewelry",
    "Electronics",
    "Home_and_Kitchen",
    "Toys_and_Games"
]

META_BASE = "data/silver/amazon2023/meta_{}_normalized.parquet"
REVIEWS_BASE = "data/silver/amazon2023/reviews_{}_normalized.parquet"
OUTPUT_PATH = "data/gold/amazon2023/joint_dataset.parquet"

def clean_and_transform_meta(df_meta, category):
    return (
        df_meta
        .withColumn("dataset_category", F.lit(category))
        .withColumn("main_category", F.when(F.col("main_category").isNull(), "unknown").otherwise(F.col("main_category")))
        .withColumn("price", F.regexp_replace("price", "[$,]", "").cast("float"))
        .withColumn("details_item_weight_value", F.regexp_extract("details_item_weight_value", r"([\d\.]+)", 1).cast("float"))
        .withColumn("details_hardcover_value", F.regexp_extract("details_hardcover_value", r"(\d+)", 1).cast("int"))
        .withColumn("details_dimensions_width", F.split("details_dimensions_value", " x ").getItem(0).cast("float"))
        .withColumn("details_dimensions_depth", F.split("details_dimensions_value", " x ").getItem(1).cast("float"))
        .withColumn("details_dimensions_height", F.split(F.split("details_dimensions_value", " x ").getItem(2), " ").getItem(0).cast("float"))
        .withColumn("details_publisher_value", F.split("details_publisher_value", ";").getItem(0))
        .select(
            "parent_asin", "average_rating", "rating_number", "main_category", "title",
            "price", "details_publisher_value", "details_language_value", "details_hardcover_value",
            "details_item_weight_value", "details_dimensions_width", "details_dimensions_depth", "details_dimensions_height",
            "dataset_category"
        )
    )

def clean_and_transform_reviews(df_reviews):
    return (
        df_reviews
        .withColumnRenamed("title", "title_review")
        .withColumn("verified_purchase", F.col("verified_purchase").cast("int"))
        .withColumn("timestamp", (F.col("timestamp") / 1000).cast("timestamp"))
        .select("parent_asin", "rating", "helpful_vote", "verified_purchase", "timestamp")
    )

def process_category(category: str, spark):
    print(f"[i] Procesando categoría: {category}")
    meta_path = META_BASE.format(category)
    reviews_path = REVIEWS_BASE.format(category)

    df_meta = spark.read.parquet(meta_path)
    df_reviews = spark.read.parquet(reviews_path)

    df_meta_clean = clean_and_transform_meta(df_meta, category)
    df_reviews_clean = clean_and_transform_reviews(df_reviews)

    return df_meta_clean.join(df_reviews_clean, on="parent_asin", how="inner")

def run_etl():
    spark = create_spark_session()
    all_dfs = []

    for category in CATEGORIES:
        df = process_category(category, spark)
        all_dfs.append(df)

    df_final = all_dfs[0]
    for df in all_dfs[1:]:
        df_final = df_final.unionByName(df)

    df_final.write.mode("overwrite").parquet(OUTPUT_PATH)
    print(f"[✓] Dataset final guardado en: {OUTPUT_PATH}")
    spark.stop()

def main():
    script_name = os.path.splitext(os.path.basename(__file__))[0]
    redirect_output(script_name)

    # Base name para el ID de ejecución
    run_base = "ETL_Join_Meta_+_Reviews"
    run_id = get_run_id(run_base)

    # Fake input para el estado (no afecta métricas)
    fake_input = f"{run_base}__run{run_id}.parquet"
    write_state("ETL Join Meta + Reviews", input_file=fake_input)

    total_start = time.time()

    def full_etl():
        run_etl()
        return None

    measure_time_with_run_id(
        operation_name="ETL Join Meta + Reviews",
        input_path=f"data/silver/amazon2023/{fake_input}",  # lógico
        func=full_etl,
        output_path=OUTPUT_PATH,
        script_name=script_name,
        log_output=False,
        run_id=run_id
    )

    total_duration = time.time() - total_start

    # Añadir tiempo total
    report_path = os.path.join("data/report", script_name, f"{script_name}_report.md")
    with open(report_path, "a", encoding="utf-8") as f:
        f.write(f"\n## Tiempo total del script\n- {total_duration:.2f} segundos\n")

    clear_state()

if __name__ == "__main__":
    main()
