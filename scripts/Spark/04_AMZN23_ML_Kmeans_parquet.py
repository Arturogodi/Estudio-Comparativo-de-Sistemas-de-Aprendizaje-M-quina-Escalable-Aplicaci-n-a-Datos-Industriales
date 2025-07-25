import os
import time
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.sql.functions import col
from pyspark.sql.types import NumericType
from pyspark import StorageLevel

from template import (
    create_spark_session,
    measure_time_with_run_id,
    redirect_output,
    write_state,
    clear_state,
    get_run_id
)

INPUT_PATH = "data/gold/amazon2023/amzn23_model_data.parquet"
OUTPUT_PATH = "data/gold/amazon2023/amzn23_kmeans_clusters.parquet"


def kmeans_clustering(spark):
    df = spark.read.parquet(INPUT_PATH)
    print(f"[i] Dataset cargado con {df.count()} filas")

    num_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, NumericType)]
    if not num_cols:
        raise ValueError("No se encontraron columnas numéricas válidas para clustering.")
    print(f"[i] Columnas numéricas seleccionadas: {num_cols}")

    assembler = VectorAssembler(inputCols=num_cols, outputCol="features_unscaled")
    df_assembled = assembler.transform(df)

    scaler = StandardScaler(inputCol="features_unscaled", outputCol="features", withStd=True, withMean=True)
    scaler_model = scaler.fit(df_assembled)
    df_scaled = scaler_model.transform(df_assembled).persist(StorageLevel.MEMORY_AND_DISK)

    best_k = None
    best_score = -1
    best_model = None
    evaluator = ClusteringEvaluator(featuresCol="features", metricName="silhouette", distanceMeasure="squaredEuclidean")

    print("[i] Iniciando búsqueda de k...")
    for k in range(5, 6):
        print(f"   → Probando k={k}")
        kmeans = KMeans(featuresCol="features", k=k, seed=42)
        model = kmeans.fit(df_scaled)
        predictions = model.transform(df_scaled)
        score = evaluator.evaluate(predictions)
        print(f"      Silhouette Score: {score:.4f}")

        if score > best_score:
            best_score = score
            best_k = k
            best_model = model

    print(f"\n[✓] Mejor k encontrado: {best_k} con Silhouette Score={best_score:.4f}")

    final_predictions = best_model.transform(df_scaled)
    final_predictions.select("prediction", "features").write.mode("overwrite").parquet(OUTPUT_PATH)
    print(f"[✓] Resultado de clustering guardado en: {OUTPUT_PATH}")
    df_scaled.unpersist()


def main():
    script_name = os.path.splitext(os.path.basename(__file__))[0]
    redirect_output(script_name)

    run_base = "KMeans_Clustering_Amazon23"
    run_id = get_run_id(run_base)
    fake_input = f"{run_base}__run{run_id}.parquet"

    write_state("KMeans Clustering Amazon23", input_file=fake_input)

    spark = create_spark_session()
    total_start = time.time()

    measure_time_with_run_id(
        operation_name="KMeans Clustering Amazon23",
        input_path=f"data/gold/amazon2023/{fake_input}",
        func=lambda: kmeans_clustering(spark),
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
