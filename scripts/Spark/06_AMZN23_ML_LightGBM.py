import os
import time
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import BinaryClassificationEvaluator
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
OUTPUT_PATH = "data/gold/amazon2023/amzn23_gbt_predictions_fast.parquet"
TARGET_COLUMN = "verified_purchase"

def gbt_classification_pipeline(spark):
    df = spark.read.parquet(INPUT_PATH)
    print(f"[i] Dataset cargado con {df.count()} filas")

    # Selección de columnas numéricas
    num_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, NumericType) and f.name != TARGET_COLUMN]
    if not num_cols:
        raise ValueError("No hay columnas numéricas válidas.")
    print(f"[i] Usando columnas: {num_cols} → label: {TARGET_COLUMN}")

    # Ensamblar features
    assembler = VectorAssembler(inputCols=num_cols, outputCol="features")
    df_vector = assembler.transform(df).select("features", TARGET_COLUMN)

    # Split train/test
    train, test = df_vector.randomSplit([0.8, 0.2], seed=42)
    train.persist(StorageLevel.MEMORY_AND_DISK)
    test.persist(StorageLevel.MEMORY_AND_DISK)

    # Entrenamiento rápido con menos iteraciones
    gbt = GBTClassifier(featuresCol="features", labelCol=TARGET_COLUMN, maxIter=1, maxDepth=5)
    model = gbt.fit(train)

    # Evaluación final
    predictions = model.transform(test)
    evaluator = BinaryClassificationEvaluator(labelCol=TARGET_COLUMN, rawPredictionCol="rawPrediction", metricName="areaUnderROC")
    auc = evaluator.evaluate(predictions)

    print(f"[✓] AUC en validación: {auc:.4f}")

    # Guardar predicciones
    predictions.select("prediction", "probability", TARGET_COLUMN).write.mode("overwrite").parquet(OUTPUT_PATH)
    print(f"[✓] Predicciones guardadas en: {OUTPUT_PATH}")

    train.unpersist()
    test.unpersist()
    return auc

def main():
    script_name = os.path.splitext(os.path.basename(__file__))[0]
    redirect_output(script_name)

    run_base = "GBT_Classification_Amazon23_Fast"
    run_id = get_run_id(run_base)
    fake_input = f"{run_base}__run{run_id}.parquet"

    write_state("GBT Fast Classification", input_file=fake_input)

    spark = create_spark_session()
    total_start = time.time()

    measure_time_with_run_id(
        operation_name="GBT Classification Amazon23 Fast",
        input_path=INPUT_PATH,
        func=lambda: gbt_classification_pipeline(spark),
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
