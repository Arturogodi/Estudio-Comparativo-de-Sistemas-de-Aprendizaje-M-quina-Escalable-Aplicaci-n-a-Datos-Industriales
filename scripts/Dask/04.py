import os
import time
import dask.dataframe as dd
from dask_ml.cluster import KMeans
from dask_ml.preprocessing import StandardScaler
from sklearn.metrics import silhouette_score
from template_dask import (
    create_dask_client,
    measure_time_with_run_id_dask,
    get_run_id,
    redirect_output,
    write_state,
    clear_state
)

INPUT_PATH = "data/gold/amazon2023/amzn23_model_data.parquet"
OUTPUT_PATH = "data/gold/amazon2023/amzn23_kmeans_clusters_dask.parquet"


def kmeans_clustering():
    client = create_dask_client()
    print(f"[✓] Conectado al clúster Dask con {len(client.nthreads())} workers")

    # Leer parquet
    df = dd.read_parquet(INPUT_PATH)

    # Filtrar columnas numéricas y eliminar nulos
    df = df.select_dtypes(include=["number"]).dropna()
    df = df.persist()
    df = df.repartition(partition_size="100MB")
    print(f"[i] Dataset reparticionado a {df.npartitions} particiones")

    # Extraer columnas
    num_cols = df.columns.tolist()
    print(f"[i] Columnas numéricas seleccionadas: {num_cols}")

    # Escalar: separado en fit y transform para evitar grafo grande
    scaler = StandardScaler()
    scaler = scaler.fit(df)
    X_scaled = scaler.transform(df)

    # Clustering
    print(f"[i] Ejecutando KMeans con k=5")
    kmeans = KMeans(n_clusters=5, random_state=42)
    kmeans.fit(X_scaled)
    labels = kmeans.predict(X_scaled)

    # Evaluación con muestra
    print(f"[i] Calculando Silhouette Score con muestra del 1%")
    X_sample = X_scaled.sample(frac=0.01, random_state=42).compute()
    y_sample = labels.sample(frac=0.01, random_state=42).compute()
    score = silhouette_score(X_sample, y_sample)
    print(f"[✓] Silhouette Score (sample): {score:.4f}")

    # Guardar resultados
    df_result = df.assign(prediction=labels)
    df_result.to_parquet(OUTPUT_PATH, write_index=False, engine="pyarrow", overwrite=True)
    print(f"[✓] Resultado guardado en: {OUTPUT_PATH}")

    client.close()


def main():
    script_name = os.path.splitext(os.path.basename(__file__))[0]
    redirect_output(script_name)

    run_base = "KMeans_Clustering_Amazon23"
    run_id = get_run_id(run_base)
    fake_input = f"{run_base}__run{run_id}.parquet"

    write_state("KMeans Clustering Amazon23", input_file=fake_input, run_id=run_id)

    total_start = time.time()
    measure_time_with_run_id_dask(
        operation_name="KMeans Clustering Amazon23",
        input_path=f"data/gold/amazon2023/{fake_input}",
        func=kmeans_clustering,
        output_path=OUTPUT_PATH,
        run_id=run_id,
        log_output=False,
        script_name=script_name
    )
    total_duration = time.time() - total_start

    report_path = f"data/report/{script_name}/{script_name}_report.md"
    os.makedirs(os.path.dirname(report_path), exist_ok=True)
    with open(report_path, "a", encoding="utf-8") as f:
        f.write(f"\n## Tiempo total del script\n- {total_duration:.2f} segundos\n")

    clear_state()


if __name__ == "__main__":
    main()
