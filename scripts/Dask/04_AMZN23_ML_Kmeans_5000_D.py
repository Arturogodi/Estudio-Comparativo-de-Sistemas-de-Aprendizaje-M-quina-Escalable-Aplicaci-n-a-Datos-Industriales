import os
import time
import numpy as np
import dask
import dask.dataframe as dd
from dask_ml.preprocessing import StandardScaler
from dask_ml.cluster import KMeans
from sklearn.metrics import silhouette_score
from template_dask import (
    create_dask_client,
    measure_time_with_run_id_dask,
    redirect_output_dask,
    write_state_dask,
    clear_state_dask,
    get_run_id_dask
)

INPUT_PATH = "data/gold/amazon2023/amzn23_model_data.parquet"
OUTPUT_PATH = "data/gold/amazon2023/amzn23_kmeans_clusters_sampled.parquet"
K_RANGE = range(5, 6)

def kmeans_clustering():
    client = create_dask_client()
    print(f"[✓] Conectado al clúster Dask con {len(client.scheduler_info()['workers'])} workers")

    # Leer y particionar
    df = dd.read_parquet(INPUT_PATH)
    df = df.repartition(npartitions=40)
    print(f"[i] Dataset reparticionado a {df.npartitions} particiones")

    # Columnas numéricas
    num_cols = [col for col, dtype in df.dtypes.items() if np.issubdtype(dtype, np.number)]
    print(f"[i] Columnas numéricas: {num_cols}")

    # Muestreo
    df_sample = df[num_cols].dropna().sample(frac=0.01, random_state=42).persist()
    sample_count = df_sample.shape[0].compute()
    print(f"[i] Muestra con {sample_count} filas")

    # Escalado
    scaler = StandardScaler()
    df_scaled = scaler.fit_transform(df_sample)

    # KMeans
    best_k = None
    best_score = -1
    best_model = None
    print("[i] Buscando mejor k...")

    for k in K_RANGE:
        print(f"   → k={k}")
        model = KMeans(n_clusters=k)
        model.fit(df_scaled)
        try:
            labels = model.labels_.compute()
            score = silhouette_score(df_scaled.compute(), labels)
            print(f"      Silhouette Score: {score:.4f}")
            if score > best_score:
                best_k = k
                best_score = score
                best_model = model
        except Exception as e:
            print(f"      Error al calcular silhouette: {e}")

    print(f"[✓] Mejor k: {best_k} con score: {best_score:.4f}")

    # Predecir y guardar
    clusters = best_model.predict(df_scaled)
    df_result = df_sample.assign(cluster=clusters)
    df_result.to_parquet(OUTPUT_PATH, overwrite=True)
    print(f"[✓] Clusters guardados en {OUTPUT_PATH}")
    client.close()

def main():
    script_name = os.path.splitext(os.path.basename(__file__))[0]
    redirect_output_dask(script_name)

    base_name = "KMeansSampled_Amazon23"
    run_id = get_run_id_dask(base_name)
    fake_input = f"{base_name}__run{run_id}.parquet"

    write_state_dask("KMeans Clustering Sampled", fake_input, run_id)

    total_start = time.time()
    measure_time_with_run_id_dask(
        operation_name="KMeans Clustering Sampled",
        input_path=fake_input,
        func=kmeans_clustering,
        output_path=OUTPUT_PATH,
        run_id=run_id,
        log_output=False,  # El log ya fue redirigido antes
        script_name=script_name
    )
    total_duration = time.time() - total_start

    report_path = f"data/report/{script_name}/{script_name}_report.md"
    with open(report_path, "a", encoding="utf-8") as f:
        f.write(f"\n## Tiempo total del script\n- {total_duration:.2f} segundos\n")

    clear_state_dask()

if __name__ == "__main__":
    main()
