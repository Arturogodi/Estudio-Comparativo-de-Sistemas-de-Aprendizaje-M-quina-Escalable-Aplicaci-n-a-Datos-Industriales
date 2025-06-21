import os
import time
import numpy as np
import dask.dataframe as dd
from dask_ml.preprocessing import StandardScaler
from dask_ml.cluster import KMeans
from dask.distributed import Client, wait
from sklearn.metrics import silhouette_score

# Configuración
INPUT_PATH = "data/gold/amazon2023/amzn23_model_data.parquet"
OUTPUT_PATH = "data/gold/amazon2023/amzn23_kmeans_clusters_sampled.parquet"
K_RANGE = range(5, 6)

def create_dask_client() -> Client:
    return Client("tcp://scheduler:8786")

def kmeans_clustering():
    client = create_dask_client()
    print(f"[✓] Conectado al clúster Dask con {len(client.scheduler_info()['workers'])} workers")

    df = dd.read_parquet(INPUT_PATH)
    df = df.repartition(npartitions=40)
    print(f"[i] Dataset reparticionado a {df.npartitions} particiones")

    num_cols = [col for col, dtype in df.dtypes.items() if "float" in str(dtype) or "int" in str(dtype)]

    print(f"[i] Columnas numéricas seleccionadas: {num_cols}")
    df = df[num_cols]

    # Obtener muestra segura para entrenamiento
    sample = df.sample(frac=0.01, random_state=42).head(5000, compute=True)
    X_sample = np.stack([sample[col] for col in num_cols], axis=1)

    print("[i] Iniciando búsqueda de k...")
    best_k = None
    best_score = -1
    best_model = None

    for k in K_RANGE:
        print(f"   → Probando k={k}")
        model = KMeans(n_clusters=k, random_state=42)
        model.fit(X_sample)
        labels = model.predict(X_sample)
        score = silhouette_score(X_sample, labels)
        print(f"      Silhouette Score: {score:.4f}")
        if score > best_score:
            best_k = k
            best_score = score
            best_model = model

    print(f"[✓] Mejor k encontrado: {best_k} con Silhouette Score: {best_score:.4f}")

    scaler = StandardScaler()
    df_scaled = scaler.fit_transform(df)

    def predict_partition(partition):
        X = np.stack([partition[col] for col in num_cols], axis=1)
        cluster = best_model.predict(X)
        partition["cluster"] = cluster
        return partition

    df_result = df_scaled.map_partitions(predict_partition, meta=df.assign(cluster=0).head(0))
    df_result = df_result.repartition(npartitions=100).persist()
    wait(df_result)

    df_result.to_parquet(OUTPUT_PATH, overwrite=True)
    print(f"[✓] Resultado guardado en {OUTPUT_PATH}")

def main():
    start = time.time()
    print("[✓] Ejecutando KMeans Clustering Sampled...")
    kmeans_clustering()
    duration = time.time() - start
    print(f"[✓] Tiempo total: {duration:.2f} segundos")

if __name__ == "__main__":
    main()
