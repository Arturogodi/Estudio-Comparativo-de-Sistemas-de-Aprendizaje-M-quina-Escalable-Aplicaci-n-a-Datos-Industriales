import os
import ray
import ray.data
import numpy as np
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import silhouette_score

from template_ray import measure_time_with_run_id_ray, get_run_id

# Inicializa Ray
ray.init(ignore_reinit_error=True)
for node in ray.nodes():
    print(f"Node: {node['NodeManagerAddress']} - Resources: {node['Resources']}")

# Configuración
INPUT_PATH = "/data/gold/amazon2023/amzn23_model_data.parquet"
OUTPUT_PATH = "/data/gold/amazon2023/amzn23_kmeans_clusters_ray.parquet"
OP_NAME = "KMeans Clustering Amazon23"
SCRIPT_NAME = os.path.basename(__file__).split('.')[0]
K_RANGE = range(5, 6)

def kmeans_clustering():
    ds = ray.data.read_parquet(INPUT_PATH)
    print(f"[i] Dataset cargado con {ds.count()} filas")

    # Obtener columnas numéricas
    batch_iter = ds.iter_batches(batch_size=5000, batch_format="numpy")
    first_batch = next(iter(batch_iter))
    num_cols = [col for col, arr in first_batch.items() if np.issubdtype(arr.dtype, np.number)]
    print(f"[i] Columnas numéricas: {num_cols}")
    ds = ds.select_columns(num_cols)

    # Escalar y entrenar sobre una muestra inicial
    X_train = np.stack([first_batch[col] for col in num_cols], axis=1)
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X_train)

    best_k = None
    best_score = -1
    best_model = None

    print("[i] Iniciando búsqueda de k...")
    for k in K_RANGE:
        print(f"   → Probando k={k}")
        model = KMeans(n_clusters=k, random_state=42).fit(X_scaled)
        labels = model.predict(X_scaled)
        score = silhouette_score(X_scaled, labels)
        print(f"      Silhouette Score: {score:.4f}")
        if score > best_score:
            best_score = score
            best_k = k
            best_model = model

    print(f"[✓] Mejor k: {best_k} con Silhouette Score: {best_score:.4f}")

    class TransformFn:
        def __init__(self):
            self.scaler = scaler
            self.kmeans = best_model
            self.columns = num_cols

        def __call__(self, batch):
            X = np.stack([batch[col] for col in self.columns], axis=1)
            X_scaled = self.scaler.transform(X)
            batch["cluster"] = self.kmeans.predict(X_scaled)
            return batch

    # USAR TODOS LOS RECURSOS: 1 actor por CPU disponible
    clustered = ds.map_batches(
        TransformFn,
        batch_format="numpy",
        batch_size=2000,
        concurrency=8,   # Número de tareas paralelas; ajusta según núcleos
        num_cpus=1
    )

    clustered.write_parquet(OUTPUT_PATH)
    print(f"[✓] Clusters guardados en {OUTPUT_PATH}")

# Main
if __name__ == "__main__":
    base_key = f"{OP_NAME.replace(' ', '_')}__{os.path.basename(INPUT_PATH).replace(' ', '_')}"
    run_id = get_run_id(base_key, output_dir="/data/metrics_ray")

    measure_time_with_run_id_ray(
        operation_name=OP_NAME,
        input_path=INPUT_PATH,
        func=kmeans_clustering,
        output_path=OUTPUT_PATH,
        run_id=run_id,
        log_output=True,
        script_name=SCRIPT_NAME
    )
