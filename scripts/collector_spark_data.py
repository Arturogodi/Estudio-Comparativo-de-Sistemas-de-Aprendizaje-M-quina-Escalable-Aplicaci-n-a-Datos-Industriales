import os
import json
import pandas as pd

# pyarrow solo si está disponible
try:
    import pyarrow.parquet as pq
except ImportError:
    pq = None

# Rutas
metrics_dir = "data/metrics"
reports_dir = "data/report"
parquet_dirs = [
    "data/bronze/amazon2023",
    "data/silver/amazon2023",
    "data/gold/amazon2023"
]
output_json = "data/analysis/info_spark.json"
os.makedirs("data/analysis", exist_ok=True)

# Cargar métricas CSV
def cargar_metricas(metrics_dir):
    metricas = {}
    for root, _, files in os.walk(metrics_dir):
        for file in files:
            if file.endswith(".csv") and "__run" in file:
                partes = file.split("__")
                if len(partes) >= 3:
                    script_key = partes[0].replace("EDA_", "")
                    run = partes[-1].replace("run", "").replace(".csv", "")
                    df = pd.read_csv(os.path.join(root, file))
                    metricas.setdefault(script_key, []).append({
                        "run": int(run),
                        "data": df.to_dict(orient="records")
                    })
    return metricas

# Cargar reportes .md
def cargar_reportes(reports_dir):
    reportes = {}
    for root, _, files in os.walk(reports_dir):
        for file in files:
            if file.endswith(".md"):
                key = os.path.splitext(file)[0].split("_report")[0].replace("01_", "")
                with open(os.path.join(root, file), encoding="utf-8") as f:
                    reportes[key] = f.read()
    return reportes

# Cargar información de parquets
def cargar_info_parquets(parquet_dirs):
    if pq is None:
        print("pyarrow no está instalado.")
        return []

    parquet_info = []
    for d in parquet_dirs:
        for file in os.listdir(d):
            if file.endswith(".parquet"):
                path = os.path.join(d, file)
                try:
                    table = pq.read_table(path)
                    schema = str(table.schema)
                    n_rows = table.num_rows
                    n_cols = len(table.schema)
                    size_bytes = os.path.getsize(path)
                    parquet_info.append({
                        "path": path,
                        "rows": n_rows,
                        "cols": n_cols,
                        "size_mb": round(size_bytes / (1024 ** 2), 2),
                        "schema": schema
                    })
                except Exception as e:
                    print(f"Error leyendo {path}: {e}")
    return parquet_info

if __name__ == "__main__":
    data_unificada = {
        "metricas": cargar_metricas(metrics_dir),
        "reportes": cargar_reportes(reports_dir),
        "parquets": cargar_info_parquets(parquet_dirs)
    }

    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(data_unificada, f, indent=2, ensure_ascii=False)

    print(f"Archivo generado: {output_json}")
