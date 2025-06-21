import ray
import ray.data

ray.init(ignore_reinit_error=True)

# Ruta CSV (ajusta si es .csv o .csv.gz)
INPUT_PATH = "/data/gold/amazon2023/amzn23_model_data_csv"

# Leer una pequeña muestra
ds = ray.data.read_csv(INPUT_PATH)

# Mostrar columnas
print("[i] Columnas del dataset:")
print(ds.schema().names)

# Mostrar primera fila
first_row = ds.take(1)[0]
print("\n[i] Ejemplo de fila:")
for k, v in first_row.items():
    print(f"  {k}: {v}")

# Comprobar si alguna columna parece binaria
print("\n[i] Posibles columnas binarias (solo 0 y 1):")
for col in ds.schema().names:
    uniques = ds.select(col).distinct().take(3)  # máximo 3 únicos
    values = set(r[col] for r in uniques)
    if values.issubset({0, 1}):
        print(f"  → {col} ({values})")
