from dask.distributed import Client
import dask.dataframe as dd
import pandas as pd
import time

# Conexión al scheduler
client = Client("tcp://scheduler:8786")
print("[✓] Conectado al scheduler")
print(client)

# Crear DataFrame de prueba con 10 millones de filas y 4 particiones
df = dd.from_pandas(
    pd.DataFrame({
        'a': range(10_000_000),
        'b': range(10_000_000, 20_000_000)
    }),
    npartitions=4
)

# Computación distribuida
start = time.time()
result = df['a'].sum().compute()
end = time.time()

print(f"\n[✓] Resultado de la suma: {result}")
print(f"[i] Tiempo total: {end - start:.2f} segundos")
