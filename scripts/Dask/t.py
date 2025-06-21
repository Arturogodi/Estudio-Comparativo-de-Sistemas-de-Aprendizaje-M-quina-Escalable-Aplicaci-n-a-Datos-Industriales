from dask.distributed import Client

client = Client("tcp://scheduler:8786")  # O sin parámetro si es local

workers_info = client.scheduler_info()["workers"]

for address, info in workers_info.items():
    memory_gb = info["memory_limit"] / (1024**3)
    print(f"Worker {address} → RAM disponible: {memory_gb:.2f} GB")
