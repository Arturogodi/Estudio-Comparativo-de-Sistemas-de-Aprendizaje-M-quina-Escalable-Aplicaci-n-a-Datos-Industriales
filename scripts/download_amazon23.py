import os
from datasets import load_dataset

# Categorías que quieres descargar (ajustable)
categories = [
    "Books",
    "Electronics",
    "Clothing_Shoes_and_Jewelry",
    "Home_and_Kitchen",
    "Toys_and_Games"
]

# Carpeta de destino
output_dir = "data/bronze/amazon2023"
os.makedirs(output_dir, exist_ok=True)

for category in categories:
    print(f"Descargando reviews de {category}...")
    reviews = load_dataset(
        "McAuley-Lab/Amazon-Reviews-2023",
        f"raw_review_{category}",
        split="full",
        trust_remote_code=True
    )
    reviews_path = os.path.join(output_dir, f"reviews_{category}.parquet")
    print(f"Guardando en {reviews_path}...")
    reviews.to_parquet(reviews_path)

    print(f"Descargando metadata de {category}...")
    meta = load_dataset(
        "McAuley-Lab/Amazon-Reviews-2023",
        f"raw_meta_{category}",
        split="full",
        trust_remote_code=True
    )
    meta_path = os.path.join(output_dir, f"meta_{category}.parquet")
    print(f"Guardando en {meta_path}...")
    meta.to_parquet(meta_path)

print("Descarga y guardado completo.")
