import os
import time
import json
from datetime import datetime
import ray
from ray.data import read_parquet
from ray.data.dataset import Dataset
from typing import Dict, Any
from ray.data.block import BlockAccessor
from ray.util.annotations import DeveloperAPI

# Utilidades ya definidas (si están en helpers.ray_utils.py)
from template_ray import create_ray_context, measure_time_with_run_id_ray

def normalize_record(row: Dict[str, Any]) -> Dict[str, Any]:
    try:
        # Normalización de arrays
        for key in ['features', 'description', 'categories']:
            if key in row and isinstance(row[key], list):
                row[f"{key}_length"] = len(row[key])
                row[f"{key}_as_list"] = row[key]

        # Campos específicos de 'images'
        if "images" in row and isinstance(row["images"], dict):
            for img_type in ["hi_res", "large", "thumb", "variant"]:
                val = row["images"].get(img_type)
                row[f"images_{img_type}_url"] = val[0] if isinstance(val, list) and val else None

        # Campos específicos de 'videos'
        if "videos" in row and isinstance(row["videos"], dict):
            for vfield in ["title", "url", "user_id"]:
                val = row["videos"].get(vfield)
                row[f"videos_{vfield}"] = val[0] if isinstance(val, list) and val else None

        # Normalización de 'details'
        if "details" in row and isinstance(row["details"], str):
            parsed = json.loads(row["details"])
            for field in ["Publisher", "Language", "Hardcover", "ISBN 10", "ISBN 13", "Item Weight", "Dimensions"]:
                key = field.lower().replace(" ", "_")
                row[f"details_{key}_exists"] = field in parsed
                row[f"details_{key}_value"] = parsed.get(field)
            del row["details"]

        return row

    except Exception as e:
        row["error"] = f"Failed normalization: {str(e)}"
        return row


def normalize_dataset_ray(input_path: str, output_path: str, script_name: str) -> Dataset:
    def execute():
        print(f"[i] Leyendo: {input_path}")
        ds = read_parquet(input_path)

        print(f"[i] Normalizando columnas...")
        ds = ds.map(normalize_record)

        print(f"[✓] Escribiendo en: {output_path}")
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        ds.write_parquet(output_path)
        return ds

    return measure_time_with_run_id_ray("Normalize Dataset", input_path, execute, output_path=output_path, script_name=script_name, log_output=True)

def main():
    script_name = os.path.splitext(os.path.basename(__file__))[0]
    create_ray_context()
    total_start = time.time()

    files = [
        "meta_Books.parquet",
        "meta_Clothing_Shoes_and_Jewelry.parquet",
        "meta_Electronics.parquet",
        "meta_Home_and_Kitchen.parquet",
        "meta_Toys_and_Games.parquet"
    ]

    for file in files:
        input_path = f"/data/bronze/amazon2023/{file}"
        output_path = f"/data/silver/amazon2023/{file.replace('.parquet', '_normalized.parquet')}"
        normalize_dataset_ray(input_path, output_path, script_name)

    total_duration = time.time() - total_start
    report_path = os.path.join("data/report", script_name, f"{script_name}_report.md")
    with open(report_path, "a", encoding="utf-8") as f:
        f.write(f"\n## Tiempo total del script\n- {total_duration:.2f} segundos\n")

if __name__ == "__main__":
    main()
