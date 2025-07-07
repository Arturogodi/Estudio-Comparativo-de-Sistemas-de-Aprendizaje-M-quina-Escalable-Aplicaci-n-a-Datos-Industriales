import os
import time
import ray
import ray.data
import pandas as pd
from template_ray import (
    write_state,
    clear_state,
    get_run_id,
    redirect_output,
    measure_time_with_run_id_ray,
)

INPUT_PATH = "/data/gold/amazon2023/amzn23_model_data.parquet"
OUTPUT_PATH = "/data/gold/amazon2023/amzn23_price_category_counts"

def process_partition(batch: pd.DataFrame) -> pd.DataFrame:
    # Filtra y cuenta en la partición
    filtered = batch[batch["price"] > 0]
    return filtered.groupby("main_category").size().reset_index(name="count")

def process_data():
    print("[i] Iniciando procesamiento Ray optimizado...")

    # Leer con paralelismo explícito
    ds = ray.data.read_parquet(INPUT_PATH, parallelism=100)
    print(f"[i] Dataset cargado con {ds.count()} filas")

    # Filtrar y contar por partición
    partial = ds.map_batches(process_partition, batch_format="pandas")
    
    # Agregar resultados finales
    result = partial.groupby("main_category").sum("count")

    # Guardar resultado
    result = result.repartition(50).materialize()
    result.write_parquet(OUTPUT_PATH)
    print(f"[✓] Resultado escrito en {OUTPUT_PATH}")

    return result.count()

def main():
    script_name = os.path.splitext(os.path.basename(__file__))[0]
    redirect_output(script_name)

    ray.init(address="auto", ignore_reinit_error=True)

    run_base = "Ray_Read_Filter_Write_Optim"
    run_id = get_run_id(run_base)
    fake_input = f"{run_base}__run{run_id}.parquet"

    write_state("Ray Read Filter Write", input_file=fake_input, run_id=run_id)

    total_start = time.time()

    measure_time_with_run_id_ray(
        operation_name="Ray Read Filter Write",
        input_path=INPUT_PATH,
        func=process_data,
        output_path=OUTPUT_PATH,
        run_id=run_id,
        log_output=False,
        script_name=script_name,
    )

    total_duration = time.time() - total_start
    report_path = f"data/report/{script_name}/{script_name}_report.md"
    os.makedirs(os.path.dirname(report_path), exist_ok=True)
    with open(report_path, "a", encoding="utf-8") as f:
        f.write(f"\n## Tiempo total del script\n- {total_duration:.2f} segundos\n")

    clear_state()
    ray.shutdown()

if __name__ == "__main__":
    main()
