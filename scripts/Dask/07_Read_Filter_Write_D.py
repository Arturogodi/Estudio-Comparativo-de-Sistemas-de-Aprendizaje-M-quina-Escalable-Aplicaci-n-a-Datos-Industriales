import os
import time
import dask.dataframe as dd
from dask.distributed import Client
from template_dask import (
    create_dask_client,
    measure_time_with_run_id_dask,
    get_run_id_dask,
    write_state_dask,
    clear_state_dask
)

INPUT_PATH = "data/gold/amazon2023/amzn23_model_data.parquet"
OUTPUT_PATH = "data/gold/amazon2023/amzn23_price_category_counts_dask.parquet"

def process_data():
    print("[i] Iniciando procesamiento Dask...")

    # Cargar datos
    df = dd.read_parquet(INPUT_PATH)
    print(f"[i] Dataset cargado con {df.shape[0].compute()} filas")

    # Filtrar precios > 0
    filtered = df[df["price"] > 0]
    print(f"[i] Filtrado: {filtered.shape[0].compute()} filas con precio > 0")

    # Agrupar y contar
    grouped = filtered.groupby("main_category").size().reset_index()
    grouped = grouped.rename(columns={0: "count"})
    print(f"[i] Resultado: {grouped.shape[0].compute()} categorías")

    # Guardar resultados
    grouped.to_parquet(OUTPUT_PATH, overwrite=True)
    print(f"[✓] Resultado guardado en: {OUTPUT_PATH}")

    return grouped.shape[0].compute()

def main():
    script_name = os.path.splitext(os.path.basename(__file__))[0]
    run_base = "Read_Filter_Write_Dask"
    run_id = get_run_id_dask(run_base)
    fake_input = f"{run_base}__run{run_id}.parquet"

    write_state_dask("Read Filter Write Dask", input_file=fake_input, run_id=run_id)
    client = create_dask_client()

    total_start = time.time()
    measure_time_with_run_id_dask(
        operation_name="Read Filter Write Dask",
        input_path=INPUT_PATH,
        func=process_data,
        output_path=OUTPUT_PATH,
        run_id=run_id,
        script_name=script_name,
        log_output=False
    )
    total_duration = time.time() - total_start

    report_path = f"data/report/{script_name}/{script_name}_report.md"
    os.makedirs(os.path.dirname(report_path), exist_ok=True)
    with open(report_path, "a", encoding="utf-8") as f:
        f.write(f"\n## Tiempo total del script\n- {total_duration:.2f} segundos\n")

    client.close()
    clear_state_dask()

if __name__ == "__main__":
    main()
