import os
import os
import ray
import ray.data
from template_ray import (
    measure_time_with_run_id_ray,
    get_run_id,
    redirect_output,
    write_state,
    clear_state
)

# Configuración
INPUT_PATH = "/data/gold/amazon2023/amzn23_model_data.parquet"
OUTPUT_PATH = "/data/gold/amazon2023/amzn23_price_category_counts_ray.parquet"
OP_NAME = "Read Filter Write Ray"
SCRIPT_NAME = os.path.basename(__file__).split('.')[0]

def process_data():
    # Inicializar Ray
    ray.init(ignore_reinit_error=True)
    for node in ray.nodes():
        print(f"Node: {node['NodeManagerAddress']} - Resources: {node['Resources']}")
    
    # Leer el dataset
    ds = ray.data.read_parquet(INPUT_PATH)
    print(f"[i] Dataset cargado con {ds.count()} filas")
    
    return ds.count()

def main():
    try:
        script_name = os.path.splitext(os.path.basename(__file__))[0]
        redirect_output(script_name)

        run_base = "Read_Filter_Write_Ray"
        run_id = get_run_id(run_base)
        fake_input = f"{run_base}__run{run_id}.parquet"

        write_state("Read Filter Write Ray", input_file=fake_input, run_id=run_id)

        result = measure_time_with_run_id_ray(
            operation_name="Read Filter Write Ray",
            input_path=INPUT_PATH,
            func=process_data,
            output_path=OUTPUT_PATH,
            script_name=script_name,
            log_output=False,
            run_id=run_id
        )

        clear_state()
        return result
    except Exception as e:
        print(f"[!] Error en el procesamiento: {str(e)}")
        clear_state()
        raise

if __name__ == "__main__":
    main()
