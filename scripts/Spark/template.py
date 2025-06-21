import sys
import os
import time
import json
from datetime import datetime
from pyspark.sql import SparkSession
from typing import Optional, Callable


def write_state(operation_name: str, input_file: Optional[str] = None, state_file: str = "data/state/operation_state.json") -> None:
    """
    Escribe el estado actual de la operación
    
    Args:
        operation_name (str): Nombre de la operación
        input_file (Optional[str]): Nombre del archivo de entrada
        state_file (str): Ruta al archivo de estado
    """
    os.makedirs(os.path.dirname(state_file), exist_ok=True)
    state = {
        "current_operation": operation_name,
        "start_time": datetime.now().strftime("%Y%m%d_%H%M%S")
    }
    if input_file:
        state["input_file"] = os.path.basename(input_file)
    with open(state_file, "w") as f:
        json.dump(state, f)

def write_state_for_etl(operation_name: str, logical_input_name: str) -> None:
    """
    Variante de write_state para ETL sin input real (como joins globales)
    Args:
        operation_name (str): Nombre de operación (ej: "ETL Join Meta + Reviews")
        logical_input_name (str): Nombre lógico del input (ej: "joint_dataset.parquet")
    """
    write_state(operation_name=operation_name, input_file=logical_input_name)

def clear_state(state_file: str = "data/state/operation_state.json") -> None:
    """
    Limpia el estado actual
    
    Args:
        state_file (str): Ruta al archivo de estado
    """
    with open(state_file, "w") as f:
        json.dump({"current_operation": "idle", "start_time": None}, f)


def create_spark_session() -> SparkSession:
    """
    Crea una sesión Spark configurada
    
    Returns:
        SparkSession: Sesión Spark configurada
    """
    return SparkSession.builder \
        .appName("Spark Benchmark") \
        .master("spark://spark-master:7077") \
        .config("spark.executor.memory", "9g") \
        .config("spark.executor.cores", "2") \
        .config("spark.cores.max", "10") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.sql.debug.maxToStringFields", "100") \
        .getOrCreate()


def get_file_size(path: str) -> float:
    """
    Calcula el tamaño total de un archivo o directorio en GB
    
    Args:
        path (str): Ruta al archivo o directorio
        
    Returns:
        float: Tamaño en GB
    """
    if not os.path.exists(path):
        return 0
    
    if os.path.isfile(path):
        return os.path.getsize(path) / (1024 * 1024 * 1024)
    elif os.path.isdir(path):
        return sum(os.path.getsize(os.path.join(root, file)) 
                   for root, _, files in os.walk(path)
                   for file in files) / (1024 * 1024 * 1024)
    return 0


def redirect_output(script_name: str) -> str:
    """
    Redirige stdout y stderr a un archivo de log
    
    Args:
        script_name (str): Nombre del script
        
    Returns:
        str: Ruta al archivo de log
    """
    log_dir = os.path.join("data/report", script_name)
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, f"{script_name}_output.log")

    if os.path.exists(log_path):
        os.remove(log_path)  

    sys.stdout = open(log_path, "w", encoding="utf-8")  
    sys.stderr = sys.stdout
    return log_path



def get_run_id(base_name: str, output_dir: str = "data/metrics") -> int:
    """
    Obtiene el siguiente ID de ejecución para un dataset
    
    Args:
        base_name (str): Nombre base del dataset
        output_dir (str): Directorio de métricas
        
    Returns:
        int: Número de ejecución
    """
    existing = [f for f in os.listdir(output_dir) if f.startswith(base_name)]
    return len(existing) + 1


def measure_time(
    operation_name: str,
    input_path: str,
    func: Callable,
    output_path: Optional[str] = None,
    run_id: Optional[int] = None,
    log_output: bool = False,
    script_name: Optional[str] = None
) -> object:
    print(f"\n=== {operation_name} ===")
    write_state(operation_name, input_file=input_path)

    if log_output:
        if script_name is None:
            script_name = os.path.basename(__file__).split('.')[0]
        log_path = redirect_output(script_name)
        print(f"[i] Log redirigido a: {log_path}")

    start_time = time.time()
    result = func()
    duration = time.time() - start_time
    clear_state()

    # Aquí es donde se usa script_name para el nombre del archivo
    if script_name is None:
        script_name = os.path.basename(__file__).split('.')[0]

    report_dir = os.path.join("data/report", script_name)
    os.makedirs(report_dir, exist_ok=True)
    report_path = os.path.join(report_dir, f"{script_name}_report.md")

    input_size_gb = get_file_size(input_path)
    output_size_gb = get_file_size(output_path) if output_path else 0

    label = f"{operation_name} - {os.path.basename(input_path)}"
    if run_id is not None:
        label += f" [Run {run_id}]"

    with open(report_path, "a", encoding="utf-8") as f:
        f.write(f"## {label}\n")
        f.write(f"- Tamaño entrada: {input_size_gb:.2f} GB\n")
        if output_path:
            f.write(f"- Tamaño salida: {output_size_gb:.2f} GB\n")
        f.write(f"- Tiempo: {duration:.2f} s\n\n")

    print(f"[✓] Reporte guardado en: {report_path}")
    print(f"Tiempo de ejecución: {duration:.2f} segundos")
    return result

def measure_time_with_run_id(
    operation_name: str,
    input_path: str,
    func: Callable,
    output_path: Optional[str] = None,
    run_id: Optional[int] = None,
    log_output: bool = False,
    script_name: Optional[str] = None
) -> object:
    """
    Variante de measure_time que ignora el basename del input y usa solo run_id en el label.
    """
    print(f"\n=== {operation_name} ===")
    write_state(operation_name, input_file=input_path)

    if log_output:
        if script_name is None:
            script_name = os.path.basename(__file__).split('.')[0]
        log_path = redirect_output(script_name)
        print(f"[i] Log redirigido a: {log_path}")

    start_time = time.time()
    result = func()
    duration = time.time() - start_time
    clear_state()

    if script_name is None:
        script_name = os.path.basename(__file__).split('.')[0]

    report_dir = os.path.join("data/report", script_name)
    os.makedirs(report_dir, exist_ok=True)
    report_path = os.path.join(report_dir, f"{script_name}_report.md")

    input_size_gb = get_file_size(input_path)
    output_size_gb = get_file_size(output_path) if output_path else 0

    label = f"{operation_name} -"
    if run_id is not None:
        label += f" [Run {run_id}]"

    with open(report_path, "a", encoding="utf-8") as f:
        f.write(f"## {label}\n")
        f.write(f"- Tamaño entrada: {input_size_gb:.2f} GB\n")
        if output_path:
            f.write(f"- Tamaño salida: {output_size_gb:.2f} GB\n")
        f.write(f"- Tiempo: {duration:.2f} s\n\n")

    print(f"[✓] Reporte guardado en: {report_path}")
    print(f"Tiempo de ejecución: {duration:.2f} segundos")
    return result


def main() -> None:
    """
    Función principal que debe ser implementada por cada script específico
    """
    raise NotImplementedError("Implementar la función main en el script específico")


if __name__ == "__main__":
    main()
