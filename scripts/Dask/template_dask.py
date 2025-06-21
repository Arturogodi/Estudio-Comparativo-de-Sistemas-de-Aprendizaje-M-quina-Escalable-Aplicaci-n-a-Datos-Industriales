import os
import json
import time
import sys
from datetime import datetime
from dask.distributed import Client

STATE_FILE_DASK = "data/state/operation_state_dask.json"
REPORT_DIR = "data/report"
METRICS_DIR = "data/metrics_dask"

def create_dask_client() -> Client:
    return Client("tcp://scheduler:8786")

def write_state_dask(operation_name: str, input_file: str, run_id: int, state_file: str = STATE_FILE_DASK):
    os.makedirs(os.path.dirname(state_file), exist_ok=True)
    with open(state_file, "w") as f:
        json.dump({
            "current_operation": operation_name,
            "start_time": datetime.now().strftime("%Y%m%d_%H%M%S"),
            "input_file": os.path.basename(input_file),
            "run_id": run_id
        }, f)

def clear_state_dask(state_file: str = STATE_FILE_DASK):
    with open(state_file, "w") as f:
        json.dump({"current_operation": "idle", "start_time": None}, f)

def get_run_id_dask(base_name: str, output_dir: str = METRICS_DIR) -> int:
    os.makedirs(output_dir, exist_ok=True)
    existing = [f for f in os.listdir(output_dir) if f.startswith(base_name)]
    return len(existing) + 1

def redirect_output_dask(script_name: str) -> str:
    log_path = os.path.join("data/logs", script_name, f"{script_name}.log")
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    sys.stdout = open(log_path, "w", encoding="utf-8")
    sys.stderr = sys.stdout
    return log_path

def get_file_size_gb(path: str) -> float:
    if not os.path.exists(path):
        return 0.0
    if os.path.isfile(path):
        return os.path.getsize(path) / (1024**3)
    total = 0
    for root, _, files in os.walk(path):
        for f in files:
            fp = os.path.join(root, f)
            total += os.path.getsize(fp)
    return total / (1024**3)

def measure_time_with_run_id_dask(
    operation_name: str,
    input_path: str,
    func,
    output_path: str = None,
    run_id: int = None,
    log_output: bool = False,
    script_name: str = None
):
    if script_name is None:
        script_name = os.path.splitext(os.path.basename(__file__))[0]

    if log_output:
        log_path = redirect_output_dask(script_name)
        print(f"[i] Log redirigido a: {log_path}")

    write_state_dask(operation_name, input_path, run_id)

    print(f"[✓] Ejecutando {operation_name}...")

    start = time.time()
    result = func()
    end = time.time()
    clear_state_dask()

    duration = end - start
    input_size = get_file_size_gb(input_path)
    output_size = get_file_size_gb(output_path) if output_path else 0

    report_dir = os.path.join(REPORT_DIR, script_name)
    os.makedirs(report_dir, exist_ok=True)
    report_path = os.path.join(report_dir, f"{script_name}_report.md")

    with open(report_path, "a", encoding="utf-8") as f:
        f.write(f"## {operation_name} [Run {run_id}]\n")
        f.write(f"- Inicio: {datetime.fromtimestamp(start)}\n")
        f.write(f"- Fin: {datetime.fromtimestamp(end)}\n")
        f.write(f"- Duración: {duration:.2f} segundos\n")
        f.write(f"- Tamaño entrada: {input_size:.2f} GB\n")
        if output_path:
            f.write(f"- Tamaño salida: {output_size:.2f} GB\n")
        f.write("\n")

    print(f"[✓] Reporte guardado en: {report_path}")
    return result
