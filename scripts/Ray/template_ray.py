import os, sys, time, json
from datetime import datetime
from typing import Callable, Optional

STATE_FILE = "/data/state/operation_state_ray.json"
REPORT_DIR = "/data/report"
METRICS_DIR = "/data/metrics_ray"

def write_state(op, input_file, run_id):
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump({
            "current_operation": op,
            "start_time": datetime.now().strftime("%Y%m%d_%H%M%S"),
            "input_file": os.path.basename(input_file),
            "run_id": run_id
        }, f)

def clear_state():
    with open(STATE_FILE, "w") as f:
        json.dump({"current_operation": "idle", "start_time": None}, f)

def get_file_size(path):
    if os.path.isdir(path):
        return sum(os.path.getsize(os.path.join(dp, f)) for dp, _, fn in os.walk(path) for f in fn) / (1024**3)
    elif os.path.isfile(path):
        return os.path.getsize(path) / (1024**3)
    return 0

def get_run_id(base, output_dir=METRICS_DIR):
    os.makedirs(output_dir, exist_ok=True)
    existing = [f for f in os.listdir(output_dir) if f.startswith(base)]
    return len(existing) + 1

def redirect_output(script_name):
    log_path = os.path.join(REPORT_DIR, script_name, f"{script_name}_output.log")
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    sys.stdout = open(log_path, "w")
    sys.stderr = sys.stdout
    return log_path

def measure_time_with_run_id_ray(
    operation_name: str,
    input_path: str,
    func: Callable,
    output_path: Optional[str],
    run_id: int,
    log_output: bool = False,
    script_name: Optional[str] = None
):
    if script_name is None:
        script_name = os.path.basename(__file__).split('.')[0]

    if log_output:
        redirect_output(script_name)

    write_state(operation_name, input_path, run_id)
    print(f"[✓] Ejecutando {operation_name}...")

    start = time.time()
    result = func()
    end = time.time()
    clear_state()

    report_path = os.path.join(REPORT_DIR, script_name, f"{script_name}_report.md")
    os.makedirs(os.path.dirname(report_path), exist_ok=True)

    input_size = get_file_size(input_path)
    output_size = get_file_size(output_path) if output_path else 0
    duration = end - start

    with open(report_path, "a") as f:
        f.write(f"## {operation_name} [Run {run_id}]\n")
        f.write(f"- Inicio: {datetime.fromtimestamp(start)}\n")
        f.write(f"- Fin: {datetime.fromtimestamp(end)}\n")
        f.write(f"- Duración: {duration:.2f} segundos\n")
        f.write(f"- Tamaño input: {input_size:.2f} GB\n")
        if output_path:
            f.write(f"- Tamaño output: {output_size:.2f} GB\n")
        f.write("\n")

    print(f"[✓] Reporte guardado en {report_path}")
    return result
