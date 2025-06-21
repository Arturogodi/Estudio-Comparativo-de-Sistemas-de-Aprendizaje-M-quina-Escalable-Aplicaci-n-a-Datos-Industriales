import csv
import subprocess
import time
from datetime import datetime
import os
import json

class DaskContainerMonitor:
    def __init__(self, state_file="data/state/operation_state_dask.json", output_dir="data/metrics_dask"):
        script_dir = os.path.dirname(os.path.abspath(__file__))
        self.state_file = os.path.abspath(os.path.join(script_dir, "..", state_file))
        self.output_dir = os.path.abspath(os.path.join(script_dir, "..", output_dir))
        self.current_key = None
        self.filename = None

        print(f"[✓] Usando archivo de estado: {self.state_file}")
        print(f"[✓] Directorio de métricas: {self.output_dir}")

        os.makedirs(os.path.dirname(self.state_file), exist_ok=True)
        os.makedirs(self.output_dir, exist_ok=True)

        if not os.path.exists(self.state_file):
            self._write_state({"current_operation": "idle", "start_time": None})
            print(f"[✓] Archivo de estado creado")

    def _write_state(self, state):
        with open(self.state_file, "w") as f:
            json.dump(state, f)

    def _read_state(self):
        try:
            with open(self.state_file, "r") as f:
                content = f.read()
                print(f"[✓] Contenido del archivo de estado: {content}")
                return json.loads(content)
        except Exception as e:
            print(f"[✗] Error leyendo estado: {e}")
            return {"current_operation": "idle", "start_time": None}

    def _capture_stats(self):
        cmd = [
            "docker", "stats", "--no-stream", "--format",
            "{{.Container}},{{.Name}},{{.CPUPerc}},{{.MemUsage}},{{.MemPerc}},{{.NetIO}},{{.BlockIO}},{{.PIDs}}"
        ]
        try:
            output = subprocess.check_output(cmd).decode().strip().split("\n")
            return output
        except Exception as e:
            print(f"[✗] Error capturando stats: {e}")
            return []

    def start_monitoring(self, interval=5):
        print(f"[✓] Iniciando monitoreo Dask cada {interval}s...")
        while True:
            try:
                state = self._read_state()
                op = state.get("current_operation", "idle").replace(" ", "_")
                file = state.get("input_file", "unknown").replace(" ", "_")
                run_id = state.get("run_id", 1)
                key = f"{op}__{file}__run{run_id}"

                if op == "idle":
                    print("[✓] Sistema en estado idle")
                    time.sleep(interval)
                    continue

                if key != self.current_key or self.filename is None:
                    self.filename = os.path.join(self.output_dir, f"{key}.csv")
                    self.current_key = key
                    print(f"[✓] Nueva operación detectada: {op} sobre {file} [Run {run_id}]")
                    print(f"[✓] Guardando métricas en: {self.filename}")
                    with open(self.filename, "w", newline="", encoding="utf-8") as f:
                        writer = csv.writer(f)
                        writer.writerow([
                            "timestamp", "container_id", "name", "cpu_perc",
                            "mem_usage", "mem_perc", "net_io", "block_io", "pids"
                        ])

                metrics = self._capture_stats()
                if not metrics:
                    print("[!] No se capturaron métricas.")
                    time.sleep(interval)
                    continue

                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                with open(self.filename, "a", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    for line in metrics:
                        writer.writerow([timestamp] + [x.strip() for x in line.split(",")])

                print(f"[✓] Métricas registradas @ {timestamp}")
                time.sleep(interval)

            except KeyboardInterrupt:
                print("[i] Monitoreo interrumpido manualmente.")
                break
            except Exception as e:
                print(f"[✗] Error en monitoreo: {e}")
                time.sleep(2)

if __name__ == "__main__":
    monitor = DaskContainerMonitor()
    monitor.start_monitoring(interval=5)
