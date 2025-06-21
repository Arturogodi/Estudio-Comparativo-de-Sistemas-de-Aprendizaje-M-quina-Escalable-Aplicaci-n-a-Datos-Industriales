import csv
import subprocess
import time
from datetime import datetime
import os
import json

class OperationMonitor:
    def __init__(self, state_file="../data/state/operation_state.json", output_dir="../data/metrics"):
        # Obtener la ruta del directorio padre del script
        script_dir = os.path.dirname(os.path.abspath(__file__))
        self.state_file = os.path.abspath(os.path.join(script_dir, state_file))
        self.output_dir = os.path.abspath(os.path.join(script_dir, output_dir))
        self.current_key = None
        self.filename = None
        print(f"[✓] Rutas absolutas:")
        print(f"    Estado: {self.state_file}")
        print(f"    Métricas: {self.output_dir}")
        
        try:
            os.makedirs(os.path.dirname(self.state_file), exist_ok=True)
            os.makedirs(self.output_dir, exist_ok=True)
            print(f"[✓] Directorios creados correctamente")
            
            if not os.path.exists(self.state_file):
                self._write_state({"current_operation": "idle", "start_time": None})
                print(f"[✓] Archivo de estado creado")
        except Exception as e:
            print(f"[✗] Error al crear directorios: {e}")
            raise

    def _write_state(self, state):
        with open(self.state_file, "w") as f:
            json.dump(state, f)

    def _read_state(self):
        try:
            with open(self.state_file, "r") as f:
                content = f.read()
                print(f"[✓] Contenido del archivo de estado: {content}")
                return json.loads(content)
        except json.JSONDecodeError as e:
            print(f"[✗] Error al decodificar JSON: {e}")
            return {"current_operation": "idle", "start_time": None}
        except Exception as e:
            print(f"[✗] Error al leer estado: {e}")
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
            print(f"[✗] Error al capturar métricas: {e}")
            return []

    def _get_run_id(self, base_name):
        existing = [f for f in os.listdir(self.output_dir) if f.startswith(base_name)]
        return len(existing) + 1

    def start_monitoring(self, interval=5):
        print(f"[✓] Iniciando monitoreo con intervalo de {interval}s...")
        while True:
            try:
                state = self._read_state()
                op = state.get("current_operation", "idle").replace(" ", "_")
                file = state.get("input_file", "unknown").replace(" ", "_")
                key = f"{op}__{file}"
                print(f"[✓] Estado actual: {op} ({file})")

                # Si el estado es idle, no crear archivo nuevo
                if op == "idle":
                    print("[✓] Sistema en estado idle")
                    time.sleep(interval)
                    continue

                if key != self.current_key or self.filename is None:
                    run_id = self._get_run_id(key)
                    self.filename = os.path.join(self.output_dir, f"{key}__run{run_id}.csv")
                    self.current_key = key
                    print(f"[✓] Archivo de métricas: {self.filename}")
                    try:
                        with open(self.filename, "w", newline="", encoding="utf-8") as f:
                            writer = csv.writer(f)
                            writer.writerow(["timestamp", "container_id", "name", "cpu_perc", "mem_usage", "mem_perc", "net_io", "block_io", "pids"])
                            print(f"[✓] Archivo creado correctamente")
                    except Exception as e:
                        print(f"[✗] Error al crear archivo: {e}")
                        raise

                metrics = self._capture_stats()
                if not metrics:
                    print("[✗] No se encontraron métricas de contenedores")
                    continue

                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

                try:
                    with open(self.filename, "a", newline="", encoding="utf-8") as f:
                        writer = csv.writer(f)
                        for line in metrics:
                            writer.writerow([timestamp] + [x.strip() for x in line.split(",")])
                    print(f"[✓] Captura registrada @ {timestamp}")
                except Exception as e:
                    print(f"[✗] Error al escribir métricas: {e}")

                time.sleep(interval)

            except KeyboardInterrupt:
                print("[i] Monitoreo interrumpido manualmente.")
                break
            except Exception as e:
                print(f"[✗] Error en monitoreo: {e}")
                time.sleep(2)

if __name__ == "__main__":
    monitor = OperationMonitor()
    monitor.start_monitoring(interval=5)
