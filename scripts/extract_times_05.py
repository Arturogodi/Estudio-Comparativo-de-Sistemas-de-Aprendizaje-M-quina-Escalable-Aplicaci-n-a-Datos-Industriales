import os
import re
import pandas as pd
from pathlib import Path

def extract_times_05():
    # Archivos 05
    files = [
        "data/report/05_AMZN23_ML_XGBoost/05_AMZN23_ML_XGBoost_report.md"
    ]

    all_data = []

    for file_path in files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()

            # Extraer metadata del archivo
            folder_name = Path(file_path).parent.parent.name
            file_name = Path(file_path).name
            
            # Patrón específico para XGBoost
            pattern = r'## GBT Verified Purchase - Fast - \[Run (\d+)\]\n.*?Tiempo: (\d+\.\d+) s'
            category = "ML XGBoost"

            # Extraer todas las secciones
            sections = re.findall(pattern, content, re.DOTALL)
            
            # Si no se encontraron secciones, intentar con patrones alternativos
            if not sections:
                print(f"Advertencia: No se encontraron secciones con patrón principal en {file_path}")
                alt_patterns = [
                    r'##.*?\[Run (\d+)\]\n.*?Tiempo: (\d+\.\d+) s',
                    r'##.*?\[Run (\d+)\]\n.*?Duración: (\d+\.\d+) segundos'
                ]
                
                # Intentar con cada patrón alternativo
                for alt_pattern in alt_patterns:
                    sections = re.findall(alt_pattern, content, re.DOTALL)
                    if sections:
                        print(f"Usando patrón alternativo: {alt_pattern}")
                        break
                
                if not sections:
                    print(f"Error: No se encontraron secciones en {file_path}")
                    continue

            # Procesar cada sección
            for run_number, time in sections:
                # Validar el tiempo
                try:
                    time = float(time)
                except ValueError:
                    print(f"Advertencia: Tiempo inválido en {file_path}: {time}")
                    continue

                # Para archivos R_O, usar el número de run correcto
                if "R_O" in file_path:
                    run = f"[Run {run_number}]"
                # Para otros archivos, limitar a 5 runs
                else:
                    if int(run_number) > 5:
                        run = "[Run X]"
                    else:
                        run = f"[Run {run_number}]"
                
                # Formatear el dataset
                dataset = f"GBT Classification - Run {run_number}"

                all_data.append({
                    'file': file_name,
                    'folder': folder_name,
                    'run': run,
                    'dataset': dataset,
                    'category': category,
                    'time_seconds': time
                })

        except Exception as e:
            print(f"Error procesando {file_path}: {str(e)}")

    # Crear DataFrame y guardar en CSV
    df = pd.DataFrame(all_data)
    output_file = "data/images/times_summary_05.csv"
    df.to_csv(output_file, index=False)
    print(f"Archivo generado: {output_file}")
    print("\nEjemplos de resultados:")
    print(df.head())

if __name__ == "__main__":
    extract_times_05()
