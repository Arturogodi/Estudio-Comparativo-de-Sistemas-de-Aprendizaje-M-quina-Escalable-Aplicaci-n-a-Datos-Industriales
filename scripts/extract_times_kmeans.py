import os
import re
import pandas as pd
from pathlib import Path

def extract_times_kmeans():
    # Archivos a procesar
    files = [
        "data/report/04_AMZN23_ML_Kmeans_parquet_R/04_AMZN23_ML_Kmeans_parquet_R_report.md",
        "data/report/04_AMZN23_ML_Kmeans_parquet_R_O/04_AMZN23_ML_Kmeans_parquet_R_O_report.md"
    ]

    all_data = []

    for file_path in files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()

            # Extraer metadata del archivo
            folder_name = Path(file_path).parent.parent.name
            file_name = Path(file_path).name
            
            # Extraer todas las secciones de KMeans
            kmeans_sections = re.findall(r'## KMeans Clustering Amazon23 \[Run (\d+)\]\n.*?Duración: (\d+\.\d+) segundos', content, re.DOTALL)
            
            for run_number, time in kmeans_sections:
                # Para archivos R_O, usar el número de run correcto
                if "R_O" in file_path:
                    run = f"[Run {run_number}]"
                # Para otros archivos, limitar a 5 runs
                else:
                    if int(run_number) > 5:
                        run = "[Run X]"
                    else:
                        run = f"[Run {run_number}]"
                
                all_data.append({
                    'file': file_name,
                    'folder': folder_name,
                    'run': run,
                    'dataset': f"KMeans Clustering Amazon23 - Run {run_number}",
                    'category': "ML KMeans",
                    'time_seconds': float(time)
                })

        except Exception as e:
            print(f"Error procesando {file_path}: {str(e)}")

    # Crear DataFrame y guardar en CSV
    df = pd.DataFrame(all_data)
    output_file = "data/images/times_summary_kmeans.csv"
    df.to_csv(output_file, index=False)
    print(f"Archivo generado: {output_file}")
    print("\nEjemplos de resultados:")
    print(df.head())

if __name__ == "__main__":
    extract_times_kmeans()
