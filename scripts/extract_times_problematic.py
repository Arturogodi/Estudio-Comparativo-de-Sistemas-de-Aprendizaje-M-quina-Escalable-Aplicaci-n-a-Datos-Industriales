import os
import re
import pandas as pd
from pathlib import Path

def extract_times_problematic():
    # Archivos problemáticos
    files = [
        "data/report/07_Read_Filter/07_Read_Filter_report.md",
        "data/report/07_Read_Filter_Write/07_Read_Filter_Write_report.md"
    ]

    # Patrones específicos para archivos 07
    patterns = {
        "Read_Filter": r'## Read Filter Ray \[Run (\d+)\]\n.*?Duración: (\d+\.\d+) segundos',
        "Read_Filter_Write": r'## Read Filter Write Ray \[Run (\d+)\]\n.*?Duración: (\d+\.\d+) segundos'
    }

    all_data = []

    for file_path in files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()

            # Extraer metadata del archivo
            folder_name = Path(file_path).parent.parent.name
            file_name = Path(file_path).name
            
            # Determinar el tipo de operación basado en el nombre del archivo
            if "Read_Filter_Write" in folder_name:
                pattern = patterns["Read_Filter_Write"]
                category = "ETL Read_Filter_Write Ray"
            else:
                pattern = patterns["Read_Filter"]
                category = "ETL Read_Filter Ray"

            # Extraer todas las secciones
            sections = re.findall(pattern, content, re.DOTALL)
            
            # Si no se encontraron secciones, intentar con patrones alternativos
            if not sections:
                print(f"Advertencia: No se encontraron secciones con patrón principal en {file_path}")
                # Patrones alternativos
                alt_patterns = [
                    r'##.*?\[Run (\d+)\]\n.*?Duración: (\d+\.\d+) segundos',
                    r'##.*?\[Run (\d+)\]\n.*?Tiempo: (\d+\.\d+) s'
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
                dataset = f"{category.replace('ETL ', '')} - Run {run_number}"

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
    output_file = "data/images/times_summary_07.csv"
    df.to_csv(output_file, index=False)
    print(f"Archivo generado: {output_file}")
    print("\nEjemplos de resultados:")
    print(df.head())

    for file_path in files:
        try:
            pattern = ""  # Inicializar pattern
            category = ""  # Inicializar category
            
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()

            # Extraer metadata del archivo
            folder_name = Path(file_path).parent.parent.name
            file_name = Path(file_path).name
            
            # Determinar el tipo de operación
            if "ML_XGBoost" in folder_name:
                pattern = r'## GBT Verified Purchase - Fast - \[Run (\d+)\]\n.*?Tiempo: (\d+\.\d+) s'
                category = "ML XGBoost"
            elif "ML_LightGBM" in folder_name:
                pattern = r'## GBT Classification Amazon23 Fast \[Run (\d+)\]\n.*?Tiempo: (\d+\.\d+) s'
                category = "ML LightGBM"
            elif "Read_Filter" in folder_name:
                if "Write" in folder_name:
                    pattern = r'## Read Filter Write Ray \[Run (\d+)\]\n.*?Duración: (\d+\.\d+) segundos'
                    category = "ETL Read_Filter_Write Ray"
                else:
                    pattern = r'## Read Filter Ray \[Run (\d+)\]\n.*?Duración: (\d+\.\d+) segundos'
                    category = "ETL Read_Filter Ray"

            # Si no se encontró patrón, usar uno genérico
            if not pattern:
                print(f"Advertencia: No se encontró patrón específico para {file_path}")
                pattern = r'##.*?\[Run (\d+)\]\n.*?Duración: (\d+\.\d+) segundos|Tiempo: (\d+\.\d+) s'
                category = "Unknown"

            # Extraer todas las secciones
            sections = re.findall(pattern, content, re.DOTALL)
            
            # Si no se encontraron secciones, intentar con patrones alternativos
            if not sections:
                print(f"Advertencia: No se encontraron secciones con patrón principal en {file_path}")
                # Patrones alternativos para XGBoost/LightGBM
                if "ML" in category:
                    alt_patterns = [
                        r'##.*?\[Run (\d+)\]\n.*?Tiempo: (\d+\.\d+) s',
                        r'##.*?\[Run (\d+)\]\n.*?Duración: (\d+\.\d+) segundos'
                    ]
                # Patrones alternativos para Read_Filter
                if sections:
                    print(f"Usando patrón alternativo para {file_path}")
                else:
                    print(f"Advertencia: No se encontraron secciones en {file_path}")
                    continue

            # Procesar cada sección
            execution_counter = 1
            for match in sections:
                # Para patrones con dos grupos
                if len(match) == 2:
                    run_number, time = match
                # Para patrones con tres grupos
                elif len(match) == 3:
                    run_number, time1, time2 = match
                    time = time1 if time1 else time2
                else:
                    print(f"Advertencia: Formato inesperado en {file_path}: {match}")
                    continue

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
                
                # Formatear el dataset según el tipo
                if "ML" in category:
                    dataset = f"GBT Classification - Run {run_number}"
                else:
                    dataset = f"{category.replace('ETL ', '')} - Run {run_number}"

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
    output_file = "data/images/times_summary_problematic.csv"
    df.to_csv(output_file, index=False)
    print(f"Archivo generado: {output_file}")
    print("\nEjemplos de resultados:")
    print(df.head())

if __name__ == "__main__":
    extract_times_problematic()
