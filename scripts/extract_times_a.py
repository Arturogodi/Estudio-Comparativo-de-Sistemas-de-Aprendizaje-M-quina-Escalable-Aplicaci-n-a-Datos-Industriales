import os
import pandas as pd
import re
from pathlib import Path

def process_file(file_path):
    """Procesa un archivo .md y extrae los tiempos de ejecución para archivos A"""
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Extrae el nombre del archivo sin extensión
    file_name = os.path.basename(file_path)
    folder_name = os.path.dirname(file_path)
    
    # Lista para almacenar los resultados
    runs = []
    times = []
    datasets = []
    categories = []
    
    # Busca todas las líneas del archivo
    lines = content.split('\n')
    
    # Variable para ignorar tiempos después de "Tiempo total del script"
    ignore_next_time = False
    
    # Identificar la categoría del archivo
    category = ""
    if "EDA" in file_name:
        category = "EDA"
    elif "Meta" in file_name:
        category = "Meta"
    
    # Contador de ejecuciones
    execution_counter = 1
    
    # Lista para almacenar los tiempos de la ejecución actual
    current_execution_times = []
    current_execution_datasets = []
    
    for i, line in enumerate(lines):
        # Si encontramos "Tiempo total del script", procesar los tiempos acumulados
        if "Tiempo total del script" in line:
            # Marca fin de ejecución y guarda datos
            run_text = f"[Run {execution_counter}]"
            for time, dataset in zip(current_execution_times, current_execution_datasets):
                runs.append(run_text)
                times.append(time)
                datasets.append(dataset)
                categories.append(category)
            current_execution_times = []
            current_execution_datasets = []
            execution_counter += 1
            continue  # No intentes extraer tiempo de esta línea

        # Buscar tiempos en esta línea
        time_match = re.search(r'\d+(?:\.\d+)?\s*(?:s|segundos)', line)
        if time_match:
            time = float(time_match.group(0).replace('s', '').replace('segundos', '').strip())
            
            # Buscar el dataset asociado en las líneas anteriores
            dataset = ""
            # Buscar patrones de dataset
            patterns = [
                "## Normalize Dataset -",
                "## EDA Meta Dataset -",
                "## EDA Dataset -"
            ]
            
            # Primero intentar en la línea actual
            for pattern in patterns:
                if pattern in line:
                    parts = line.split('-')
                    if len(parts) > 1:
                        dataset = parts[1].strip()
                        break
            
            # Si no se encontró, buscar en las líneas anteriores
            if not dataset:
                for j in range(1, 5):
                    if i - j >= 0:
                        prev_line = lines[i - j].strip()
                        for pattern in patterns:
                            if pattern in prev_line:
                                parts = prev_line.split('-')
                                if len(parts) > 1:
                                    dataset = parts[1].strip()
                                    break
                        if dataset:
                            break
            
            # Para archivos A, almacenar tiempos en la lista temporal
            if "_A_" in file_name:
                current_execution_times.append(time)
                current_execution_datasets.append(dataset)
    
    return {
        'file': [file_name] * len(runs),
        'folder': [folder_name] * len(runs),
        'run': runs,
        'dataset': datasets,
        'category': categories,
        'time_seconds': times
    }

def main():
    # Directorio de reportes
    report_dir = os.path.join(os.getcwd(), 'data', 'report')
    
    # Lista para almacenar todos los resultados
    all_results = []
    
    # Lista de archivos A específicos a procesar
    files_to_process = [
        "00_AMZN23_A_Normalization_Preprocess_all_files_report.md",
        "01_AMZN23_A_EDA_all_files_report.md"
    ]
    
    # Buscar archivos específicos
    for root, dirs, files in os.walk(report_dir):
        for file in files:
            if file in files_to_process:
                file_path = os.path.join(root, file)
                print(f"Procesando archivo: {file_path}")
                try:
                    results = process_file(file_path)
                    all_results.append(pd.DataFrame(results))
                except Exception as e:
                    print(f"Error procesando {file_path}: {str(e)}")
    
    if all_results:
        # Concatenar todos los resultados
        df = pd.concat(all_results, ignore_index=True)
        
        # Guardar en CSV
        output_file = os.path.join(os.getcwd(), 'data', 'images', 'times_summary_a.csv')
        df.to_csv(output_file, index=False, encoding='utf-8')
        print(f"\nArchivo generado: {output_file}")
        print("\nEjemplos de resultados:")
        print(df.head())
    else:
        print("No se encontraron archivos A para procesar")

if __name__ == "__main__":
    main()
