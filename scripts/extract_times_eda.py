import os
import pandas as pd
import re
from pathlib import Path

def process_file(file_path):
    """Procesa un archivo .md y extrae los tiempos de ejecución para 00_ y 01_"""
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
    
    # Diccionario para mantener un contador de run por dataset
    dataset_runs = {}
    
    # Busca todas las líneas del archivo
    lines = content.split('\n')
    
    # Variable para ignorar tiempos después de "Tiempo total del script"
    ignore_next_time = False
    
    # Identificar la categoría del archivo
    category = ""
    if "EDA" in file_name:
        category = "EDA"
    elif "Reviews" in file_name:
        category = "Reviews"
    else:
        category = "Meta"
    
    # Contador de ejecuciones para archivos que necesitan runs por ejecución
    execution_counter = 1
    
    # Lista para almacenar los tiempos de la ejecución actual
    current_execution_times = []
    current_execution_datasets = []
    
    for i, line in enumerate(lines):
        # Si encontramos "Tiempo total del script", ignorar el siguiente tiempo
        if "Tiempo total del script" in line:
            ignore_next_time = True
            continue
            
        # Si estamos ignorando el siguiente tiempo, saltar esta línea
        if ignore_next_time:
            ignore_next_time = False
            continue
            
        # Buscar tiempos en esta línea
        time_match = re.search(r'\d+(?:\.\d+)?\s*(?:s|segundos)', line)
        if time_match:
            time = float(time_match.group(0).replace('s', '').replace('segundos', '').strip())
            
            # Buscar el dataset asociado en las líneas anteriores
            dataset = ""
            for j in range(1, 5):  # Buscar en las 5 líneas anteriores
                if i - j >= 0:
                    prev_line = lines[i - j]
                    # Buscar patrones de dataset
                    dataset_match = re.search(r'## (?:Normalize|EDA) (?:Dataset|Reviews Dataset) - ([^\n]+)', prev_line)
                    if dataset_match:
                        dataset = dataset_match.group(1).strip()
                        break
            
            # Para archivos 00_ y 01_
            if "00" in file_name or "01" in file_name:
                # Para archivos que necesitan runs por ejecución (00_A y 01_A)
                if "_A_" in file_name:
                    current_execution_times.append(time)
                    current_execution_datasets.append(dataset)
                else:
                    if dataset:
                        if dataset not in dataset_runs:
                            dataset_runs[dataset] = 1
                        run_text = f"[Run {dataset_runs[dataset]}]"
                        dataset_runs[dataset] += 1
                    else:
                        run_text = "[Run X]"
                    runs.append(run_text)
                    times.append(time)
                    datasets.append(dataset)
                    categories.append(category)
            
            # Si encontramos el final de una ejecución, procesar la lista
            if "_A_" in file_name and "Tiempo total del script" in lines[i + 1]:
                # Agregar todos los tiempos de la ejecución actual con el mismo número de run
                run_text = f"[Run {execution_counter}]"
                for time, dataset in zip(current_execution_times, current_execution_datasets):
                    runs.append(run_text)
                    times.append(time)
                    datasets.append(dataset)
                    categories.append(category)
                # Limpiar la lista para la siguiente ejecución
                current_execution_times = []
                current_execution_datasets = []
                execution_counter += 1
    
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
    
    # Buscar archivos .md que empiecen con 00_ o 01_
    for root, dirs, files in os.walk(report_dir):
        for file in files:
            if file.endswith('.md') and (file.startswith('00_') or file.startswith('01_')):
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
        output_file = os.path.join(os.getcwd(), 'data', 'images', 'times_summary_eda.csv')
        df.to_csv(output_file, index=False, encoding='utf-8')
        print(f"\nArchivo generado: {output_file}")
        print("\nEjemplos de resultados:")
        print(df.head())
    else:
        print("No se encontraron archivos 01_ o 02_ para procesar")

if __name__ == "__main__":
    main()
