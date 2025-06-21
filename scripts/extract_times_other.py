import os
import pandas as pd
import re
from pathlib import Path

def process_file(file_path):
    """Procesa un archivo .md y extrae los tiempos de ejecución para archivos 02_, 03_, 04_"""
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
    
    # Identificar la categoría del archivo
    category = ""
    if "ETLs" in file_name:
        if "Clean_Join" in file_name:
            category = "ETL Clean Join"
        elif "Model_Data" in file_name:
            category = "ETL Model Data"
    elif "ML" in file_name:
        category = "ML KMeans"
    
    # Contador de ejecuciones
    execution_counter = 1
    
    # Lista para almacenar los tiempos de la ejecución actual
    current_execution_times = []
    current_execution_datasets = []
    
    for i, line in enumerate(lines):
        # Buscar tiempos en esta línea
        time_match = re.search(r'\d+(?:\.\d+)?\s*(?:s|segundos)', line)
        if time_match:
            time = float(time_match.group(0).replace('s', '').replace('segundos', '').strip())
            
            # Buscar el dataset asociado en las líneas anteriores
            dataset = ""
            # Determinar el tipo de archivo basado en el nombre
            if "KMeans" in file_name:
                # Patrones para KMeans
                patterns = [
                    r'## KMeans Clustering Amazon23 - \[Run \d+\]',  # Para archivos KMeans
                    r'## KMeans Clustering Sampled',  # Para archivos 5000
                    r'## ([^\n]+) - \[Run \d+\]'
                ]
                # Extraer el framework del nombre del archivo
                framework = ""
                if "R" in file_name:
                    framework = "R"
                elif "Ray" in file_name:
                    framework = "Ray"
                elif "R_O" in file_name:
                    framework = "R_O"
                
            elif "GBT" in file_name or "XGBoost" in file_name or "LightGBM" in file_name:
                # Patrones para GBT/XGBoost/LightGBM
                patterns = [
                    r'## GBT Classification Amazon23 Fast - \[Run \d+\]',  # Para LightGBM
                    r'## GBT Verified Purchase - ([^\n]+) - \[Run \d+\]',  # Para GBT/XGBoost
                    r'## ([^\n]+) - \[Run \d+\]'
                ]
                # Extraer el tipo de ejecución
                execution_type = "Fast" if "Fast" in file_name else "Normal"
                # Determinar el tipo de modelo
                model_type = "LightGBM" if "LightGBM" in file_name else "GBT"
                
            elif "Read_Filter" in file_name:
                # Patrones para Read_Filter
                patterns = [
                    r'## Read Filter Execution - ([^\n]+)',
                    r'## ([^\n]+) - ([^\n]+)'
                ]
                # Extraer el tipo de operación
                operation_type = "D" if "D" in file_name else "R" if "R" in file_name else ""
                
            else:
                # Patrones generales para otros formatos
                patterns = [
                    r'## ETL Clean Join - ([^\n]+)',
                    r'## ETL Model Data - ([^\n]+)',
                    r'## ([^\n]+) - ([^\n]+)'
                ]
            
            # Primero intentar en la línea actual
            for pattern in patterns:
                match = re.search(pattern, line)
                if match:
                    if "KMeans" in file_name:
                        # Para KMeans, usar el número de run
                        run_match = re.search(r'\[Run (\d+)\]', line)
                        if run_match:
                            run_number = int(run_match.group(1))
                            dataset = f"KMeans Clustering {framework} - Run {run_number}"
                        else:
                            dataset = f"KMeans Clustering {framework}"
                    elif "GBT" in file_name or "XGBoost" in file_name:
                        if match.group(1):
                            dataset = f"GBT Verified Purchase - {execution_type} - {match.group(1)}"
                        else:
                            dataset = f"GBT Verified Purchase - {execution_type}"
                    elif "LightGBM" in file_name:
                        if match.group(1):
                            dataset = f"LightGBM Training - {match.group(1)}"
                        else:
                            dataset = "LightGBM Training"
                    elif "Read_Filter" in file_name:
                        if match.group(1):
                            dataset = f"Read Filter Execution - {match.group(1)}"
                        else:
                            dataset = "Read Filter Execution"
                    else:
                        if match.group(1):
                            dataset = match.group(1).strip()
                        else:
                            dataset = line.split('-')[1].strip()
                    break
            
            # Si no se encontró, buscar en las líneas anteriores
            if not dataset:
                for j in range(1, 5):
                    if i - j >= 0:
                        prev_line = lines[i - j].strip()
                        for pattern in patterns:
                            match = re.search(pattern, prev_line)
                            if match:
                                if "KMeans" in file_name:
                                    run_match = re.search(r'\[Run (\d+)\]', prev_line)
                                    if run_match:
                                        run_number = int(run_match.group(1))
                                        dataset = f"KMeans Clustering {framework} - Run {run_number}"
                                    else:
                                        dataset = f"KMeans Clustering {framework}"
                                elif "GBT" in file_name or "XGBoost" in file_name:
                                    if match.group(1):
                                        dataset = f"GBT Verified Purchase - {execution_type} - {match.group(1)}"
                                    else:
                                        dataset = f"GBT Verified Purchase - {execution_type}"
                                elif "LightGBM" in file_name:
                                    if match.group(1):
                                        dataset = f"LightGBM Training - {match.group(1)}"
                                    else:
                                        dataset = "LightGBM Training"
                                elif "Read_Filter" in file_name:
                                    if match.group(1):
                                        dataset = f"Read Filter Execution - {match.group(1)}"
                                    else:
                                        dataset = "Read Filter Execution"
                                else:
                                    if match.group(1):
                                        dataset = match.group(1).strip()
                                    else:
                                        dataset = prev_line.split('-')[1].strip()
                                break
                        if dataset:
                            break
            
            # Para archivos 02_, 03_, 04_, usar runs por dataset
            if "02_" in file_name or "03_" in file_name or "04_" in file_name:
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
    
    # Lista de prefijos a procesar
    prefixes = ["02_", "03_", "04_", "05_", "06_", "07_"]
    
    # Buscar archivos con los prefijos especificados
    for root, dirs, files in os.walk(report_dir):
        for file in files:
            if file.endswith('.md'):
                # Verificar si el archivo empieza con alguno de los prefijos
                for prefix in prefixes:
                    if file.startswith(prefix):
                        file_path = os.path.join(root, file)
                        print(f"Procesando archivo: {file_path}")
                        try:
                            results = process_file(file_path)
                            all_results.append(pd.DataFrame(results))
                        except Exception as e:
                            print(f"Error procesando {file_path}: {str(e)}")
                        break
    
    if all_results:
        # Concatenar todos los resultados
        df = pd.concat(all_results, ignore_index=True)
        
        # Guardar en CSV
        output_file = os.path.join(os.getcwd(), 'data', 'images', 'times_summary_other.csv')
        df.to_csv(output_file, index=False, encoding='utf-8')
        print(f"\nArchivo generado: {output_file}")
        print("\nEjemplos de resultados:")
        print(df.head())
    else:
        print("No se encontraron archivos para procesar")

if __name__ == "__main__":
    main()
