import os
import pandas as pd
import re
from pathlib import Path

def process_file(file_path):
    """Procesa un archivo .md y extrae los tiempos de ejecución para archivos 06 y 07"""
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
    if "LightGBM" in file_name:
        category = "ML LightGBM"
    elif "Read_Filter" in file_name:
        category = "ETL Read Filter"
    
    # Contador de ejecuciones
    execution_counter = 1
    
    # Lista para almacenar los tiempos de la ejecución actual
    current_execution_times = []
    current_execution_datasets = []
    
    for i, line in enumerate(lines):
        # Buscar tiempos en esta línea
        time_match = re.search(r'\d+(?:\.\d+)?\s*(?:s|segundos)', line)
        # Ignorar tiempos totales del script
        if time_match and not re.search(r'## Tiempo total del script', line):
            time = float(time_match.group(0).replace('s', '').replace('segundos', '').strip())
            
            # Buscar el dataset asociado en las líneas anteriores
            dataset = ""
            # Buscar patrones de dataset
            patterns = [
                r'## GBT Classification Amazon23 Fast - \[Run \d+\]',  # Para LightGBM
                r'## GBT Verified Purchase - ([^\n]+) - \[Run \d+\]',  # Para GBT
                r'## Read Filter Execution - ([^\n]+)',  # Para Read_Filter
                r'## ([^\n]+) - \[Run \d+\]',
                r'## ([^\n]+) - ([^\n]+)'
            ]
            
            # Verificar si es un tiempo total del script
            if re.search(r'## Tiempo total del script', line):
                continue  # Saltar este tiempo
            
            # Primero intentar en la línea actual
            for pattern in patterns:
                match = re.search(pattern, line)
                if match:
                    # Para LightGBM/GBT
                    if "GBT" in line:
                        run_match = re.search(r'\[Run (\d+)\]', line)
                        if run_match:
                            run_number = int(run_match.group(1))
                            model_type = "LightGBM" if "LightGBM" in file_name else "GBT"
                            dataset = f"{model_type} Classification - Run {run_number}"
                    # Para Read_Filter
                    elif "Read_Filter" in file_name:
                        if match.group(1):
                            operation_type = "D" if "D" in file_name else "R"
                            dataset = f"Read Filter Execution - {operation_type}"
                    elif match.group(1):
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
                                # Para LightGBM/GBT
                                if "GBT" in prev_line:
                                    run_match = re.search(r'\[Run (\d+)\]', prev_line)
                                    if run_match:
                                        run_number = int(run_match.group(1))
                                        model_type = "LightGBM" if "LightGBM" in file_name else "GBT"
                                        dataset = f"{model_type} Classification - Run {run_number}"
                                # Para Read_Filter
                                elif "Read_Filter" in file_name:
                                    if match.group(1):
                                        operation_type = "D" if "D" in file_name else "R"
                                        dataset = f"Read Filter Execution - {operation_type}"
                                elif match.group(1):
                                    dataset = match.group(1).strip()
                                else:
                                    dataset = prev_line.split('-')[1].strip()
                                break
                        if dataset:
                            break
            
            # Para archivos R_O, usar el número de run correcto
            if "R_O" in file_name:
                runs.append(f"[Run {execution_counter}]")
            # Para archivos Ray, limitar a 5 runs
            elif "Ray" in file_name:
                if execution_counter > 5:
                    runs.append("[Run X]")
                else:
                    runs.append(f"[Run {execution_counter}]")
            # Para otros archivos (LightGBM, etc.)
            else:
                if execution_counter > 5:
                    runs.append("[Run X]")
                else:
                    runs.append(f"[Run {execution_counter}]")
            
            times.append(time)
            datasets.append(dataset)
            categories.append(category)
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
    
    # Lista de prefijos a procesar
    prefixes = ["06_", "07_"]
    
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
        output_file = os.path.join(os.getcwd(), 'data', 'images', 'times_summary_06_07.csv')
        df.to_csv(output_file, index=False, encoding='utf-8')
        print(f"\nArchivo generado: {output_file}")
        print("\nEjemplos de resultados:")
        print(df.head())
    else:
        print("No se encontraron archivos para procesar")

if __name__ == "__main__":
    main()
