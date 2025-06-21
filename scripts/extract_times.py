import os
import re
import pandas as pd
from pathlib import Path

def extract_time(text):
    """Extrae el tiempo de ejecución de un texto, puede estar en formatos diferentes"""
    # Busca patrones como "237.08 s", "20.33 segundos", "24.08 segundos"
    time_match = re.search(r'\d+(?:\.\d+)?\s*(?:s|segundos)', text)
    if time_match:
        # Extrae el número y lo convierte a float
        time_str = time_match.group(0).replace('s', '').replace('segundos', '').strip()
        return float(time_str)
    return None

def process_file(file_path):
    """Procesa un archivo .md y extrae los tiempos de ejecución"""
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
    elif "Reviews" in content:
        category = "Reviews"
    else:
        category = "Meta"
    
    # Contador de ejecuciones para EDA A
    eda_a_execution = 1
    
    # Lista para almacenar los tiempos de la ejecución actual de EDA A
    eda_a_times = []
    eda_a_datasets = []
    
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
            
            # Si es archivo 01 o 12, usar categorías y runs por dataset
            if "01" in file_name or "02" in file_name:
                # Para EDA A, almacenar tiempos en la lista temporal
                if "01_AMZN23_A" in file_name:
                    eda_a_times.append(time)
                    eda_a_datasets.append(dataset)
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
            else:
                # Para otros archivos, usar runs globales
                run_text = f"[Run {len(runs) + 1}]"
                runs.append(run_text)
                times.append(time)
                datasets.append(dataset)
                categories.append(category)
            
            # Si es EDA A y encontramos el final de una ejecución, procesar la lista
            if "01_AMZN23_A" in file_name and "Tiempo total del script" in lines[i + 1]:
                # Agregar todos los tiempos de la ejecución actual con el mismo número de run
                run_text = f"[Run {eda_a_execution}]"
                for time, dataset in zip(eda_a_times, eda_a_datasets):
                    runs.append(run_text)
                    times.append(time)
                    datasets.append(dataset)
                    categories.append(category)
                # Limpiar la lista para la siguiente ejecución
                eda_a_times = []
                eda_a_datasets = []
                eda_a_execution += 1
    
    return {
        'file': [file_name] * len(runs),
        'folder': [folder_name] * len(runs),
        'run': runs,
        'dataset': datasets,
        'category': categories,
        'time_seconds': times
    }

def main():
    # Directorio base de reportes
    base_dir = Path('data/report')
    
    # Lista para almacenar todos los resultados
    all_results = []
    
    # Busca todos los archivos .md
    for md_file in base_dir.rglob('*.md'):
        try:
            results = process_file(md_file)
            all_results.append(pd.DataFrame(results))
            print(f"Procesado: {md_file}")
        except Exception as e:
            print(f"Error procesando {md_file}: {str(e)}")
    
    # Combina todos los resultados en un DataFrame
    if all_results:
        df = pd.concat(all_results, ignore_index=True)
        
        # Guarda en CSV
        output_path = "data/images/times_summary.csv"
        df.to_csv(output_path, index=False)
        print(f"\nResultados guardados en: {output_path}")
        print("\nResumen de tiempos:")
        print(df.groupby(['folder', 'file'])['time_seconds'].agg(['mean', 'std', 'min', 'max']))
    else:
        print("No se encontraron archivos procesables")

if __name__ == "__main__":
    main()
