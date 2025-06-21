import os
import pandas as pd
import re
from pathlib import Path

def extract_metadata(file_path):
    """Extract metadata from file path"""
    path = Path(file_path)
    
    # Extraer framework (Spark/Ray/Dask)
    framework = re.search(r'(Spark|Ray|Dask)', file_path).group(1)

    # Extraer operation type desde el nombre de la carpeta (3ra parte del nombre)
    folder_name = path.parent.parent.name
    operation_type = folder_name.split('_')[2]

    # Extraer categoría del nombre del archivo
    match = re.search(r'(_meta_|_review_)([A-Za-z_]+)_stats', file_path)
    category = match.group(2) if match else 'General'

    # Extraer número de ejecución
    run_match = re.search(r'run(\d+)', file_path)
    run_number = int(run_match.group(1)) if run_match else None

    # Extraer nombre del dataset raíz (ej. 00_AMZN_Spark_Preprocesado)
    dataset_name = path.parts[path.parts.index('images') + 1]

    return {
        'framework': framework,
        'operation_type': operation_type,
        'category': category,
        'run_number': run_number,
        'dataset_name': dataset_name
    }

def main():
    base_dir = Path('data/images')
    output_file = base_dir / 'csv_all_stats.csv'
    
    # Buscar todos los archivos CSV
    csv_files = list(base_dir.rglob('*.csv'))
    
    all_dfs = []
    
    for csv_file in csv_files:
        try:
            df = pd.read_csv(csv_file)
            metadata = extract_metadata(str(csv_file))
            
            for key, value in metadata.items():
                df[key] = value
            
            all_dfs.append(df)
            print(f"Processed: {csv_file}")
        except Exception as e:
            print(f"Error processing {csv_file}: {str(e)}")
    
    if all_dfs:
        result_df = pd.concat(all_dfs, ignore_index=True)
        result_df.to_csv(output_file, index=False)
        print(f"\nConsolidated data saved to: {output_file}")
    else:
        print("No valid CSV files found or processed.")

if __name__ == "__main__":
    main()
