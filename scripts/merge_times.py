import pandas as pd
import os
from pathlib import Path

def merge_times():
    # Lista de archivos a unir
    files = [
        "data/images/times_summary_problematic.csv",
        "data/images/times_summary_other.csv",
        "data/images/times_summary_ml_etl.csv",
        "data/images/times_summary_eda.csv",
        "data/images/times_summary_a.csv",
        "data/images/times_summary_b.csv",
        "data/images/times_summary_05.csv",
        "data/images/times_summary_04_05.csv"
    ]

    # Columnas requeridas (orden específico)
    required_columns = ['file', 'folder', 'run', 'dataset', 'category', 'time_seconds']
    
    all_dfs = []
    
    for file_path in files:
        try:
            # Leer el archivo CSV
            df = pd.read_csv(file_path)
            
            # Verificar y ajustar las columnas
            if not all(col in df.columns for col in required_columns):
                # Añadir columnas faltantes con valores vacíos
                for col in required_columns:
                    if col not in df.columns:
                        df[col] = ''
                
                # Reordenar las columnas
                df = df[required_columns]
            
            # Añadir el dataframe a la lista
            all_dfs.append(df)
            print(f"Archivo procesado: {file_path}")
            
        except Exception as e:
            print(f"Error procesando {file_path}: {str(e)}")

    # Unir todos los dataframes
    if all_dfs:
        final_df = pd.concat(all_dfs, ignore_index=True)
        
        # Guardar el resultado
        output_file = "data/images/times_summary_all.csv"
        final_df.to_csv(output_file, index=False)
        print(f"\nArchivo final generado: {output_file}")
        print("\nEjemplos de resultados:")
        print(final_df.head())
    else:
        print("No se encontraron archivos válidos para procesar")

if __name__ == "__main__":
    merge_times()
