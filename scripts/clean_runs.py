import pandas as pd
import os

def clean_runs():
    try:
        # Leer el archivo CSV
        input_file = "data/images/times_summary_all.csv"
        df = pd.read_csv(input_file)
        
        # Contar filas antes de la limpieza
        print(f"Filas antes de la limpieza: {len(df)}")
        
        # Filtrar solo las filas que no contengan "Run X"
        df = df[~df['run'].str.contains('Run X')]
        
        # Contar filas después de la limpieza
        print(f"Filas después de la limpieza: {len(df)}")
        
        # Guardar el resultado en un nuevo archivo
        output_file = "data/images/times_summary_clean.csv"
        df.to_csv(output_file, index=False)
        
        print(f"\nArchivo limpio generado: {output_file}")
        print("\nEjemplos de resultados:")
        print(df.head())
        
    except Exception as e:
        print(f"Error al procesar el archivo: {str(e)}")

if __name__ == "__main__":
    clean_runs()
