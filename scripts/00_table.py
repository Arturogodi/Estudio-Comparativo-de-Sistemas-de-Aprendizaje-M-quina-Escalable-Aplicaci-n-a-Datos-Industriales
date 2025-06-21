import os
import pandas as pd
from pathlib import Path

def process_csv_files():
    # Directorio base de los archivos CSV
    base_dir = Path("../data/images/00_AMZN23")
    
    # Lista para almacenar todos los DataFrames
    all_dfs = []
    
    # Mapeo de categorías
    categoria_map = {
        'Books': 'Books',
        'Clothing': 'Clothing_Shoes_and_Jewelry',
        'Electronics': 'Electronics',
        'Home': 'Home_and_Kitchen',
        'Toys': 'Toys_and_Games'
    }
    
    # Para cada tipo (meta y review)
    for tipo in ['meta', 'review']:
        tipo_dir = base_dir / tipo
        if not tipo_dir.exists():
            print(f"Directorio no encontrado: {tipo_dir}")
            continue
            
        # Para cada categoría
        for cat_dir in tipo_dir.iterdir():
            if cat_dir.is_dir():
                cat = cat_dir.name
                if cat in categoria_map:
                    cat_full = categoria_map[cat]
                    
                    # Para cada archivo CSV en la categoría
                    for csv_file in cat_dir.glob("*.csv"):
                        try:
                            # Leer el archivo CSV
                            df = pd.read_csv(csv_file)
                            
                            # Extraer el número de run del nombre del archivo
                            run_num = csv_file.stem.split('_')[0]
                            
                            # Añadir columnas de metadatos
                            df['tipo'] = tipo
                            df['categoria'] = cat_full
                            df['run'] = run_num
                            
                            # Añadir al listado de DataFrames
                            all_dfs.append(df)
                            print(f"Archivo procesado: {csv_file}")
                            
                        except Exception as e:
                            print(f"Error procesando {csv_file}: {str(e)}")
    
    if all_dfs:
        # Concatenar todos los DataFrames
        combined_df = pd.concat(all_dfs, ignore_index=True)
        
        # Crear el directorio de salida si no existe
        output_dir = Path("../data/tables")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Guardar la tabla combinada
        output_path = output_dir / "00_AMZN23_combined_table.csv"
        combined_df.to_csv(output_path, index=False)
        print(f"\nTabla combinada guardada en: {output_path}")
        
        return combined_df
    
    return None

if __name__ == "__main__":
    combined_df = process_csv_files()
    if combined_df is not None:
        print("\nEstructura de la tabla combinada:")
        print(combined_df.info())
        print("\nPrimeras filas de la tabla:")
        print(combined_df.head())
