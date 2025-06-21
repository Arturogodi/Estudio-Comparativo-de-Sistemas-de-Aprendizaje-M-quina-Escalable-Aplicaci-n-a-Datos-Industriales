import os
import pandas as pd
from pathlib import Path

# Función para extraer información relevante del nombre del archivo
def extraer_info_archivo(archivo):
    # Obtener el nombre del archivo y el directorio
    nombre = archivo.stem
    
    # Obtener el directorio completo hasta la raíz de data/images
    partes_dir = str(archivo.parent).split(os.sep)
    
    # Buscar el proceso en la estructura de directorios esperada
    # La estructura debería ser: data/images/00_AMZN/.../archivo.csv
    proceso = None
    for i, parte in enumerate(partes_dir):
        if parte.startswith(('00_', '01_', '02_', '03_', '04_', '05_', '06_', '07_', '077_', '055_')):
            proceso = parte.split('_')[0]
            break
    
    if proceso is None:
        print(f"Advertencia: No se encontró proceso en {archivo}")
        proceso = '00'
    
    # Agregar el proceso al nombre del archivo para diferenciarlos
    archivo_con_proceso = f"{proceso}_{archivo.name}"
    
    partes = nombre.split('_')
    if len(partes) >= 3:
        # Extraer el tipo de operación (meta, review, kmeans, etc.)
        tipo = partes[1]
        # Extraer el dataset (Books, Clothing, etc.) si existe
        dataset = partes[2] if len(partes) > 3 else None
        # Extraer el número de run
        run = partes[-1].split('.')[0]
        
        # Verificar que el proceso se encontró correctamente
        if proceso == '00':
            print(f"Advertencia: No se encontró proceso en {archivo}")
        
        return tipo, dataset, run, proceso
    return None, None, None, None

# Directorio base de imágenes
base_dir = Path("data/images")

# Lista para almacenar todos los DataFrames
all_dfs = []

# Recorrer todos los archivos CSV en el directorio
for archivo in base_dir.rglob("*.csv"):
    if archivo.name.endswith('_stats.csv'):
        try:
            # Leer el archivo CSV
            df = pd.read_csv(archivo)
            
            # Extraer información del archivo
            tipo, dataset, run, proceso = extraer_info_archivo(archivo)
            
            # Añadir columnas con la información del archivo
            df['tipo'] = tipo
            df['dataset'] = dataset
            df['run'] = run
            df['proceso'] = proceso
            # Usar el archivo con proceso para diferenciar los archivos
            df['archivo'] = archivo_con_proceso
            
            # Añadir el DataFrame a la lista
            all_dfs.append(df)
            
            print(f"Procesado archivo: {archivo}")
            
        except Exception as e:
            print(f"Error procesando {archivo}: {str(e)}")

# Combinar todos los DataFrames
if all_dfs:
    df_final = pd.concat(all_dfs, ignore_index=True)
    
    # Reorganizar las columnas para que la información del archivo esté al principio
    cols = df_final.columns.tolist()
    cols = ['tipo', 'dataset', 'run', 'proceso', 'archivo'] + [c for c in cols if c not in ['tipo', 'dataset', 'run', 'proceso', 'archivo']]
    df_final = df_final[cols]
    
    # Guardar el archivo final
    salida_path = base_dir / "all_stats_summary.csv"
    df_final.to_csv(salida_path, index=False)
    print(f"\nArchivo final generado en: {salida_path}")
else:
    print("No se encontraron archivos _stats.csv")
