import pandas as pd
import os

# Cargar datos
df = pd.read_csv("data/images/csv_all_stats.csv")

# Crear carpeta de salida si no existe
output_dir = "data/images/anexos"
os.makedirs(output_dir, exist_ok=True)

# Diccionario con traducción al español
stat_labels = {
    "cpu_perc_mean": "Media CPU (%)",
    "cpu_perc_std": "Desviación típica CPU (%)",
    "cpu_perc_min": "Mínimo CPU (%)",
    "cpu_perc_max": "Máximo CPU (%)",
    "cpu_perc_median": "Mediana CPU (%)",
    "mem_used_GB_mean": "Media Memoria (GB)",
    "mem_used_GB_std": "Desviación típica Memoria (GB)",
    "mem_used_GB_min": "Mínimo Memoria (GB)",
    "mem_used_GB_max": "Máximo Memoria (GB)",
    "mem_used_GB_median": "Mediana Memoria (GB)"
}

# Lista de columnas que vamos a mostrar
stat_cols = list(stat_labels.keys())

# Agrupar por dataset_name
for dataset_name, df_dataset in df.groupby("dataset_name"):
    output_path = os.path.join(output_dir, f"{dataset_name}.md")
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(f"# Anexo: {dataset_name}\n\n")
        f.write(f"_Framework: {df_dataset['framework'].iloc[0]}_\n\n")

        for worker, df_worker in df_dataset.groupby("name_clean"):
            f.write(f"## Worker: {worker.replace('_', ' ').title()}\n\n")

            # Ordenar por run
            df_worker = df_worker.sort_values(by="run_number")

            # Encabezado de la tabla
            f.write("| Estadística | " + " | ".join(f"Run {int(r)}" for r in df_worker["run_number"]) + " |\n")
            f.write("|-------------" + "|--------" * df_worker.shape[0] + "|\n")

            # Filas de estadísticas
            for stat in stat_cols:
                nombre = stat_labels[stat]
                valores = [f"{v:.3f}" for v in df_worker[stat]]
                f.write("| " + nombre + " | " + " | ".join(valores) + " |\n")
            f.write("\n")
