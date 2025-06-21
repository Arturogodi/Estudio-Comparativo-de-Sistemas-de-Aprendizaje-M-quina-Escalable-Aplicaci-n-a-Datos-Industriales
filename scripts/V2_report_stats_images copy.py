import pandas as pd
import matplotlib.pyplot as plt
import os

# Cargar CSV
df = pd.read_csv("data/images/csv_all_stats.csv")

# Traducción al español
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

stat_cols = list(stat_labels.keys())

# Carpeta de salida
output_dir = "data/images/anexos_img"
os.makedirs(output_dir, exist_ok=True)

# Generar imágenes
for dataset_name, df_dataset in df.groupby("dataset_name"):
    for worker, df_worker in df_dataset.groupby("name_clean"):
        df_worker = df_worker.sort_values("run_number")
        data = {
            stat_labels[stat]: [f"{v:.3f}" for v in df_worker[stat]]
            for stat in stat_cols
        }
        table_df = pd.DataFrame(data, index=[f"Run {int(r)}" for r in df_worker["run_number"]]).T

        # Crear imagen
        fig, ax = plt.subplots(figsize=(10, len(stat_cols) * 0.5))
        ax.axis("off")
        table = ax.table(cellText=table_df.values,
                         rowLabels=table_df.index,
                         colLabels=table_df.columns,
                         cellLoc='center',
                         loc='center')
        table.auto_set_font_size(False)
        table.set_fontsize(10)
        table.scale(1.2, 1.2)

        # Guardar
        filename = f"{dataset_name}_{worker}.png".replace(" ", "_")
        plt.savefig(os.path.join(output_dir, filename), bbox_inches="tight", dpi=300)
        plt.close()
