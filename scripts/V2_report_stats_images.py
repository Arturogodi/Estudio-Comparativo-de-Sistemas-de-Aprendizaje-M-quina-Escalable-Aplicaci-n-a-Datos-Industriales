import pandas as pd
import matplotlib.pyplot as plt
import os

# Estilo de fuente
plt.rcParams["font.family"] = "Times New Roman"

# Cargar CSV
df = pd.read_csv("data/images/csv_all_stats.csv")

# Traducción de métricas
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

# Carpeta de salida raíz
output_root = "data/images/anexos_img"
os.makedirs(output_root, exist_ok=True)

# Procesar por dataset
for dataset_name, df_dataset in df.groupby("dataset_name"):
    dataset_dir = os.path.join(output_root, dataset_name)
    os.makedirs(dataset_dir, exist_ok=True)

    for worker, df_worker in df_dataset.groupby("name_clean"):
        df_worker = df_worker.sort_values("run_number")
        runs = [f"Run {int(r)}" for r in df_worker["run_number"]]

        # Construir tabla base
        data = {
            stat_labels[stat]: [round(v, 2) for v in df_worker[stat]]
            for stat in stat_cols
        }
        df_table = pd.DataFrame(data, index=runs).T

        # Agregar columnas de resumen
        df_table["Media"] = df_table.mean(axis=1).round(2)
        df_table["Desviación típica"] = df_table.std(axis=1).round(2)

        # Ajustar alto según número de filas
        fig_height = len(stat_cols) * 0.45 + 0.8
        fig, ax = plt.subplots(figsize=(9, len(stat_cols) * 0.45))
        ax.axis("off")

        # Crear tabla
        table = ax.table(
            cellText=df_table.values,
            rowLabels=df_table.index,
            colLabels=df_table.columns,
            cellLoc='center',
            loc='center'
        )
        table.auto_set_font_size(False)
        table.set_fontsize(10)
        table.scale(1.1, 1.1)

        # Estilo: anchos y fondo
        table.auto_set_column_width(col=list(range(len(df_table.columns))))
        for (row, col), cell in table.get_celld().items():
            if col == 0:
                cell.set_width(cell.get_width() * 0.5)
            else:
                cell.set_width(cell.get_width() * 0.75)
            if row > 0 and row % 2 == 0:
                cell.set_facecolor('#f0f0f0')

        # Guardar solo la tabla con margen mínimo
        plt.savefig(os.path.join(dataset_dir, f"{worker}.png"),
                    dpi=300, bbox_inches='tight', pad_inches=0.02)
        plt.close()
