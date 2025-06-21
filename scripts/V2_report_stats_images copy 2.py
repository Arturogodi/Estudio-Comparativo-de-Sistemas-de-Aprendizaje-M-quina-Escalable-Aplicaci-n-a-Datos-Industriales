import pandas as pd
import matplotlib.pyplot as plt
import os

# Cargar CSV
df = pd.read_csv("data/images/csv_all_stats.csv")

# Traducción
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

# Carpeta raíz
root_output = "data/images/anexos_img"
os.makedirs(root_output, exist_ok=True)

# Procesar por dataset
for dataset_name, df_dataset in df.groupby("dataset_name"):
    dataset_dir = os.path.join(root_output, dataset_name)
    os.makedirs(dataset_dir, exist_ok=True)

    resumen_global = {}

    for worker, df_worker in df_dataset.groupby("name_clean"):
        df_worker = df_worker.sort_values("run_number")
        runs = [f"Run {int(r)}" for r in df_worker["run_number"]]

        # Tabla visual
        data = {
            stat_labels[stat]: [round(v, 2) for v in df_worker[stat]]
            for stat in stat_cols
        }
        df_table = pd.DataFrame(data, index=runs).T
        df_table["Promedio"] = df_table.mean(axis=1).round(2)
        df_table["Desv. Típica"] = df_table.std(axis=1).round(2)

        # Guardar imagen
        fig, ax = plt.subplots(figsize=(12, len(stat_cols)*0.6 + 1))
        ax.axis("off")
        table_plot = ax.table(cellText=df_table.values,
                              rowLabels=df_table.index,
                              colLabels=df_table.columns,
                              cellLoc='center',
                              loc='center')
        table_plot.auto_set_font_size(False)
        table_plot.set_fontsize(10)
        table_plot.scale(1.2, 1.2)
        title = f"Worker: {worker.replace('_', ' ').title()} – Estadísticas de rendimiento (5 ejecuciones)"
        plt.title(title, fontsize=12, weight="bold", pad=20)

        filename_img = f"{worker}.png"
        plt.savefig(os.path.join(dataset_dir, filename_img), bbox_inches="tight", dpi=300)
        plt.close()

        # Guardar resumen para CSV agregado
        resumen_global[worker] = {
            stat_labels[stat] + " (Media Runs)": round(df_worker[stat].mean(), 2)
            for stat in stat_cols
        }

    # Guardar CSV resumen agregado por worker
    df_resumen = pd.DataFrame(resumen_global).T
    resumen_path = os.path.join(dataset_dir, "resumen_agregado.csv")
    df_resumen.to_csv(resumen_path, encoding="utf-8")
