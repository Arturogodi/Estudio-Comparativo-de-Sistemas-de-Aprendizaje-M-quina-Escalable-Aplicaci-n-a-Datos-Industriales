import pandas as pd
import matplotlib.pyplot as plt
import os

plt.rcParams["font.family"] = "Times New Roman"

# Cargar CSV
df = pd.read_csv("data/images/csv_all_stats.csv")

# Métricas válidas para resumen
valid_stats = {
    "cpu_perc_mean": "Media CPU (%)",
    "cpu_perc_std": "Desv. CPU (%)",
    "cpu_perc_min": "Mínimo CPU (%)",
    "cpu_perc_max": "Máximo CPU (%)",
    "mem_used_GB_mean": "Media RAM (GB)",
    "mem_used_GB_std": "Desv. RAM (GB)",
    "mem_used_GB_min": "Mínimo RAM (GB)",
    "mem_used_GB_max": "Máximo RAM (GB)"
}

output_root = "data/images/anexos_img"
os.makedirs(output_root, exist_ok=True)

for dataset_name, df_dataset in df.groupby("dataset_name"):
    dataset_dir = os.path.join(output_root, dataset_name)
    os.makedirs(dataset_dir, exist_ok=True)

    resumen = {}

    for worker, df_worker in df_dataset.groupby("name_clean"):
        resumen[worker] = {}
        for stat, label in valid_stats.items():
            mean_val = df_worker[stat].mean()
            std_val = df_worker[stat].std()
            resumen[worker][f"{label} (Media)"] = round(mean_val, 2)
            resumen[worker][f"{label} (Desv.)"] = round(std_val, 2)

    df_resumen = pd.DataFrame(resumen).T

    # -------- Crear imagen ----------
    n_cols = len(df_resumen.columns)
    n_rows = len(df_resumen)

    max_width = 14
    max_height = 8
    fig_width = min(n_cols * 0.7 + 2, max_width)
    fig_height = min(n_rows * 0.5 + 1, max_height)

    fig, ax = plt.subplots(figsize=(fig_width, fig_height))
    ax.axis("off")

    table = ax.table(
        cellText=df_resumen.values,
        rowLabels=df_resumen.index,
        colLabels=df_resumen.columns,
        cellLoc='center',
        loc='center'
    )
    table.auto_set_font_size(False)
    table.set_fontsize(9)
    table.scale(1.1, 1.1)

    # Estilo visual (gris claro alterno)
    for (row, col), cell in table.get_celld().items():
        if row > 0 and row % 2 == 0:
            cell.set_facecolor('#f0f0f0')

    # Guardar imagen
    fig.tight_layout(pad=0.3)
    plt.savefig(os.path.join(dataset_dir, "resumen_agregado.png"),
                dpi=300, bbox_inches='tight', pad_inches=0.01)
    plt.close()
