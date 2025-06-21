import pandas as pd
import matplotlib.pyplot as plt
import os

plt.rcParams["font.family"] = "Times New Roman"

# Cargar datos
df = pd.read_csv("data/images/times_summary_clean.csv")

# Limpiar nombres de columnas y dataset
df["dataset"] = df["dataset"].fillna("No especificado")
df["dataset"] = df["dataset"].str.replace(".parquet", "", regex=False)
df["dataset"] = df["dataset"].str.replace("meta_", "", regex=False)

# Salida
output_root = "data/images/anexos_tiempos"
os.makedirs(output_root, exist_ok=True)

# Procesar por carpeta
for folder_name, df_folder in df.groupby("folder"):
    folder_clean = os.path.basename(folder_name)
    folder_out = os.path.join(output_root, folder_clean)
    os.makedirs(folder_out, exist_ok=True)

    resumen = df_folder.groupby("dataset")["time_seconds"].agg(
        n_runs="count",
        media="mean",
        desviacion="std",
        minimo="min",
        maximo="max"
    ).round(2)

    resumen = resumen.reset_index()

    # Guardar tabla como Markdown
    md_path = os.path.join(folder_out, "resumen_tiempos.md")
    with open(md_path, "w", encoding="utf-8") as f:
        f.write(f"# Resumen de tiempos para: {folder_clean}\n\n")
        f.write(resumen.to_markdown(index=False))

    # Crear imagen
    fig_height = len(resumen) * 0.5 + 1
    fig, ax = plt.subplots(figsize=(9, fig_height))
    ax.axis("off")

    table = ax.table(
        cellText=resumen.values,
        colLabels=resumen.columns,
        cellLoc='center',
        loc='center'
    )
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1.1, 1.1)

    # Color alterno
    for (row, col), cell in table.get_celld().items():
        if row > 0 and row % 2 == 0:
            cell.set_facecolor('#f0f0f0')

    # Guardar imagen
    fig.tight_layout(pad=0.3)
    img_path = os.path.join(folder_out, "resumen_tiempos.png")
    plt.savefig(img_path, dpi=300, bbox_inches='tight', pad_inches=0.01)
    plt.close()
