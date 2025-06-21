import os
import ray
import ray.data
import xgboost as xgb
import numpy as np
from ray.train.xgboost import XGBoostTrainer
from ray.train import ScalingConfig
from sklearn.metrics import accuracy_score

from template_ray import measure_time_with_run_id_ray, get_run_id

# Inicializa Ray
ray.init(ignore_reinit_error=True)

# Configuración
INPUT_PATH = "/data/gold/amazon2023/amzn23_model_data.parquet"
OP_NAME = "XGBoost VerifiedPurchase Classification - Parquet"
SCRIPT_NAME = os.path.basename(__file__).split('.')[0]
TARGET_COLUMN = "verified_purchase"

def train_xgboost_model():
    # Leer Parquet como dataset distribuido
    ds = ray.data.read_parquet(INPUT_PATH)
    print(f"[i] Dataset cargado con {ds.count()} filas")

    # Obtener nombres de columnas numéricas (con una pequeña muestra)
    sample = ds.take(1)[0]
    num_cols = [k for k, v in sample.items() if isinstance(v, (int, float)) and k != TARGET_COLUMN]
    print(f"[i] Usando columnas: {num_cols}")

    # Mezclar y dividir el dataset
    ds_random = ds.randomize_block_order(seed=42)
    split_index = int(ds.count() * 0.8)
    train_ds, valid_ds = ds_random.split_at_indices([split_index])

    # Entrenamiento distribuido rápido (1 iteración)
    trainer = XGBoostTrainer(
        label_column=TARGET_COLUMN,
        params={
            "objective": "binary:logistic",
            "eval_metric": "logloss",
            "tree_method": "hist",
        },
        datasets={"train": train_ds, "validation": valid_ds},
        num_boost_round=1,
        scaling_config=ScalingConfig(num_workers=5, use_gpu=False),
    )

    result = trainer.fit()
    print(f"[✓] Entrenamiento finalizado. Métricas: {result.metrics}")

    # Evaluar en validación
    checkpoint_path = result.checkpoint.path
    model = xgb.Booster()
    model.load_model(os.path.join(checkpoint_path, "model.json"))

    df_valid = valid_ds.to_pandas()
    X_val = df_valid[num_cols]
    y_true = df_valid[TARGET_COLUMN]

    dval = xgb.DMatrix(X_val)
    y_pred = model.predict(dval)
    acc = accuracy_score(y_true, y_pred > 0.5)
    print(f"[✓] Accuracy en validación: {acc:.4f}")
    return acc

# Main
if __name__ == "__main__":
    base_key = f"{OP_NAME.replace(' ', '_')}__{os.path.basename(INPUT_PATH).replace(' ', '_')}"
    run_id = get_run_id(base_key, output_dir="/data/metrics_ray")

    measure_time_with_run_id_ray(
        operation_name=OP_NAME,
        input_path=INPUT_PATH,
        func=train_xgboost_model,
        output_path=None,
        run_id=run_id,
        log_output=True,
        script_name=SCRIPT_NAME
    )
