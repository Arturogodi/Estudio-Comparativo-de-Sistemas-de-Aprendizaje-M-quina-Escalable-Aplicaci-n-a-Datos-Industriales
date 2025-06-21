import os
import ray
import ray.data
from ray.train.lightgbm import LightGBMTrainer
from ray.train import ScalingConfig
from sklearn.metrics import roc_auc_score
from ray.train.lightgbm import load_model

from template_ray import measure_time_with_run_id_ray, get_run_id

ray.init(ignore_reinit_error=True)

INPUT_PATH = "/data/gold/amazon2023/amzn23_model_data.parquet"
OP_NAME = "LightGBM Classification Amazon23"
SCRIPT_NAME = os.path.basename(__file__).split('.')[0]
TARGET_COLUMN = "label"  # cambia si tu columna objetivo tiene otro nombre

def train_lightgbm_model():
    ds = ray.data.read_parquet(INPUT_PATH)
    print(f"[i] Dataset cargado con {ds.count()} filas")

    # Asume que todas las columnas excepto la etiqueta son numéricas y válidas
    feature_columns = [col for col in ds.schema().names if col != TARGET_COLUMN]
    print(f"[i] Usando columnas: {feature_columns}")

    # Split del dataset
    train_ds, valid_ds = ds.random_split([0.8, 0.2], seed=42)

    # Configuración del entrenamiento distribuido
    trainer = LightGBMTrainer(
        label_column=TARGET_COLUMN,
        params={
            "objective": "binary",         # cambia a "regression" si es regresión
            "metric": "auc",
            "boosting_type": "gbdt"
        },
        dtrain=train_ds,
        dvalid=valid_ds,
        scaling_config=ScalingConfig(num_workers=5, use_gpu=False),
        num_boost_round=100
    )

    result = trainer.fit()
    print(f"[✓] Entrenamiento completado. Métricas: {result.metrics}")

    # Evaluar con sklearn por separado
    model = load_model(result.checkpoint)
    X_val = valid_ds.to_pandas()[feature_columns]
    y_val = valid_ds.to_pandas()[TARGET_COLUMN]
    y_pred = model.predict(X_val)
    auc = roc_auc_score(y_val, y_pred)
    print(f"[✓] ROC AUC en validación: {auc:.4f}")
    return auc

if __name__ == "__main__":
    base_key = f"{OP_NAME.replace(' ', '_')}__{os.path.basename(INPUT_PATH).replace(' ', '_')}"
    run_id = get_run_id(base_key, output_dir="/data/metrics_ray")

    measure_time_with_run_id_ray(
        operation_name=OP_NAME,
        input_path=INPUT_PATH,
        func=train_lightgbm_model,
        output_path=None,
        run_id=run_id,
        log_output=True,
        script_name=SCRIPT_NAME
    )
