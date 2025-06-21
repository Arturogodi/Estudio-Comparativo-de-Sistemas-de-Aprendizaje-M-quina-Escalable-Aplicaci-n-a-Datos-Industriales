import os
import time
import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score
from template_ray import (
    measure_time_with_run_id_ray,
    redirect_output,
    write_state,
    clear_state,
    get_run_id
)

INPUT_PATH = "data/gold/amazon2023/amzn23_model_data_csv"
OUTPUT_PATH = "data/gold/amazon2023/amzn23_xgb_verified_predictions_fast.parquet"
TARGET_COLUMN = "verified_purchase"


def xgb_classification_pipeline():
    # Leer datos usando pandas
    df = pd.read_csv(INPUT_PATH)
    print(f"[i] Dataset cargado con {len(df)} filas")

    # Seleccionar columnas numéricas
    num_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    num_cols.remove(TARGET_COLUMN)
    print(f"[i] Usando columnas: {num_cols}")

    # Preparar datos
    X = df[num_cols]
    y = df[TARGET_COLUMN]

    # División train/test
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    # Convertir a DMatrix de XGBoost
    dtrain = xgb.DMatrix(X_train, label=y_train)
    dtest = xgb.DMatrix(X_test, label=y_test)

    # Parámetros del modelo (limitados para versión rápida)
    params = {
        'objective': 'binary:logistic',
        'max_depth': 5,
        'eta': 0.3,
        'eval_metric': 'auc',
        'seed': 42
    }

    # Entrenamiento
    model = xgb.train(
        params,
        dtrain,
        num_boost_round=1,  # Solo 1 ronda para versión rápida
        evals=[(dtrain, 'train'), (dtest, 'test')],
        verbose_eval=True
    )

    # Predicciones y evaluación
    y_pred_proba = model.predict(dtest)
    auc = roc_auc_score(y_test, y_pred_proba)
    print(f"[✓] AUC en validación: {auc:.4f}")

    # Preparar resultados para guardar
    predictions = pd.DataFrame({
        'prediction': (y_pred_proba > 0.5).astype(int),
        'probability': y_pred_proba,
        'verified_purchase': y_test
    })

    # Guardar predicciones
    predictions.to_parquet(OUTPUT_PATH, index=False)
    print(f"[✓] Predicciones guardadas en: {OUTPUT_PATH}")

    return auc


def main():
    script_name = os.path.splitext(os.path.basename(__file__))[0]
    redirect_output(script_name)

    run_base = "XGB_Verified_QuickTest"
    run_id = get_run_id(run_base)
    fake_input = f"{run_base}__run{run_id}.csv"

    write_state("XGB Fast Test", input_file=fake_input)

    auc = measure_time_with_run_id_ray(
        operation_name="XGBoost Verified Purchase - Fast",
        input_path=INPUT_PATH,
        func=xgb_classification_pipeline,
        output_path=OUTPUT_PATH,
        script_name=script_name,
        log_output=False,
        run_id=run_id
    )

    clear_state()

if __name__ == "__main__":
    main()
