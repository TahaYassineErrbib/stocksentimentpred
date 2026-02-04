from __future__ import annotations

from pathlib import Path
import joblib
import numpy as np
import pandas as pd
import tensorflow as tf

from src.utils.config import load_config


def main() -> None:
    cfg = load_config("config.yaml")
    data_path = Path(cfg["paths"]["processed_dir"]) / "features_daily.csv"
    out_path = Path(cfg["paths"]["processed_dir"]) / "predictions_daily.csv"

    df = pd.read_csv(data_path)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df = df.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)

    model_dir = Path("models")

    # --- RF ---
    rf_bundle = joblib.load(model_dir / "rf_direction_tsla.joblib")
    rf_model = rf_bundle["model"]
    feature_names = rf_bundle["feature_names"]
    rf_metrics = rf_bundle.get("metrics", {})
    t_best = float(rf_metrics.get("best_threshold", 0.5))

    X = df[feature_names].astype(float).values
    rf_proba = rf_model.predict_proba(X)[:, 1]
    df["rf_proba_up"] = rf_proba
    df["rf_pred_up"] = (rf_proba >= t_best).astype(int)
    df["rf_threshold"] = t_best

    # --- LSTM ---
    lookback = int(cfg["modeling"]["lookback_days"])
    scaler = joblib.load(model_dir / "lstm_direction_tsla.joblib")["scaler"]
    lstm = tf.keras.models.load_model(model_dir / "lstm_direction_tsla.keras")

    X_scaled = scaler.transform(X)

    # build sequences aligned to current row (prediction for that row)
    X_seq = []
    idx = []
    for i in range(lookback, len(df)):
        X_seq.append(X_scaled[i - lookback:i])
        idx.append(i)
    X_seq = np.array(X_seq, dtype=np.float32)

    lstm_proba = lstm.predict(X_seq, verbose=0).reshape(-1)
    df["lstm_proba_up"] = np.nan
    df.loc[idx, "lstm_proba_up"] = lstm_proba
    df["lstm_pred_up"] = (df["lstm_proba_up"] >= 0.5).astype("Int64")

    df.to_csv(out_path, index=False)
    print("=== Predictions written ===")
    print(f"csv: {out_path.as_posix()}")
    print(f"rows: {len(df)}")


if __name__ == "__main__":
    main()
