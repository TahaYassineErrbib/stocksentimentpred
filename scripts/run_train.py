from __future__ import annotations

from pathlib import Path
import joblib
import pandas as pd

from src.utils.config import load_config
from src.modeling.rf_model import train_rf_classifier
from src.modeling.lstm_model import train_lstm_classifier


def main() -> None:
    cfg = load_config("config.yaml")

    features_path = Path(cfg["paths"]["processed_dir"]) / "features_daily.csv"
    df = pd.read_csv(features_path)

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df = df.dropna(subset=["timestamp"]).copy()

    model_dir = Path("models")
    model_dir.mkdir(exist_ok=True)

    # --- RandomForest baseline ---
    rf_res = train_rf_classifier(df, target_col="direction_next", test_ratio=0.2)
    rf_path = model_dir / "rf_direction_tsla.joblib"
    joblib.dump(
        {"model": rf_res.model, "feature_names": rf_res.feature_names, "metrics": rf_res.metrics},
        rf_path,
    )

    print("=== RandomForest baseline done ===")
    for k, v in rf_res.metrics.items():
        print(f"rf_{k}: {v}")
    print(f"rf_model: {rf_path.as_posix()}")

    # --- LSTM deep model (same features) ---
    lookback = int(cfg["modeling"]["lookback_days"])
    lstm_res = train_lstm_classifier(
        df,
        feature_cols=rf_res.feature_names,
        target_col="direction_next",
        lookback=lookback,
        test_ratio=0.2,
    )
    lstm_path = model_dir / "lstm_direction_tsla.joblib"
    joblib.dump(
        {
            "keras_model": lstm_res.model,   # joblib can store it, but .keras is better (see below)
            "scaler": lstm_res.scaler,
            "feature_names": lstm_res.feature_names,
            "metrics": lstm_res.metrics,
        },
        lstm_path,
    )
    # also save in native Keras format (recommended)
    keras_path = model_dir / "lstm_direction_tsla.keras"
    lstm_res.model.save(keras_path)

    print("\n=== LSTM model done ===")
    for k, v in lstm_res.metrics.items():
        print(f"lstm_{k}: {v}")
    print(f"lstm_model: {keras_path.as_posix()}")


if __name__ == "__main__":
    main()
