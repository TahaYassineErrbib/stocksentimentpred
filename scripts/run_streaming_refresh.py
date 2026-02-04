from __future__ import annotations

import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import joblib
import numpy as np
import tensorflow as tf
from pymongo import MongoClient

from src.utils.config import load_config
from src.processing.indicators import add_indicators


def _load_price_from_mongo(mongo_uri: str, db: str, collection: str) -> pd.DataFrame:
    client = MongoClient(mongo_uri)
    col = client[db][collection]
    docs = list(col.find({}, {"_id": 0}))
    if not docs:
        return pd.DataFrame()
    df = pd.DataFrame(docs)
    df = df.rename(
        columns={
            "timestamp": "timestamp",
            "open": "Open",
            "high": "High",
            "low": "Low",
            "close": "Close",
            "adj_close": "Adj Close",
            "volume": "Volume",
        }
    )
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df = df.dropna(subset=["timestamp"])
    df["day"] = df["timestamp"].dt.strftime("%Y-%m-%d")
    return df


def _merge_features(prices: pd.DataFrame, sent_path: Path) -> pd.DataFrame | None:
    if prices.empty or not sent_path.exists():
        return None

    prices_ind = add_indicators(prices)
    sent = pd.read_csv(sent_path)

    merged = prices_ind.merge(sent, on="day", how="left")

    merged["post_count"] = merged["post_count"].fillna(0).astype(int)
    for col in ["mean_sentiment", "mean_influence", "sum_influence", "mean_score"]:
        if col in merged.columns:
            merged[col] = merged[col].fillna(0.0)

    merged = merged.sort_values("timestamp").reset_index(drop=True)
    merged["close_next"] = merged["Close"].shift(-1)
    merged["direction_next"] = (merged["close_next"] > merged["Close"]).astype(int)
    merged = merged.iloc[:-1].copy()
    merged = merged.dropna(subset=["ma_14", "rsi_14", "macd_signal"]).reset_index(drop=True)

    return merged


def _write_predictions(df: pd.DataFrame, cfg: Dict[str, Any], out_path: Path) -> None:
    model_dir = Path("models")
    rf_path = model_dir / "rf_direction_tsla.joblib"
    lstm_path = model_dir / "lstm_direction_tsla.keras"
    lstm_bundle_path = model_dir / "lstm_direction_tsla.joblib"

    if not rf_path.exists() or not lstm_path.exists() or not lstm_bundle_path.exists():
        return

    rf_bundle = joblib.load(rf_path)
    rf_model = rf_bundle["model"]
    feature_names = rf_bundle["feature_names"]
    rf_metrics = rf_bundle.get("metrics", {})
    t_best = float(rf_metrics.get("best_threshold", 0.5))

    X = df[feature_names].astype(float).values
    rf_proba = rf_model.predict_proba(X)[:, 1]
    df = df.copy()
    df["rf_proba_up"] = rf_proba
    df["rf_pred_up"] = (rf_proba >= t_best).astype(int)
    df["rf_threshold"] = t_best

    lookback = int(cfg["modeling"]["lookback_days"])
    scaler = joblib.load(lstm_bundle_path)["scaler"]
    lstm = tf.keras.models.load_model(lstm_path)

    X_scaled = scaler.transform(X)
    X_seq = []
    idx = []
    for i in range(lookback, len(df)):
        X_seq.append(X_scaled[i - lookback : i])
        idx.append(i)
    X_seq = np.array(X_seq, dtype=np.float32)

    lstm_proba = lstm.predict(X_seq, verbose=0).reshape(-1)
    df["lstm_proba_up"] = np.nan
    df.loc[idx, "lstm_proba_up"] = lstm_proba
    df["lstm_pred_up"] = (df["lstm_proba_up"] >= 0.5).astype("Int64")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)


def main() -> None:
    cfg = load_config("config.yaml")
    refresh_seconds = int(cfg.get("streaming", {}).get("refresh_seconds", 60))

    sent_path = Path(cfg["paths"]["processed_dir"]) / "sentiment_daily.csv"
    features_path = Path(cfg["paths"]["processed_dir"]) / "features_daily.csv"
    predictions_path = Path(cfg["paths"]["processed_dir"]) / "predictions_daily.csv"

    while True:
        prices = _load_price_from_mongo(
            cfg["mongo"]["uri"],
            cfg["mongo"]["db"],
            cfg["mongo"]["collections"]["price_raw"],
        )

        merged = _merge_features(prices, sent_path)
        if merged is not None and not merged.empty:
            features_path.parent.mkdir(parents=True, exist_ok=True)
            merged.to_csv(features_path, index=False)
            _write_predictions(merged, cfg, predictions_path)

        time.sleep(refresh_seconds)


if __name__ == "__main__":
    main()
