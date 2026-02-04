from __future__ import annotations

from pathlib import Path
import pandas as pd

from src.utils.config import load_config
from src.processing.indicators import add_indicators


def main() -> None:
    cfg = load_config("config.yaml")

    prices_path = Path(cfg["paths"]["raw_prices_csv"])
    sent_path = Path(cfg["paths"]["processed_dir"]) / "sentiment_daily.csv"
    out_path = Path(cfg["paths"]["processed_dir"]) / "features_daily.csv"

    prices = pd.read_csv(prices_path)
    prices["timestamp"] = pd.to_datetime(prices["timestamp"], utc=True)
    prices["day"] = prices["timestamp"].dt.strftime("%Y-%m-%d")

    prices_ind = add_indicators(prices)

    sent = pd.read_csv(sent_path)
    # sent already has 'day' string

    merged = prices_ind.merge(sent, on="day", how="left")

    # Fill missing social features with neutral defaults (0 sentiment, 0 influence, 0 posts)
    merged["post_count"] = merged["post_count"].fillna(0).astype(int)
    for col in ["mean_sentiment", "mean_influence", "sum_influence", "mean_score"]:
        if col in merged.columns:
            merged[col] = merged[col].fillna(0.0)

    # Label: next-day direction
    merged = merged.sort_values("timestamp").reset_index(drop=True)
    merged["close_next"] = merged["Close"].shift(-1)
    merged["direction_next"] = (merged["close_next"] > merged["Close"]).astype(int)

    # Drop last row (no next day label)
    merged = merged.iloc[:-1].copy()

    # Drop rows where indicators are NaN (warm-up period)
    feature_cols = [
        "ma_7","ma_14","vol_7","vol_14","rsi_14","macd","macd_signal","macd_hist",
        "post_count","mean_sentiment","mean_influence","sum_influence","mean_score"
    ]
    merged = merged.dropna(subset=["ma_14","rsi_14","macd_signal"]).copy()

    out_path.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(out_path, index=False)

    print("=== Merge done ===")
    print(f"rows: {len(merged)}")
    print(f"csv: {out_path.as_posix()}")
    print(f"label_balance(direction_next mean): {merged['direction_next'].mean():.3f}")


if __name__ == "__main__":
    main()
