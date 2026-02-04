from __future__ import annotations

import json
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, Any

import yfinance as yf
import pandas as pd
from kafka import KafkaProducer

from src.utils.config import load_config


def _json_bytes(d: Dict[str, Any]) -> bytes:
    return json.dumps(d, ensure_ascii=False).encode("utf-8")


def _utc_today() -> datetime:
    return datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)


def main() -> None:
    cfg = load_config("config.yaml")
    topic = cfg["kafka"]["topic_price"]
    bootstrap = cfg["kafka"]["bootstrap"]

    ticker = cfg["data"]["ticker"]
    interval = cfg["data"]["interval"]

    producer = KafkaProducer(bootstrap_servers=bootstrap)
    seen = set()

    print(f"[producer] price stream to Kafka topic: {topic}")

    while True:
        # pull recent window to avoid gaps
        end_dt = _utc_today()
        start_dt = end_dt - timedelta(days=10)
        df = yf.download(
            tickers=ticker,
            start=start_dt.date().isoformat(),
            end=end_dt.date().isoformat(),
            interval=interval,
            auto_adjust=False,
            progress=False,
            threads=True,
        )

        if not df.empty:
            df = df.reset_index()
            if "Date" in df.columns:
                df.rename(columns={"Date": "timestamp"}, inplace=True)
            elif "Datetime" in df.columns:
                df.rename(columns={"Datetime": "timestamp"}, inplace=True)

            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")

            for _, row in df.iterrows():
                ts = row["timestamp"]
                if ts is None or ts is pd.NaT:
                    continue
                key = f"{ticker}_{ts.isoformat()}"
                if key in seen:
                    continue

                event = {
                    "ticker": ticker,
                    "timestamp": ts.isoformat(),
                    "open": float(row["Open"]),
                    "high": float(row["High"]),
                    "low": float(row["Low"]),
                    "close": float(row["Close"]),
                    "adj_close": float(row.get("Adj Close", row["Close"])),
                    "volume": int(row["Volume"]),
                }
                producer.send(topic, _json_bytes(event))
                seen.add(key)

        producer.flush()

        # cap memory
        if len(seen) > 50000:
            seen = set(list(seen)[-20000:])

        time.sleep(60)


if __name__ == "__main__":
    main()
