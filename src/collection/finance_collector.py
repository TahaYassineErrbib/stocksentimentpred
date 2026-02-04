from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Any, Tuple

import pandas as pd
import yfinance as yf

from src.utils.mongo import get_collection, bulk_upsert_by_key


def _utc_today() -> datetime:
    return datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)


def fetch_prices(ticker: str, start: str, end: str, interval: str = "1d") -> pd.DataFrame:
    df = yf.download(
        tickers=ticker,
        start=start,
        end=end,
        interval=interval,
        auto_adjust=False,
        progress=False,
        threads=True,
    )
    if df.empty:
        raise RuntimeError("yfinance returned empty dataframe. Check ticker/date range.")

    # If timestamp is still in the index, reset it
    df = df.reset_index()
        # Flatten MultiIndex columns from yfinance (e.g., ('Close','TSLA') -> 'Close')
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]

    # Robust timestamp column detection
    if "Date" in df.columns:
        df.rename(columns={"Date": "timestamp"}, inplace=True)
    elif "Datetime" in df.columns:
        df.rename(columns={"Datetime": "timestamp"}, inplace=True)
    elif "index" in df.columns:
        # yfinance sometimes uses unnamed index -> 'index'
        df.rename(columns={"index": "timestamp"}, inplace=True)
    else:
        # Last-resort: try the first column if it looks like datetime
        first_col = df.columns[0]
        if pd.api.types.is_datetime64_any_dtype(df[first_col]) or "date" in str(first_col).lower():
            df.rename(columns={first_col: "timestamp"}, inplace=True)
        else:
            raise RuntimeError(f"Could not find timestamp column. Columns: {list(df.columns)}")

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df = df.dropna(subset=["timestamp"])

    # Keep standard OHLCV columns
    keep = ["timestamp", "Open", "High", "Low", "Close", "Adj Close", "Volume"]
    for c in keep:
        if c not in df.columns:
            if c == "Adj Close":
                df[c] = df["Close"]
            else:
                raise RuntimeError(f"Missing column '{c}' in yfinance output. Columns: {list(df.columns)}")

    df = df[keep].copy()
    df["ticker"] = ticker
    return df


def save_csv(df: pd.DataFrame, out_path: str | Path) -> None:
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)


def upsert_mongo(df: pd.DataFrame, mongo_uri: str, db: str, collection: str) -> int:
    col = get_collection(mongo_uri, db, collection)
    docs = []
    for _, row in df.iterrows():
        docs.append(
            {
                "ticker": row["ticker"],
                # store as ISO string for easy keying + debugging
                "timestamp": row["timestamp"].isoformat(),
                "open": float(row["Open"]),
                "high": float(row["High"]),
                "low": float(row["Low"]),
                "close": float(row["Close"]),
                "adj_close": float(row["Adj Close"]),
                "volume": int(row["Volume"]),
            }
        )
    # Use composite key: ticker+timestamp
    # simplest: key_field = "_id" as f"{ticker}_{timestamp}"
    for d in docs:
        d["_id"] = f'{d["ticker"]}_{d["timestamp"]}'
    return bulk_upsert_by_key(col, docs, key_field="_id")


def collect_finance(
    ticker: str,
    years_back: int,
    interval: str,
    out_csv: str,
    mongo_uri: str,
    mongo_db: str,
    mongo_collection: str,
) -> Dict[str, Any]:
    end_dt = _utc_today()
    start_dt = end_dt - timedelta(days=int(years_back * 365.25))

    df = fetch_prices(
        ticker=ticker,
        start=start_dt.date().isoformat(),
        end=end_dt.date().isoformat(),
        interval=interval,
    )

    save_csv(df, out_csv)
    upsert_count = upsert_mongo(df, mongo_uri, mongo_db, mongo_collection)

    return {
        "ticker": ticker,
        "rows": int(len(df)),
        "csv": out_csv,
        "mongo_upserts": int(upsert_count),
        "start": start_dt.date().isoformat(),
        "end": end_dt.date().isoformat(),
        "interval": interval,
    }
