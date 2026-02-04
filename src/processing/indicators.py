from __future__ import annotations
import pandas as pd


def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    df must have columns: timestamp, Close, Volume (as in raw csv)
    Returns df with indicator columns added.
    """
    out = df.copy()
    out = out.sort_values("timestamp")

    close = out["Close"].astype(float)

    # Moving averages
    out["ma_7"] = close.rolling(7).mean()
    out["ma_14"] = close.rolling(14).mean()

    # Volatility (rolling std of returns)
    ret = close.pct_change()
    out["vol_7"] = ret.rolling(7).std()
    out["vol_14"] = ret.rolling(14).std()

    # RSI (14)
    delta = close.diff()
    gain = delta.clip(lower=0).rolling(14).mean()
    loss = (-delta.clip(upper=0)).rolling(14).mean()
    rs = gain / (loss.replace(0, pd.NA))
    out["rsi_14"] = 100 - (100 / (1 + rs))

    # MACD (12,26,9)
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    out["macd"] = ema12 - ema26
    out["macd_signal"] = out["macd"].ewm(span=9, adjust=False).mean()
    out["macd_hist"] = out["macd"] - out["macd_signal"]

    return out
