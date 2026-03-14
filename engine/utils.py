# engine/utils.py

import os
from datetime import datetime, timedelta
import pandas as pd
from binance.client import Client


def get_binance_client(api_key: str, api_secret: str) -> Client:
    return Client(api_key, api_secret)


def fetch_klines(client: Client, symbol: str, interval: str, lookback_months: int = 3) -> pd.DataFrame:
    start_time = (datetime.utcnow() - timedelta(days=30 * lookback_months)).strftime("%d %b %Y %H:%M:%S")
    klines = client.get_historical_klines(symbol, interval, start_time)

    df = pd.DataFrame(klines, columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "qav", "num_trades", "taker_base",
        "taker_quote", "ignore"
    ])

    df["open"] = df["open"].astype(float)
    df["high"] = df["high"].astype(float)
    df["low"] = df["low"].astype(float)
    df["close"] = df["close"].astype(float)
    df["time"] = pd.to_datetime(df["open_time"], unit="ms")
    df["idx"] = df.index

    return df


def get_export_path(prefix: str = "OB_engine") -> str:
    save_path = os.path.expanduser("~/Desktop")
    os.makedirs(save_path, exist_ok=True)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M")
    filename = f"{save_path}/{prefix}_{timestamp}.xlsx"
    return filename