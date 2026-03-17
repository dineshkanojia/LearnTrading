# engine/utils.py

import os
from datetime import datetime, timedelta
import pandas as pd
from binance.client import Client
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Optional, List, Tuple
from datetime import datetime, timedelta, UTC



def get_binance_client(api_key: str, api_secret: str) -> Client:
    return Client(api_key, api_secret)

def fetch_klines(
    client,
    symbol: str,
    interval: str,
    lookback_months: int = 3,
    fixed_start: str | None = None,
) -> pd.DataFrame:
    if fixed_start is not None:
        start_time = fixed_start
    else:
        start_dt = datetime.now(UTC) - timedelta(days=30 * lookback_months)
        start_dt = start_dt.replace(minute=(start_dt.minute // 15) * 15, second=0, microsecond=0)
        start_time = start_dt.strftime("%d %b %Y %H:%M:%S")

    klines = client.futures_historical_klines(symbol, interval, start_time)

    df = pd.DataFrame(
        klines,
        columns=[
            "open_time", "open", "high", "low", "close", "volume",
            "close_time", "qav", "num_trades", "taker_base",
            "taker_quote", "ignore"
        ]
    )

    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = df[col].astype(float)

    df["time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df["idx"] = range(len(df))

    return df

def get_export_path(prefix: str = "OB_engine") -> str:
    save_path = os.path.expanduser("~/Desktop")
    os.makedirs(save_path, exist_ok=True)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M")
    filename = f"{save_path}/{prefix}_{timestamp}.xlsx"
    return filename

class OBState(Enum):
    ACTIVE = auto()
    SUPPRESSED = auto()
    INVALIDATED = auto()
    USED = auto()


class TradeDirection(Enum):
    LONG = auto()
    SHORT = auto()


@dataclass
class BearishOB:
    id: int
    hh_time: float
    hh_idx: int
    low: float
    high: float
    state: OBState = OBState.ACTIVE
    priority: int = 0  # higher = more priority


@dataclass
class Trade:
    scenario: str
    direction: TradeDirection
    ob_id: int
    hh_time: float
    hh_idx: int
    hh_low: float
    hh_high: float
    entry_time: float
    entry_idx: int
    entry_price: float
    exit_time: Optional[float] = None
    exit_idx: Optional[int] = None
    exit_price: Optional[float] = None
    exit_reason: Optional[str] = None
    ll_anchor_time: Optional[float] = None
    ll_anchor_idx: Optional[int] = None
    sl_at_entry: Optional[float] = None
    sl_at_hit: Optional[float] = None


def ranges_overlap(low1: float, high1: float, low2: float, high2: float) -> bool:
    return not (high1 < low2 or high2 < low1)


def close_above_ob_high(close_price: float, ob: BearishOB) -> bool:
    return close_price > ob.high
