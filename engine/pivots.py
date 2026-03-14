# engine/pivots.py

import numpy as np
import pandas as pd


def detect_pivots(df: pd.DataFrame, left: int = 3, right: int = 3) -> pd.DataFrame:
    highs = df["high"].values
    lows = df["low"].values

    pivot_high = np.zeros(len(df), dtype=bool)
    pivot_low = np.zeros(len(df), dtype=bool)

    for i in range(left, len(df) - right):
        if highs[i] == max(highs[i-left:i+right+1]):
            pivot_high[i] = True
        if lows[i] == min(lows[i-left:i+right+1]):
            pivot_low[i] = True

    df["pivot_high"] = pivot_high
    df["pivot_low"] = pivot_low
    return df