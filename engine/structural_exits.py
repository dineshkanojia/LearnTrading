# engine/structural_exits.py

import pandas as pd


def generate_bullish_structural_exits(df: pd.DataFrame, bull_rev_df: pd.DataFrame) -> pd.DataFrame:
    trades = []

    for row in bull_rev_df.itertuples():
        ll_low = row.LL_low

        entry_idx = row.OB_retest_idx
        entry_time = row.OB_retest_time
        entry_price = df.loc[entry_idx, "close"]

        exit_idx = None
        exit_time = None
        exit_price = None

        for i in range(entry_idx + 1, len(df)):
            if df.loc[i, "close"] < ll_low:
                exit_idx = i
                exit_time = df.loc[i, "time"]
                exit_price = df.loc[i, "close"]
                break

        trades.append({
            "direction": "long",
            "LL_time": row.LL_time,
            "LL_idx": row.LL_idx,
            "LL_low": ll_low,
            "LL_high": row.LL_high,
            "entry_time": entry_time,
            "entry_idx": entry_idx,
            "entry_price": entry_price,
            "exit_time": exit_time,
            "exit_idx": exit_idx,
            "exit_price": exit_price
        })

    return pd.DataFrame(trades)


def generate_bearish_structural_exits(df: pd.DataFrame, bear_rev_df: pd.DataFrame) -> pd.DataFrame:
    trades = []

    for row in bear_rev_df.itertuples():
        hh_high = row.HH_high

        entry_idx = row.OB_retest_idx
        entry_time = row.OB_retest_time
        entry_price = df.loc[entry_idx, "close"]

        exit_idx = None
        exit_time = None
        exit_price = None

        for i in range(entry_idx + 1, len(df)):
            if df.loc[i, "close"] > hh_high:
                exit_idx = i
                exit_time = df.loc[i, "time"]
                exit_price = df.loc[i, "close"]
                break

        trades.append({
            "direction": "short",
            "HH_time": row.HH_time,
            "HH_idx": row.HH_idx,
            "HH_low": row.HH_low,
            "HH_high": hh_high,
            "entry_time": entry_time,
            "entry_idx": entry_idx,
            "entry_price": entry_price,
            "exit_time": exit_time,
            "exit_idx": exit_idx,
            "exit_price": exit_price
        })

    return pd.DataFrame(trades)