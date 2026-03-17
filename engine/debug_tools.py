# engine/debug_tools.py

from __future__ import annotations

import pandas as pd


def debug_raw_window_by_idx(df: pd.DataFrame, start_idx: int, end_idx: int) -> pd.DataFrame:
    """
    Dump raw OHLC candles by dataframe index range.
    """
    out = df.loc[start_idx:end_idx, ["time", "open", "high", "low", "close"]].copy()
    out.insert(0, "idx", out.index)
    print("\n[RAW WINDOW BY IDX]")
    print(out.to_string(index=False))
    return out


def debug_raw_window_by_time(df: pd.DataFrame, start_time: str, end_time: str) -> pd.DataFrame:
    t1 = pd.Timestamp(start_time, tz="UTC")
    t2 = pd.Timestamp(end_time, tz="UTC")

    out = df[(df["time"] >= t1) & (df["time"] <= t2)][["time", "open", "high", "low", "close"]].copy()
    out.insert(0, "idx", out.index)

    print("\n[RAW WINDOW BY TIME]")
    print(out.to_string(index=False))
    return out


def debug_structure_rows(
    sw_df: pd.DataFrame,
    indices: list[int],
) -> pd.DataFrame:
    """
    Dump matching rows from Structure_Swings for the given indices.
    """
    out = sw_df[sw_df["idx"].isin(indices)].copy()

    print("\n[STRUCTURE ROWS]")
    if out.empty:
        print("No matching structure rows found.")
    else:
        print(out.to_string(index=False))
    return out


def debug_single_candle(df: pd.DataFrame, idx: int, tag: str = "") -> dict:
    """
    Print one raw candle from df by index.
    """
    row = df.loc[idx]
    payload = {
        "tag": tag,
        "idx": idx,
        "time": row["time"],
        "open": row["open"],
        "high": row["high"],
        "low": row["low"],
        "close": row["close"],
    }
    print(f"\n[SINGLE CANDLE] {tag}")
    print(payload)
    return payload


def debug_bullish_reversal_row(
    df: pd.DataFrame,
    sw_df: pd.DataFrame,
    bull_rev_df: pd.DataFrame,
    row_idx: int,
    window_before: int = 3,
    window_after: int = 3,
) -> None:
    """
    Full audit for one bullish reversal row from Bullish_OB_Reversals.

    It prints:
    - the reversal row itself
    - raw LL / IHH / MSS / RETEST candles
    - structure rows matching LL/IHH/MSS/RETEST indices
    - validation checks for MSS body-break and retest touch
    - surrounding raw windows
    """
    if row_idx not in bull_rev_df.index:
        raise IndexError(f"row_idx={row_idx} not found in bull_rev_df.index")

    row = bull_rev_df.loc[row_idx]

    ll_idx = int(row["LL_idx"])
    ihh_idx = int(row["IHH_idx"])
    mss_idx = int(row["MSS_idx"])
    retest_idx = int(row["OB_retest_idx"])

    ll_low = float(row["LL_low"])
    ll_high = float(row["LL_high"])
    ihh_high = float(row["IHH_high"])

    print("\n" + "=" * 100)
    print("[BULLISH REVERSAL ROW]")
    print(row.to_string())
    print("=" * 100)

    ll_c = debug_single_candle(df, ll_idx, "LL")
    ihh_c = debug_single_candle(df, ihh_idx, "IHH")
    mss_c = debug_single_candle(df, mss_idx, "MSS")
    retest_c = debug_single_candle(df, retest_idx, "RETEST")

    debug_structure_rows(sw_df, [ll_idx, ihh_idx, mss_idx, retest_idx])

    # Hard validations
    mss_body_break_valid = (mss_c["open"] <= ihh_high) and (mss_c["close"] > ihh_high)
    retest_zone_touch_valid = (retest_c["low"] <= ll_high) and (retest_c["high"] >= ll_low)

    retest_body_inside_zone = (
        (ll_low <= retest_c["open"] <= ll_high) or
        (ll_low <= retest_c["close"] <= ll_high)
    )

    retest_close_inside_zone = ll_low <= retest_c["close"] <= ll_high

    print("\n[VALIDATION CHECKS]")
    print({
        "ll_zone_low": ll_low,
        "ll_zone_high": ll_high,
        "ihh_high": ihh_high,
        "mss_open": mss_c["open"],
        "mss_close": mss_c["close"],
        "mss_body_break_valid": mss_body_break_valid,
        "retest_low": retest_c["low"],
        "retest_high": retest_c["high"],
        "retest_open": retest_c["open"],
        "retest_close": retest_c["close"],
        "retest_zone_touch_valid": retest_zone_touch_valid,
        "retest_body_inside_zone": retest_body_inside_zone,
        "retest_close_inside_zone": retest_close_inside_zone,
    })

    start_idx = max(0, min(ll_idx, ihh_idx, mss_idx, retest_idx) - window_before)
    end_idx = min(len(df) - 1, max(ll_idx, ihh_idx, mss_idx, retest_idx) + window_after)

    debug_raw_window_by_idx(df, start_idx, end_idx)


def debug_exact_time_match(df: pd.DataFrame, t: str) -> pd.DataFrame:
    ts = pd.Timestamp(t, tz="UTC")
    out = df[df["time"] == ts][["time", "open", "high", "low", "close"]].copy()
    out.insert(0, "idx", out.index)

    print(f"\n[EXACT TIME MATCH] {t} UTC")
    if out.empty:
        print("No exact match found.")
    else:
        print(out.to_string(index=False))
    return out


def debug_bearish_exit_source_by_time(
    bull_rev_df: pd.DataFrame,
    bear_flow_df: pd.DataFrame,
    exit_time: str,
) -> None:
    """
    Find bearish flow trade(s) by exit_time, then show which bullish reversal(s)
    match that same exit candle by time or idx.

    exit_time example:
        "2026-02-22 12:00:00"
    """
    ts = pd.Timestamp(exit_time, tz="UTC")

    bear_matches = bear_flow_df[bear_flow_df["exit_time"] == ts]

    print("\n" + "=" * 100)
    print(f"[BEARISH FLOW ROWS @ EXIT TIME = {ts}]")
    if bear_matches.empty:
        print("No Bearish_Flow_Trades row matched this exit_time.")
        print("=" * 100)
        return
    else:
        print(bear_matches.to_string())
    print("=" * 100)

    for row_idx, bear_row in bear_matches.iterrows():
        exit_idx = bear_row["exit_idx"]

        print("\n" + "-" * 100)
        print(f"[MATCHING BULLISH REVERSALS FOR BEAR FLOW ROW INDEX = {row_idx}]")
        print("-" * 100)

        print("\n[MATCH BY EXIT TIME]")
        match_time = bull_rev_df[bull_rev_df["OB_retest_time"] == ts]
        if match_time.empty:
            print("No bullish reversal matched by OB_retest_time.")
        else:
            print(match_time.to_string(index=False))

        print("\n[MATCH BY EXIT IDX]")
        match_idx = bull_rev_df[bull_rev_df["OB_retest_idx"] == exit_idx]
        if pd.isna(exit_idx) or match_idx.empty:
            print("No bullish reversal matched by OB_retest_idx.")
        else:
            print(match_idx.to_string(index=False))


def debug_bullish_retests_near_exit_by_time(
    bull_rev_df: pd.DataFrame,
    exit_time: str,
    hours_before: int = 24,
    hours_after: int = 6,
) -> pd.DataFrame:
    """
    Show all bullish reversal retests near a given bearish exit time.

    exit_time example:
        "2026-02-22 12:00:00"
    """
    ts = pd.Timestamp(exit_time, tz="UTC")
    t1 = ts - pd.Timedelta(hours=hours_before)
    t2 = ts + pd.Timedelta(hours=hours_after)

    out = bull_rev_df[
        (bull_rev_df["OB_retest_time"] >= t1) &
        (bull_rev_df["OB_retest_time"] <= t2)
    ].copy()

    print("\n" + "=" * 100)
    print(f"[BULLISH RETESTS NEAR EXIT TIME = {ts}]")
    print(f"Window: {t1}  -->  {t2}")
    if out.empty:
        print("No bullish reversal retests found in this window.")
    else:
        print(out.to_string(index=False))
    print("=" * 100)

    return out