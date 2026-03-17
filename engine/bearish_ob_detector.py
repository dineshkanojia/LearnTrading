# engine/bearish_ob.py
import pandas as pd

def detect_bearish_ob(df: pd.DataFrame, sw_df: pd.DataFrame):
    bear_reversals = []
    bear_mitigated = []

    # Only HH swings matter for bearish OB
    hh_swings = sw_df[(sw_df["type"] == "high") & (sw_df["label"] == "HH")]

    for hh_row in hh_swings.itertuples():

        hh_idx = hh_row.idx
        hh_time = hh_row.time
        hh_high = df.loc[hh_idx, "high"]
        hh_low = df.loc[hh_idx, "low"]

        # ---------------------------------------------------------
        # 1) IHL = last candle BEFORE HH whose close < HH_low
        # ---------------------------------------------------------
        ihl_idx = None
        ihl_price = None
        ihl_time = None

        for j in range(hh_idx - 1, -1, -1):

            # If price closes ABOVE HH high → invalidates IHL search
            if df.loc[j, "close"] > hh_high:
                ihl_idx = None
                break

            # Strict IHL condition
            if df.loc[j, "close"] < hh_low:
                ihl_idx = j
                ihl_price = df.loc[j, "low"]
                ihl_time = df.loc[j, "time"]
                break

        if ihl_idx is None:
            continue

        # ---------------------------------------------------------
        # 2) MSS-DOWN = first close < IHL low AFTER IHL
        # ---------------------------------------------------------
        mss_idx = None
        mss_time = None
        mss_close = None

        for k in range(ihl_idx + 1, len(df)):

            candle_close = df.loc[k, "close"]

            # Mitigation before MSS
            if candle_close > hh_high:
                bear_mitigated.append({
                    "HH_time": hh_time,
                    "HH_idx": hh_idx,
                    "HH_low": hh_low,
                    "HH_high": hh_high,
                    "mitigation_time": df.loc[k, "time"],
                    "mitigation_idx": k,
                    "reason": "Close above HH before MSS"
                })
                mss_idx = None
                break

            if candle_close < ihl_price:
                mss_idx = k
                mss_time = df.loc[k, "time"]
                mss_close = candle_close
                break

        if mss_idx is None:
            continue

        # ---------------------------------------------------------
        # 3) OB RETEST = first candle that trades inside HH candle
        # ---------------------------------------------------------
        ob_retest_idx = None
        ob_retest_time = None
        ob_retest_open = None
        ob_retest_high = None
        ob_retest_low = None
        ob_retest_close = None

        for r in range(mss_idx + 1, len(df)):

            candle_open = df.loc[r, "open"]
            candle_low = df.loc[r, "low"]
            candle_high = df.loc[r, "high"]
            candle_close = df.loc[r, "close"]

            # Mitigation before retest
            if candle_close > hh_high:
                bear_mitigated.append({
                    "HH_time": hh_time,
                    "HH_idx": hh_idx,
                    "HH_low": hh_low,
                    "HH_high": hh_high,
                    "mitigation_time": df.loc[r, "time"],
                    "mitigation_idx": r,
                    "reason": "Close above HH before OB retest"
                })
                ob_retest_idx = None
                break

            # OB retest condition
            if candle_low <= hh_high and candle_high >= hh_low:
                ob_retest_idx = r
                ob_retest_time = df.loc[r, "time"]
                ob_retest_open = candle_open
                ob_retest_high = candle_high
                ob_retest_low = candle_low
                ob_retest_close = candle_close
                break

        if ob_retest_idx is not None:
            bear_reversals.append({
                "HH_time": hh_time,
                "HH_idx": hh_idx,
                "HH_low": hh_low,
                "HH_high": hh_high,
                "IHL_time": ihl_time,
                "IHL_idx": ihl_idx,
                "IHL_price": ihl_price,
                "MSS_time": mss_time,
                "MSS_idx": mss_idx,
                "MSS_close": mss_close,
                "OB_retest_time": ob_retest_time,
                "OB_retest_idx": ob_retest_idx,
                "OB_retest_open": ob_retest_open,
                "OB_retest_high": ob_retest_high,
                "OB_retest_low": ob_retest_low,
                "OB_retest_close": ob_retest_close,
            })

    return pd.DataFrame(bear_reversals), pd.DataFrame(bear_mitigated)