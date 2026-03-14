# engine/bullish_ob.py

import pandas as pd


def detect_bullish_ob(df: pd.DataFrame, sw_df: pd.DataFrame):
    bull_reversals = []
    bull_mitigated = []

    for ll_row in sw_df[(sw_df["type"] == "low") & (sw_df["label"] == "LL")].itertuples():

        ll_idx = ll_row.idx
        ll_high = df.loc[ll_idx, "high"]
        ll_low = df.loc[ll_idx, "low"]
        ll_time = ll_row.time

        # 1) IHH = pivot high with close > LL high (strict)
        ihh_idx = None
        ihh_price = None
        ihh_time = None

        for j in range(ll_idx + 1, len(df)):

            if df.loc[j, "close"] < ll_low:
                bull_mitigated.append({
                    "LL_time": ll_time,
                    "LL_idx": ll_idx,
                    "LL_low": ll_low,
                    "LL_high": ll_high,
                    "mitigation_time": df.loc[j, "time"],
                    "mitigation_idx": j,
                    "reason": "Close below LL before IHH"
                })
                ihh_idx = None
                break

            if df.loc[j, "pivot_high"] and df.loc[j, "close"] > ll_high:
                ihh_idx = j
                ihh_price = df.loc[j, "high"]
                ihh_time = df.loc[j, "time"]
                break

        if ihh_idx is None:
            continue

        # 2) MSS-UP = close > IHH high (strict)
        mss_idx = None
        mss_time = None
        mss_close = None

        for k in range(ihh_idx + 1, len(df)):

            candle_close = df.loc[k, "close"]

            if candle_close < ll_low:
                bull_mitigated.append({
                    "LL_time": ll_time,
                    "LL_idx": ll_idx,
                    "LL_low": ll_low,
                    "LL_high": ll_high,
                    "mitigation_time": df.loc[k, "time"],
                    "mitigation_idx": k,
                    "reason": "Close below LL before MSS"
                })
                mss_idx = None
                break

            if candle_close > ihh_price:
                mss_idx = k
                mss_time = df.loc[k, "time"]
                mss_close = candle_close
                break

        if mss_idx is None:
            continue

        # 3) OB RETEST or MITIGATION (after MSS)
        ob_retest_idx = None
        ob_retest_time = None

        for r in range(mss_idx + 1, len(df)):

            candle_low = df.loc[r, "low"]
            candle_high = df.loc[r, "high"]
            candle_close = df.loc[r, "close"]

            if candle_close < ll_low:
                bull_mitigated.append({
                    "LL_time": ll_time,
                    "LL_idx": ll_idx,
                    "LL_low": ll_low,
                    "LL_high": ll_high,
                    "mitigation_time": df.loc[r, "time"],
                    "mitigation_idx": r,
                    "reason": "Close below LL before OB retest"
                })
                ob_retest_idx = None
                break

            if candle_low <= ll_high and candle_high >= ll_low:
                ob_retest_idx = r
                ob_retest_time = df.loc[r, "time"]
                break

        if ob_retest_idx is not None:
            bull_reversals.append({
                "LL_time": ll_time,
                "LL_idx": ll_idx,
                "LL_low": ll_low,
                "LL_high": ll_high,
                "IHH_time": ihh_time,
                "IHH_idx": ihh_idx,
                "IHH_price": ihh_price,
                "MSS_time": mss_time,
                "MSS_idx": mss_idx,
                "MSS_close": mss_close,
                "OB_retest_time": ob_retest_time,
                "OB_retest_idx": ob_retest_idx
            })

    return pd.DataFrame(bull_reversals), pd.DataFrame(bull_mitigated)