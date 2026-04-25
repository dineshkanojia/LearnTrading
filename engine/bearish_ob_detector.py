import pandas as pd
import numpy as np


def detect_bearish_ob(df: pd.DataFrame, sw_df: pd.DataFrame, min_leg_pct=1.2, min_mss_pct=0.6):
    """
    Improved bearish OB detection for BTC 15m (ICT/SMC style)

    Params:
        min_leg_pct:     minimum % move from previous swing low → HH (filter weak HH)
        min_mss_pct:     minimum % drop from HH high → MSS close (filter weak breaks)
    """
    bear_ob_list = []
    mitigated_list = []

    # Only consider confirmed HH swings
    hh_swings = sw_df[
        (sw_df["type"] == "high") &
        (sw_df["label"] == "HH") &
        (sw_df["leg_size_pct"].notna()) &
        (sw_df["leg_size_pct"] >= min_leg_pct / 100)
    ].sort_values("idx")

    for hh in hh_swings.itertuples():

        hh_idx   = int(hh.idx)
        hh_time  = hh.time
        hh_high  = df.loc[hh_idx, "high"]
        hh_low   = df.loc[hh_idx, "low"]
        hh_close = df.loc[hh_idx, "close"]

        # ────────────────────────────────────────────────
        # 1. Find IHL — last close < HH low BEFORE HH formed
        #    → allow temporary closes above HH high (wicks)
        # ────────────────────────────────────────────────
        ihl_idx = None
        ihl_low = None
        ihl_time = None

        for j in range(hh_idx - 1, -1, -1):
            if df.loc[j, "close"] < hh_low:
                ihl_idx = j
                ihl_low = df.loc[j, "low"]
                ihl_time = df.loc[j, "time"]
                break

        if ihl_idx is None:
            continue

        # ────────────────────────────────────────────────
        # 2. MSS-Down — first decisive break below IHL low
        # ────────────────────────────────────────────────
        mss_idx = None
        mss_time = None
        mss_close = None

        for k in range(ihl_idx + 1, len(df)):
            c = df.iloc[k]

            # Early mitigation: strong reclaim of HH high
            if c["close"] > hh_high * 1.002:  # small buffer for noise
                mitigated_list.append({
                    "type": "early_mitigation",
                    "HH_time": hh_time,
                    "HH_idx": hh_idx,
                    "mit_time": c["time"],
                    "mit_idx": k,
                    "reason": "close above HH high before MSS"
                })
                break

            if c["close"] < ihl_low:
                drop_pct = (hh_high - c["close"]) / hh_high
                if drop_pct >= min_mss_pct / 100:
                    mss_idx = k
                    mss_time = c["time"]
                    mss_close = c["close"]
                break

        if mss_idx is None:
            continue

        # ────────────────────────────────────────────────
        # 3. OB Retest — first meaningful touch of HH range
        #    (low enters HH candle, not just tiny wick)
        # ────────────────────────────────────────────────
        ob_idx = None
        ob_time = None
        ob_o = ob_h = ob_l = ob_c = None

        for r in range(mss_idx + 1, len(df)):
            c = df.iloc[r]

            # Mitigation check after MSS
            if c["close"] > hh_high * 1.002:
                mitigated_list.append({
                    "type": "post_mss_mitigation",
                    "HH_time": hh_time,
                    "HH_idx": hh_idx,
                    "mit_time": c["time"],
                    "mit_idx": r,
                    "reason": "close above HH after MSS"
                })
                break

            # Retest condition — low must penetrate meaningfully
            if (c["low"] <= hh_high) and (c["high"] >= hh_low * 0.998):
                penetration = (hh_high - c["low"]) / (hh_high - hh_low)
                if penetration >= 0.25:  # at least 25% of HH candle range touched
                    ob_idx = r
                    ob_time = c["time"]
                    ob_o, ob_h, ob_l, ob_c = c["open"], c["high"], c["low"], c["close"]
                    break

        if ob_idx is not None:
            bear_ob_list.append({
                "HH_time": hh_time,
                "HH_idx": hh_idx,
                "HH_low": hh_low,
                "HH_high": hh_high,
                "IHL_time": ihl_time,
                "IHL_idx": ihl_idx,
                "IHL_low": ihl_low,
                "MSS_time": mss_time,
                "MSS_idx": mss_idx,
                "MSS_close": mss_close,
                "OB_retest_time": ob_time,
                "OB_retest_idx": ob_idx,
                "OB_open": ob_o,
                "OB_high": ob_h,
                "OB_low": ob_l,
                "OB_close": ob_c,
                "leg_pct": hh.leg_size_pct * 100 if hasattr(hh, 'leg_size_pct') else None,
            })

    bear_obs = pd.DataFrame(bear_ob_list)
    mitigated = pd.DataFrame(mitigated_list)

    if not bear_obs.empty:
        bear_obs = bear_obs.sort_values("HH_time").reset_index(drop=True)

    return bear_obs, mitigated