from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Optional, List

import pandas as pd


@dataclass
class BullishOBRecord:
    side: str
    source_label: str           # LL
    source_idx: int
    source_time: pd.Timestamp
    source_open: float
    source_high: float
    source_low: float
    source_close: float

    internal_idx: Optional[int]         # IHH candle idx
    internal_time: Optional[pd.Timestamp]
    internal_level: Optional[float]     # IHH high

    confirm_idx: Optional[int]
    confirm_time: Optional[pd.Timestamp]
    confirm_close: Optional[float]

    ob_low: float
    ob_high: float
    status: str                         # CONFIRMED / NO_IHH_FOUND / NO_CONFIRM_BREAK_ABOVE_IHH
    tf: str             # Multi-TimeFrame 15m/ 1hr/ 4hr / 1D 



# -----------------------------------------------------------------------------
# Pivot helpers
# -----------------------------------------------------------------------------

def _is_pivot_high(df: pd.DataFrame, idx: int, span: int = 1) -> bool:
    """
    Simple local pivot high.
    """
    if idx - span < 0 or idx + span >= len(df):
        return False

    this_high = float(df.loc[idx, "high"])
    left_max = float(df.loc[idx - span: idx - 1, "high"].max())
    right_max = float(df.loc[idx + 1: idx + span, "high"].max())

    return this_high >= left_max and this_high >= right_max


# -----------------------------------------------------------------------------
# Internal HH / IHH scan
# -----------------------------------------------------------------------------

def find_recent_ihh_after_ll(
    df: pd.DataFrame,
    ll_idx: int,
    max_scan_bars: int = 80,
    pivot_span: int = 1,
) -> Optional[int]:
    """
    Scan forward from LL+1 and return the first pivot high.
    This is the IHH candidate that price must later break above by close.
    """
    end_idx = min(len(df) - 2, ll_idx + max_scan_bars)

    for j in range(ll_idx + 1, end_idx + 1):
        if _is_pivot_high(df, j, span=pivot_span):
            return j

    return None


# -----------------------------------------------------------------------------
# Confirmation search
# -----------------------------------------------------------------------------

def find_bullish_confirm_after_ihh_break(
    df: pd.DataFrame,
    ll_idx: int,
    ihh_idx: int,
) -> Optional[int]:
    """
    First candle AFTER the IHH that body-closes above the IHH high.
    """
    ihh_high = float(df.loc[ihh_idx, "high"])

    for j in range(ihh_idx + 1, len(df)):
        if float(df.loc[j, "close"]) > ihh_high:
            return j

    return None


# -----------------------------------------------------------------------------
# Main builder
# -----------------------------------------------------------------------------

def build_bullish_obs(
    df: pd.DataFrame,
    sw_df: pd.DataFrame,
    tf_name: str = "15m",
    max_scan_bars: int = 80,
    pivot_span: int = 1,
    require_anchor_valid: bool = False,
) -> pd.DataFrame:
    """
    Build bullish OBs from EVERY LL candle in sw_df.

    Required df columns:
      time, open, high, low, close

    Required sw_df columns:
      idx, time, label

    Bullish confirmation rule:
    1. candle should be marked as LL
    2. scan forward LL+1 until recent IHH is found
    3. wait for first close above that IHH high
    4. then mark LL candle high/low as bullish OB
    """
    required_df_cols = {"time", "open", "high", "low", "close"}
    required_sw_cols = {"idx", "time", "label"}

    missing_df = required_df_cols - set(df.columns)
    missing_sw = required_sw_cols - set(sw_df.columns)

    if missing_df:
        raise ValueError(f"df missing required columns: {sorted(missing_df)}")
    if missing_sw:
        raise ValueError(f"sw_df missing required columns: {sorted(missing_sw)}")

    ll_mask = sw_df["label"] == "LL"

    if require_anchor_valid and "anchor_valid" in sw_df.columns:
        ll_mask &= sw_df["anchor_valid"] == True

    ll_rows = (
        sw_df[ll_mask]
        .sort_values("idx")
        .reset_index(drop=True)
    )

    rows: List[dict] = []

    for row in ll_rows.itertuples(index=False):
        ll_idx = int(row.idx)

        if ll_idx < 0 or ll_idx >= len(df) - 2:
            continue

        source_time = pd.Timestamp(df.loc[ll_idx, "time"])
        source_open = float(df.loc[ll_idx, "open"])
        source_high = float(df.loc[ll_idx, "high"])
        source_low = float(df.loc[ll_idx, "low"])
        source_close = float(df.loc[ll_idx, "close"])
        

        ihh_idx = find_recent_ihh_after_ll(
            df=df,
            ll_idx=ll_idx,
            max_scan_bars=max_scan_bars,
            pivot_span=pivot_span,
        )

        if ihh_idx is None:
            rec = BullishOBRecord(
                side="LONG",
                source_label="LL",
                source_idx=ll_idx,
                tf = tf_name,
                source_time=source_time,
                source_open=source_open,
                source_high=source_high,
                source_low=source_low,
                source_close=source_close,
                internal_idx=None,
                internal_time=None,
                internal_level=None,
                confirm_idx=None,
                confirm_time=None,
                confirm_close=None,
                ob_low=source_low,
                ob_high=source_high,
                status="NO_IHH_FOUND",
            )
            rows.append(asdict(rec))
            continue

        confirm_idx = find_bullish_confirm_after_ihh_break(
            df=df,
            ll_idx=ll_idx,
            ihh_idx=ihh_idx,
        )

        if confirm_idx is None:
            rec = BullishOBRecord(
                side="LONG",
                source_label="LL",
                source_idx=ll_idx,
                tf = tf_name,
                source_time=source_time,
                source_open=source_open,
                source_high=source_high,
                source_low=source_low,
                source_close=source_close,
                internal_idx=ihh_idx,
                internal_time=pd.Timestamp(df.loc[ihh_idx, "time"]),
                internal_level=float(df.loc[ihh_idx, "high"]),
                confirm_idx=None,
                confirm_time=None,
                confirm_close=None,
                ob_low=source_low,
                ob_high=source_high,
                status="NO_CONFIRM_BREAK_ABOVE_IHH",
            )
            rows.append(asdict(rec))
            continue

        rec = BullishOBRecord(
            side="LONG",
            source_label="LL",
            source_idx=ll_idx,
            tf = tf_name,
            source_time=source_time,
            source_open=source_open,
            source_high=source_high,
            source_low=source_low,
            source_close=source_close,
            internal_idx=ihh_idx,
            internal_time=pd.Timestamp(df.loc[ihh_idx, "time"]),
            internal_level=float(df.loc[ihh_idx, "high"]),
            confirm_idx=confirm_idx,
            confirm_time=pd.Timestamp(df.loc[confirm_idx, "time"]),
            confirm_close=float(df.loc[confirm_idx, "close"]),
            ob_low=source_low,
            ob_high=source_high,
            status="CONFIRMED",
        )
        rows.append(asdict(rec))

    out = pd.DataFrame(rows)

    if out.empty:
        return pd.DataFrame(
            columns=[
                "side",
                "source_label",
                "source_idx",
                "tf",
                "source_time",
                "source_open",
                "source_high",
                "source_low",
                "source_close",
                "internal_idx",
                "internal_time",
                "internal_level",
                "confirm_idx",
                "confirm_time",
                "confirm_close",
                "ob_low",
                "ob_high",
                "status",
            ]
        )

    return out.sort_values(["source_idx"]).reset_index(drop=True)


def get_confirmed_bullish_obs(
    df: pd.DataFrame,
    sw_df: pd.DataFrame,
    max_scan_bars: int = 80,
    pivot_span: int = 1,
    require_anchor_valid: bool = False,
) -> pd.DataFrame:
    """
    Convenience wrapper: returns only confirmed bullish OBs.
    """
    obs = build_bullish_obs(
        df=df,
        sw_df=sw_df,
        max_scan_bars=max_scan_bars,
        pivot_span=pivot_span,
        require_anchor_valid=require_anchor_valid,
    )
    return obs[obs["status"] == "CONFIRMED"].reset_index(drop=True)


# -----------------------------------------------------------------------------
# Backward compatibility wrapper
# -----------------------------------------------------------------------------

def detect_bullish_ob(df: pd.DataFrame, sw_df: pd.DataFrame):
    """
    Backward-compatible wrapper so old call sites do not break immediately.

    Returns:
      bull_reversals, bull_mitigated

    In the rebuilt architecture:
    - bull_reversals = confirmed bullish OBs
    - bull_mitigated = non-confirmed / diagnostic rows
    """
    obs = build_bullish_obs(df=df, sw_df=sw_df)

    bull_reversals = obs[obs["status"] == "CONFIRMED"].reset_index(drop=True)
    bull_mitigated = obs[obs["status"] != "CONFIRMED"].reset_index(drop=True)

    return bull_reversals, bull_mitigated