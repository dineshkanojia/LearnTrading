# engine/swings.py

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import pandas as pd


SwingType = Literal["high", "low"]


@dataclass(slots=True)
class SwingPoint:
    idx: int
    time: pd.Timestamp
    price: float
    swing_type: SwingType
    label: str
    open: float
    high: float
    low: float
    close: float
    pivot_high: bool
    pivot_low: bool


def _require_columns(df: pd.DataFrame, required: list[str]) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"build_structure_swings: missing required columns: {missing}")


def _safe_idx(df: pd.DataFrame, pos: int) -> int:
    return int(df["idx"].iloc[pos]) if "idx" in df.columns else int(pos)


def _is_pivot_high(df: pd.DataFrame, i: int, left: int, right: int) -> bool:
    h = float(df["high"].iloc[i])
    return all(h > float(df["high"].iloc[i - j]) for j in range(1, left + 1)) and all(
        h > float(df["high"].iloc[i + j]) for j in range(1, right + 1)
    )


def _is_pivot_low(df: pd.DataFrame, i: int, left: int, right: int) -> bool:
    l = float(df["low"].iloc[i])
    return all(l < float(df["low"].iloc[i - j]) for j in range(1, left + 1)) and all(
        l < float(df["low"].iloc[i + j]) for j in range(1, right + 1)
    )


def _same_side_distance_ok(
    price: float,
    last_price: float | None,
    min_swing_pct: float,
) -> tuple[bool, float | None]:
    if last_price is None or last_price == 0:
        return True, None
    move_pct = abs(price - last_price) / abs(last_price)
    return move_pct >= min_swing_pct, move_pct


def _make_row(
    df: pd.DataFrame,
    pos: int,
    swing_type: SwingType,
    label: str,
    prev_same_side_idx: int | None,
    prev_same_side_price: float | None,
    same_side_distance_pct: float | None,
    same_side_spacing_bars: int | None,
    tf_name: str,
) -> dict:
    return {
        "time": pd.Timestamp(df["time"].iloc[pos]),
        "tf": tf_name,
        "idx": _safe_idx(df, pos),
        "type": swing_type,
        "price": float(df["high"].iloc[pos] if swing_type == "high" else df["low"].iloc[pos]),
        "label": label,
        "open": float(df["open"].iloc[pos]),
        "high": float(df["high"].iloc[pos]),
        "low": float(df["low"].iloc[pos]),
        "close": float(df["close"].iloc[pos]),
        "pivot_high": swing_type == "high",
        "pivot_low": swing_type == "low",
        "source": "swing_detector",
        "confirmed": True,
        "confirmation_idx": _safe_idx(df, pos),
        "confirmation_time": pd.Timestamp(df["time"].iloc[pos]),
        "confirmation_level": float(df["high"].iloc[pos] if swing_type == "high" else df["low"].iloc[pos]),
        "confirmation_move_pct": None,
        "bars_to_confirmation": 0,
        "prev_same_side_idx": prev_same_side_idx,
        "prev_same_side_price": prev_same_side_price,
        "same_side_distance_pct": same_side_distance_pct,
        "same_side_spacing_bars": same_side_spacing_bars,
        "replacement_count_before_confirmation": 0,
        "opposite_candidate_idx": None,
        "opposite_candidate_time": None,
        "opposite_candidate_spacing_bars": None,
        "opposite_candidate_spacing_ok": None,
        "min_confirmation_bars_rule": None,
        "anchor_valid": True,
    }


def _prev_label_idx_series(sw_df: pd.DataFrame, target_label: str) -> pd.Series:
    """
    For each row, return the idx value of the most recent previous row
    where label == target_label.
    """
    prev_idx = []
    last_seen_idx = None

    for row in sw_df.itertuples(index=False):
        prev_idx.append(last_seen_idx)
        if row.label == target_label:
            last_seen_idx = int(row.idx)

    return pd.Series(prev_idx, index=sw_df.index, dtype="float64")


def _build_leg_context_columns(sw_df: pd.DataFrame) -> pd.DataFrame:
    """
    Add previous HH/LL references plus:
    - prev_opposite_price
    - prev_same_price
    - leg_size_pct
    - retracement_pct
    """
    out = sw_df.copy()
    idx_to_price = out.set_index("idx")["price"].to_dict()

    out["prev_hh_idx"] = _prev_label_idx_series(out, "HH")
    out["prev_ll_idx"] = _prev_label_idx_series(out, "LL")

    out["prev_hh_price"] = out["prev_hh_idx"].map(idx_to_price)
    out["prev_ll_price"] = out["prev_ll_idx"].map(idx_to_price)

    out["prev_opposite_price"] = out.apply(
        lambda r: r["prev_ll_price"] if r["type"] == "high" else r["prev_hh_price"],
        axis=1,
    )

    out["prev_same_price"] = out.apply(
        lambda r: r["prev_hh_price"] if r["type"] == "high" else r["prev_ll_price"],
        axis=1,
    )

    def leg_size_pct(row: pd.Series) -> float | None:
        prev_opp = row["prev_opposite_price"]
        price = row["price"]
        if pd.isna(prev_opp) or prev_opp == 0:
            return None
        return abs(price - prev_opp) / abs(prev_opp)

    out["leg_size_pct"] = out.apply(leg_size_pct, axis=1)

    def retracement_pct(row: pd.Series) -> float | None:
        label = row["label"]
        if label not in ("HL", "LH"):
            return None

        prev_opp = row["prev_opposite_price"]
        price = row["price"]
        if pd.isna(prev_opp):
            return None

        if label == "HL":
            impulse_start_price = row["prev_hh_price"]
            impulse_end_price = prev_opp
            if pd.isna(impulse_start_price):
                return None
            denom = impulse_start_price - impulse_end_price
            if denom == 0:
                return None
            return (impulse_start_price - price) / denom

        impulse_start_price = row["prev_ll_price"]
        impulse_end_price = prev_opp
        if pd.isna(impulse_start_price):
            return None
        denom = impulse_start_price - impulse_end_price
        if denom == 0:
            return None
        return (price - impulse_end_price) / denom

    out["retracement_pct"] = out.apply(retracement_pct, axis=1)

    return out


def build_structure_swings(
    df: pd.DataFrame,
    tf_name: str = "15m",
    left_strength: int = 8,
    right_strength: int = 8,
    min_bars_between_same_side: int = 8,
    min_swing_pct: float = 0.003,
) -> pd.DataFrame:
    """
    Raw visually aligned swing detector.

    Notes
    -----
    - compares highs to last high, lows to last low
    - keeps same-side minimum bar spacing
    - keeps same-side minimum distance %
    - skips ambiguous dual-pivot candles
    - outputs columns aligned with existing OB model
    """
    _require_columns(df, ["time", "open", "high", "low", "close"])

    swings: list[dict] = []

    last_high_pos: int | None = None
    last_low_pos: int | None = None
    last_high_price: float | None = None
    last_low_price: float | None = None

    for i in range(left_strength, len(df) - right_strength):
        is_high = _is_pivot_high(df, i, left_strength, right_strength)
        is_low = _is_pivot_low(df, i, left_strength, right_strength)

        if is_high and is_low:
            continue

        if is_high:
            price = float(df["high"].iloc[i])

            spacing_ok = last_high_pos is None or (i - last_high_pos) >= min_bars_between_same_side
            distance_ok, distance_pct = _same_side_distance_ok(price, last_high_price, min_swing_pct)

            if spacing_ok and distance_ok:
                label = "HH" if last_high_price is None or price > last_high_price else "LH"
                prev_idx = _safe_idx(df, last_high_pos) if last_high_pos is not None else None
                prev_price = last_high_price
                spacing_bars = (i - last_high_pos) if last_high_pos is not None else None

                swings.append(
                    _make_row(
                        df=df,
                        tf_name= tf_name,
                        pos=i,
                        swing_type="high",
                        label=label,
                        prev_same_side_idx=prev_idx,
                        prev_same_side_price=prev_price,
                        same_side_distance_pct=distance_pct,
                        same_side_spacing_bars=spacing_bars,
                    )
                )
                last_high_pos = i
                last_high_price = price

        if is_low:
            price = float(df["low"].iloc[i])

            spacing_ok = last_low_pos is None or (i - last_low_pos) >= min_bars_between_same_side
            distance_ok, distance_pct = _same_side_distance_ok(price, last_low_price, min_swing_pct)

            if spacing_ok and distance_ok:
                label = "LL" if last_low_price is None or price < last_low_price else "HL"
                prev_idx = _safe_idx(df, last_low_pos) if last_low_pos is not None else None
                prev_price = last_low_price
                spacing_bars = (i - last_low_pos) if last_low_pos is not None else None

                swings.append(
                    _make_row(
                        df=df,
                        tf_name= tf_name,
                        pos=i,
                        swing_type="low",
                        label=label,
                        prev_same_side_idx=prev_idx,
                        prev_same_side_price=prev_price,
                        same_side_distance_pct=distance_pct,
                        same_side_spacing_bars=spacing_bars,
                    )
                )
                last_low_pos = i
                last_low_price = price

    sw_df = pd.DataFrame(swings)

    if sw_df.empty:
        return pd.DataFrame(
            columns=[
                "time",
                "tf",
                "idx",
                "type",
                "price",
                "label",
                "open",
                "high",
                "low",
                "close",
                "pivot_high",
                "pivot_low",
                "source",
                "confirmed",
                "confirmation_idx",
                "confirmation_time",
                "confirmation_level",
                "confirmation_move_pct",
                "bars_to_confirmation",
                "prev_same_side_idx",
                "prev_same_side_price",
                "same_side_distance_pct",
                "same_side_spacing_bars",
                "replacement_count_before_confirmation",
                "opposite_candidate_idx",
                "opposite_candidate_time",
                "opposite_candidate_spacing_bars",
                "opposite_candidate_spacing_ok",
                "min_confirmation_bars_rule",
                "anchor_valid",
                "prev_hh_idx",
                "prev_ll_idx",
                "prev_hh_price",
                "prev_ll_price",
                "prev_opposite_price",
                "prev_same_price",
                "leg_size_pct",
                "retracement_pct",
            ]
        )

    sw_df = sw_df.sort_values("idx").reset_index(drop=True)
    sw_df = _build_leg_context_columns(sw_df)
    return sw_df


def apply_anchor_quality_filter(sw_df: pd.DataFrame, df: pd.DataFrame) -> pd.DataFrame:
    """
    Compatibility layer for existing code paths.

    For now, preserve all detected swings as valid anchors.
    """
    out = sw_df.copy()
    if "anchor_valid" not in out.columns:
        out["anchor_valid"] = True
    else:
        out["anchor_valid"] = out["anchor_valid"].fillna(True)
    return out