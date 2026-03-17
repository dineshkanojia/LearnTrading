# engine/swings.py

import pandas as pd


def _make_swing_row(
    df: pd.DataFrame,
    idx: int,
    swing_type: str,
    price: float,
    label: str | None,
) -> tuple:
    return (
        df.loc[idx, "time"],        # time
        swing_type,                 # type
        price,                      # price
        label,                      # label
        idx,                        # idx
        df.loc[idx, "open"],        # open
        df.loc[idx, "high"],        # high
        df.loc[idx, "low"],         # low
        df.loc[idx, "close"],       # close
        df.loc[idx, "pivot_high"],  # pivot_high
        df.loc[idx, "pivot_low"],   # pivot_low
    )


def build_structure_swings(df: pd.DataFrame) -> pd.DataFrame:
    swings = []
    last_high_price = None
    last_low_price = None

    for i in range(len(df)):

        if df.loc[i, "pivot_high"]:
            price = df.loc[i, "high"]
            label = None

            if last_high_price is not None:
                label = "HH" if price > last_high_price else "LH"

            swings.append(
                _make_swing_row(
                    df=df,
                    idx=i,
                    swing_type="high",
                    price=price,
                    label=label,
                )
            )

            last_high_price = price

        if df.loc[i, "pivot_low"]:
            price = df.loc[i, "low"]
            label = None

            if last_low_price is not None:
                label = "HL" if price > last_low_price else "LL"

            swings.append(
                _make_swing_row(
                    df=df,
                    idx=i,
                    swing_type="low",
                    price=price,
                    label=label,
                )
            )

            last_low_price = price

    sw_df = pd.DataFrame(
        swings,
        columns=[
            "time",
            "type",
            "price",
            "label",
            "idx",
            "open",
            "high",
            "low",
            "close",
            "pivot_high",
            "pivot_low",
        ],
    )

    return sw_df


def apply_anchor_quality_filter(
    sw_df: pd.DataFrame,
    df: pd.DataFrame,
    lookback_bars: int = 24,
    min_break_pct: float = 0.0015,
    min_range_pct: float = 0.012,
) -> pd.DataFrame:
    """
    Flag likely false HH / LL anchors without changing the original swing labels.

    Logic:
    - HH/LL must make a meaningful break beyond the previous same-type swing.
    - If the break is tiny and recent market range is compressed, mark as false.
    - HL/LH are left as valid by default.

    Parameters:
    - lookback_bars: recent candle window to judge sideways compression
    - min_break_pct: minimum break beyond previous same-type swing (e.g. 0.0015 = 0.15%)
    - min_range_pct: minimum recent range % to avoid sideways classification
    """
    out = sw_df.copy()

    out["prev_same_type_price"] = pd.NA
    out["break_distance"] = pd.NA
    out["break_pct"] = pd.NA
    out["recent_range"] = pd.NA
    out["recent_range_pct"] = pd.NA
    out["is_sideways_context"] = False
    out["is_false_anchor"] = False
    out["anchor_valid"] = True

    last_high_price = None
    last_low_price = None

    for row_idx in out.index:
        swing_type = out.at[row_idx, "type"]
        label = out.at[row_idx, "label"]
        price = float(out.at[row_idx, "price"])
        candle_idx = int(out.at[row_idx, "idx"])

        # recent range context from raw candles
        start_idx = max(0, candle_idx - lookback_bars)
        recent_slice = df.loc[start_idx:candle_idx]

        recent_high = float(recent_slice["high"].max())
        recent_low = float(recent_slice["low"].min())
        recent_range = recent_high - recent_low
        recent_range_pct = (recent_range / price) if price != 0 else 0.0

        out.at[row_idx, "recent_range"] = recent_range
        out.at[row_idx, "recent_range_pct"] = recent_range_pct

        is_sideways = recent_range_pct < min_range_pct
        out.at[row_idx, "is_sideways_context"] = is_sideways

        if swing_type == "high":
            prev_price = last_high_price
            if prev_price is not None:
                break_distance = price - prev_price
                break_pct = (break_distance / prev_price) if prev_price != 0 else 0.0

                out.at[row_idx, "prev_same_type_price"] = prev_price
                out.at[row_idx, "break_distance"] = break_distance
                out.at[row_idx, "break_pct"] = break_pct

                if label == "HH":
                    false_anchor = (break_pct < min_break_pct) and is_sideways
                    out.at[row_idx, "is_false_anchor"] = false_anchor
                    out.at[row_idx, "anchor_valid"] = not false_anchor

            last_high_price = price

        elif swing_type == "low":
            prev_price = last_low_price
            if prev_price is not None:
                break_distance = prev_price - price
                break_pct = (break_distance / prev_price) if prev_price != 0 else 0.0

                out.at[row_idx, "prev_same_type_price"] = prev_price
                out.at[row_idx, "break_distance"] = break_distance
                out.at[row_idx, "break_pct"] = break_pct

                if label == "LL":
                    false_anchor = (break_pct < min_break_pct) and is_sideways
                    out.at[row_idx, "is_false_anchor"] = false_anchor
                    out.at[row_idx, "anchor_valid"] = not false_anchor

            last_low_price = price

    return out