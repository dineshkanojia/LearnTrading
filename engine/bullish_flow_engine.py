# engine/bullish_flow_engine.py

from __future__ import annotations

import numpy as np
import pandas as pd


def _find_next_hh_after(df_swings: pd.DataFrame, start_idx: int) -> pd.Series | None:
    """Return the first HH swing row after a given candle index."""
    hh_rows = df_swings[
        (df_swings["type"] == "high")
        & (df_swings["label"] == "HH")
        & (df_swings["idx"] > start_idx)
    ]

    if hh_rows.empty:
        return None

    return hh_rows.iloc[0]


def _bearish_ob_retests_between(
    bear_rev_df: pd.DataFrame,
    retest_start_idx: int,
    retest_end_idx: int | None,
    min_hh_idx: int | None = None,
) -> pd.DataFrame:
    """
    Return confirmed bearish OB retests in a retest window, optionally requiring
    that the bearish OB itself was formed after a minimum HH index.
    """
    if bear_rev_df.empty:
        return bear_rev_df.copy()

    valid = bear_rev_df.dropna(subset=["OB_retest_idx", "HH_idx"]).copy()

    mask = valid["OB_retest_idx"] > retest_start_idx

    if retest_end_idx is not None:
        mask &= valid["OB_retest_idx"] <= retest_end_idx

    if min_hh_idx is not None:
        mask &= valid["HH_idx"] > min_hh_idx

    return valid[mask].sort_values("OB_retest_idx")


def _get_overlapping_bullish_group(
    bull_rev_df: pd.DataFrame,
    current_row,
) -> pd.DataFrame:
    """
    For a given bullish OB, find later bullish OBs that were confirmed
    after this one and before this OB's retest (if any).
    """
    ll_time = current_row.LL_time
    ob_retest_time = getattr(current_row, "OB_retest_time", pd.NaT)

    if pd.isna(ob_retest_time):
        group = bull_rev_df[bull_rev_df["LL_time"] > ll_time]
    else:
        group = bull_rev_df[
            (bull_rev_df["LL_time"] > ll_time)
            & (bull_rev_df["LL_time"] < ob_retest_time)
        ]

    return group.sort_values("LL_time")


def _find_structural_break_exit(
    df: pd.DataFrame,
    entry_idx: int,
    ll_low: float,
) -> tuple[int | None, pd.Timestamp | None, float | None, str]:
    """
    Structural invalidation for a long:
    first candle close below LL_low after entry.
    """
    for candle_idx in range(entry_idx + 1, len(df)):
        if df.loc[candle_idx, "close"] < ll_low:
            return (
                candle_idx,
                df.loc[candle_idx, "time"],
                df.loc[candle_idx, "close"],
                "structural_break_LL",
            )

    return None, None, None, "no_structural_break_found"


def _make_trade_record(
    *,
    scenario: str,
    primary_ob_row,
    entry_idx: int,
    entry_time,
    entry_price: float,
    exit_idx: int | None,
    exit_time,
    exit_price: float | None,
    exit_reason: str | None,
    hh_anchor_time,
    hh_anchor_idx: int | None,
) -> dict:
    """Build one bullish flow trade record."""
    return {
        "scenario": scenario,
        "direction": "long",
        "LL_time": primary_ob_row.LL_time,
        "LL_idx": primary_ob_row.LL_idx,
        "LL_low": primary_ob_row.LL_low,
        "LL_high": primary_ob_row.LL_high,
        "entry_time": entry_time,
        "entry_idx": entry_idx,
        "entry_price": entry_price,
        "exit_time": exit_time,
        "exit_idx": exit_idx,
        "exit_price": exit_price,
        "exit_reason": exit_reason,
        "HH_anchor_time": hh_anchor_time,
        "HH_anchor_idx": hh_anchor_idx,
    }


def _resolve_primary_bullish_ob(
    bull_rev_df: pd.DataFrame,
    row,
    row_ob_id: int,
    used_as_primary: set[int],
) -> tuple[object, int, str]:
    """
    Resolve which bullish OB should be treated as primary for execution
    when later bullish OBs appear before the current OB retest.

    Behavior mirrors bearish_flow_engine structure.
    """
    overlapping = _get_overlapping_bullish_group(bull_rev_df, row)

    scenario = "Scenario_1_or_2"
    primary_ob_row = row
    primary_ob_id = row_ob_id

    if overlapping.empty:
        return primary_ob_row, primary_ob_id, scenario

    earliest_retest_idx = int(row.OB_retest_idx)
    earliest_retest_owner = "OB1"

    for ob2 in overlapping.itertuples():
        if pd.isna(ob2.OB_retest_idx):
            continue

        ob2_retest_idx = int(ob2.OB_retest_idx)

        if ob2_retest_idx < earliest_retest_idx:
            earliest_retest_idx = ob2_retest_idx
            earliest_retest_owner = "OB2"
            primary_ob_row = ob2
            primary_ob_id = ob2.Index

    same_level = np.isclose(primary_ob_row.LL_high, row.LL_high) and np.isclose(
        primary_ob_row.LL_low,
        row.LL_low,
    )

    if earliest_retest_owner == "OB2":
        scenario = "Scenario_3_OB2_preferred"
        used_as_primary.add(primary_ob_id)
    else:
        if same_level:
            scenario = "Scenario_4_OB1_preferred_same_level"
            used_as_primary.add(primary_ob_id)
        else:
            scenario = "Scenario_3_OB1_kept_primary"

    return primary_ob_row, primary_ob_id, scenario


def _resolve_exit_after_first_hh(
    df: pd.DataFrame,
    sw_df: pd.DataFrame,
    bear_rev_df: pd.DataFrame,
    primary_ob_row,
    entry_idx: int,
    hh_anchor_idx: int,
) -> tuple[
    int | None,
    pd.Timestamp | None,
    float | None,
    str | None,
    str,
]:
    """
    Resolve exit once first HH after entry exists.

    Important:
    We trust bear_rev_df as the source of truth for confirmed bearish OBs.

    Returns:
        exit_idx,
        exit_time,
        exit_price,
        exit_reason,
        scenario_used
    """
    exit_idx = None
    exit_time = None
    exit_price = None
    exit_reason = None
    scenario_used = "Scenario_1_or_2"

    # Scenario 1:
    # Exit on first CONFIRMED bearish OB retest after first HH anchor,
    # but only if that bearish OB was formed after long entry.
    bear_retests_after_hh = _bearish_ob_retests_between(
        bear_rev_df,
        retest_start_idx=hh_anchor_idx,
        retest_end_idx=None,
        min_hh_idx=entry_idx,
    )

    if not bear_retests_after_hh.empty:
        first_bear = bear_retests_after_hh.iloc[0]
        exit_idx = int(first_bear.OB_retest_idx)
        exit_time = first_bear.OB_retest_time
        exit_price = df.loc[exit_idx, "close"]
        exit_reason = "bearish_OB_retest_after_HH"
        scenario_used = "Scenario_1_normal_flow"
        return exit_idx, exit_time, exit_price, exit_reason, scenario_used

    # Scenario 2: continuation flow
    next_hh = _find_next_hh_after(sw_df, hh_anchor_idx)

    if next_hh is not None:
        bear_between = _bearish_ob_retests_between(
            bear_rev_df,
            retest_start_idx=hh_anchor_idx,
            retest_end_idx=int(next_hh.idx),
            min_hh_idx=entry_idx,
        )

        if not bear_between.empty:
            first_bear = bear_between.iloc[0]
            exit_idx = int(first_bear.OB_retest_idx)
            exit_time = first_bear.OB_retest_time
            exit_price = df.loc[exit_idx, "close"]
            exit_reason = "bearish_OB_retest_between_HH_chain"
            scenario_used = "Scenario_2_continuation_exit_on_bearish"
            return exit_idx, exit_time, exit_price, exit_reason, scenario_used

        scenario_used = "Scenario_2_continuation_no_bearish_retest"
        return exit_idx, exit_time, exit_price, exit_reason, scenario_used

    scenario_used = "Scenario_2_continuation_no_next_HH"
    return exit_idx, exit_time, exit_price, exit_reason, scenario_used


def generate_bullish_flow_trades(
    df: pd.DataFrame,
    sw_df: pd.DataFrame,
    bull_rev_df: pd.DataFrame,
    bear_rev_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Implements bullish OB flow with:
    - Scenario 1: normal flow (exit on first confirmed bearish OB retest after HH)
    - Scenario 2: continuation flow
    - Scenario 3/4: overlapping bullish OBs with priority rules

    Bullish entries come from confirmed bullish OB retests.
    Bearish exits come only from confirmed bearish OB retests.
    """
    trades: list[dict] = []

    bull_rev_df = bull_rev_df.sort_values("LL_time").reset_index(drop=True)
    used_as_primary: set[int] = set()

    for row in bull_rev_df.itertuples():
        ob_id = row.Index

        if ob_id in used_as_primary:
            continue

        if pd.isna(row.OB_retest_idx):
            continue

        primary_ob_row, primary_ob_id, overlap_scenario = _resolve_primary_bullish_ob(
            bull_rev_df=bull_rev_df,
            row=row,
            row_ob_id=ob_id,
            used_as_primary=used_as_primary,
        )

        if pd.isna(primary_ob_row.OB_retest_idx):
            continue

        entry_idx = int(primary_ob_row.OB_retest_idx)
        entry_time = primary_ob_row.OB_retest_time
        entry_price = df.loc[entry_idx, "close"]

        first_hh = _find_next_hh_after(sw_df, entry_idx)

        if first_hh is None:
            exit_idx, exit_time, exit_price, exit_reason = _find_structural_break_exit(
                df=df,
                entry_idx=entry_idx,
                ll_low=primary_ob_row.LL_low,
            )

            scenario_used = overlap_scenario

            trades.append(
                _make_trade_record(
                    scenario=scenario_used,
                    primary_ob_row=primary_ob_row,
                    entry_idx=entry_idx,
                    entry_time=entry_time,
                    entry_price=entry_price,
                    exit_idx=exit_idx,
                    exit_time=exit_time,
                    exit_price=exit_price,
                    exit_reason=(
                        "no_HH_structural_only"
                        if exit_reason == "no_structural_break_found"
                        else exit_reason
                    ),
                    hh_anchor_time=None,
                    hh_anchor_idx=None,
                )
            )
            continue

        hh_anchor_idx = int(first_hh.idx)
        hh_anchor_time = first_hh.time

        (
            exit_idx,
            exit_time,
            exit_price,
            exit_reason,
            scenario_used,
        ) = _resolve_exit_after_first_hh(
            df=df,
            sw_df=sw_df,
            bear_rev_df=bear_rev_df,
            primary_ob_row=primary_ob_row,
            entry_idx=entry_idx,
            hh_anchor_idx=hh_anchor_idx,
        )

        if exit_idx is None:
            structural_exit_idx, structural_exit_time, structural_exit_price, structural_exit_reason = (
                _find_structural_break_exit(
                    df=df,
                    entry_idx=entry_idx,
                    ll_low=primary_ob_row.LL_low,
                )
            )

            if structural_exit_idx is not None:
                exit_idx = structural_exit_idx
                exit_time = structural_exit_time
                exit_price = structural_exit_price
                exit_reason = structural_exit_reason

        trades.append(
            _make_trade_record(
                scenario=scenario_used,
                primary_ob_row=primary_ob_row,
                entry_idx=entry_idx,
                entry_time=entry_time,
                entry_price=entry_price,
                exit_idx=exit_idx,
                exit_time=exit_time,
                exit_price=exit_price,
                exit_reason=exit_reason,
                hh_anchor_time=hh_anchor_time,
                hh_anchor_idx=hh_anchor_idx,
            )
        )

    print("BULL FLOW TRADES:", len(trades))
    return pd.DataFrame(trades)