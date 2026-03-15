# engine/bearish_flow_engine.py

import pandas as pd
import numpy as np


def _find_next_ll_after(df_swings: pd.DataFrame, start_idx: int) -> pd.Series | None:
    """Return the first LL swing row after a given pivot index."""
    ll_rows = df_swings[(df_swings["type"] == "low") & (df_swings["label"] == "LL") & (df_swings["idx"] > start_idx)]
    if ll_rows.empty:
        return None
    return ll_rows.iloc[0]


def _bullish_ob_retests_between(bull_rev_df: pd.DataFrame, start_idx: int, end_idx: int | None) -> pd.DataFrame:
    """Return bullish OB retests between two candle indices."""
    if end_idx is None:
        mask = bull_rev_df["OB_retest_idx"] > start_idx
    else:
        mask = (bull_rev_df["OB_retest_idx"] > start_idx) & (bull_rev_df["OB_retest_idx"] <= end_idx)
    return bull_rev_df[mask].sort_values("OB_retest_idx")


def _get_overlapping_bearish_group(bear_rev_df: pd.DataFrame, current_row) -> pd.DataFrame:
    """
    For a given bearish OB (current_row), find other bearish OBs that are:
    - confirmed after this one
    - before its retest (if any)
    """
    hh_time = current_row.HH_time
    ob_retest_time = getattr(current_row, "OB_retest_time", pd.NaT)

    if pd.isna(ob_retest_time):
        group = bear_rev_df[bear_rev_df["HH_time"] > hh_time]
    else:
        group = bear_rev_df[(bear_rev_df["HH_time"] > hh_time) & (bear_rev_df["HH_time"] < ob_retest_time)]

    return group.sort_values("HH_time")


def generate_bearish_flow_trades(
    df: pd.DataFrame,
    sw_df: pd.DataFrame,
    bear_rev_df: pd.DataFrame,
    bull_rev_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Implements bearish OB flow with:
    - Scenario 1: normal flow (exit on first bullish OB retest after LL)
    - Scenario 2: continuation flow (no bullish retest → ride to next LL, exit on later bullish OB retest)
    - Scenario 3/4: overlapping bearish OBs with priority rules
    """

    trades = []
    bear_rev_df = bear_rev_df.sort_values("HH_time").reset_index(drop=True)

    # Track which bearish OB has been used as primary in overlapping groups
    used_as_primary = set()

    for row in bear_rev_df.itertuples():
        ob_id = row.Index

        # Skip if this OB has already been superseded as secondary in an overlapping scenario
        if ob_id in used_as_primary:
            continue

        hh_idx = row.HH_idx
        hh_high = row.HH_high
        hh_low = row.HH_low
        hh_time = row.HH_time

        ob_retest_idx = row.OB_retest_idx
        ob_retest_time = row.OB_retest_time

        # If no retest → no trade
        if pd.isna(ob_retest_idx):
            continue

        ob_retest_idx = int(ob_retest_idx)

        # -------------------------------------------------
        # Overlapping logic: find later bearish OBs before this retest
        # -------------------------------------------------
        overlapping = _get_overlapping_bearish_group(bear_rev_df, row)

        scenario = None
        primary_ob_row = row
        primary_ob_id = ob_id

        if not overlapping.empty:
            # Check which OB retests first and whether levels match
            earliest_retest_idx = ob_retest_idx
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

            # Price level equality check (Scenario 4)
            same_level = np.isclose(primary_ob_row.HH_high, row.HH_high) and np.isclose(
                primary_ob_row.HH_low, row.HH_low
            )

            if earliest_retest_owner == "OB2":
                # Scenario 3: OB2 confirmed before OB1 retest and retests first → OB2 preferred
                scenario = "Scenario_3_OB2_preferred"
                used_as_primary.add(primary_ob_id)
            else:
                # OB1 retests first
                if same_level:
                    scenario = "Scenario_4_OB1_preferred_same_level"
                    used_as_primary.add(primary_ob_id)
                else:
                    scenario = "Scenario_3_OB1_kept_primary"

        if scenario is None:
            scenario = "Scenario_1_or_2"

        # From here, we trade using primary_ob_row
        entry_idx = int(primary_ob_row.OB_retest_idx)
        entry_time = primary_ob_row.OB_retest_time
        entry_price = df.loc[entry_idx, "close"]

        # -------------------------------------------------
        # Step 1: find first LL after entry (expected TP anchor)
        # -------------------------------------------------
        first_ll = _find_next_ll_after(sw_df, entry_idx)
        if first_ll is None:
            # No LL formed after entry → fallback: structural exit only
            exit_idx = None
            exit_time = None
            exit_price = None
            exit_reason = "no_LL_structural_only"

            # Structural invalidation: close above HH_high
            for i in range(entry_idx + 1, len(df)):
                if df.loc[i, "close"] > primary_ob_row.HH_high:
                    exit_idx = i
                    exit_time = df.loc[i, "time"]
                    exit_price = df.loc[i, "close"]
                    exit_reason = "structural_break_HH"
                    break

            trades.append(
                {
                    "scenario": scenario,
                    "direction": "short",
                    "HH_time": primary_ob_row.HH_time,
                    "HH_idx": primary_ob_row.HH_idx,
                    "HH_low": primary_ob_row.HH_low,
                    "HH_high": primary_ob_row.HH_high,
                    "entry_time": entry_time,
                    "entry_idx": entry_idx,
                    "entry_price": entry_price,
                    "exit_time": exit_time,
                    "exit_idx": exit_idx,
                    "exit_price": exit_price,
                    "exit_reason": exit_reason,
                    "LL_anchor_time": None,
                    "LL_anchor_idx": None,
                }
            )
            continue

        ll_anchor_idx = int(first_ll.idx)
        ll_anchor_time = first_ll.time

        # -------------------------------------------------
        # Step 2: Scenario 1 – check for bullish OB retest after LL
        # -------------------------------------------------
        # First, find bullish OBs whose MSS/LL chain is after this LL anchor
        # For now, we only care about retest timing
        bull_retests_after_ll = _bullish_ob_retests_between(bull_rev_df, ll_anchor_idx, None)

        exit_idx = None
        exit_time = None
        exit_price = None
        exit_reason = None

        if not bull_retests_after_ll.empty:
            # First bullish OB retest after LL → Scenario 1 exit
            first_bull = bull_retests_after_ll.iloc[0]
            exit_idx = int(first_bull.OB_retest_idx)
            exit_time = first_bull.OB_retest_time
            exit_price = df.loc[exit_idx, "close"]
            exit_reason = "bullish_OB_retest_after_LL"
            scenario_used = "Scenario_1_normal_flow"
        else:
            # -------------------------------------------------
            # Scenario 2 – continuation: no bullish retest, ride to next LL
            # -------------------------------------------------
            next_ll = _find_next_ll_after(sw_df, ll_anchor_idx)
            if next_ll is not None:
                # Look for any bullish OB retest between first LL and next LL
                bull_between = _bullish_ob_retests_between(
                    bull_rev_df, ll_anchor_idx, int(next_ll.idx)
                )
                if not bull_between.empty:
                    first_bull = bull_between.iloc[0]
                    exit_idx = int(first_bull.OB_retest_idx)
                    exit_time = first_bull.OB_retest_time
                    exit_price = df.loc[exit_idx, "close"]
                    exit_reason = "bullish_OB_retest_between_LL_chain"
                    scenario_used = "Scenario_2_continuation_exit_on_bullish"
                else:
                    # No bullish OB retest even by next LL → hold until structural break or end
                    scenario_used = "Scenario_2_continuation_no_bullish_retest"
            else:
                scenario_used = "Scenario_2_continuation_no_next_LL"

        # If still no exit decided → structural invalidation
        if exit_idx is None:
            for i in range(entry_idx + 1, len(df)):
                if df.loc[i, "close"] > primary_ob_row.HH_high:
                    exit_idx = i
                    exit_time = df.loc[i, "time"]
                    exit_price = df.loc[i, "close"]
                    exit_reason = "structural_break_HH"
                    break

        trades.append(
            {
                "scenario": scenario_used,
                "direction": "short",
                "HH_time": primary_ob_row.HH_time,
                "HH_idx": primary_ob_row.HH_idx,
                "HH_low": primary_ob_row.HH_low,
                "HH_high": primary_ob_row.HH_high,
                "entry_time": entry_time,
                "entry_idx": entry_idx,
                "entry_price": entry_price,
                "exit_time": exit_time,
                "exit_idx": exit_idx,
                "exit_price": exit_price,
                "exit_reason": exit_reason,
                "LL_anchor_time": ll_anchor_time,
                "LL_anchor_idx": ll_anchor_idx,
            }
        )

    return pd.DataFrame(trades)