from __future__ import annotations

import logging

import pandas as pd
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class OBRef:
    confirm_idx: int
    confirm_time: pd.Timestamp
    zone_low: float
    zone_high: float
    side: str  # "bullish" or "bearish"


@dataclass
class ActiveLongTrade:
    entry_idx: int
    entry_time: pd.Timestamp
    entry_price: float

    entry_ob_confirm_idx: int
    entry_ob_confirm_time: pd.Timestamp
    entry_ob_low: float
    entry_ob_high: float

    latest_bull_ob_confirm_idx: int
    latest_bull_ob_confirm_time: pd.Timestamp
    latest_bull_ob_low: float
    latest_bull_ob_high: float

    exit_bear_ob_confirm_idx: Optional[int] = None
    exit_bear_ob_confirm_time: Optional[pd.Timestamp] = None
    exit_bear_ob_low: Optional[float] = None
    exit_bear_ob_high: Optional[float] = None


def _zone_touched(
    candle_low: float,
    candle_high: float,
    zone_low: float,
    zone_high: float,
) -> bool:
    """
    True only when candle range actually overlaps the OB zone.
    Exact boundary touch counts as a touch.
    """
    return candle_high >= zone_low and candle_low <= zone_high


def _prepare_bullish_obs(bull_rev_df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize bullish OB dataframe into:
      confirm_idx, confirm_time, zone_low, zone_high

    Assumes bullish OB zone is the LL candle range.
    """
    if bull_rev_df.empty:
        return pd.DataFrame(columns=["confirm_idx", "confirm_time", "zone_low", "zone_high"])

    out = bull_rev_df.copy().rename(
        columns={
            "LL_idx": "confirm_idx",
            "LL_time": "confirm_time",
            "LL_low": "zone_low",
            "LL_high": "zone_high",
        }
    )

    required = ["confirm_idx", "confirm_time", "zone_low", "zone_high"]
    missing = [c for c in required if c not in out.columns]
    if missing:
        raise ValueError(
            f"bull_rev_df missing required bullish OB columns: {missing}. "
            f"Available columns: {list(bull_rev_df.columns)}"
        )

    return out[required].sort_values("confirm_idx").reset_index(drop=True)


def _prepare_bearish_obs(bear_rev_df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize bearish OB dataframe into:
      confirm_idx, confirm_time, zone_low, zone_high

    Assumes bearish OB zone is the HH candle range.
    """
    if bear_rev_df.empty:
        return pd.DataFrame(columns=["confirm_idx", "confirm_time", "zone_low", "zone_high"])

    out = bear_rev_df.copy().rename(
        columns={
            "HH_idx": "confirm_idx",
            "HH_time": "confirm_time",
            "HH_low": "zone_low",
            "HH_high": "zone_high",
        }
    )

    required = ["confirm_idx", "confirm_time", "zone_low", "zone_high"]
    missing = [c for c in required if c not in out.columns]
    if missing:
        raise ValueError(
            f"bear_rev_df missing required bearish OB columns: {missing}. "
            f"Available columns: {list(bear_rev_df.columns)}"
        )

    return out[required].sort_values("confirm_idx").reset_index(drop=True)


def generate_bullish_flow_trades(
    df: pd.DataFrame,
    sw_df: pd.DataFrame,   # kept only for signature compatibility
    bull_rev_df: pd.DataFrame,
    bear_rev_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Strict one-active-trade bullish flow engine.

    Rules:
    1. Only one long trade active at a time.
    2. If already in a long and a newer bullish OB confirms, update active bullish OB reference.
    3. Do not open overlapping longs.
    4. Exit long only when opposite bearish OB is truly retested.
    5. Retest requires actual zone touch, not near-zone.
    6. Same-candle confirmation does not count as retest.
    """

    if df.empty:
        return pd.DataFrame()

    candles = df.reset_index(drop=True).copy()
    bullish_obs = _prepare_bullish_obs(bull_rev_df)
    bearish_obs = _prepare_bearish_obs(bear_rev_df)

    bull_ptr = 0
    bear_ptr = 0

    latest_bull_ob: Optional[OBRef] = None
    latest_bear_ob: Optional[OBRef] = None
    active_trade: Optional[ActiveLongTrade] = None

    trades: list[dict] = []

    for i, row in candles.iterrows():
        candle_time = pd.Timestamp(row["time"])
        candle_high = float(row["high"])
        candle_low = float(row["low"])
        candle_close = float(row["close"])

        # --------------------------------------------------
        # 1) absorb all newly confirmed bullish OBs on this candle
        # --------------------------------------------------
        while bull_ptr < len(bullish_obs) and int(bullish_obs.iloc[bull_ptr]["confirm_idx"]) == i:
            ob = bullish_obs.iloc[bull_ptr]
            latest_bull_ob = OBRef(
                confirm_idx=int(ob["confirm_idx"]),
                confirm_time=pd.Timestamp(ob["confirm_time"]),
                zone_low=float(ob["zone_low"]),
                zone_high=float(ob["zone_high"]),
                side="bullish",
            )

            # while long is open, newest bullish OB becomes active same-side reference
            if active_trade is not None:
                active_trade.latest_bull_ob_confirm_idx = latest_bull_ob.confirm_idx
                active_trade.latest_bull_ob_confirm_time = latest_bull_ob.confirm_time
                active_trade.latest_bull_ob_low = latest_bull_ob.zone_low
                active_trade.latest_bull_ob_high = latest_bull_ob.zone_high

            bull_ptr += 1

        # --------------------------------------------------
        # 2) absorb all newly confirmed bearish OBs on this candle
        # --------------------------------------------------
        while bear_ptr < len(bearish_obs) and int(bearish_obs.iloc[bear_ptr]["confirm_idx"]) == i:
            ob = bearish_obs.iloc[bear_ptr]
            latest_bear_ob = OBRef(
                confirm_idx=int(ob["confirm_idx"]),
                confirm_time=pd.Timestamp(ob["confirm_time"]),
                zone_low=float(ob["zone_low"]),
                zone_high=float(ob["zone_high"]),
                side="bearish",
            )

            # while long is open, newest bearish OB becomes exit candidate
            if active_trade is not None:
                active_trade.exit_bear_ob_confirm_idx = latest_bear_ob.confirm_idx
                active_trade.exit_bear_ob_confirm_time = latest_bear_ob.confirm_time
                active_trade.exit_bear_ob_low = latest_bear_ob.zone_low
                active_trade.exit_bear_ob_high = latest_bear_ob.zone_high

            bear_ptr += 1

        # --------------------------------------------------
        # 3) flat -> enter only on true bullish OB retest
        # --------------------------------------------------
        if active_trade is None:
            if latest_bull_ob is not None:
                # confirmation candle itself cannot count as retest
                if i > latest_bull_ob.confirm_idx:
                    if _zone_touched(
                        candle_low=candle_low,
                        candle_high=candle_high,
                        zone_low=latest_bull_ob.zone_low,
                        zone_high=latest_bull_ob.zone_high,
                    ):
                        active_trade = ActiveLongTrade(
                            entry_idx=i,
                            entry_time=candle_time,
                            entry_price=candle_close,

                            entry_ob_confirm_idx=latest_bull_ob.confirm_idx,
                            entry_ob_confirm_time=latest_bull_ob.confirm_time,
                            entry_ob_low=latest_bull_ob.zone_low,
                            entry_ob_high=latest_bull_ob.zone_high,

                            latest_bull_ob_confirm_idx=latest_bull_ob.confirm_idx,
                            latest_bull_ob_confirm_time=latest_bull_ob.confirm_time,
                            latest_bull_ob_low=latest_bull_ob.zone_low,
                            latest_bull_ob_high=latest_bull_ob.zone_high,
                        )

                        # prevent same candle from also exiting
                        continue

        # --------------------------------------------------
        # 4) active long -> exit only on true bearish OB retest
        # --------------------------------------------------
        else:
            if active_trade.exit_bear_ob_confirm_idx is not None:
                if i > active_trade.exit_bear_ob_confirm_idx:
                    if _zone_touched(
                        candle_low=candle_low,
                        candle_high=candle_high,
                        zone_low=float(active_trade.exit_bear_ob_low),
                        zone_high=float(active_trade.exit_bear_ob_high),
                    ):
                        trades.append(
                            {
                                "scenario": "state_machine_long_flow",
                                "direction": "long",

                                "entry_time": active_trade.entry_time,
                                "entry_idx": active_trade.entry_idx,
                                "entry_price": active_trade.entry_price,

                                "entry_ob_time": active_trade.entry_ob_confirm_time,
                                "entry_ob_idx": active_trade.entry_ob_confirm_idx,
                                "entry_ob_low": active_trade.entry_ob_low,
                                "entry_ob_high": active_trade.entry_ob_high,

                                "latest_bull_ob_time": active_trade.latest_bull_ob_confirm_time,
                                "latest_bull_ob_idx": active_trade.latest_bull_ob_confirm_idx,
                                "latest_bull_ob_low": active_trade.latest_bull_ob_low,
                                "latest_bull_ob_high": active_trade.latest_bull_ob_high,

                                "exit_ob_time": active_trade.exit_bear_ob_confirm_time,
                                "exit_ob_idx": active_trade.exit_bear_ob_confirm_idx,
                                "exit_ob_low": active_trade.exit_bear_ob_low,
                                "exit_ob_high": active_trade.exit_bear_ob_high,

                                "exit_time": candle_time,
                                "exit_idx": i,
                                "exit_price": candle_close,
                                "exit_reason": "bearish_OB_true_retest",
                            }
                        )

                        active_trade = None
                        latest_bear_ob = None
                        continue

    logger.info("BULL FLOW TRADES: %d", len(trades))
    return pd.DataFrame(trades)