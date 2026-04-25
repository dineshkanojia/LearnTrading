from __future__ import annotations

import logging

import pandas as pd
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class BearOBRef:
    source_idx: int
    source_time: pd.Timestamp
    source_label: str
    confirm_idx: int
    confirm_time: pd.Timestamp
    mitigation_idx: Optional[int]
    mitigation_time: Optional[pd.Timestamp]
    ob_low: float
    ob_high: float
    coexist_group: int
    attempt_no: int
    event_sequence: str


@dataclass
class BullOBRef:
    source_idx: int
    source_time: pd.Timestamp
    confirm_idx: int
    confirm_time: pd.Timestamp
    ob_low: float
    ob_high: float


@dataclass
class ActiveShortTrade:
    entry_idx: int
    entry_time: pd.Timestamp
    entry_price: float

    entry_ob_source_idx: int
    entry_ob_source_time: pd.Timestamp
    entry_ob_source_label: str
    entry_ob_confirm_idx: int
    entry_ob_confirm_time: pd.Timestamp
    entry_ob_low: float
    entry_ob_high: float

    latest_bear_ob_source_idx: int
    latest_bear_ob_source_time: pd.Timestamp
    latest_bear_ob_source_label: str
    latest_bear_ob_confirm_idx: int
    latest_bear_ob_confirm_time: pd.Timestamp
    latest_bear_ob_low: float
    latest_bear_ob_high: float

    exit_bull_ob_source_idx: Optional[int] = None
    exit_bull_ob_source_time: Optional[pd.Timestamp] = None
    exit_bull_ob_confirm_idx: Optional[int] = None
    exit_bull_ob_confirm_time: Optional[pd.Timestamp] = None
    exit_bull_ob_low: Optional[float] = None
    exit_bull_ob_high: Optional[float] = None


def _touches_level(candle_low: float, candle_high: float, level: float) -> bool:
    return candle_low <= level <= candle_high


def _prepare_bearish_obs(bear_obs_df: pd.DataFrame) -> pd.DataFrame:
    if bear_obs_df.empty:
        return pd.DataFrame(
            columns=[
                "source_idx", "source_time", "source_label",
                "confirm_idx", "confirm_time",
                "mitigation_idx", "mitigation_time",
                "ob_low", "ob_high",
                "coexist_group", "attempt_no", "event_sequence",
            ]
        )

    required = [
        "source_idx", "source_time", "source_label",
        "confirm_idx", "confirm_time",
        "mitigation_idx", "mitigation_time",
        "ob_low", "ob_high",
        "coexist_group", "attempt_no", "event_sequence",
    ]
    missing = [c for c in required if c not in bear_obs_df.columns]
    if missing:
        raise ValueError(
            f"bear_obs_df missing required columns: {missing}. "
            f"Available columns: {list(bear_obs_df.columns)}"
        )

    out = bear_obs_df[bear_obs_df["confirm_idx"].notna()].copy()
    out = out.sort_values(["confirm_idx", "source_idx"]).reset_index(drop=True)
    return out


def _prepare_bullish_obs(bull_obs_df: pd.DataFrame) -> pd.DataFrame:
    if bull_obs_df.empty:
        return pd.DataFrame(
            columns=[
                "source_idx", "source_time",
                "confirm_idx", "confirm_time",
                "ob_low", "ob_high", "status"
            ]
        )

    required = [
        "source_idx", "source_time",
        "confirm_idx", "confirm_time",
        "ob_low", "ob_high",
        "status",
    ]
    missing = [c for c in required if c not in bull_obs_df.columns]
    if missing:
        raise ValueError(
            f"bull_obs_df missing required columns: {missing}. "
            f"Available columns: {list(bull_obs_df.columns)}"
        )

    out = bull_obs_df[bull_obs_df["status"] == "CONFIRMED"].copy()
    out = out.sort_values(["confirm_idx", "source_idx"]).reset_index(drop=True)
    return out


def _build_first_touch_map(df: pd.DataFrame, bearish_obs: pd.DataFrame) -> dict[int, dict]:
    touch_map: dict[int, dict] = {}

    for row in bearish_obs.itertuples(index=False):
        source_idx = int(row.source_idx)
        confirm_idx = int(row.confirm_idx)
        level = float(row.ob_low)
        mitigation_idx = None if pd.isna(row.mitigation_idx) else int(row.mitigation_idx)

        first_touch_idx = None
        first_touch_time = None

        scan_end = mitigation_idx if mitigation_idx is not None else len(df)

        for i in range(confirm_idx + 1, scan_end):
            if _touches_level(
                candle_low=float(df.loc[i, "low"]),
                candle_high=float(df.loc[i, "high"]),
                level=level,
            ):
                first_touch_idx = i
                first_touch_time = pd.Timestamp(df.loc[i, "time"])
                break

        touch_map[source_idx] = {
            "first_touch_idx": first_touch_idx,
            "first_touch_time": first_touch_time,
            "first_touch_level": level,
        }

    return touch_map


def _is_ob_alive_at_idx(ob: BearOBRef, idx: int) -> bool:
    if idx <= ob.confirm_idx:
        return False
    if ob.mitigation_idx is not None and idx >= ob.mitigation_idx:
        return False
    return True

def _is_ob_tradable_at_idx(ob: BearOBRef, idx: int) -> bool:
    """
    Tradable means:
    - after confirm
    - before mitigation
    """
    if idx <= ob.confirm_idx:
        return False
    if ob.mitigation_idx is not None and idx >= ob.mitigation_idx:
        return False
    return True


def generate_bearish_flow_trades(
    df: pd.DataFrame,
    sw_df: pd.DataFrame,
    bear_obs_df: pd.DataFrame,
    bull_obs_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if df.empty:
        return pd.DataFrame(), pd.DataFrame()

    candles = df.reset_index(drop=True).copy()
    bearish_obs = _prepare_bearish_obs(bear_obs_df)
    bullish_obs = _prepare_bullish_obs(bull_obs_df)

    logger.debug("prepared bearish obs: %d", len(bearish_obs))
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(
            "bearish obs tail:\n%s",
            bearish_obs[
                [
                    "source_idx",
                    "source_label",
                    "confirm_idx",
                    "mitigation_idx",
                    "event_sequence",
                    "ob_low",
                    "ob_high",
                ]
            ].tail(30),
        )

    first_touch_map = _build_first_touch_map(candles, bearish_obs)

    if logger.isEnabledFor(logging.DEBUG):
        sample = []
        for row in bearish_obs.tail(20).itertuples(index=False):
            t = first_touch_map.get(int(row.source_idx), {})
            sample.append({
                "source_idx": int(row.source_idx),
                "confirm_idx": int(row.confirm_idx),
                "mitigation_idx": None if pd.isna(row.mitigation_idx) else int(row.mitigation_idx),
                "first_touch_idx": t.get("first_touch_idx"),
            })
        logger.debug("first-touch sample:\n%s", pd.DataFrame(sample))

    alive_bearish_obs: dict[int, BearOBRef] = {}
    used_bear_ob_sources: set[int] = set()

    bull_ptr = 0
    bear_ptr = 0

    latest_bull_ob: Optional[BullOBRef] = None
    active_trade: Optional[ActiveShortTrade] = None

    trades: list[dict] = []
    audit_rows: list[dict] = []

    audit_state: dict[int, dict] = {}
    for row in bearish_obs.itertuples(index=False):
        source_idx = int(row.source_idx)
        touch = first_touch_map.get(source_idx, {})
        audit_state[source_idx] = {
            "direction": "short",
            "ob_source_idx": source_idx,
            "ob_source_time": pd.Timestamp(row.source_time),
            "ob_source_label": str(row.source_label),
            "ob_confirm_idx": int(row.confirm_idx),
            "ob_confirm_time": pd.Timestamp(row.confirm_time),
            "ob_mitigation_idx": None if pd.isna(row.mitigation_idx) else int(row.mitigation_idx),
            "ob_mitigation_time": None if pd.isna(row.mitigation_time) else pd.Timestamp(row.mitigation_time),
            "ob_low": float(row.ob_low),
            "ob_high": float(row.ob_high),
            "event_sequence": str(row.event_sequence),
            "first_touch_idx": touch.get("first_touch_idx"),
            "first_touch_time": touch.get("first_touch_time"),
            "entered": False,
            "entry_idx": None,
            "entry_time": None,
            "skip_reason": None,
            "engine_state_at_touch": None,
            "coexist_group": int(row.coexist_group),
            "attempt_no": int(row.attempt_no),
        }

    for i, row in candles.iterrows():
        candle_time = pd.Timestamp(row["time"])
        candle_high = float(row["high"])
        candle_low = float(row["low"])
        candle_close = float(row["close"])

        exit_happened_this_bar = False

        # 1) absorb newly confirmed bearish OBs
        while bear_ptr < len(bearish_obs) and int(bearish_obs.iloc[bear_ptr]["confirm_idx"]) == i:
            ob = bearish_obs.iloc[bear_ptr]
            source_idx = int(ob["source_idx"])

            if source_idx not in used_bear_ob_sources:
                alive_bearish_obs[source_idx] = BearOBRef(
                    source_idx=source_idx,
                    source_time=pd.Timestamp(ob["source_time"]),
                    source_label=str(ob["source_label"]),
                    confirm_idx=int(ob["confirm_idx"]),
                    confirm_time=pd.Timestamp(ob["confirm_time"]),
                    mitigation_idx=None if pd.isna(ob["mitigation_idx"]) else int(ob["mitigation_idx"]),
                    mitigation_time=None if pd.isna(ob["mitigation_time"]) else pd.Timestamp(ob["mitigation_time"]),
                    ob_low=float(ob["ob_low"]),
                    ob_high=float(ob["ob_high"]),
                    coexist_group=int(ob["coexist_group"]),
                    attempt_no=int(ob["attempt_no"]),
                    event_sequence=str(ob["event_sequence"]),
                )

                if active_trade is not None:
                    alive_now = {k: v for k, v in alive_bearish_obs.items() if _is_ob_tradable_at_idx(v, i)}
                    lhs = [ob for ob in alive_now.values() if ob.source_label == "LH"]
                    chosen = max(lhs, key=lambda x: (x.confirm_idx, x.source_idx)) if lhs else (
                        max(alive_now.values(), key=lambda x: (x.confirm_idx, x.source_idx)) if alive_now else None
                    )
                    if chosen is not None:
                        active_trade.latest_bear_ob_source_idx = chosen.source_idx
                        active_trade.latest_bear_ob_source_time = chosen.source_time
                        active_trade.latest_bear_ob_source_label = chosen.source_label
                        active_trade.latest_bear_ob_confirm_idx = chosen.confirm_idx
                        active_trade.latest_bear_ob_confirm_time = chosen.confirm_time
                        active_trade.latest_bear_ob_low = chosen.ob_low
                        active_trade.latest_bear_ob_high = chosen.ob_high

            bear_ptr += 1

        # 2) remove dead bearish OBs by timing
        # remove only OBs that are truly dead by mitigation timing
        stale_keys = [
            k for k, ob in alive_bearish_obs.items()
            if ob.mitigation_idx is not None and i >= ob.mitigation_idx
        ]
        for k in stale_keys:
            alive_bearish_obs.pop(k, None)

        # 3) absorb newly confirmed bullish OBs
        while bull_ptr < len(bullish_obs) and int(bullish_obs.iloc[bull_ptr]["confirm_idx"]) == i:
            ob = bullish_obs.iloc[bull_ptr]
            latest_bull_ob = BullOBRef(
                source_idx=int(ob["source_idx"]),
                source_time=pd.Timestamp(ob["source_time"]),
                confirm_idx=int(ob["confirm_idx"]),
                confirm_time=pd.Timestamp(ob["confirm_time"]),
                ob_low=float(ob["ob_low"]),
                ob_high=float(ob["ob_high"]),
            )

            if active_trade is not None:
                active_trade.exit_bull_ob_source_idx = latest_bull_ob.source_idx
                active_trade.exit_bull_ob_source_time = latest_bull_ob.source_time
                active_trade.exit_bull_ob_confirm_idx = latest_bull_ob.confirm_idx
                active_trade.exit_bull_ob_confirm_time = latest_bull_ob.confirm_time
                active_trade.exit_bull_ob_low = latest_bull_ob.ob_low
                active_trade.exit_bull_ob_high = latest_bull_ob.ob_high

            bull_ptr += 1

        # 4) active short invalidation
        if active_trade is not None:
            active_bear_high = float(active_trade.latest_bear_ob_high)
            if candle_close > active_bear_high:
                trades.append(
                    {
                        "scenario": "state_machine_short_flow",
                        "direction": "short",
                        "entry_time": active_trade.entry_time,
                        "entry_idx": active_trade.entry_idx,
                        "entry_price": active_trade.entry_price,
                        "entry_ob_time": active_trade.entry_ob_source_time,
                        "entry_ob_idx": active_trade.entry_ob_source_idx,
                        "entry_ob_label": active_trade.entry_ob_source_label,
                        "entry_ob_confirm_time": active_trade.entry_ob_confirm_time,
                        "entry_ob_confirm_idx": active_trade.entry_ob_confirm_idx,
                        "entry_ob_low": active_trade.entry_ob_low,
                        "entry_ob_high": active_trade.entry_ob_high,
                        "latest_bear_ob_time": active_trade.latest_bear_ob_source_time,
                        "latest_bear_ob_idx": active_trade.latest_bear_ob_source_idx,
                        "latest_bear_ob_label": active_trade.latest_bear_ob_source_label,
                        "latest_bear_ob_confirm_time": active_trade.latest_bear_ob_confirm_time,
                        "latest_bear_ob_confirm_idx": active_trade.latest_bear_ob_confirm_idx,
                        "latest_bear_ob_low": active_trade.latest_bear_ob_low,
                        "latest_bear_ob_high": active_trade.latest_bear_ob_high,
                        "exit_ob_time": None,
                        "exit_ob_idx": None,
                        "exit_ob_confirm_time": None,
                        "exit_ob_confirm_idx": None,
                        "exit_ob_low": None,
                        "exit_ob_high": None,
                        "exit_time": candle_time,
                        "exit_idx": i,
                        "exit_price": candle_close,
                        "exit_reason": "close_above_active_bear_ob_high",
                    }
                )
                active_trade = None
                latest_bull_ob = None
                exit_happened_this_bar = True

        # 5) active short bullish exit
        if active_trade is not None and active_trade.exit_bull_ob_confirm_idx is not None:
            if i > active_trade.exit_bull_ob_confirm_idx:
                exit_level = float(active_trade.exit_bull_ob_high)
                if _touches_level(candle_low=candle_low, candle_high=candle_high, level=exit_level):
                    trades.append(
                        {
                            "scenario": "state_machine_short_flow",
                            "direction": "short",
                            "entry_time": active_trade.entry_time,
                            "entry_idx": active_trade.entry_idx,
                            "entry_price": active_trade.entry_price,
                            "entry_ob_time": active_trade.entry_ob_source_time,
                            "entry_ob_idx": active_trade.entry_ob_source_idx,
                            "entry_ob_label": active_trade.entry_ob_source_label,
                            "entry_ob_confirm_time": active_trade.entry_ob_confirm_time,
                            "entry_ob_confirm_idx": active_trade.entry_ob_confirm_idx,
                            "entry_ob_low": active_trade.entry_ob_low,
                            "entry_ob_high": active_trade.entry_ob_high,
                            "latest_bear_ob_time": active_trade.latest_bear_ob_source_time,
                            "latest_bear_ob_idx": active_trade.latest_bear_ob_source_idx,
                            "latest_bear_ob_label": active_trade.latest_bear_ob_source_label,
                            "latest_bear_ob_confirm_time": active_trade.latest_bear_ob_confirm_time,
                            "latest_bear_ob_confirm_idx": active_trade.latest_bear_ob_confirm_idx,
                            "latest_bear_ob_low": active_trade.latest_bear_ob_low,
                            "latest_bear_ob_high": active_trade.latest_bear_ob_high,
                            "exit_ob_time": active_trade.exit_bull_ob_source_time,
                            "exit_ob_idx": active_trade.exit_bull_ob_source_idx,
                            "exit_ob_confirm_time": active_trade.exit_bull_ob_confirm_time,
                            "exit_ob_confirm_idx": active_trade.exit_bull_ob_confirm_idx,
                            "exit_ob_low": active_trade.exit_bull_ob_low,
                            "exit_ob_high": active_trade.exit_bull_ob_high,
                            "exit_time": candle_time,
                            "exit_idx": i,
                            "exit_price": exit_level,
                            "exit_reason": "bullish_ob_confirmed_and_retested",
                        }
                    )
                    active_trade = None
                    latest_bull_ob = None
                    exit_happened_this_bar = True

        # 6) mark audit touch states
        for source_idx, st in audit_state.items():
            if st["first_touch_idx"] == i and not st["entered"] and st["skip_reason"] is None:
                st["engine_state_at_touch"] = "SHORT_OPEN" if active_trade is not None else "FLAT"

                if active_trade is not None:
                    st["skip_reason"] = "SHORT_ALREADY_OPEN"
                    continue

                if exit_happened_this_bar:
                    st["skip_reason"] = "EXIT_AND_TOUCH_SAME_BAR"
                    continue

                mit_idx = st["ob_mitigation_idx"]
                if mit_idx is not None and i >= mit_idx:
                    st["skip_reason"] = "MITIGATED_BEFORE_TOUCH"
                    continue

                if source_idx not in alive_bearish_obs:
                    st["skip_reason"] = "NOT_ACTIVE_CANDIDATE"
                    continue

        # 7) entry logic -- OUTSIDE audit loop
        if not exit_happened_this_bar and active_trade is None:
            tradable_now = []

            for ob in alive_bearish_obs.values():
                if ob.source_idx in used_bear_ob_sources:
                    continue
                if i <= ob.confirm_idx:
                    continue
                if ob.mitigation_idx is not None and i >= ob.mitigation_idx:
                    continue
                if not _touches_level(candle_low=candle_low, candle_high=candle_high, level=ob.ob_low):
                    continue
                tradable_now.append(ob)

            if tradable_now:
                lhs = [ob for ob in tradable_now if ob.source_label == "LH"]
                if lhs:
                    chosen = max(lhs, key=lambda x: (x.confirm_idx, x.source_idx))
                else:
                    chosen = max(tradable_now, key=lambda x: (x.confirm_idx, x.source_idx))

                entry_level = chosen.ob_low

                logger.debug(
                    "OPEN SHORT | idx=%s time=%s source_idx=%s label=%s "
                    "confirm_idx=%s mitigation_idx=%s",
                    i,
                    candle_time,
                    chosen.source_idx,
                    chosen.source_label,
                    chosen.confirm_idx,
                    chosen.mitigation_idx,
                )

                active_trade = ActiveShortTrade(
                    entry_idx=i,
                    entry_time=candle_time,
                    entry_price=entry_level,
                    entry_ob_source_idx=chosen.source_idx,
                    entry_ob_source_time=chosen.source_time,
                    entry_ob_source_label=chosen.source_label,
                    entry_ob_confirm_idx=chosen.confirm_idx,
                    entry_ob_confirm_time=chosen.confirm_time,
                    entry_ob_low=chosen.ob_low,
                    entry_ob_high=chosen.ob_high,
                    latest_bear_ob_source_idx=chosen.source_idx,
                    latest_bear_ob_source_time=chosen.source_time,
                    latest_bear_ob_source_label=chosen.source_label,
                    latest_bear_ob_confirm_idx=chosen.confirm_idx,
                    latest_bear_ob_confirm_time=chosen.confirm_time,
                    latest_bear_ob_low=chosen.ob_low,
                    latest_bear_ob_high=chosen.ob_high,
                )

                used_bear_ob_sources.add(chosen.source_idx)
                alive_bearish_obs.pop(chosen.source_idx, None)

                st = audit_state[chosen.source_idx]
                st["entered"] = True
                st["entry_idx"] = i
                st["entry_time"] = candle_time
                st["engine_state_at_touch"] = "FLAT"
                st["skip_reason"] = "ENTERED"

    # include still-open trade at end of data
    if active_trade is not None:
        trades.append(
            {
                "scenario": "state_machine_short_flow",
                "direction": "short",
                "entry_time": active_trade.entry_time,
                "entry_idx": active_trade.entry_idx,
                "entry_price": active_trade.entry_price,
                "entry_ob_time": active_trade.entry_ob_source_time,
                "entry_ob_idx": active_trade.entry_ob_source_idx,
                "entry_ob_label": active_trade.entry_ob_source_label,
                "entry_ob_confirm_time": active_trade.entry_ob_confirm_time,
                "entry_ob_confirm_idx": active_trade.entry_ob_confirm_idx,
                "entry_ob_low": active_trade.entry_ob_low,
                "entry_ob_high": active_trade.entry_ob_high,
                "latest_bear_ob_time": active_trade.latest_bear_ob_source_time,
                "latest_bear_ob_idx": active_trade.latest_bear_ob_source_idx,
                "latest_bear_ob_label": active_trade.latest_bear_ob_source_label,
                "latest_bear_ob_confirm_time": active_trade.latest_bear_ob_confirm_time,
                "latest_bear_ob_confirm_idx": active_trade.latest_bear_ob_confirm_idx,
                "latest_bear_ob_low": active_trade.latest_bear_ob_low,
                "latest_bear_ob_high": active_trade.latest_bear_ob_high,
                "exit_ob_time": active_trade.exit_bull_ob_source_time,
                "exit_ob_idx": active_trade.exit_bull_ob_source_idx,
                "exit_ob_confirm_time": active_trade.exit_bull_ob_confirm_time,
                "exit_ob_confirm_idx": active_trade.exit_bull_ob_confirm_idx,
                "exit_ob_low": active_trade.exit_bull_ob_low,
                "exit_ob_high": active_trade.exit_bull_ob_high,
                "exit_time": pd.Timestamp(df.iloc[-1]["time"]),
                "exit_idx": int(df.index[-1]),
                "exit_price": float(df.iloc[-1]["close"]),
                "exit_reason": "open_trade_at_end_of_data",
            }
        )

    # finalize audit
    for source_idx, st in audit_state.items():
        if st["skip_reason"] is None:
            if st["entered"]:
                st["skip_reason"] = "ENTERED"
            elif st["ob_mitigation_idx"] is not None and (
                st["first_touch_idx"] is None or st["ob_mitigation_idx"] <= st["first_touch_idx"]
            ):
                st["skip_reason"] = "MITIGATED_BEFORE_TOUCH"
            elif st["first_touch_idx"] is None:
                st["skip_reason"] = "NO_TOUCH_FOUND"
            else:
                st["skip_reason"] = "NOT_ACTIVE_CANDIDATE"

        audit_rows.append(st)

    trades_df = pd.DataFrame(trades)
    audit_df = pd.DataFrame(audit_rows).reset_index(drop=True)

    logger.info("BEAR FLOW TRADES: %d", len(trades_df))
    logger.info("BEAR FLOW AUDIT: %d", len(audit_df))
    return trades_df, audit_df