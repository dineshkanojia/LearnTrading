# engine/pipeline.py
"""
End-to-end backtest pipeline.

Takes a StrategyConfig + a Binance client, returns a PipelineResult that the
report layer can serialise. Pipeline never writes to disk and never calls
print(); the CLI / report layers handle I/O.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import pandas as pd
from binance.client import Client

from engine.bearish_flow_engine import generate_bearish_flow_trades
from engine.bearish_ob import build_bearish_obs
from engine.bullish_ob import build_bullish_obs
from engine.config import StrategyConfig
from engine.mtf_pipeline import MTFContext, build_mtf_context
from engine.pivots import detect_pivots
from engine.swings import apply_anchor_quality_filter, build_structure_swings
from engine.utils import fetch_klines

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class PipelineResult:
    """Everything the report layer needs."""

    config: StrategyConfig
    df_active: pd.DataFrame              # active-TF candles (with pivots)
    swings_active: pd.DataFrame          # active-TF swing labels
    bull_obs_active: pd.DataFrame        # active-TF bullish OBs
    bear_obs_active: pd.DataFrame        # active-TF bearish OBs
    bear_trades: pd.DataFrame            # generated bearish flow trades
    bear_audit: pd.DataFrame             # audit log of touch decisions
    mtf: MTFContext                      # full multi-timeframe context


def run_pipeline(client: Client, config: StrategyConfig) -> PipelineResult:
    """
    Execute the full backtest pipeline.

    Steps:
      1. Fetch klines for the configured symbol/interval.
      2. Build the multi-timeframe context (structure + OBs on each TF).
      3. Pull the active-TF slice and overlay pivot flags.
      4. Generate bearish flow trades.

    Returns a PipelineResult. Does not touch the filesystem.
    """
    logger.info(
        "fetching klines: symbol=%s interval=%s start=%s",
        config.fetch.symbol,
        config.fetch.interval,
        config.fetch.fixed_start,
    )
    df_15m = fetch_klines(
        client,
        config.fetch.symbol,
        config.fetch.interval,
        lookback_months=config.fetch.lookback_months,
        fixed_start=config.fetch.fixed_start,
    )
    logger.info("fetched %d candles", len(df_15m))

    logger.info("building MTF context")
    mtf = build_mtf_context(
        df_15m=df_15m,
        structure_builder=build_structure_swings,
        bearish_ob_builder=build_bearish_obs,
        bullish_ob_builder=build_bullish_obs,
        structure_kwargs_by_tf=config.structure_kwargs_by_tf,
        bearish_kwargs_by_tf=config.bearish_ob_kwargs_by_tf,
        bullish_kwargs_by_tf=config.bullish_ob_kwargs_by_tf,
    )

    active_tf = config.active_tf
    if active_tf not in mtf.tf_map:
        raise KeyError(
            f"active_tf={active_tf!r} not in MTF context (available: {list(mtf.tf_map)})"
        )

    # Active-TF outputs come straight from the MTF context — no recomputation.
    df_active = mtf.tf_map[active_tf].copy()
    bull_obs_active = mtf.ob_map[active_tf]["bull"]
    bear_obs_active = mtf.ob_map[active_tf]["bear"]

    # Pivot overlay is consumed by report tooling; safe to apply on the
    # already-resampled active frame.
    df_active = detect_pivots(
        df_active,
        left=config.pivots.left,
        right=config.pivots.right,
    )

    # Anchor-quality filter is a pass-through today but kept on the path so
    # downstream code can rely on the column being present.
    swings_active = apply_anchor_quality_filter(
        mtf.structure_map[active_tf],
        df_active,
    )

    logger.info(
        "active_tf=%s candles=%d swings=%d bull_obs=%d bear_obs=%d",
        active_tf,
        len(df_active),
        len(swings_active),
        len(bull_obs_active),
        len(bear_obs_active),
    )

    logger.info("generating bearish flow trades")
    bear_trades, bear_audit = generate_bearish_flow_trades(
        df=df_active,
        sw_df=swings_active,
        bear_obs_df=bear_obs_active,
        bull_obs_df=bull_obs_active,
    )
    logger.info(
        "bearish flow trades: %d, audit rows: %d",
        len(bear_trades),
        len(bear_audit),
    )

    return PipelineResult(
        config=config,
        df_active=df_active,
        swings_active=swings_active,
        bull_obs_active=bull_obs_active,
        bear_obs_active=bear_obs_active,
        bear_trades=bear_trades,
        bear_audit=bear_audit,
        mtf=mtf,
    )


__all__ = ["PipelineResult", "run_pipeline"]
