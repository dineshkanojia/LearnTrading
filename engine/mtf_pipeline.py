from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Optional

import pandas as pd


@dataclass(slots=True)
class MTFContext:
    tf_map: dict[str, pd.DataFrame]
    structure_map: dict[str, pd.DataFrame]
    ob_map: dict[str, dict[str, pd.DataFrame]]


def _require_ohlc(df: pd.DataFrame) -> None:
    required = ["time", "open", "high", "low", "close"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing OHLC columns: {missing}")


def _normalize_time(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["time"] = pd.to_datetime(out["time"], utc=True)
    out = out.sort_values("time").reset_index(drop=True)
    return out


def _ensure_idx(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["idx"] = range(len(out))
    return out


def _resample_ohlc(df_15m: pd.DataFrame, rule: str) -> pd.DataFrame:
    """
    Assumes df_15m has UTC timestamps and standard OHLCV columns.
    """
    _require_ohlc(df_15m)

    df = _normalize_time(df_15m).copy()
    df = df.set_index("time")

    agg = {
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
    }

    if "volume" in df.columns:
        agg["volume"] = "sum"

    out = df.resample(rule, label="left", closed="left").agg(agg).dropna().reset_index()
    out = _ensure_idx(out)
    return out


def build_timeframe_map(
    df_15m: pd.DataFrame,
    include_15m: bool = True,
) -> dict[str, pd.DataFrame]:
    """
    Build raw candle map from 15m source.
    """
    df_15m = _normalize_time(df_15m)
    if "idx" not in df_15m.columns:
        df_15m = _ensure_idx(df_15m)

    tf_map: dict[str, pd.DataFrame] = {}

    if include_15m:
        tf_map["15m"] = df_15m.copy()

    tf_map["1H"] = _resample_ohlc(df_15m, "1h")
    tf_map["4H"] = _resample_ohlc(df_15m, "4h")
    tf_map["1D"] = _resample_ohlc(df_15m, "1d")

    return tf_map


def build_structure_map(
    tf_map: dict[str, pd.DataFrame],
    structure_builder: Callable[..., pd.DataFrame],
    structure_kwargs_by_tf: Optional[dict[str, dict]] = None,
) -> dict[str, pd.DataFrame]:
    """
    Run generic structure builder on every timeframe.
    """
    structure_kwargs_by_tf = structure_kwargs_by_tf or {}
    structure_map: dict[str, pd.DataFrame] = {}

    for tf_name, df_tf in tf_map.items():
        kwargs = structure_kwargs_by_tf.get(tf_name, {})
        sw_df = structure_builder(df_tf.copy(), tf_name=tf_name, **kwargs)
        structure_map[tf_name] = sw_df

    return structure_map


def build_ob_map(
    tf_map: dict[str, pd.DataFrame],
    structure_map: dict[str, pd.DataFrame],
    bearish_ob_builder: Callable[..., pd.DataFrame],
    bullish_ob_builder: Callable[..., pd.DataFrame],
    bearish_kwargs_by_tf: Optional[dict[str, dict]] = None,
    bullish_kwargs_by_tf: Optional[dict[str, dict]] = None,
) -> dict[str, dict[str, pd.DataFrame]]:
    """
    Run bearish/bullish OB builders on every timeframe.
    """
    bearish_kwargs_by_tf = bearish_kwargs_by_tf or {}
    bullish_kwargs_by_tf = bullish_kwargs_by_tf or {}

    ob_map: dict[str, dict[str, pd.DataFrame]] = {}

    for tf_name, df_tf in tf_map.items():
        sw_df = structure_map[tf_name]

        bear_kwargs = bearish_kwargs_by_tf.get(tf_name, {})
        bull_kwargs = bullish_kwargs_by_tf.get(tf_name, {})

        bear_df = bearish_ob_builder(df_tf.copy(), sw_df.copy(), tf_name=tf_name, **bear_kwargs)
        bull_df = bullish_ob_builder(df_tf.copy(), sw_df.copy(), tf_name=tf_name, **bull_kwargs)

        ob_map[tf_name] = {
            "bear": bear_df,
            "bull": bull_df,
        }

    return ob_map


def build_mtf_context(
    df_15m: pd.DataFrame,
    structure_builder: Callable[..., pd.DataFrame],
    bearish_ob_builder: Callable[..., pd.DataFrame],
    bullish_ob_builder: Callable[..., pd.DataFrame],
    structure_kwargs_by_tf: Optional[dict[str, dict]] = None,
    bearish_kwargs_by_tf: Optional[dict[str, dict]] = None,
    bullish_kwargs_by_tf: Optional[dict[str, dict]] = None,
) -> MTFContext:
    """
    End-to-end multi-timeframe context builder.
    """
    tf_map = build_timeframe_map(df_15m)
    structure_map = build_structure_map(
        tf_map=tf_map,
        structure_builder=structure_builder,
        structure_kwargs_by_tf=structure_kwargs_by_tf,
    )
    ob_map = build_ob_map(
        tf_map=tf_map,
        structure_map=structure_map,
        bearish_ob_builder=bearish_ob_builder,
        bullish_ob_builder=bullish_ob_builder,
        bearish_kwargs_by_tf=bearish_kwargs_by_tf,
        bullish_kwargs_by_tf=bullish_kwargs_by_tf,
    )
    return MTFContext(
        tf_map=tf_map,
        structure_map=structure_map,
        ob_map=ob_map,
    )