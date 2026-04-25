# engine/config.py
"""
Single source of truth for strategy parameters.

Defaults reproduce the values previously inlined in main.py so behavior is
unchanged unless a caller overrides specific fields.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


# ---------------------------------------------------------------------------
# Per-timeframe parameter dictionaries — kept as plain dicts so they thread
# straight through to mtf_pipeline.build_mtf_context without translation.
# ---------------------------------------------------------------------------

DEFAULT_STRUCTURE_KWARGS_BY_TF: dict[str, dict[str, Any]] = {
    "15m": {
        "left_strength": 8,
        "right_strength": 8,
        "min_bars_between_same_side": 8,
        "min_swing_pct": 0.003,
    },
    "1H": {
        "left_strength": 6,
        "right_strength": 6,
        "min_bars_between_same_side": 4,
        "min_swing_pct": 0.004,
    },
    "4H": {
        "left_strength": 4,
        "right_strength": 4,
        "min_bars_between_same_side": 3,
        "min_swing_pct": 0.006,
    },
    "1D": {
        "left_strength": 3,
        "right_strength": 3,
        "min_bars_between_same_side": 2,
        "min_swing_pct": 0.01,
    },
}

DEFAULT_BEARISH_OB_KWARGS_BY_TF: dict[str, dict[str, Any]] = {
    "15m": {"max_scan_bars": 80, "max_followup_attempts": 1},
    "1H": {"max_scan_bars": 40, "max_followup_attempts": 1},
    "4H": {"max_scan_bars": 20, "max_followup_attempts": 1},
    "1D": {"max_scan_bars": 12, "max_followup_attempts": 1},
}

DEFAULT_BULLISH_OB_KWARGS_BY_TF: dict[str, dict[str, Any]] = {
    "15m": {"max_scan_bars": 80, "pivot_span": 1},
    "1H": {"max_scan_bars": 40, "pivot_span": 1},
    "4H": {"max_scan_bars": 20, "pivot_span": 1},
    "1D": {"max_scan_bars": 12, "pivot_span": 1},
}


# ---------------------------------------------------------------------------
# Top-level configuration
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class FetchConfig:
    """How and what to pull from Binance."""

    symbol: str = "BTCUSDT"
    interval: str = "15m"  # Client.KLINE_INTERVAL_15MINUTE resolves to "15m"
    fixed_start: str | None = "15 Dec 2025 00:00:00"
    lookback_months: int = 3
    output_folder: str = "data"


@dataclass(slots=True)
class PivotConfig:
    """Parameters for the standalone pivot detector applied to the active
    timeframe (currently the 15m frame)."""

    left: int = 3
    right: int = 3


@dataclass(slots=True)
class HybridSwingConfig:
    """Reserved hybrid-swing knobs from the previous main.py.

    Not consumed by the current pipeline (build_structure_swings is
    parameterised via DEFAULT_STRUCTURE_KWARGS_BY_TF), but kept so existing
    inline notes stay traceable.
    """

    min_swing_pct: float = 0.0030
    min_spacing_bars: int = 4
    confirm_pct: float = 0.0025
    min_confirmation_bars: int = 2
    min_opposite_candidate_spacing_bars: int = 3


@dataclass(slots=True)
class StrategyConfig:
    """All knobs the pipeline needs to run a backtest.

    Defaults reproduce the prior inline values exactly. Construct with
    overrides as needed:

        cfg = StrategyConfig()
        cfg.fetch.fixed_start = "01 Jan 2026 00:00:00"
    """

    fetch: FetchConfig = field(default_factory=FetchConfig)
    pivots: PivotConfig = field(default_factory=PivotConfig)
    hybrid: HybridSwingConfig = field(default_factory=HybridSwingConfig)

    structure_kwargs_by_tf: dict[str, dict[str, Any]] = field(
        default_factory=lambda: {tf: dict(v) for tf, v in DEFAULT_STRUCTURE_KWARGS_BY_TF.items()}
    )
    bearish_ob_kwargs_by_tf: dict[str, dict[str, Any]] = field(
        default_factory=lambda: {tf: dict(v) for tf, v in DEFAULT_BEARISH_OB_KWARGS_BY_TF.items()}
    )
    bullish_ob_kwargs_by_tf: dict[str, dict[str, Any]] = field(
        default_factory=lambda: {tf: dict(v) for tf, v in DEFAULT_BULLISH_OB_KWARGS_BY_TF.items()}
    )

    # Active timeframe whose flow trades drive the report.
    active_tf: str = "15m"


__all__ = [
    "FetchConfig",
    "PivotConfig",
    "HybridSwingConfig",
    "StrategyConfig",
    "DEFAULT_STRUCTURE_KWARGS_BY_TF",
    "DEFAULT_BEARISH_OB_KWARGS_BY_TF",
    "DEFAULT_BULLISH_OB_KWARGS_BY_TF",
]
