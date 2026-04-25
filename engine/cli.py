# engine/cli.py
"""
Command-line entry point.

Wires together env loading, logging, the pipeline, and the report writer.
Usage:
    python main.py
    python -m engine.cli --verbose --start "01 Jan 2026 00:00:00"
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

from dotenv import load_dotenv

from engine.config import StrategyConfig
from engine.pipeline import run_pipeline
from engine.report import default_report_path, write_excel_report
from engine.utils import get_binance_client

logger = logging.getLogger(__name__)


def configure_logging(verbose: bool) -> None:
    """Set up root logging once, with a sensible default format."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-7s %(name)s :: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def load_credentials() -> tuple[str, str]:
    """Read Binance creds from environment (with .env support)."""
    load_dotenv()

    api_key = os.environ.get("BINANCE_API_KEY")
    api_secret = os.environ.get("BINANCE_API_SECRET")

    if not api_key or not api_secret:
        raise RuntimeError(
            "Missing Binance credentials. Copy .env.example to .env and fill "
            "in BINANCE_API_KEY / BINANCE_API_SECRET, or export them in your "
            "shell."
        )

    return api_key, api_secret


def build_config(args: argparse.Namespace) -> StrategyConfig:
    """Compose a StrategyConfig from defaults + CLI overrides + env overrides."""
    config = StrategyConfig()

    # CLI overrides (take precedence over env)
    if args.symbol:
        config.fetch.symbol = args.symbol
    if args.interval:
        config.fetch.interval = args.interval
    if args.start:
        config.fetch.fixed_start = args.start
    if args.output_folder:
        config.fetch.output_folder = args.output_folder

    # Env-var overrides for the small set we keep in main.py historically
    if not args.symbol and (env_symbol := os.environ.get("SYMBOL")):
        config.fetch.symbol = env_symbol
    if not args.interval and (env_interval := os.environ.get("INTERVAL")):
        config.fetch.interval = env_interval
    if not args.output_folder and (env_out := os.environ.get("OUTPUT_FOLDER")):
        config.fetch.output_folder = env_out

    return config


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="learntrading",
        description="Run the SMC backtest pipeline and write a multi-sheet xlsx report.",
    )
    parser.add_argument("--symbol", help="Override trading pair (default: BTCUSDT).")
    parser.add_argument("--interval", help='Override kline interval (default: "15m").')
    parser.add_argument(
        "--start",
        help='Fixed start time, e.g. "15 Dec 2025 00:00:00". Overrides lookback_months.',
    )
    parser.add_argument(
        "--output-folder",
        dest="output_folder",
        help="Where to write the xlsx report (default: data).",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable DEBUG-level logging.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    configure_logging(args.verbose)

    try:
        api_key, api_secret = load_credentials()
    except RuntimeError as exc:
        logger.error("%s", exc)
        return 2

    config = build_config(args)
    logger.debug("resolved config: %r", config)

    client = get_binance_client(api_key, api_secret)
    result = run_pipeline(client, config)

    report_path = default_report_path(
        output_folder=config.fetch.output_folder,
        symbol=f"{config.fetch.symbol[:3]}{_interval_suffix(config.fetch.interval)}",
    )
    write_excel_report(result, report_path)

    logger.info("done. report: %s", report_path)
    return 0


def _interval_suffix(interval: str) -> str:
    """Convert "15m"/"1h"/"4h"/"1d" -> "15"/"1"/"4"/"1" for filename suffix.

    Best-effort: falls back to a sanitised version of the interval if the
    format is unfamiliar.
    """
    for suffix in ("m", "h", "d", "M", "w"):
        if interval.endswith(suffix):
            return interval[: -len(suffix)]
    return "".join(c for c in interval if c.isalnum())


if __name__ == "__main__":
    sys.exit(main())
