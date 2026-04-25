# engine/report.py
"""
Excel report writer.

Takes a PipelineResult and produces the multi-sheet workbook that drove the
original main.py output. The sheet layout is intentionally unchanged.
"""

from __future__ import annotations

import logging
import os
from datetime import UTC, datetime

import pandas as pd

from engine.pipeline import PipelineResult

logger = logging.getLogger(__name__)


def make_excel_safe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Strip timezone info from datetime columns so openpyxl can serialise them.

    Lifted verbatim from the original main.py.
    """
    out = df.copy()

    for col in out.columns:
        if isinstance(out[col].dtype, pd.DatetimeTZDtype):
            out[col] = out[col].dt.tz_localize(None)
            continue

        if out[col].dtype == "object":
            sample = out[col].dropna()
            if not sample.empty:
                first = sample.iloc[0]
                if isinstance(first, pd.Timestamp) and first.tz is not None:
                    out[col] = out[col].apply(
                        lambda x: x.tz_localize(None)
                        if isinstance(x, pd.Timestamp) and x.tz is not None
                        else x
                    )

    return out


def default_report_path(output_folder: str, symbol: str = "BTC15") -> str:
    """Build the output path used historically: <folder>/<prefix>_<DD_MM_YYYY>.xlsx."""
    today = datetime.now(UTC).strftime("%d_%m_%Y")
    os.makedirs(output_folder, exist_ok=True)
    return os.path.join(output_folder, f"{symbol}_{today}.xlsx")


def write_excel_report(result: PipelineResult, path: str) -> str:
    """
    Serialise the pipeline result into a multi-sheet xlsx.

    Sheet layout (matches the previous main.py output):
      - ohlcv_pivots
      - swing
      - Bull_OB
      - bear_OB
      - bear_OB_1H        (from MTF context)
      - bear_flow_trades
      - bear_audit_df
      - bull_OB_1H        (from MTF context)
    """
    mtf = result.mtf

    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        make_excel_safe(result.df_active).to_excel(writer, sheet_name="ohlcv_pivots", index=False)
        make_excel_safe(result.swings_active).to_excel(writer, sheet_name="swing", index=False)
        make_excel_safe(result.bull_obs_active).to_excel(writer, sheet_name="Bull_OB", index=False)
        make_excel_safe(result.bear_obs_active).to_excel(writer, sheet_name="bear_OB", index=False)

        if "1H" in mtf.ob_map:
            make_excel_safe(mtf.ob_map["1H"]["bear"]).to_excel(
                writer, sheet_name="bear_OB_1H", index=False
            )
            make_excel_safe(mtf.ob_map["1H"]["bull"]).to_excel(
                writer, sheet_name="bull_OB_1H", index=False
            )

        make_excel_safe(result.bear_trades).to_excel(
            writer, sheet_name="bear_flow_trades", index=False
        )
        make_excel_safe(result.bear_audit).to_excel(
            writer, sheet_name="bear_audit_df", index=False
        )

    logger.info("wrote report to %s", path)
    return path


__all__ = ["make_excel_safe", "default_report_path", "write_excel_report"]
