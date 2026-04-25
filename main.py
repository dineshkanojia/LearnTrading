# main.py

import os
from datetime import UTC, datetime

import pandas as pd
from binance.client import Client
from dotenv import load_dotenv

from engine.utils import get_binance_client, fetch_klines
from engine.pivots import detect_pivots
from engine.bullish_ob import build_bullish_obs
from engine.bearish_ob_detector import detect_bearish_ob
from engine.bearish_flow_engine import generate_bearish_flow_trades
from engine.bullish_flow_engine import generate_bullish_flow_trades
from engine.swings import build_structure_swings,apply_anchor_quality_filter
from engine.bearish_ob import build_bearish_obs
from engine.mtf_pipeline import build_mtf_context


# ---------------------------------------------------------
# CONFIG
# ---------------------------------------------------------
# Load credentials from a local `.env` file (see .env.example).
# Never commit real keys to source control.
load_dotenv()

API_KEY = os.environ.get("BINANCE_API_KEY")
API_SECRET = os.environ.get("BINANCE_API_SECRET")

if not API_KEY or not API_SECRET:
    raise RuntimeError(
        "Missing Binance credentials. Copy .env.example to .env and fill in "
        "BINANCE_API_KEY / BINANCE_API_SECRET, or export them in your shell."
    )

SYMBOL = os.environ.get("SYMBOL", "BTCUSDT")
INTERVAL = os.environ.get("INTERVAL", Client.KLINE_INTERVAL_15MINUTE)
OUTPUT_FOLDER = os.environ.get("OUTPUT_FOLDER", "data")

PIVOT_LEFT = 3
PIVOT_RIGHT = 3

# Hybrid baseline
HYBRID_MIN_SWING_PCT = 0.0030
HYBRID_MIN_SPACING_BARS = 4
HYBRID_CONFIRM_PCT = 0.0025
HYBRID_MIN_CONFIRMATION_BARS = 2
HYBRID_MIN_OPPOSITE_CANDIDATE_SPACING_BARS = 3


# ---------------------------------------------------------
# HELPERS
# ---------------------------------------------------------
def make_excel_safe(df: pd.DataFrame) -> pd.DataFrame:
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


def ensure_anchor_valid_column(sw_df: pd.DataFrame) -> pd.DataFrame:
    out = sw_df.copy()
    if "anchor_valid" not in out.columns:
        out["anchor_valid"] = True
    return out


def build_summary_df(
    swings: pd.DataFrame,
    bull_rev_df: pd.DataFrame,
    bull_mit_df: pd.DataFrame,
    bear_rev_df: pd.DataFrame,
    bear_mit_df: pd.DataFrame,
    bull_flow_trades_df: pd.DataFrame,
    bear_flow_trades_df: pd.DataFrame,
) -> pd.DataFrame:
    labels = (
        swings["label"].value_counts(dropna=False).to_dict()
        if not swings.empty and "label" in swings.columns
        else {}
    )

    return pd.DataFrame(
        [
            {"metric": "hybrid_swings_len", "value": len(swings)},
            {"metric": "hybrid_HH", "value": labels.get("HH", 0)},
            {"metric": "hybrid_HL", "value": labels.get("HL", 0)},
            {"metric": "hybrid_LH", "value": labels.get("LH", 0)},
            {"metric": "hybrid_LL", "value": labels.get("LL", 0)},
            {"metric": "hybrid_bull_rev_len", "value": len(bull_rev_df)},
            {"metric": "hybrid_bull_mit_len", "value": len(bull_mit_df)},
            {"metric": "hybrid_bear_rev_len", "value": len(bear_rev_df)},
            {"metric": "hybrid_bear_mit_len", "value": len(bear_mit_df)},
            {"metric": "hybrid_bull_flow_trades_len", "value": len(bull_flow_trades_df)},
            {"metric": "hybrid_bear_flow_trades_len", "value": len(bear_flow_trades_df)},
        ]
    )


def debug_bear_tradable_candidates(df: pd.DataFrame, bear_obs_df: pd.DataFrame) -> pd.DataFrame:
    rows = []

    src = bear_obs_df[bear_obs_df["confirm_idx"].notna()].copy()

    for r in src.itertuples(index=False):
        source_idx = int(r.source_idx)
        confirm_idx = int(r.confirm_idx)
        mitigation_idx = None if pd.isna(r.mitigation_idx) else int(r.mitigation_idx)

        first_touch_idx = None
        first_touch_time = None

        scan_end = mitigation_idx if mitigation_idx is not None else len(df)

        for i in range(confirm_idx + 1, scan_end):
            low_i = float(df.loc[i, "low"])
            high_i = float(df.loc[i, "high"])
            level = float(r.ob_low)

            if low_i <= level <= high_i:
                first_touch_idx = i
                first_touch_time = pd.Timestamp(df.loc[i, "time"])
                break

        tradable = first_touch_idx is not None

        rows.append(
            {
                "source_idx": source_idx,
                "source_label": r.source_label,
                "confirm_idx": confirm_idx,
                "confirm_time": r.confirm_time,
                "mitigation_idx": mitigation_idx,
                "mitigation_time": r.mitigation_time,
                "event_sequence": r.event_sequence,
                "ob_low": r.ob_low,
                "ob_high": r.ob_high,
                "first_touch_idx_before_mitigation": first_touch_idx,
                "first_touch_time_before_mitigation": first_touch_time,
                "tradable_before_mitigation": tradable,
            }
        )

    out = pd.DataFrame(rows)
    print("confirmed bear candidates:", len(out))
    if not out.empty:
        print(out["tradable_before_mitigation"].value_counts(dropna=False))
        print(
            out[
                [
                    "source_idx",
                    "source_label",
                    "confirm_idx",
                    "mitigation_idx",
                    "event_sequence",
                    "first_touch_idx_before_mitigation",
                    "tradable_before_mitigation",
                ]
            ].tail(40)
        )
    return out

# ---------------------------------------------------------
# MAIN PIPELINE
# ---------------------------------------------------------
def main() -> None:
    client = get_binance_client(API_KEY, API_SECRET)

    # 1. Fetch OHLCV
    df_15m = fetch_klines(client, SYMBOL, INTERVAL, fixed_start="15 Dec 2025 00:00:00")

    # 2. Build MTF context from 15m source
    mtf = build_mtf_context(
        df_15m=df_15m,
        structure_builder=build_structure_swings,
        bearish_ob_builder=build_bearish_obs,
        bullish_ob_builder=build_bullish_obs,
        structure_kwargs_by_tf={
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
        },
        bearish_kwargs_by_tf={
            "15m": {"max_scan_bars": 80, "max_followup_attempts": 1},
            "1H": {"max_scan_bars": 40, "max_followup_attempts": 1},
            "4H": {"max_scan_bars": 20, "max_followup_attempts": 1},
            "1D": {"max_scan_bars": 12, "max_followup_attempts": 1},
        },
        bullish_kwargs_by_tf={
            "15m": {"max_scan_bars": 80, "pivot_span": 1},
            "1H": {"max_scan_bars": 40, "pivot_span": 1},
            "4H": {"max_scan_bars": 20, "pivot_span": 1},
            "1D": {"max_scan_bars": 12, "pivot_span": 1},
        },
    )


    # active strategy still uses only 15m for now
    df = mtf.tf_map["15m"]
    swing = mtf.structure_map["15m"]
    bear_obs = mtf.ob_map["15m"]["bear"]
    bull_obs_df = mtf.ob_map["15m"]["bull"]


    # 2. Detect pivots
    df = detect_pivots(df, left=PIVOT_LEFT, right=PIVOT_RIGHT)

    # 3. Build hybrid swings only
    swing = build_structure_swings(df)
    swing = apply_anchor_quality_filter(swing,df)

    # 4. Detect OBs from hybrid swings
    bull_obs_df = build_bullish_obs(df, swing, max_scan_bars=80, pivot_span=1)

    # print("All LL rows:", (swing["label"] == "LL").sum())
    # print("Bullish OB rows:", len(bull_obs_df))
    # print(bull_obs_df["status"].value_counts(dropna=False))

    # print(
    #     bull_obs_df[
    #         ["source_time", "source_idx", "internal_time", "confirm_time", "status"]
    #     ].tail(30)
    # )


    bear_obs =build_bearish_obs(df, swing, max_scan_bars=80, max_followup_attempts=1)

    bear_debug_df = debug_bear_tradable_candidates(df, bear_obs)

   

    # if not bear_obs.empty:
    #     print(bear_obs["status"].value_counts(dropna=False))
    #     print(
    #         bear_obs[
    #             ["source_idx", "source_time", "internal_idx", "internal_time", "confirm_idx", "confirm_time", "status"]
    #         ].tail(20)
    #     )
    # else:
    #     print("bear_obs is EMPTY")


    # 5. Generate hybrid-based flow trades
    bear_trades_df, bear_audit_df = generate_bearish_flow_trades(
        df=df,
        sw_df=swing,
        bear_obs_df=bear_obs,
        bull_obs_df=bull_obs_df,
    )

    # print(bear_trades_df.tail(20))
    # print(bear_audit_df.tail(20))

    # print(
    #         bear_audit_df[
    #             ["ob_source_idx", "ob_confirm_idx", "first_touch_idx", "entered", "skip_reason", "engine_state_at_touch"]
    #         ].tail(40)
    #     )
    
    # bear_flow_trades_df = generate_bearish_flow_trades(df, swing, bear_obs, bull_rev_df)
    # bull_flow_trades_df = generate_bullish_flow_trades(df, swing, bull_rev_df, bear_obs)

    # 6. Export
    today = datetime.now(UTC).strftime("%d_%m_%Y")
    filename = f"{OUTPUT_FOLDER}/BTC15_{today}.xlsx"

    print(bear_obs[["source_idx","source_label","confirm_idx","mitigation_idx","event_sequence","candidate_state"]].tail(30))
    print(bear_trades_df.tail(20))
    print(bear_audit_df[["ob_source_idx","ob_source_label","ob_confirm_idx","ob_mitigation_idx","first_touch_idx","entered","skip_reason"]].tail(30))

    with pd.ExcelWriter(filename, engine="openpyxl") as writer:
        make_excel_safe(df).to_excel(writer, sheet_name="ohlcv_pivots", index=False)
        make_excel_safe(swing).to_excel(writer, sheet_name="swing", index=False)
        make_excel_safe(bull_obs_df).to_excel(writer, sheet_name="Bull_OB", index=False)
        make_excel_safe(bear_obs).to_excel(writer, sheet_name="bear_OB", index=False)

        make_excel_safe(mtf.ob_map["1H"]["bear"]).to_excel(writer, sheet_name="bear_OB_1H", index=False)

        make_excel_safe(bear_trades_df).to_excel(writer, sheet_name="bear_flow_trades", index=False)
        make_excel_safe(bear_audit_df).to_excel(writer, sheet_name="bear_audit_df", index=False)


        # make_excel_safe(mtf.tf_map["1H"]).to_excel(writer, sheet_name="ohlcv_1H", index=False)
        # make_excel_safe(mtf.tf_map["4H"]).to_excel(writer, sheet_name="ohlcv_4H", index=False)
        # make_excel_safe(mtf.tf_map["1D"]).to_excel(writer, sheet_name="ohlcv_1D", index=False)

        #make_excel_safe(mtf.structure_map["1H"]).to_excel(writer, sheet_name="swing_1H", index=False)
        # make_excel_safe(mtf.structure_map["4H"]).to_excel(writer, sheet_name="swing_4H", index=False)
        # make_excel_safe(mtf.structure_map["1D"]).to_excel(writer, sheet_name="swing_1D", index=False)

        make_excel_safe(mtf.ob_map["1H"]["bear"]).to_excel(writer, sheet_name="bear_OB_1H", index=False)
        make_excel_safe(mtf.ob_map["1H"]["bull"]).to_excel(writer, sheet_name="bull_OB_1H", index=False)

        # make_excel_safe(mtf.ob_map["4H"]["bear"]).to_excel(writer, sheet_name="bear_OB_4H", index=False)
        # make_excel_safe(mtf.ob_map["4H"]["bull"]).to_excel(writer, sheet_name="bull_OB_4H", index=False)

        # make_excel_safe(mtf.ob_map["1D"]["bear"]).to_excel(writer, sheet_name="bear_OB_1D", index=False)
        # make_excel_safe(mtf.ob_map["1D"]["bull"]).to_excel(writer, sheet_name="bull_OB_1D", index=False)

    print(f"\nDataset saved successfully:\n{filename}")
    print(bear_debug_df["tradable_before_mitigation"].value_counts(dropna=False))


if __name__ == "__main__":
    main()