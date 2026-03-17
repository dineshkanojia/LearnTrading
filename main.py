# main.py

import pandas as pd
from datetime import datetime
from binance.client import Client

from engine.utils import get_binance_client, fetch_klines
from engine.pivots import detect_pivots
from engine.bullish_ob import detect_bullish_ob
from engine.structural_exits import (
    generate_bullish_structural_exits,
    generate_bearish_structural_exits,
)
from engine.bearish_flow_engine import generate_bearish_flow_trades
from engine.bullish_flow_engine import generate_bullish_flow_trades
# from engine.bearish_ob import BearishFlowEngine
#from engine.bearish_flow_engine import BearishFlowEngine

from engine.swings import build_structure_swings, apply_anchor_quality_filter
import pandas as pd
from engine.bearish_ob_detector import detect_bearish_ob
from pandas import DatetimeTZDtype


from engine.debug_tools import (
    debug_raw_window_by_idx,
    debug_raw_window_by_time,
    debug_bullish_reversal_row,
    debug_structure_rows,
    debug_exact_time_match,
    debug_bearish_exit_source_by_time,
    debug_bullish_retests_near_exit_by_time
)

#from engine.swings import build_structure_swings, apply_anchor_quality_filter


# ---------------------------------------------------------
# CONFIG
# ---------------------------------------------------------
API_KEY = "qgIgkUqGiPcGP7rbjmqcrEsnDsZH7TwFuxt0DW9yG1xou75Ksu1E1FwRWMXf7X7Y"
API_SECRET = "Jz9Ep8iD5vGNHp3E9flLLnLV2N4eTJeqBTZSNgzpiqiN55WuSb8Zv39hJ0ttb8c7"

SYMBOL = "BTCUSDT"
INTERVAL = Client.KLINE_INTERVAL_15MINUTE
LOOKBACK_MONTHS = 3

PIVOT_LEFT = 3
PIVOT_RIGHT = 3

OUTPUT_FOLDER = "data"   # Save Excel files here

# ---------------------------------------------------------
# MAIN PIPELINE
# ---------------------------------------------------------
def main():

    # 1. Connect to Binance
    client = get_binance_client(API_KEY, API_SECRET)

    # 2. Fetch OHLCV
    #df = fetch_klines(client, SYMBOL, INTERVAL, LOOKBACK_MONTHS)
    df = fetch_klines(client, SYMBOL, INTERVAL, fixed_start="15 Dec 2025 00:00:00")

    # 3. Detect pivots
    df = detect_pivots(df, left=PIVOT_LEFT, right=PIVOT_RIGHT)

    # 4. Build structure swings (HH, HL, LH, LL)
    sw_df = build_structure_swings(df)
    sw_df = apply_anchor_quality_filter(sw_df, df)

    # 5. Detect bullish OBs
    bull_rev_df, bull_mit_df = detect_bullish_ob(df, sw_df)

    # 6. Detect bearish OBs
    bear_rev_df, bear_mit_df = detect_bearish_ob(df, sw_df)

    # 7. Generate structural exit trades
    bear_flow_trades_df = generate_bearish_flow_trades(df, sw_df, bear_rev_df, bull_rev_df)
    bull_flow_trades_df = generate_bullish_flow_trades(df, sw_df, bull_rev_df, bear_rev_df)

    # debug_exact_time_match(df, "2026-02-22 11:00:00")
    # debug_exact_time_match(df, "2026-02-22 11:15:00")
    # debug_exact_time_match(df, "2026-02-22 12:00:00")
    # debug_raw_window_by_idx(df, 6502, 6508)
    # debug_raw_window_by_idx(df, 6502, 6508)
    # debug_structure_rows(sw_df, [6461, 6485, 6504, 6508])
    # debug_raw_window_by_time(df, "2026-02-22 10:30:00", "2026-02-22 12:30:00")
    #debug_bearish_exit_source(bull_rev_df, bear_flow_trades_df, bear_trade_row_idx=33)
    # debug_bearish_exit_source_by_time(
    #     bull_rev_df,
    #     bear_flow_trades_df,
    #     exit_time="2026-02-22 13:30:00",
    # )

    # debug_bullish_retests_near_exit_by_time(
    #     bull_rev_df,
    #     exit_time="2026-02-22 17:30:00",
    #     hours_before=24,
    #     hours_after=6,
    # )

    # 8. Build filename BTC15_<dd_MM_yyyy>.xlsx
    from datetime import datetime, UTC
    today = datetime.now(UTC).strftime("%d_%m_%Y")

    filename = f"{OUTPUT_FOLDER}/BTC15_{today}.xlsx"


    # 9. Export all sheets
    with pd.ExcelWriter(filename, engine="openpyxl") as writer:
        make_excel_safe(sw_df).to_excel(writer, sheet_name="Structure_Swings", index=False)
        make_excel_safe(bull_rev_df).to_excel(writer, sheet_name="Bullish_OB_Reversals", index=False)
        make_excel_safe(bull_mit_df).to_excel(writer, sheet_name="Bullish_OB_Mitigated", index=False)
        make_excel_safe(bear_rev_df).to_excel(writer, sheet_name="Bearish_OB_Reversals", index=False)
        make_excel_safe(bear_mit_df).to_excel(writer, sheet_name="Bearish_OB_Mitigated", index=False)
        #make_excel_safe(bull_trades_df).to_excel(writer, sheet_name="Bullish_Trades", index=False)
        make_excel_safe(bear_flow_trades_df).to_excel(writer, sheet_name="Bearish_Flow_Trades", index=False)
        make_excel_safe(bull_flow_trades_df).to_excel(writer, sheet_name="Bullish_Flow_Trades", index=False)
        # sw_df.to_excel(writer, sheet_name="Structure_Swings", index=False)
        # bull_rev_df.to_excel(writer, sheet_name="Bullish_OB_Reversals", index=False)
        # bull_mit_df.to_excel(writer, sheet_name="Bullish_OB_Mitigated", index=False)
        # bear_rev_df.to_excel(writer, sheet_name="Bearish_OB_Reversals", index=False)
        # bear_mit_df.to_excel(writer, sheet_name="Bearish_OB_Mitigated", index=False)
        # bull_trades_df.to_excel(writer, sheet_name="Bullish_Trades", index=False)
        # # bear_trades_df.to_excel(writer, sheet_name="Bearish_Trades", index=False)
        # bear_flow_trades_df.to_excel(writer, sheet_name="Bearish_Flow_Trades", index=False)


    print(f"\nDataset saved successfully:\n{filename}")
    



def make_excel_safe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in df.columns:
        if isinstance(df[col].dtype, DatetimeTZDtype):
            df[col] = df[col].dt.tz_localize(None)
    return df

if __name__ == "__main__":
    main()