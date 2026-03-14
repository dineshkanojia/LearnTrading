# main.py

import pandas as pd
from datetime import datetime
from binance.client import Client

from engine.utils import get_binance_client, fetch_klines
from engine.pivots import detect_pivots
from engine.swings import build_structure_swings
from engine.bullish_ob import detect_bullish_ob
from engine.bearish_ob import detect_bearish_ob
from engine.structural_exits import (
    generate_bullish_structural_exits,
    generate_bearish_structural_exits,
)

# ---------------------------------------------------------
# CONFIG
# ---------------------------------------------------------
API_KEY = "YOUR_API_KEY"
API_SECRET = "YOUR_API_SECRET"

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
    df = fetch_klines(client, SYMBOL, INTERVAL, LOOKBACK_MONTHS)

    # 3. Detect pivots
    df = detect_pivots(df, left=PIVOT_LEFT, right=PIVOT_RIGHT)

    # 4. Build structure swings (HH, HL, LH, LL)
    sw_df = build_structure_swings(df)

    # 5. Detect bullish OBs
    bull_rev_df, bull_mit_df = detect_bullish_ob(df, sw_df)

    # 6. Detect bearish OBs
    bear_rev_df, bear_mit_df = detect_bearish_ob(df, sw_df)

    # 7. Generate structural exit trades
    bull_trades_df = generate_bullish_structural_exits(df, bull_rev_df)
    bear_trades_df = generate_bearish_structural_exits(df, bear_rev_df)

    # 8. Build filename BTC15_<dd_MM_yyyy>.xlsx
    today = datetime.utcnow().strftime("%d_%m_%Y")
    filename = f"{OUTPUT_FOLDER}/BTC15_{today}.xlsx"

    # 9. Export all sheets
    with pd.ExcelWriter(filename, engine="openpyxl") as writer:
        sw_df.to_excel(writer, sheet_name="Structure_Swings", index=False)
        bull_rev_df.to_excel(writer, sheet_name="Bullish_OB_Reversals", index=False)
        bull_mit_df.to_excel(writer, sheet_name="Bullish_OB_Mitigated", index=False)
        bear_rev_df.to_excel(writer, sheet_name="Bearish_OB_Reversals", index=False)
        bear_mit_df.to_excel(writer, sheet_name="Bearish_OB_Mitigated", index=False)
        bull_trades_df.to_excel(writer, sheet_name="Bullish_Trades", index=False)
        bear_trades_df.to_excel(writer, sheet_name="Bearish_Trades", index=False)

    print(f"\nDataset saved successfully:\n{filename}")


if __name__ == "__main__":
    main()