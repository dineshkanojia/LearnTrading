# engine/swings.py

import pandas as pd


def build_structure_swings(df: pd.DataFrame) -> pd.DataFrame:
    swings = []
    last_high_price = None
    last_low_price = None

    for i in range(len(df)):

        if df.loc[i, "pivot_high"]:
            price = df.loc[i, "high"]
            label = None
            if last_high_price is not None:
                label = "HH" if price > last_high_price else "LH"
            swings.append((df.loc[i, "time"], "high", price, label, i))
            last_high_price = price

        if df.loc[i, "pivot_low"]:
            price = df.loc[i, "low"]
            label = None
            if last_low_price is not None:
                label = "HL" if price > last_low_price else "LL"
            swings.append((df.loc[i, "time"], "low", price, label, i))
            last_low_price = price

    sw_df = pd.DataFrame(swings, columns=["time", "type", "price", "label", "idx"])
    return sw_df