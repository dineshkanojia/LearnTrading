# engine/flow_exit_rules.py

def is_entry_bar_for_bearish_ob(
    ob,
    time,
    idx,
    high,
    low,
    close,
    swing_type,
    *,
    last_ll_idx,
    last_ll_price,
    mitigation_buffer=0.0,
    max_distance_factor=2.0,
    debug=False
):
    """
    Institutional bearish OB entry logic.
    """

    # ---------------------------------------------------------
    # 1. Must have LL anchor BEFORE entry
    # ---------------------------------------------------------
    if last_ll_idx is None:
        if debug:
            print(f"[ENTRY FAIL] No LL anchor yet. idx={idx}")
        return False

    if idx <= last_ll_idx:
        if debug:
            print(f"[ENTRY FAIL] idx={idx} <= last_ll_idx={last_ll_idx}")
        return False

    # ---------------------------------------------------------
    # 2. Price must retest OB zone
    # ---------------------------------------------------------
    if close > ob.high:
        if debug:
            print(f"[ENTRY FAIL] close={close} > ob.high={ob.high}")
        return False

    if close < (ob.low - mitigation_buffer):
        if debug:
            print(f"[ENTRY FAIL] close={close} < ob.low-buffer={ob.low - mitigation_buffer}")
        return False

    # ---------------------------------------------------------
    # 3. Noise filter
    # ---------------------------------------------------------
    ob_range = ob.high - ob.low
    distance_from_ob = abs(close - ob.low)

    if distance_from_ob > (ob_range * max_distance_factor):
        if debug:
            print(f"[ENTRY FAIL] distance_from_ob={distance_from_ob} too large")
        return False

    # ---------------------------------------------------------
    # 4. Entry must NOT occur on LL bar
    # ---------------------------------------------------------
    if swing_type == "LL":
        if debug:
            print(f"[ENTRY FAIL] swing_type=LL (entry cannot be on LL bar)")
        return False

    # ---------------------------------------------------------
    # 5. Entry must NOT occur on HH bar
    # ---------------------------------------------------------
    if swing_type == "HH":
        if debug:
            print(f"[ENTRY FAIL] swing_type=HH (invalid for bearish entry)")
        return False

    # ---------------------------------------------------------
    # 6. VALID ENTRY
    # ---------------------------------------------------------
    if debug:
        print(f"[ENTRY OK] idx={idx}, close={close}, OB=({ob.low}, {ob.high})")

    return True


def did_break_ihh(df, ll_idx, ihh_idx):
    """
    Returns True if price closed above the IHH high AFTER the LL.
    """
    ihh_high = df.loc[ihh_idx, "high"]

    for i in range(ll_idx + 1, len(df)):
        if df.loc[i, "close"] > ihh_high:
            return True

    return False
