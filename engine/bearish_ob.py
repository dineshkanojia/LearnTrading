from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Optional, List

import pandas as pd


@dataclass
class BearishOBCandidate:
    tf: str
    side: str                  # SHORT
    source_label: str          # HH or LH
    source_idx: int
    source_time: pd.Timestamp
    source_open: float
    source_high: float
    source_low: float
    source_close: float

    parent_hh_idx: Optional[int]
    parent_hh_time: Optional[pd.Timestamp]

    internal_idx: Optional[int]
    internal_time: Optional[pd.Timestamp]
    internal_level: Optional[float]   # IHL low

    confirm_idx: Optional[int]
    confirm_time: Optional[pd.Timestamp]
    confirm_close: Optional[float]

    mitigation_idx: Optional[int]
    mitigation_time: Optional[pd.Timestamp]
    mitigation_close: Optional[float]

    candidate_state: str       # CONFIRMED_UNUSED / MITIGATED / ABANDONED
    status: str
    event_sequence: str        # CONFIRMED_ONLY / MITIGATED_BEFORE_CONFIRM / CONFIRMED_THEN_MITIGATED / NO_EVENT
    attempt_no: int            # 0 for HH, 1..2 for follow-up LH attempts
    coexist_group: int         # parent chain id

    ob_low: float
    ob_high: float


def _require_columns(df: pd.DataFrame, required: list[str]) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"build_bearish_obs: missing required columns: {missing}")


def _is_local_pivot_low(df: pd.DataFrame, idx: int, span: int = 1) -> bool:
    if idx - span < 0 or idx + span >= len(df):
        return False

    this_low = float(df.loc[idx, "low"])
    left_lows = [float(df.loc[idx - j, "low"]) for j in range(1, span + 1)]
    right_lows = [float(df.loc[idx + j, "low"]) for j in range(1, span + 1)]

    return all(this_low <= x for x in left_lows) and all(this_low <= x for x in right_lows)


def _find_recent_ihl_before_source(
    df: pd.DataFrame,
    source_idx: int,
    max_scan_bars: int = 80,
    pivot_span: int = 1,
) -> Optional[int]:
    """
    Raw backtracking from source_idx - 1 to find the nearest local internal low.
    This intentionally ignores labeled major HL/LL swings and follows the
    user's required internal-HL/IHL logic.
    """
    start_idx = max(pivot_span, source_idx - max_scan_bars)

    for j in range(source_idx - 1, start_idx - 1, -1):
        if _is_local_pivot_low(df, j, span=pivot_span):
            return j

    return None


def _scan_candidate_events(
    df: pd.DataFrame,
    source_idx: int,
    ihl_idx: Optional[int],
    end_idx_exclusive: int,
) -> tuple[Optional[int], Optional[int], str, str]:
    """
    Capture both events inside the candidate scan window:
    - first confirm: close below IHL low
    - first mitigation: close above source high

    Returns:
      confirm_idx, mitigation_idx, candidate_state, event_sequence

    candidate_state is descriptive.
    Flow tradability must be decided later from timing:
      tradable if confirm exists and touch happens before mitigation.
    """
    source_high = float(df.loc[source_idx, "high"])
    ihl_low = float(df.loc[ihl_idx, "low"]) if ihl_idx is not None else None

    first_confirm_idx = None
    first_mitigation_idx = None

    for j in range(source_idx + 1, end_idx_exclusive):
        close_j = float(df.loc[j, "close"])

        if first_mitigation_idx is None and close_j > source_high:
            first_mitigation_idx = j

        if ihl_low is not None and first_confirm_idx is None and close_j < ihl_low:
            first_confirm_idx = j

        if first_confirm_idx is not None and first_mitigation_idx is not None:
            break

    if first_confirm_idx is None and first_mitigation_idx is None:
        return None, None, "ABANDONED", "NO_EVENT"

    if first_confirm_idx is not None and first_mitigation_idx is None:
        return first_confirm_idx, None, "CONFIRMED_UNUSED", "CONFIRMED_ONLY"

    if first_confirm_idx is None and first_mitigation_idx is not None:
        return None, first_mitigation_idx, "MITIGATED", "MITIGATED_BEFORE_CONFIRM"

    if first_mitigation_idx < first_confirm_idx:
        return first_confirm_idx, first_mitigation_idx, "MITIGATED", "MITIGATED_BEFORE_CONFIRM"

    return first_confirm_idx, first_mitigation_idx, "MITIGATED", "CONFIRMED_THEN_MITIGATED"


def _build_candidate_record(
    *,
    tf_name: str,
    source_label: str,
    source_idx: int,
    df: pd.DataFrame,
    parent_hh_idx: Optional[int],
    parent_hh_time: Optional[pd.Timestamp],
    internal_idx: Optional[int],
    confirm_idx: Optional[int],
    mitigation_idx: Optional[int],
    candidate_state: str,
    event_sequence: str,
    attempt_no: int,
    coexist_group: int,
) -> dict:
    source_time = pd.Timestamp(df.loc[source_idx, "time"])
    source_open = float(df.loc[source_idx, "open"])
    source_high = float(df.loc[source_idx, "high"])
    source_low = float(df.loc[source_idx, "low"])
    source_close = float(df.loc[source_idx, "close"])

    internal_time = pd.Timestamp(df.loc[internal_idx, "time"]) if internal_idx is not None else None
    internal_level = float(df.loc[internal_idx, "low"]) if internal_idx is not None else None

    confirm_time = pd.Timestamp(df.loc[confirm_idx, "time"]) if confirm_idx is not None else None
    confirm_close = float(df.loc[confirm_idx, "close"]) if confirm_idx is not None else None

    mitigation_time = pd.Timestamp(df.loc[mitigation_idx, "time"]) if mitigation_idx is not None else None
    mitigation_close = float(df.loc[mitigation_idx, "close"]) if mitigation_idx is not None else None

    rec = BearishOBCandidate(
        tf=tf_name,
        side="SHORT",
        source_label=source_label,
        source_idx=int(source_idx),
        source_time=source_time,
        source_open=source_open,
        source_high=source_high,
        source_low=source_low,
        source_close=source_close,
        parent_hh_idx=parent_hh_idx,
        parent_hh_time=parent_hh_time,
        internal_idx=internal_idx,
        internal_time=internal_time,
        internal_level=internal_level,
        confirm_idx=confirm_idx,
        confirm_time=confirm_time,
        confirm_close=confirm_close,
        mitigation_idx=mitigation_idx,
        mitigation_time=mitigation_time,
        mitigation_close=mitigation_close,
        candidate_state=candidate_state,
        status=candidate_state,
        event_sequence=event_sequence,
        attempt_no=attempt_no,
        coexist_group=coexist_group,
        ob_low=source_low,
        ob_high=source_high,
    )
    return asdict(rec)


def build_bearish_obs(
    df: pd.DataFrame,
    sw_df: pd.DataFrame,
    tf_name: str = "15m",
    max_scan_bars: int = 80,
    max_followup_attempts: int = 2,
) -> pd.DataFrame:
    """
    Bearish candidate-chain builder.

    Locked rules:
    - Start from HH.
    - If HH confirms first, continue with HH only.
    - LH is considered only while HH is not yet confirmed.
    - Max LH follow-up attempts = 2.
    - If HH and LH both confirm in the chain, both may coexist.
    - Mitigation is simple: close above source high kills that OB, confirmed or not.
    """

    _require_columns(df, ["time", "open", "high", "low", "close"])
    _require_columns(sw_df, ["idx", "time", "label", "type"])

    highs = sw_df[
        (sw_df["type"] == "high") &
        (sw_df["label"].isin(["HH", "LH"]))
    ].copy().sort_values("idx").reset_index(drop=True)

    rows: List[dict] = []
    coexist_group = 0
    i = 0

    while i < len(highs):
        row = highs.iloc[i]

        if row["label"] != "HH":
            i += 1
            continue

        coexist_group += 1

        hh_idx = int(row["idx"])
        hh_time = pd.Timestamp(row["time"])

        next_scan_positions: list[int] = []
        j = i + 1
        attempts = 0

        while j < len(highs) and attempts < max_followup_attempts:
            next_row = highs.iloc[j]
            next_idx = int(next_row["idx"])

            lows_between = sw_df[
                (sw_df["type"] == "low") &
                (sw_df["idx"] > hh_idx) &
                (sw_df["idx"] < next_idx)
            ]
            if not lows_between.empty:
                break

            next_scan_positions.append(j)
            attempts += 1
            j += 1

        # end_idx_for_hh = int(highs.iloc[next_scan_positions[-1]]["idx"]) if next_scan_positions else len(df) - 1
        # end_idx_for_hh = min(end_idx_for_hh + 1, len(df))
        end_idx_for_hh = len(df)

        hh_ihl_idx = _find_recent_ihl_before_source(
            df=df,
            source_idx=hh_idx,
            max_scan_bars=max_scan_bars,
            pivot_span=1,
        )

        hh_confirm_idx, hh_mitigation_idx, hh_state, hh_event_sequence = _scan_candidate_events(
            df=df,
            source_idx=hh_idx,
            ihl_idx=hh_ihl_idx,
            end_idx_exclusive=end_idx_for_hh,
        )

        hh_confirmed_first = (
            hh_confirm_idx is not None and
            (hh_mitigation_idx is None or hh_confirm_idx < hh_mitigation_idx)
        )

        # If HH confirms first, continue with HH only.
        if hh_confirmed_first:
            rows.append(
                _build_candidate_record(
                    tf_name=tf_name,
                    source_label="HH",
                    source_idx=hh_idx,
                    df=df,
                    parent_hh_idx=hh_idx,
                    parent_hh_time=hh_time,
                    internal_idx=hh_ihl_idx,
                    confirm_idx=hh_confirm_idx,
                    mitigation_idx=hh_mitigation_idx,
                    candidate_state=hh_state,
                    event_sequence=hh_event_sequence,
                    attempt_no=0,
                    coexist_group=coexist_group,
                )
            )
            i += 1
            continue

        # HH did not confirm first: record HH and then allow LH follow-up attempts.
        rows.append(
            _build_candidate_record(
                tf_name=tf_name,
                source_label="HH",
                source_idx=hh_idx,
                df=df,
                parent_hh_idx=hh_idx,
                parent_hh_time=hh_time,
                internal_idx=hh_ihl_idx,
                confirm_idx=hh_confirm_idx,
                mitigation_idx=hh_mitigation_idx,
                candidate_state=hh_state,
                event_sequence=hh_event_sequence,
                attempt_no=0,
                coexist_group=coexist_group,
            )
        )

        for attempt_no, pos in enumerate(next_scan_positions, start=1):
            cand = highs.iloc[pos]
            cand_idx = int(cand["idx"])
            cand_label = str(cand["label"])

            if cand_label != "LH":
                continue

            # if attempt_no < len(next_scan_positions):
            #     next_pos = next_scan_positions[attempt_no]
            #     end_idx = int(highs.iloc[next_pos]["idx"]) + 1
            # else:
            #     end_idx = len(df)
            
            end_idx = len(df)

            ihl_idx = _find_recent_ihl_before_source(
                df=df,
                source_idx=cand_idx,
                max_scan_bars=max_scan_bars,
                pivot_span=1,
            )

            confirm_idx, mitigation_idx, state, event_sequence = _scan_candidate_events(
                df=df,
                source_idx=cand_idx,
                ihl_idx=ihl_idx,
                end_idx_exclusive=end_idx,
            )

            rows.append(
                _build_candidate_record(
                    tf_name=tf_name,
                    source_label="LH",
                    source_idx=cand_idx,
                    df=df,
                    parent_hh_idx=hh_idx,
                    parent_hh_time=hh_time,
                    internal_idx=ihl_idx,
                    confirm_idx=confirm_idx,
                    mitigation_idx=mitigation_idx,
                    candidate_state=state,
                    event_sequence=event_sequence,
                    attempt_no=attempt_no,
                    coexist_group=coexist_group,
                )
            )

        i += 1

    out = pd.DataFrame(rows)

    if out.empty:
        return pd.DataFrame(
            columns=[
                "tf", "side", "source_label", "source_idx", "source_time",
                "source_open", "source_high", "source_low", "source_close",
                "parent_hh_idx", "parent_hh_time",
                "internal_idx", "internal_time", "internal_level",
                "confirm_idx", "confirm_time", "confirm_close",
                "mitigation_idx", "mitigation_time", "mitigation_close",
                "candidate_state", "status", "event_sequence",
                "attempt_no", "coexist_group",
                "ob_low", "ob_high",
            ]
        )

    return out.sort_values(["source_idx", "attempt_no"]).reset_index(drop=True)


def get_confirmed_bearish_obs(
    df: pd.DataFrame,
    sw_df: pd.DataFrame,
    tf_name: str = "15m",
    max_scan_bars: int = 80,
    max_followup_attempts: int = 2,
) -> pd.DataFrame:
    out = build_bearish_obs(
        df=df,
        sw_df=sw_df,
        tf_name=tf_name,
        max_scan_bars=max_scan_bars,
        max_followup_attempts=max_followup_attempts,
    )
    return out[out["confirm_idx"].notna()].reset_index(drop=True)