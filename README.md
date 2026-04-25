# LearnTrading

A research backtester for a discretionary-style **SMC / ICT** trading strategy on
**BTCUSDT 15m** Binance futures. The README name **SSV** stands for
**Structure, Space, Volume** — Structure (HH/HL/LH/LL labels) and Space
(supply/demand Order Blocks) are implemented today; Volume is a planned addition.

> Status: research / personal project. Not financial advice. Not yet wired
> for live or paper trading.

---

## What it does

The pipeline, end to end:

1. **Fetch** 15m BTCUSDT futures klines from Binance (`engine/utils.py`).
2. **Detect pivots** — local pivot highs / lows (`engine/pivots.py`).
3. **Build structure swings** — label each swing as HH / HL / LH / LL with
   leg-size and retracement context (`engine/swings.py`).
4. **Build Order Blocks**
   - bullish OBs from `LL -> IHH -> close-break above IHH` (`engine/bullish_ob.py`)
   - bearish OBs from `HH -> IHL -> close-break below IHL`, tracking
     `confirm_idx` and `mitigation_idx` lifecycle (`engine/bearish_ob.py`).
5. **Multi-timeframe context** — resample 15m -> 1H / 4H / 1D and rerun
   structure + OB detection on each (`engine/mtf_pipeline.py`).
6. **Generate flow trades** — walk the candle stream and emit shorts on the
   first touch of an active bear OB before mitigation; exit on a confirmed
   bullish OB (`engine/bearish_flow_engine.py`). The bullish counterpart in
   `engine/bullish_flow_engine.py` exists but is not yet wired into `main.py`.
7. **Export** a multi-sheet Excel workbook to
   `data/BTC15_<DD_MM_YYYY>.xlsx`.

---

## Setup

Requires Python 3.12+.

```bash
# 1. Clone
git clone https://github.com/dineshkanojia/LearnTrading.git
cd LearnTrading

# 2. Virtual environment
python -m venv .venv
# Windows
.venv\Scripts\activate
# macOS / Linux
source .venv/bin/activate

# 3. Dependencies
pip install -r requirements.txt

# 4. Credentials
cp .env.example .env       # Windows: copy .env.example .env
# then edit .env and paste your Binance API key/secret
```

`.env` is git-ignored. Never commit real credentials.

---

## Run

```bash
python main.py
```

Output: `data/BTC15_<today>.xlsx` containing sheets for OHLCV+pivots, swings,
bullish OBs, bearish OBs, the 1H bear/bull OBs, the bear flow trades, and the
bear audit log.

---

## Configuration

Strategy parameters are set inline in `main.py` (in `build_mtf_context(...)`)
and per-call kwargs to the OB builders. Knobs worth knowing:

| Parameter | Where | Meaning |
|---|---|---|
| `left_strength` / `right_strength` | `build_structure_swings` | Pivot lookback/lookahead per timeframe. |
| `min_swing_pct` | `build_structure_swings` | Min same-side move % to count as a new swing. |
| `min_bars_between_same_side` | `build_structure_swings` | Spacing filter to suppress noise. |
| `max_scan_bars` | OB builders | How far forward/backward to scan for IHH/IHL. |
| `pivot_span` | bullish OB builder | Local-pivot window for the IHH search. |

These will be consolidated into a single `StrategyConfig` dataclass in Phase 1.

---

## Repo layout

```
LearnTrading/
├── main.py                       # entry point: orchestration + Excel export
├── requirements.txt
├── .env.example                  # copy to .env and fill in
├── data/                         # Excel outputs (git-ignored)
└── engine/
    ├── pivots.py                 # local pivot high/low detector
    ├── swings.py                 # HH/HL/LH/LL structure labeller
    ├── bullish_ob.py             # bullish OB builder (LL -> IHH -> break)
    ├── bearish_ob.py             # bearish OB builder (HH -> IHL -> break)
    ├── bearish_ob_detector.py    # legacy bearish detector (to be removed)
    ├── bullish_flow_engine.py    # long-side trade generator (not yet wired)
    ├── bearish_flow_engine.py    # short-side trade generator (active)
    ├── flow_exit_rules.py        # exit-rule helpers (some TODO stubs)
    ├── structural_exits.py       # structural-break exit generator
    ├── mtf_pipeline.py           # 15m -> 1H / 4H / 1D resampling + dispatch
    ├── debug_tools.py            # ad-hoc inspection helpers
    └── utils.py                  # Binance client + klines fetcher + dataclasses
```

---

## Roadmap

**Phase 0 — engineering hygiene (this commit).** Secrets out of source,
`.gitignore`, `requirements.txt`, expanded README.

**Phase 1 — tame the pipeline.** Drop the legacy detector, remove duplicated
15m work in `main.py`, pull strategy params into a `StrategyConfig`, split
`main.py` into `cli.py` + `pipeline.py` + `report.py`, swap `print` for
`logging`.

**Phase 2 — trustable backtest.** `pytest` golden-file tests on small
fixtures, a `metrics.py` for win rate / expectancy / drawdown / equity curve,
fees + slippage + funding, parquet-cached klines.

**Phase 3 — strategy depth.** Wire in the bullish flow engine, gate 15m
entries on 1H/4H structural alignment via the MTF context, add the **V**
(volume) filter, vectorize the inner OB scans, add a walk-forward harness.

**Phase 4 (later).** Binance testnet paper-trading with risk controls
(position sizing, max concurrent trades, daily loss cap).

---

## Glossary

- **HH / HL / LH / LL** — Higher High, Higher Low, Lower High, Lower Low.
- **IHH / IHL** — Internal Higher High / Internal Higher Low between major swings.
- **OB (Order Block)** — Last candle before a structural break, used as a
  supply/demand zone for retest entries.
- **Mitigation** — Price returning into and closing through the OB zone,
  invalidating it.
- **MSS (Market Structure Shift)** — A close that breaks the most recent
  internal swing in the opposite direction of the prior trend.
