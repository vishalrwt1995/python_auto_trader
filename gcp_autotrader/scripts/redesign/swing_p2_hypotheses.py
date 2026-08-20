"""P2 — test the three P1 hypotheses against the SHIPPED config, 11-yr continuous walk.

Loads bars + regime + indicators ONCE and runs every arm in-process: the indicator
build is the expensive stage, so 5 arms here cost far less than 5 CLI invocations.

ARM 0 IS A CONTROL, not a result. It re-runs the shipped config to prove the two knobs
I added to swing_final.py (MOM_B200_FLOOR, MOM_TOV_EXCL_REGIMES) default to exact
no-ops. If arm 0 does not match the known baseline (265 trades / +Rs530,568 / 9.5%/yr /
maxDD -15%), every other arm here is void and the edits are wrong.

Hypotheses (all from the P1 trade-level decomposition; all TIGHTENINGS):
  H1  b200 gate 70 -> 80      -- 80+ bucket averaged +Rs2,126/trade vs +Rs479 for 70-80
  H2  turnover dead-zone also in RANGE -- 2024 RANGE median turnover was 8.7x historical
                              and 8 of 12 stop-outs sat inside the Rs5-40cr band.
                              NOTE: swing_final.py carries an explicit warning that all
                              three TU filters SIGN-FLIP on RANGE. This is a re-validation
                              test of a documented negative, so a positive result here is
                              suspicious until it survives per-year + OOS.
  H3  same-day cap 1 in RANGE -- the 2024 stop-outs came in 4 same-day PAIRS (8 of 12).
                              Uses the harness's existing regime-scoped cap key.
  H4  drop RANGE entirely     -- upper bound on the "RANGE is the problem" thesis.

Judge on Calmar + per-year, NOT on total net. Aggregate is exactly what hid this.
Read-only; no prod module mutated, no state/env written.
"""
from __future__ import annotations

import json
import os
import pickle
import sys

sys.path.insert(0, os.path.dirname(__file__))
import swing_final as H  # noqa: E402

BARS = os.path.expanduser("~/.autotrader_backtest_cache/swing_adj_bars_2015.pkl")
D0, D1 = "2015-01-01", "2026-12-31"

# the shipped config (PR #51), expressed as run() kwargs
SHIPPED = dict(
    setups=("MOMENTUM", "PULLBACK"),
    emit_floor=45,
    compound_pct=2.0,
    liq_cap_pct=1.0,
    mom_month_block={1},
    mom_turnover_exclude=(5.0, 40.0),
    setup_daily_cap={"MOMENTUM": 2},
    pb_month_block={1, 4, 7},
    range_bucket_by_regime=True,
    range_group_cap=2,
    total_slots=7,
    tu_slot_cap=5,
)

ARMS = [
    ("ARM0 CONTROL  shipped (expect 265 / +530,568 / 9.5%/yr / -15%)", {}, {}),
    ("H1  b200 floor 70 -> 80", {"MOM_B200_FLOOR": 80.0}, {}),
    ("H2  turnover dead-zone in TREND_UP+RANGE", {"MOM_TOV_EXCL_REGIMES": ("TREND_UP", "RANGE")}, {}),
    ("H3  same-day cap RANGE:1 (TREND_UP stays 2)", {},
     {"setup_daily_cap": {"MOMENTUM@TREND_UP": 2, "MOMENTUM@RANGE": 1}}),
    ("H4  drop RANGE (MOMENTUM in TREND_UP only)", {}, {"mom_regimes": ("TREND_UP",)}),
]


def main() -> None:
    print("loading bars + regime + market_inputs ...")
    raw = pickle.load(open(BARS, "rb"))
    regime = json.load(open(H.REGIME_JSON))
    market_inputs = json.load(open(H.MARKET_INPUTS_JSON))
    print(f"  {len(raw)} symbols | regime {len(regime)} days | inputs {len(market_inputs)} days")
    print("building indicator series (once) ...")
    symdata = {s: H.Sym(b) for s, b in raw.items() if len(b) >= H.MIN_BARS_SWING}
    print(f"  {len(symdata)} symbols >= {H.MIN_BARS_SWING} bars\n")

    base_floor, base_tov = H.MOM_B200_FLOOR, H.MOM_TOV_EXCL_REGIMES
    results = []
    for label, knobs, kwargs in ARMS:
        # restore, then apply this arm's knobs
        H.MOM_B200_FLOOR, H.MOM_TOV_EXCL_REGIMES = base_floor, base_tov
        for k, v in knobs.items():
            setattr(H, k, v)
        cfg = dict(SHIPPED)
        cfg.update(kwargs)
        print(f"\n{'='*78}\n=== {label}\n{'='*78}")
        if knobs:
            print(f"    knobs: {knobs}")
        r = H.run(symdata, regime, market_inputs, d0=D0, d1=D1, verbose=True, **cfg)
        results.append((label, r))
    H.MOM_B200_FLOOR, H.MOM_TOV_EXCL_REGIMES = base_floor, base_tov

    print(f"\n\n{'='*96}\n=== SUMMARY — judge on Calmar and per-year, not net\n{'='*96}")
    print(f"  {'arm':<46} {'n':>4} {'WR%':>6} {'net':>11} {'%/yr':>7} {'maxDD':>7} {'Calmar':>7}")
    for label, r in results:
        if not r:
            print(f"  {label:<46}  (no result)")
            continue
        print(f"  {label:<46} {r.get('n',0):>4} {r.get('wr',0):>6.1f} "
              f"{r.get('net',0):>11,.0f} {r.get('cagr',0):>7.2f} {r.get('mdd',0):>7.1f} "
              f"{r.get('calmar',0):>7.2f}")

    ctrl = results[0][1] if results and results[0][1] else None
    if ctrl:
        ok = abs(ctrl.get("net", 0) - 530568) < 1500 and ctrl.get("n") == 265
        print(f"\n  CONTROL CHECK: {'PASS — knobs default to no-ops' if ok else '*** FAIL — edits changed default behaviour; all arms VOID ***'}"
              f"  (got n={ctrl.get('n')} net={ctrl.get('net',0):,.0f})")


if __name__ == "__main__":
    main()
