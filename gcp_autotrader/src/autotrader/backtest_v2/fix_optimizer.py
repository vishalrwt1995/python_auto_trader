"""Systematic fix optimizer — test parameter variations + additional fixes.

Uses the saved 1,186 swing trades from swing_alpha_finder (Apr 10 - May 21, 2026
window with production scoring code) to evaluate candidate improvements
WITHOUT re-running the backtest. Each rule is applied as a post-filter on
the trade list and net P&L is computed.

Tests:
  A. Tune existing Fix A (MR direction): different daily_trend conditions
  B. Tune existing Fix B (BREAKOUT exhaustion): RSI + vol_ratio thresholds
  C. NEW: block MR in RANGE
  D. NEW: block losing shorts (mirror SELL with high ADX)
  E. NEW: PULLBACK stage-2 filter
  F. NEW: setup-specific score floors

Reports ranked by net P&L impact.
"""
from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from typing import Callable

DATA_PATH = Path.home() / ".autotrader_backtest_cache" / "swing_alpha_2026-04-10_2026-05-21.json"


def report(name: str, trades: list[dict]) -> dict:
    n = len(trades)
    if n == 0:
        return {"name": name, "n": 0, "wr": 0, "avg_r": 0, "net": 0}
    wins = sum(1 for t in trades if t["net_pnl"] > 0)
    avg_r = sum(t["r_realized"] for t in trades) / n
    net = sum(t["net_pnl"] for t in trades)
    return {"name": name, "n": n, "wr": wins / n * 100, "avg_r": avg_r, "net": net}


def filter_apply(trades: list[dict], filters: list[Callable[[dict], bool]]) -> list[dict]:
    """Keep trades where ALL filters return True."""
    return [t for t in trades if all(f(t) for f in filters)]


def main() -> None:
    with open(DATA_PATH) as f:
        all_trades = json.load(f)

    # Apply production hard_blocks for a fair baseline.
    # CHOP blocks: BREAKOUT, SHORT_BREAKDOWN, PULLBACK, SHORT_PULLBACK, OPEN_DRIVE, PHASE1_MOMENTUM, MOMENTUM
    # RANGE blocks: BREAKOUT, SHORT_BREAKDOWN, OPEN_DRIVE, PHASE1_MOMENTUM, SHORT_PULLBACK
    # TREND_UP blocks: BREAKOUT, MORNING_FADE, SHORT_BREAKDOWN, SHORT_PULLBACK, PHASE1_MOMENTUM
    HARD_BLOCKS = {
        "CHOP": {"BREAKOUT", "SHORT_BREAKDOWN", "PULLBACK", "SHORT_PULLBACK", "OPEN_DRIVE", "PHASE1_MOMENTUM", "MOMENTUM"},
        "RANGE": {"BREAKOUT", "SHORT_BREAKDOWN", "OPEN_DRIVE", "PHASE1_MOMENTUM", "SHORT_PULLBACK"},
        "TREND_UP": {"BREAKOUT", "MORNING_FADE", "SHORT_BREAKDOWN", "SHORT_PULLBACK", "PHASE1_MOMENTUM"},
        "TREND_DOWN": {"BREAKOUT"},
        "PANIC": {"BREAKOUT", "PULLBACK", "OPEN_DRIVE", "PHASE1_MOMENTUM", "MOMENTUM", "MORNING_FADE"},
        "RECOVERY": {"MORNING_FADE"},
    }

    def not_hard_blocked(t: dict) -> bool:
        regime = t.get("regime", "")
        setup = t.get("setup", "")
        if not regime or not setup:
            return True
        return setup not in HARD_BLOCKS.get(regime, set())

    # Apply current fixes (Fix A + Fix B) — what's deployed
    def fix_a_applied(t: dict) -> bool:
        # Block MR SELL when daily_trend=DOWN (current Fix A)
        if t["setup"] in ("MEAN_REVERSION", "VWAP_REVERSAL") and t["direction"] == "SELL" and t["daily_trend"] == "DOWN":
            return False
        return True

    def fix_b_applied(t: dict) -> bool:
        # Block BREAKOUT when RSI>=70 AND vol>=1.8 (current Fix B)
        if t["setup"] == "BREAKOUT" and t["rsi"] >= 70 and t["vol_ratio"] >= 1.8:
            return False
        return True

    # Baselines
    baseline_no_blocks = all_trades  # raw simulator output
    baseline_prod_blocks = [t for t in all_trades if not_hard_blocked(t)]  # what production allows
    baseline_with_fix_ab = [t for t in baseline_prod_blocks if fix_a_applied(t) and fix_b_applied(t)]

    print("=" * 95)
    print("FIX OPTIMIZER — testing variations on the 1,186 alpha-finder trades")
    print("=" * 95)
    print()
    print("Baselines:")
    print(f"  {'baseline_raw (no blocks at all)':50s} N={len(baseline_no_blocks):>4d}  Net=₹{sum(t['net_pnl'] for t in baseline_no_blocks):+,.0f}")
    print(f"  {'baseline_prod_blocks (CURRENT production behavior)':50s} N={len(baseline_prod_blocks):>4d}  Net=₹{sum(t['net_pnl'] for t in baseline_prod_blocks):+,.0f}")
    print(f"  {'baseline + Fix A + Fix B (CURRENTLY DEPLOYED)':50s} N={len(baseline_with_fix_ab):>4d}  Net=₹{sum(t['net_pnl'] for t in baseline_with_fix_ab):+,.0f}")
    print()
    base = sum(t["net_pnl"] for t in baseline_with_fix_ab)

    # ===== A. Tune Fix A (MR direction) =====
    print("=" * 95)
    print("[A] TUNING Fix A — MR direction (currently: block SELL when daily=DOWN)")
    print("=" * 95)
    print(f"\n  {'Variation':60s} {'N':>5s} {'Net':>10s} {'Δbase':>9s}")
    variations_a = [
        ("Fix A as deployed: block MR SELL/daily=DOWN", lambda t: not (t["setup"] in ("MEAN_REVERSION","VWAP_REVERSAL") and t["direction"]=="SELL" and t["daily_trend"]=="DOWN")),
        ("+ also block MR BUY when daily=UP", lambda t: not ((t["setup"] in ("MEAN_REVERSION","VWAP_REVERSAL") and t["direction"]=="SELL" and t["daily_trend"]=="DOWN")
                                                              or (t["setup"] in ("MEAN_REVERSION","VWAP_REVERSAL") and t["direction"]=="BUY" and t["daily_trend"]=="UP"))),
        ("+ also block MR when ADX<15 (weak trend)", lambda t: not (t["setup"] in ("MEAN_REVERSION","VWAP_REVERSAL") and t["adx"] < 15)),
        ("Strict MR: only allow MR BUY in RANGE with RSI<35", lambda t: t["setup"] not in ("MEAN_REVERSION","VWAP_REVERSAL") or (t["direction"]=="BUY" and t["regime"]=="RANGE" and t["rsi"] < 35)),
    ]
    for name, fn in variations_a:
        filtered = filter_apply(baseline_prod_blocks, [fix_b_applied, fn])
        net = sum(t["net_pnl"] for t in filtered)
        print(f"  {name:60s} {len(filtered):>5d} ₹{net:>+8,.0f} ₹{net-base:>+8,.0f}")

    # ===== B. Tune Fix B (BREAKOUT exhaustion) =====
    print("\n" + "=" * 95)
    print("[B] TUNING Fix B — BREAKOUT exhaustion (currently: RSI>=70 AND vol>=1.8)")
    print("=" * 95)
    print(f"\n  {'Variation':60s} {'N':>5s} {'Net':>10s} {'Δbase':>9s}")
    variations_b = [
        ("Fix B as deployed: RSI>=70 AND vol>=1.8", lambda t: not (t["setup"]=="BREAKOUT" and t["rsi"]>=70 and t["vol_ratio"]>=1.8)),
        ("Tighter: RSI>=75 AND vol>=2.0", lambda t: not (t["setup"]=="BREAKOUT" and t["rsi"]>=75 and t["vol_ratio"]>=2.0)),
        ("Looser: RSI>=65 AND vol>=1.5", lambda t: not (t["setup"]=="BREAKOUT" and t["rsi"]>=65 and t["vol_ratio"]>=1.5)),
        ("RSI-only (block if RSI>=75)", lambda t: not (t["setup"]=="BREAKOUT" and t["rsi"]>=75)),
        ("Vol-only (block if vol>=2.5)", lambda t: not (t["setup"]=="BREAKOUT" and t["vol_ratio"]>=2.5)),
        ("Block all BREAKOUT vol>=1.5 (broader)", lambda t: not (t["setup"]=="BREAKOUT" and t["vol_ratio"]>=1.5)),
    ]
    for name, fn in variations_b:
        filtered = filter_apply(baseline_prod_blocks, [fix_a_applied, fn])
        net = sum(t["net_pnl"] for t in filtered)
        print(f"  {name:60s} {len(filtered):>5d} ₹{net:>+8,.0f} ₹{net-base:>+8,.0f}")

    # ===== C. NEW Fix C: Block MR in RANGE =====
    print("\n" + "=" * 95)
    print("[C] NEW Fix C — block MEAN_REVERSION in RANGE (was -₹3,388 across 127 trades)")
    print("=" * 95)
    print(f"\n  {'Variation':60s} {'N':>5s} {'Net':>10s} {'Δbase':>9s}")
    variations_c = [
        ("Block ALL MR in RANGE", lambda t: not (t["setup"]=="MEAN_REVERSION" and t["regime"]=="RANGE")),
        ("Block MR SELL in RANGE (keep BUY)", lambda t: not (t["setup"]=="MEAN_REVERSION" and t["regime"]=="RANGE" and t["direction"]=="SELL")),
        ("Block ALL MR in RANGE + CHOP", lambda t: not (t["setup"]=="MEAN_REVERSION" and t["regime"] in ("RANGE","CHOP"))),
        ("Require MR RSI extreme: BUY<30 / SELL>70", lambda t: t["setup"]!="MEAN_REVERSION" or
                                                                 (t["direction"]=="BUY" and t["rsi"]<30) or
                                                                 (t["direction"]=="SELL" and t["rsi"]>70)),
    ]
    for name, fn in variations_c:
        filtered = filter_apply(baseline_prod_blocks, [fix_a_applied, fix_b_applied, fn])
        net = sum(t["net_pnl"] for t in filtered)
        print(f"  {name:60s} {len(filtered):>5d} ₹{net:>+8,.0f} ₹{net-base:>+8,.0f}")

    # ===== D. NEW Fix D: Block losing shorts =====
    print("\n" + "=" * 95)
    print("[D] NEW Fix D — block losing SHORT setups (mirror SELL -₹3,193 in 6 weeks)")
    print("=" * 95)
    print(f"\n  {'Variation':60s} {'N':>5s} {'Net':>10s} {'Δbase':>9s}")
    variations_d = [
        ("Block all SELL direction trades", lambda t: t["direction"] != "SELL"),
        ("Block SHORT_BREAKDOWN entirely", lambda t: t["setup"] != "SHORT_BREAKDOWN"),
        ("Block SELL when daily_trend != DOWN", lambda t: t["direction"] != "SELL" or t["daily_trend"] == "DOWN"),
        ("Mirror SELL block: SELL + bear_stack + ST=DOWN + ADX>=25",
            lambda t: not (t["direction"]=="SELL" and t["ema_state"]=="BEAR_STACK" and t["supertrend"]=="DOWN" and t["adx"]>=25)),
    ]
    for name, fn in variations_d:
        filtered = filter_apply(baseline_prod_blocks, [fix_a_applied, fix_b_applied, fn])
        net = sum(t["net_pnl"] for t in filtered)
        print(f"  {name:60s} {len(filtered):>5d} ₹{net:>+8,.0f} ₹{net-base:>+8,.0f}")

    # ===== E. NEW Fix E: Setup-specific score floors =====
    print("\n" + "=" * 95)
    print("[E] NEW Fix E — setup-specific score floors (different thresholds per setup)")
    print("=" * 95)
    print(f"\n  {'Variation':60s} {'N':>5s} {'Net':>10s} {'Δbase':>9s}")
    variations_e = [
        ("Score>=45 for all (current Tier 1)", lambda t: t["raw_score"] >= 45),
        ("MOMENTUM>=40, BREAKOUT>=50, MR>=60", lambda t:
            (t["setup"]=="MOMENTUM" and t["raw_score"]>=40) or
            (t["setup"]=="BREAKOUT" and t["raw_score"]>=50) or
            (t["setup"]=="MEAN_REVERSION" and t["raw_score"]>=60) or
            (t["setup"] not in ("MOMENTUM","BREAKOUT","MEAN_REVERSION"))),
        ("MOMENTUM>=35, others>=50", lambda t:
            (t["setup"]=="MOMENTUM" and t["raw_score"]>=35) or
            (t["setup"]!="MOMENTUM" and t["raw_score"]>=50)),
        ("Highest impact: MR>=70 (only extreme MR signals)", lambda t:
            (t["setup"] not in ("MEAN_REVERSION","VWAP_REVERSAL")) or t["raw_score"]>=70),
    ]
    for name, fn in variations_e:
        filtered = filter_apply(baseline_prod_blocks, [fix_a_applied, fix_b_applied, fn])
        net = sum(t["net_pnl"] for t in filtered)
        print(f"  {name:60s} {len(filtered):>5d} ₹{net:>+8,.0f} ₹{net-base:>+8,.0f}")

    # ===== COMBINATIONS — best stacks =====
    print("\n" + "=" * 95)
    print("[F] COMBINATIONS — best multi-fix stacks")
    print("=" * 95)
    print(f"\n  {'Stack':70s} {'N':>5s} {'Net':>10s} {'Δbase':>9s}")
    stacks = [
        ("Current deploy (Fix A + Fix B)", [fix_a_applied, fix_b_applied]),
        ("+ block MR in RANGE", [fix_a_applied, fix_b_applied,
            lambda t: not (t["setup"]=="MEAN_REVERSION" and t["regime"]=="RANGE")]),
        ("+ block MR in RANGE + block SELL trades", [fix_a_applied, fix_b_applied,
            lambda t: not (t["setup"]=="MEAN_REVERSION" and t["regime"]=="RANGE"),
            lambda t: t["direction"] != "SELL"]),
        ("MOMENTUM-only swing trades", [lambda t: t["setup"]=="MOMENTUM"]),
        ("BUY-only + stage-2 quality (BULL_STACK + ST=UP + ADX>=25)",
            [lambda t: t["direction"]=="BUY" and t["ema_state"]=="BULL_STACK" and t["supertrend"]=="UP" and t["adx"]>=25]),
        ("THE OPTIMAL: Fix A + Fix B + block MR/RANGE + BUY-only stage-2",
            [fix_a_applied, fix_b_applied,
             lambda t: not (t["setup"]=="MEAN_REVERSION" and t["regime"]=="RANGE"),
             lambda t: t["direction"]=="BUY" and t["ema_state"]=="BULL_STACK" and t["supertrend"]=="UP" and t["adx"]>=25]),
    ]
    for name, fns in stacks:
        filtered = filter_apply(baseline_prod_blocks, fns)
        net = sum(t["net_pnl"] for t in filtered)
        n = len(filtered)
        wins = sum(1 for t in filtered if t["net_pnl"] > 0)
        wr = wins / n * 100 if n else 0
        print(f"  {name:70s} {n:>5d} ₹{net:>+8,.0f} ₹{net-base:>+8,.0f}  (WR {wr:.1f}%)")

    print("\n" + "=" * 95)
    print("✅ Fix optimizer complete")
    print("=" * 95)


if __name__ == "__main__":
    main()
