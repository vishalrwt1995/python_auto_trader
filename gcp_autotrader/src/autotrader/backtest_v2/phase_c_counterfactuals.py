"""Phase C — Counterfactual tests.

For each variant, post-process the Phase A trades.json (saved by
phase_a_comprehensive.py) and re-compute aggregate metrics.

Filter-only counterfactuals (exact):
  - C1: Disable PULLBACK strategy (drop all PULLBACK trades)
  - C2: Score threshold ≥ 60 (was 45) — drop low-score trades
  - C3: TREND_UP-only regime (drop other regimes)
  - C4: Disable MOMENTUM (0% live WR)
  - C5: Whitelist [MEAN_REVERSION only] — only known winner

Exit-logic counterfactuals (approximations):
  - C6: Exit at 0.5R if MFE ≥ 0.5R (capture half MFE)
  - C7: MAX_HOLD_5D instead of MAX_HOLD_11D (linear approximation)

Run:
    PYTHONPATH=src python -m autotrader.backtest_v2.phase_c_counterfactuals \
        ~/.autotrader_backtest_cache/phase_a_trades_2026-03-09_2026-05-21.json
"""
from __future__ import annotations

import json
import sys
from collections import defaultdict
from pathlib import Path


def summarize(trades: list[dict], label: str) -> dict:
    if not trades:
        return {"label": label, "n": 0, "wr": 0, "net": 0, "avg_r": 0}
    n = len(trades)
    wins = sum(1 for t in trades if t["net_pnl"] > 0)
    net = sum(t["net_pnl"] for t in trades)
    avg_r = sum(t["r_realized"] for t in trades) / n
    avg_mfe = sum(t["r_mfe"] for t in trades) / n
    return {
        "label": label, "n": n, "wins": wins,
        "wr": round(wins / n * 100, 1),
        "net": round(net, 2),
        "avg_pnl": round(net / n, 2),
        "avg_r": round(avg_r, 3),
        "avg_mfe": round(avg_mfe, 3),
    }


def filter_disable_setup(trades: list[dict], setup_to_drop: str) -> list[dict]:
    return [t for t in trades if t["setup"] != setup_to_drop]


def filter_score_min(trades: list[dict], min_score: int) -> list[dict]:
    return [t for t in trades if t["affinity_score"] >= min_score]


def filter_regimes(trades: list[dict], allowed: list[str]) -> list[dict]:
    return [t for t in trades if t["entry_regime"] in allowed]


def filter_whitelist_setup(trades: list[dict], allowed: list[str]) -> list[dict]:
    return [t for t in trades if t["setup"] in allowed]


def transform_exit_half_target(trades: list[dict]) -> list[dict]:
    """If MFE >= 0.5R and exit wasn't TARGET, assume we'd have locked in 0.5R."""
    out = []
    for t in trades:
        t = dict(t)
        if t.get("r_mfe", 0) >= 0.5 and t.get("exit_reason") != "TARGET":
            # We'd have exited at 0.5R
            sl_dist_per_share = abs(t["entry_price"] - t["sl"])
            new_pnl_per_share = 0.5 * sl_dist_per_share
            if t["direction"] == "SELL":
                new_pnl_per_share = 0.5 * sl_dist_per_share
            qty = t.get("qty", 1)
            gross = new_pnl_per_share * qty
            net = gross - t.get("brokerage", 0)
            t["r_realized"] = 0.5
            t["net_pnl"] = round(net, 2)
            t["gross_pnl"] = round(gross, 2)
            t["exit_reason"] = "HALF_R_CF"
        out.append(t)
    return out


def transform_max_hold_5d(trades: list[dict]) -> list[dict]:
    """Approximate: if holding > 5d and exit was MAX_HOLD or late TARGET/SL,
    assume exit at day 5. Use linear interpolation."""
    out = []
    for t in trades:
        t = dict(t)
        hd = t.get("holding_days", 0)
        if hd > 5:
            # Linear approximation: assume R progresses linearly from 0 → r_realized
            # If MAX_HOLD: position closed early at day 5
            # If TARGET: target was hit AFTER day 5, so under 5D rule we'd exit at day 5 (no target)
            # If SL: SL hit after day 5, so under 5D rule we'd exit at day 5 (no SL hit)
            if t["exit_reason"] == "TARGET":
                # We'd miss the target — exit at day 5 with partial gain
                new_r = t["r_realized"] * (5.0 / hd)
            elif t["exit_reason"] == "SL":
                # SL would not have been hit — exit at day 5 with partial loss
                # Use MAE up to day 5 as estimate
                new_r = max(t.get("r_mae", 0), -0.9)
            else:  # MAX_HOLD
                new_r = t["r_realized"] * (5.0 / hd)
            sl_dist_per_share = abs(t["entry_price"] - t["sl"])
            qty = t.get("qty", 1)
            gross = new_r * sl_dist_per_share * qty
            net = gross - t.get("brokerage", 0)
            t["r_realized"] = round(new_r, 3)
            t["holding_days"] = 5
            t["gross_pnl"] = round(gross, 2)
            t["net_pnl"] = round(net, 2)
            t["exit_reason"] = "MAX_HOLD_5D_CF"
        out.append(t)
    return out


def main() -> int:
    trades_path = sys.argv[1] if len(sys.argv) > 1 else None
    if not trades_path:
        # default: most recent
        d = Path.home() / ".autotrader_backtest_cache"
        files = sorted(d.glob("phase_a_trades_*.json"))
        if not files:
            print("No trades file found. Run phase_a_comprehensive first.")
            return 1
        trades_path = str(files[-1])

    trades = json.loads(Path(trades_path).read_text())
    print(f"Loaded {len(trades)} trades from {trades_path}\n")

    # Baseline
    print("=" * 80)
    print("PHASE C — Counterfactual analysis")
    print("=" * 80)
    base = summarize(trades, "BASELINE")
    print(f"\n## Baseline (current production config)")
    print(f"  N={base['n']}  WR={base['wr']}%  Net=₹{base['net']:+.2f}  AvgR={base['avg_r']:+.3f}")

    variants = [
        ("C1: disable PULLBACK", filter_disable_setup(trades, "PULLBACK")),
        ("C2: score >= 60", filter_score_min(trades, 60)),
        ("C3: TREND_UP only", filter_regimes(trades, ["TREND_UP"])),
        ("C4: disable MOMENTUM", filter_disable_setup(trades, "MOMENTUM")),
        ("C5: MR only", filter_whitelist_setup(trades, ["MEAN_REVERSION"])),
        ("C6: disable PULLBACK + MOMENTUM", [t for t in trades if t["setup"] not in ("PULLBACK", "MOMENTUM")]),
        ("C7: score >= 60 AND no PULLBACK", filter_score_min(filter_disable_setup(trades, "PULLBACK"), 60)),
        ("C8: exit at 0.5R if MFE>=0.5R", transform_exit_half_target(trades)),
        ("C9: MAX_HOLD_5D (approx)", transform_max_hold_5d(trades)),
        ("C10: MR only + score>=55", filter_score_min(filter_whitelist_setup(trades, ["MEAN_REVERSION"]), 55)),
    ]

    print(f"\n## Counterfactuals vs baseline")
    print(f"  {'variant':40s} {'N':>5s} {'WR%':>6s} {'NetP&L':>12s} {'ΔNetP&L':>11s} {'AvgR':>7s}")
    print(f"  {'BASELINE':40s} {base['n']:>5d} {base['wr']:>5.1f}% {base['net']:>+12.2f} {'':>11s} {base['avg_r']:>+7.3f}")
    for label, variant in variants:
        s = summarize(variant, label)
        delta = s["net"] - base["net"]
        marker = "↑" if delta > 0 else "↓"
        print(f"  {label:40s} {s['n']:>5d} {s['wr']:>5.1f}% {s['net']:>+12.2f} {marker} {delta:>+9.2f} {s['avg_r']:>+7.3f}")

    print(f"\n## Best variants (by NetP&L delta)")
    results_sorted = sorted(
        [(label, summarize(v, label), summarize(v, label)["net"] - base["net"]) for label, v in variants],
        key=lambda x: -x[2]
    )
    for label, s, delta in results_sorted[:5]:
        print(f"  {label:40s} Δ=₹{delta:+.2f}  Net=₹{s['net']:+.2f}  WR={s['wr']:.1f}%  N={s['n']}")

    # Per-setup details for variants that show improvement
    print(f"\n## Detailed breakdown for BEST variant")
    best_label, best_summary, best_delta = results_sorted[0]
    print(f"  {best_label}: Δ=₹{best_delta:+.2f}")
    best_variant = next(v for l, v in variants if l == best_label)
    per_setup = defaultdict(list)
    for t in best_variant:
        per_setup[t["setup"]].append(t)
    print(f"  {'setup':18s} {'N':>4s} {'WR%':>6s} {'NetP&L':>10s} {'AvgR':>7s}")
    for setup, ts in sorted(per_setup.items()):
        s = summarize(ts, setup)
        print(f"  {setup:18s} {s['n']:>4d} {s['wr']:>5.1f}% {s['net']:>+10.2f} {s['avg_r']:>+7.3f}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
