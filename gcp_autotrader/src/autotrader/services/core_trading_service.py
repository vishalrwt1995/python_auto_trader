"""CORE channel trading service — quarterly REBALANCE to the target basket (PAPER). The CORE
is its own channel: own capital (``channel_capital("core")``), long-only buy-and-HOLD, cash
(CNC). Tagged ``channel="core"`` + ``wl_type="core"`` so it's isolated AND treated as a pure
overnight HOLD (no stops, no intraday/EOD square-off — ws_monitor must skip it, see Rule 8).

Unlike the slot-based channels this is a REBALANCING book: each quarter compute the top-30
target basket, then sell holdings that dropped out and buy the new names at equal weight.
Stayers keep their position (low-turnover add/drop rebalance — matches the backtest's cost).
The system's BETA engine: ~10% return with a -35-40% drawdown (sized to tolerance). PAPER
until explicitly enabled."""
from __future__ import annotations

import logging
from typing import Any, Sequence

from autotrader.domain import core_signals as cs
from autotrader.services import core_signal_service

logger = logging.getLogger(__name__)


def plan_core_rebalance(
    target_basket: Sequence[dict[str, Any]],
    current_holdings: Sequence[dict[str, Any]],
    channel_capital: float,
    cfg: Any,
    max_weight_mult: float = cs.MAX_WEIGHT_MULT,
) -> dict[str, list[dict[str, Any]]]:
    """PURE: rebalance current CORE holdings to the target basket. Returns
    ``{"sells": [...], "buys": [...]}``:
      - SELL (full exit) every held name NOT in the target,
      - BUY every target name NOT currently held, equal-weight (capital / topn) PLUS a
        residual-cash sweep that deploys the leftover budget so the channel isn't left
        ~18% in idle cash at small capital (integer-share rounding + names priced above the
        slice). Stayers are left as-is (low-turnover; topping them up would hit the same-day
        BUY idempotency guard and duplicate the position — so the sweep is new-buys-only).

    Sizing (new names only):
      slice = capital/TOPN (equal-weight notional); cap = max_weight_mult*slice (per-name ceiling).
      Pass 1 — base equal-weight integer shares (``floor(slice/price)``), capped by budget.
      Pass 2 — greedily buy 1 more share of the most-underweight name (lowest notional/slice)
               that fits the remaining budget AND stays under ``cap``; repeat until cash is
               ~exhausted. This admits names priced between slice and cap (1 share) and sweeps
               rounding residual into the cheapest names. Names priced > cap are excluded
               (can't be equal-weighted at this capital) and logged.
    Empty plan if capital<=0 or the basket is empty. Side-effect-free + unit-testable."""
    if channel_capital <= 0 or not target_basket:
        return {"sells": [], "buys": []}
    target = {str(c["symbol"]).strip().upper(): c for c in target_basket}
    held = {str(p.get("symbol")).strip().upper(): p for p in current_holdings}
    slice_amt = channel_capital / cs.TOPN
    cap_amt = slice_amt * float(max_weight_mult or 1.0)
    sells = [{"symbol": s, "qty": int(p.get("qty") or 0), "instrument_key": str(p.get("instrument_key", "")),
              "position_tag": str(p.get("position_tag", "")), "reason": "CORE_DROP"}
             for s, p in held.items() if s not in target and int(p.get("qty") or 0) > 0]

    # candidates = target names NOT already held (stayers keep their position)
    cand = []
    for s, c in target.items():
        if s in held:
            continue
        price = float(c.get("ref_price") or 0.0)
        if price > 0:
            cand.append((s, c, price))

    # budget for new buys = full capital minus cost-basis already deployed in stayers
    held_cost = sum(int(p.get("qty") or 0) * float(p.get("entry_price") or 0.0)
                    for s, p in held.items() if s in target)
    budget = max(0.0, channel_capital - held_cost)

    qty = {s: 0 for s, _, _ in cand}
    # Pass 1 — equal-weight base
    for s, c, price in cand:
        base = int(slice_amt // price)
        if base >= 1 and base * price <= budget:
            qty[s] = base
            budget -= base * price
    # Pass 2 — residual-cash sweep toward equal-weight, capped per name
    while True:
        best_s = best_price = None
        best_ratio = None
        for s, c, price in cand:
            if price > budget:                       # can't afford one more share
                continue
            if qty[s] * price + price > cap_amt:     # would breach per-name cap
                continue
            ratio = (qty[s] * price) / slice_amt
            if best_ratio is None or ratio < best_ratio:
                best_s, best_price, best_ratio = s, price, ratio
        if best_s is None:
            break
        qty[best_s] += 1
        budget -= best_price

    buys = []
    for s, c, price in cand:
        if qty[s] < 1:
            logger.info("core_skip_qty_zero sym=%s entry=%.2f cap=%.0f (price>cap or no budget)",
                        s, price, cap_amt)
            continue
        buys.append({"symbol": s, "qty": qty[s], "entry_price": round(price, 2),
                     "instrument_key": str(c.get("instrument_key", "")), "reason": "CORE_ADD"})
    logger.info("core_rebalance target=%d held=%d sells=%d buys=%d budget_left=%.0f",
                len(target), len(held), len(sells), len(buys), budget)
    return {"sells": sells, "buys": buys}


# ── live I/O wrapper (validated in PAPER, not unit-tested — fail-closed) ───────
def run_core_rebalance_once(*, settings, upstox, state, order_service, bq=None,
                            asof: str | None = None) -> dict[str, Any]:
    """Live quarterly CORE rebalance (PAPER). Own channel; fail-closed. Default-off until
    CAPITAL_CORE + CORE_ENABLED are set. Buy-and-HOLD — no stops; ws_monitor must treat
    wl_type="core" as overnight-hold (Rule 8)."""
    from autotrader.time_utils import now_ist
    cfg = settings.strategy
    asof = asof or now_ist().strftime("%Y-%m-%d")
    channel_capital = cfg.channel_capital("core")
    if channel_capital <= 0 or not getattr(cfg, "core_enabled", False):
        return {"skipped": "core_disabled", "asof": asof}

    from autotrader.services.gap_fade_trading_service import fetch_fno_universe
    keymap = fetch_fno_universe()                              # large-cap liquid universe + NSE_EQ keys
    if not keymap:
        return {"skipped": "no_universe", "asof": asof}
    history = core_signal_service.fetch_universe_history(list(keymap), keymap, upstox, asof)
    basket = core_signal_service.build_target_basket(history)
    for c in basket:
        c["instrument_key"] = keymap.get(c["symbol"], "")
    if not basket:
        return {"asof": asof, "universe": len(keymap), "basket": 0, "note": "no_basket"}

    holdings = [p for p in state.list_open_positions() if str(p.get("channel", "")).strip().lower() == "core"]
    plan = plan_core_rebalance(basket, holdings, channel_capital, cfg)

    sold = []; bought = []
    for s in plan["sells"]:
        tag = str(s.get("position_tag") or "")
        if not tag:
            logger.warning("core_sell_no_tag sym=%s — cannot exit without position_tag", s["symbol"])
            continue
        try:
            # CORE sells are full exits of dropped names. place_exit_order keys on the
            # position_tag and (in PAPER) closes + books P&L itself; (LIVE) places the
            # CNC market exit. Run in-market so the paper LTP proxy is a real quote.
            r = order_service.place_exit_order(
                position_tag=tag, instrument_key=s["instrument_key"], exit_reason="CORE_DROP")
            if r and not r.get("error"):
                sold.append(s["symbol"])
            else:
                logger.warning("core_sell_rejected sym=%s tag=%s res=%s", s["symbol"], tag, r)
        except Exception:
            logger.exception("core_sell_failed sym=%s tag=%s", s["symbol"], tag)
    for b in plan["buys"]:
        try:
            entry = float(b["entry_price"])
            # CORE is a stopless buy-and-HOLD, but place_entry_order requires 0<sl<entry<target.
            # Supply a deep catastrophe stop + unreachable target (see core_signals docstring):
            # protective only, ~0 fidelity impact, never interferes with the designed drawdown.
            r = order_service.place_entry_order(
                symbol=b["symbol"], exchange="NSE", segment="CASH", side="BUY", qty=b["qty"],
                entry_price=entry, sl_price=cs.catastrophe_stop(entry),
                target=cs.unreachable_target(entry), atr=0.0, product="CNC",
                score=50, reason="CORE_ADD", instrument_key=b["instrument_key"], strategy="CORE",
                wl_type="core", channel="core")
            if r and not r.get("error") and not r.get("skipped"):
                bought.append(b["symbol"])
            elif r and (r.get("error") or r.get("skipped")):
                logger.warning("core_buy_rejected sym=%s res=%s", b["symbol"], r)
        except Exception:
            logger.exception("core_buy_failed sym=%s", b["symbol"])

    summary = {"asof": asof, "universe": len(keymap), "basket": len(basket),
               "held_before": len(holdings), "sold": len(sold), "bought": len(bought)}
    logger.info("core_rebalance_summary %s", summary)
    return summary
