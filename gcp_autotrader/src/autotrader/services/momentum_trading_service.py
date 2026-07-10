"""Momentum x Low-Vol channel trading service — monthly REBALANCE to the target basket (PAPER).
Own channel: own capital (``channel_capital("momentum")``), long-only buy-and-HOLD, cash (CNC),
tagged ``channel="momentum"`` + ``wl_type="momentum"`` so it's isolated AND treated as a pure
overnight HOLD (ws_monitor must skip it; EOD square-off must exempt it — Rule 8).

A REBALANCING book (like CORE, not slot-based): each month build the top-20 target (with the
hysteresis buffer + Nifty-100DMA regime overlay), sell holdings that dropped out, buy the new
names equal-weight. Regime CASH = empty basket = sell all, hold cash. Compounding sizes off the
channel's current NAV (matches the backtest). PAPER until explicitly enabled."""
from __future__ import annotations

import logging
from typing import Any, Sequence

from autotrader.domain import momentum_signals as ms
from autotrader.services import momentum_signal_service as mss

logger = logging.getLogger(__name__)


def plan_momentum_rebalance(
    target_basket: Sequence[dict[str, Any]],
    current_holdings: Sequence[dict[str, Any]],
    channel_capital: float,
    cfg: Any,
    max_weight_mult: float = ms.MAX_WEIGHT_MULT,
    nav_sizing: bool = False,
) -> dict[str, list[dict[str, Any]]]:
    """PURE: rebalance current momentum holdings to the target basket. Returns
    ``{"sells": [...], "buys": [...]}`` — SELL every held name NOT in the target (full exit),
    BUY every target name NOT held, equal-weight (capital/TOPN) + a residual-cash sweep so the
    channel isn't left idle. Stayers are kept as-is (buffer already decided them in the basket;
    topping up would hit the same-day BUY idempotency guard). Empty basket -> sell all (regime
    CASH). Side-effect-free + unit-testable. Mirrors plan_core_rebalance with TOPN=20."""
    held = {str(p.get("symbol")).strip().upper(): p for p in current_holdings}
    target = {str(c["symbol"]).strip().upper(): c for c in target_basket}
    # SELLs: every held name not in target (incl. ALL when basket empty = regime CASH)
    sells = [{"symbol": s, "qty": int(p.get("qty") or 0), "instrument_key": str(p.get("instrument_key", "")),
              "position_tag": str(p.get("position_tag", "")), "reason": "MOM_DROP"}
             for s, p in held.items() if s not in target and int(p.get("qty") or 0) > 0]
    if channel_capital <= 0 or not target_basket:
        return {"sells": sells, "buys": []}

    slice_amt = channel_capital / ms.TOPN
    cap_amt = slice_amt * float(max_weight_mult or 1.0)
    cand = []
    for s, c in target.items():
        if s in held:
            continue
        price = float(c.get("ref_price") or 0.0)
        if price > 0:
            cand.append((s, c, price))

    # budget for new buys = capital minus value already deployed in stayers. nav_sizing
    # (compounding): channel_capital is the current NAV and stayers are valued at CURRENT price.
    if nav_sizing:
        held_cost = sum(int(p.get("qty") or 0) * float(target[s].get("ref_price") or 0.0)
                        for s, p in held.items() if s in target)
    else:
        held_cost = sum(int(p.get("qty") or 0) * float(p.get("entry_price") or 0.0)
                        for s, p in held.items() if s in target)
    budget = max(0.0, channel_capital - held_cost)

    qty = {s: 0 for s, _, _ in cand}
    for s, c, price in cand:                          # Pass 1: equal-weight base
        base = int(slice_amt // price)
        if base >= 1 and base * price <= budget:
            qty[s] = base
            budget -= base * price
    while True:                                       # Pass 2: residual-cash sweep, capped per name
        best_s = best_price = best_ratio = None
        for s, c, price in cand:
            if price > budget or qty[s] * price + price > cap_amt:
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
            logger.info("momentum_skip_qty_zero sym=%s entry=%.2f cap=%.0f", s, price, cap_amt)
            continue
        buys.append({"symbol": s, "qty": qty[s], "entry_price": round(price, 2),
                     "instrument_key": str(c.get("instrument_key", "")), "reason": "MOM_ADD"})
    logger.info("momentum_rebalance target=%d held=%d sells=%d buys=%d budget_left=%.0f",
                len(target), len(held), len(sells), len(buys), budget)
    return {"sells": sells, "buys": buys}


# ── live I/O wrapper (validated in PAPER, not unit-tested — fail-closed) ───────
def run_momentum_rebalance_once(*, settings, upstox, state, order_service, bq=None,
                                asof: str | None = None) -> dict[str, Any]:
    """Live monthly Momentum x Low-Vol rebalance (PAPER). Own channel; fail-closed. Default-off
    until CAPITAL_MOMENTUM + MOMENTUM_ENABLED are set. Buy-and-HOLD — ws_monitor treats
    wl_type="momentum" as overnight-hold; EOD square-off exempts it (Rule 8)."""
    from autotrader.time_utils import now_ist
    cfg = settings.strategy
    asof = asof or now_ist().strftime("%Y-%m-%d")
    channel_capital = cfg.channel_capital("momentum")
    if channel_capital <= 0 or not getattr(cfg, "momentum_enabled", False):
        return {"skipped": "momentum_disabled", "asof": asof}

    keymap = mss.fetch_universe(state)                       # broad NSE universe + NSE_EQ keys
    if not keymap:
        return {"skipped": "no_universe", "asof": asof}

    holdings = [p for p in state.list_open_positions()
                if str(p.get("channel", "")).strip().lower() == "momentum"]
    prev_holds = [str(p.get("symbol") or "").strip().upper() for p in holdings]

    regime_ok = mss.fetch_nifty_regime(upstox, getattr(cfg, "nifty50_instrument_key", "NSE_INDEX|Nifty 50"), asof)
    history = mss.fetch_universe_history(list(keymap), keymap, upstox, asof)
    basket = mss.build_target_basket(history, prev_holds=prev_holds, regime_ok=regime_ok)
    for c in basket:
        c["instrument_key"] = keymap.get(c["symbol"], "")

    # Compounding (mirror CORE, gated by momentum_compound_sizing): size off current NAV so the
    # live channel reinvests gains like the validated backtest. Fixed capital bootstraps run 1.
    sizing_capital, nav_sizing = channel_capital, False
    if bool(getattr(cfg, "momentum_compound_sizing", True)) and holdings:
        def _cur_px(hp: dict[str, Any]) -> float:
            bars = history.get(str(hp.get("symbol") or ""))
            return float(bars[-1][4]) if bars else float(hp.get("entry_price") or 0.0)
        nav = sum(int(p.get("qty") or 0) * _cur_px(p) for p in holdings)
        if nav > 0:
            sizing_capital, nav_sizing = nav, True
            logger.info("momentum_compound_sizing NAV=%.0f fixed_capital=%.0f", nav, channel_capital)

    plan = plan_momentum_rebalance(basket, holdings, sizing_capital, cfg, nav_sizing=nav_sizing)

    sold = []; bought = []
    for s in plan["sells"]:
        tag = str(s.get("position_tag") or "")
        if not tag:
            logger.warning("momentum_sell_no_tag sym=%s", s["symbol"])
            continue
        try:
            r = order_service.place_exit_order(position_tag=tag, instrument_key=s["instrument_key"],
                                               exit_reason="MOM_DROP")
            if r and not r.get("error"):
                sold.append(s["symbol"])
            else:
                logger.warning("momentum_sell_rejected sym=%s tag=%s res=%s", s["symbol"], tag, r)
        except Exception:
            logger.exception("momentum_sell_failed sym=%s tag=%s", s["symbol"], tag)
    for b in plan["buys"]:
        try:
            entry = float(b["entry_price"])
            r = order_service.place_entry_order(
                symbol=b["symbol"], exchange="NSE", segment="CASH", side="BUY", qty=b["qty"],
                entry_price=entry, sl_price=ms.catastrophe_stop(entry),
                target=ms.unreachable_target(entry), atr=0.0, product="CNC",
                score=50, reason="MOM_ADD", instrument_key=b["instrument_key"], strategy="MOM_LOWVOL",
                wl_type="momentum", channel="momentum")
            if r and not r.get("error") and not r.get("skipped"):
                bought.append(b["symbol"])
            elif r and (r.get("error") or r.get("skipped")):
                logger.warning("momentum_buy_rejected sym=%s res=%s", b["symbol"], r)
        except Exception:
            logger.exception("momentum_buy_failed sym=%s", b["symbol"])

    summary = {"asof": asof, "universe": len(keymap), "basket": len(basket), "regime_ok": regime_ok,
               "held_before": len(holdings), "sold": len(sold), "bought": len(bought)}
    logger.info("momentum_rebalance_summary %s", summary)
    return summary
