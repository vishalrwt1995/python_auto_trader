"""Unit tests for the Momentum x Low-Vol rebalance plan (services/momentum_trading_service).
Pure plan logic only (the live run_* wrapper is PAPER-validated, not unit-tested)."""
from __future__ import annotations

from autotrader.services.momentum_trading_service import plan_momentum_rebalance
from autotrader.services.momentum_signal_service import build_target_basket


class _Cfg:
    pass


def _daily_bars(mom_target, n=280, price0=100.0, vol_shares=3_000_000):
    """Synthetic daily bars [date,o,h,l,c,v] that pass every momentum universe gate:
    a smooth geometric ramp (controls 12-1 momentum via ``mom_target``) + a small
    alternating oscillation so realized-vol > 0. price ~Rs100-200, turnover ~Rs5cr+/day."""
    bars = []
    for k in range(n):
        base = price0 * (1.0 + mom_target) ** (k / float(n))
        c = base * (1.005 if k % 2 == 0 else 0.995)              # +/-0.5% -> vol>0
        bars.append([f"2025-{1 + k // 31:02d}-{1 + k % 31:02d}", c, c * 1.001, c * 0.999, c, vol_shares])
    return bars


def _basket(syms, price=100.0):
    return [{"symbol": s, "ref_price": price, "instrument_key": f"K{s}"} for s in syms]


def _held(syms, qty=10, price=100.0):
    return [{"symbol": s, "qty": qty, "entry_price": price, "instrument_key": f"K{s}",
             "position_tag": f"T{s}"} for s in syms]


def test_fresh_buys_equal_weight():
    tb = _basket([f"S{i}" for i in range(20)], price=100.0)
    plan = plan_momentum_rebalance(tb, [], 200000.0, _Cfg())
    assert len(plan["sells"]) == 0
    assert len(plan["buys"]) == 20                     # Rs2L / 20 / Rs100 = 100 shares each
    assert all(b["qty"] >= 1 for b in plan["buys"])
    assert {b["reason"] for b in plan["buys"]} == {"MOM_ADD"}


def test_regime_cash_sells_all_no_buys():
    plan = plan_momentum_rebalance([], _held([f"S{i}" for i in range(20)]), 200000.0, _Cfg())
    assert len(plan["sells"]) == 20 and len(plan["buys"]) == 0
    assert {s["reason"] for s in plan["sells"]} == {"MOM_DROP"}


def test_stayers_kept_drops_sold():
    tb = _basket(["A", "B", "C"] + [f"N{i}" for i in range(17)])
    plan = plan_momentum_rebalance(tb, _held(["A", "B", "X", "Y"]), 200000.0, _Cfg())
    sold = {s["symbol"] for s in plan["sells"]}
    bought = {b["symbol"] for b in plan["buys"]}
    assert sold == {"X", "Y"}                           # dropped names exited
    assert "A" not in bought and "B" not in bought      # stayers not re-bought (idempotency)
    assert "C" in bought                                # new name bought


def test_capital_zero_no_buys_but_still_sells_drops():
    plan = plan_momentum_rebalance(_basket(["A", "B"]), _held(["X"]), 0.0, _Cfg())
    assert plan["buys"] == []
    assert {s["symbol"] for s in plan["sells"]} == {"X"}


def test_per_name_cap_excludes_unaffordable():
    # one name priced above the per-name cap (1.5x slice) can't be equal-weighted -> skipped
    tb = _basket(["CHEAP"], price=100.0) + [{"symbol": "PRICEY", "ref_price": 40000.0, "instrument_key": "KP"}]
    plan = plan_momentum_rebalance(tb, [], 200000.0, _Cfg())   # slice=10k, cap=15k < 40k
    bought = {b["symbol"] for b in plan["buys"]}
    assert "CHEAP" in bought and "PRICEY" not in bought


# ── run_momentum_rebalance_once: regime-first fetch optimization ──────────────
class _RunCfg:
    momentum_enabled = True
    momentum_compound_sizing = True
    nifty50_instrument_key = "NSE_INDEX|Nifty 50"
    def channel_capital(self, ch):
        return 200000.0


class _RunSettings:
    strategy = _RunCfg()


class _RunState:
    def list_open_positions(self):
        return []


class _RunOrder:
    def place_exit_order(self, **k):
        return {}
    def place_entry_order(self, **k):
        return {}


def test_run_once_cash_regime_skips_history_fetch(monkeypatch):
    """Optimization: when Nifty < 100DMA (regime cash), do NOT fetch the ~1,096-name history."""
    from autotrader.services import momentum_trading_service as mts
    from autotrader.services import momentum_signal_service as mss
    calls = {"history": 0}
    monkeypatch.setattr(mss, "fetch_universe", lambda state, **k: {"AAA": "K|AAA"})
    monkeypatch.setattr(mss, "fetch_nifty_regime", lambda *a, **k: False)      # CASH
    def _fetch(*a, **k):
        calls["history"] += 1
        return {}
    monkeypatch.setattr(mss, "fetch_universe_history", _fetch)
    out = mts.run_momentum_rebalance_once(settings=_RunSettings(), upstox=None, state=_RunState(),
                                          order_service=_RunOrder(), asof="2026-07-10")
    assert calls["history"] == 0                       # <-- fetch skipped in cash regime
    assert out["regime_ok"] is False
    assert out["bought"] == 0 and out["sold"] == 0 and out["basket"] == 0


def test_run_once_hold_regime_does_fetch(monkeypatch):
    """When Nifty > 100DMA (regime hold), the universe history IS fetched to build the basket."""
    from autotrader.services import momentum_trading_service as mts
    from autotrader.services import momentum_signal_service as mss
    calls = {"history": 0}
    monkeypatch.setattr(mss, "fetch_universe", lambda state, **k: {"AAA": "K|AAA"})
    monkeypatch.setattr(mss, "fetch_nifty_regime", lambda *a, **k: True)       # HOLD
    def _fetch(*a, **k):
        calls["history"] += 1
        return {}
    monkeypatch.setattr(mss, "fetch_universe_history", _fetch)
    monkeypatch.setattr(mss, "build_target_basket", lambda *a, **k: [])        # empty -> no orders
    out = mts.run_momentum_rebalance_once(settings=_RunSettings(), upstox=None, state=_RunState(),
                                          order_service=_RunOrder(), asof="2026-07-10")
    assert calls["history"] == 1                       # <-- fetch happens in hold regime
    assert out["regime_ok"] is True


def test_run_once_drops_fund_isin_before_history_fetch(monkeypatch):
    """STOCK-ONLY (2026-08-31): a fund whose ticker matches no name/pattern (only its ISIN gives
    it away) must never reach fetch_universe_history. MON100 itself is already caught by name in
    build_target_basket; this is the ISIN fail-safe layer momentum was missing (the other 5
    channels got it 2026-08-28) -- a NOT-yet-curated fund is the case it exists for."""
    from autotrader.services import momentum_trading_service as mts
    from autotrader.services import momentum_signal_service as mss
    seen_keymap = {}
    monkeypatch.setattr(mss, "fetch_universe", lambda state, **k: {
        "GOODSTOCK": "NSE_EQ|INE002A01018", "SOMEFUND": "NSE_EQ|INF740KA1SW3",
    })
    monkeypatch.setattr(mss, "fetch_nifty_regime", lambda *a, **k: True)       # HOLD -> fetch runs
    def _fetch(symbols, keymap, *a, **k):
        seen_keymap.update(keymap)
        return {}
    monkeypatch.setattr(mss, "fetch_universe_history", _fetch)
    monkeypatch.setattr(mss, "build_target_basket", lambda *a, **k: [])
    mts.run_momentum_rebalance_once(settings=_RunSettings(), upstox=None, state=_RunState(),
                                    order_service=_RunOrder(), asof="2026-07-10")
    assert "SOMEFUND" not in seen_keymap                # fund dropped before the fetch
    assert seen_keymap == {"GOODSTOCK": "NSE_EQ|INE002A01018"}   # equity untouched


def test_build_target_basket_excludes_etf_even_when_top_ranked():
    """Stock-only: an ETF is dropped from the basket even with the STRONGEST momentum +
    lowest vol (it would otherwise rank #1). 20 plain stocks fill exactly the top-20; MON100
    (curated ETF) passes every universe gate yet must not appear. Regression for the MON100
    leak — proves the filter, not a gate failure, keeps it out."""
    history = {f"STK{i}": _daily_bars(0.30) for i in range(20)}   # 20 qualifying stocks
    history["MON100"] = _daily_bars(1.20)                         # ETF: steepest ramp -> top momentum
    picks = [p["symbol"] for p in build_target_basket(history, regime_ok=True)]
    assert "MON100" not in picks                                  # excluded despite ranking #1
    assert len(picks) == 20 and set(picks) == {f"STK{i}" for i in range(20)}
