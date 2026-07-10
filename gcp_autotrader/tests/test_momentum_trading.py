"""Unit tests for the Momentum x Low-Vol rebalance plan (services/momentum_trading_service).
Pure plan logic only (the live run_* wrapper is PAPER-validated, not unit-tested)."""
from __future__ import annotations

from autotrader.services.momentum_trading_service import plan_momentum_rebalance


class _Cfg:
    pass


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
