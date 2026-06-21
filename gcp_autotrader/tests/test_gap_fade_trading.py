"""GF-3 tests for gap_fade_trading_service.plan_gap_fade_entries (pure book/sizing/breaker)."""
from __future__ import annotations

import types
import pytest

from autotrader.services import gap_fade_trading_service as svc


def _cfg(**over):
    base = dict(daily_loss_pct=0.03, daily_profit_pct=0.06, gapfade_max_positions=3,
                gapfade_notional_cap_pct=0.20, gapfade_stop_pct=0.03)
    base.update(over)
    return types.SimpleNamespace(**base)


def _cand(sym, gap=0.06, ref_open=100.0):
    return {"symbol": sym, "ref_open": ref_open, "gap": gap, "instrument_key": f"ik_{sym}"}


def test_happy_short_specs():
    cands = [_cand("AAA", 0.08, 100.0), _cand("BBB", 0.06, 50.0)]
    specs = svc.plan_gap_fade_entries(cands, [], 0, 0.0, 100000.0, _cfg())
    assert len(specs) == 2
    a = specs[0]
    assert a["symbol"] == "AAA" and a["side"] == "SELL" and a["strategy"] == "GAP_FADE"
    assert a["qty"] == int(0.20 * 100000 // 100.0)            # 200
    assert a["sl_price"] == pytest.approx(103.0)              # 3% buy-stop ABOVE entry
    assert specs[1]["qty"] == int(0.20 * 100000 // 50.0)      # 400


def test_breaker_loss_blocks():
    specs = svc.plan_gap_fade_entries([_cand("AAA")], [], 0, -3001.0, 100000.0, _cfg())
    assert specs == []                                        # realized <= -3% of 1L


def test_breaker_profit_blocks():
    specs = svc.plan_gap_fade_entries([_cand("AAA")], [], 0, 6001.0, 100000.0, _cfg())
    assert specs == []                                        # realized >= +6% of 1L


def test_slot_cap_full():
    specs = svc.plan_gap_fade_entries([_cand("AAA")], ["X", "Y", "Z"], 3, 0.0, 100000.0, _cfg())
    assert specs == []                                        # 3/3 slots used


def test_room_limits_count():
    cands = [_cand("AAA", 0.09), _cand("BBB", 0.08), _cand("CCC", 0.07)]
    specs = svc.plan_gap_fade_entries(cands, ["X"], 2, 0.0, 100000.0, _cfg())   # room = 3-2 = 1
    assert [s["symbol"] for s in specs] == ["AAA"]


def test_dedup_held_symbol():
    cands = [_cand("AAA", 0.09), _cand("BBB", 0.07)]
    specs = svc.plan_gap_fade_entries(cands, ["AAA"], 1, 0.0, 100000.0, _cfg())
    assert [s["symbol"] for s in specs] == ["BBB"]


def test_capital_zero():
    assert svc.plan_gap_fade_entries([_cand("AAA")], [], 0, 0.0, 0.0, _cfg()) == []


def test_qty_zero_skipped():
    # entry price > notional (0.20*1L = 20k) -> qty 0 -> skipped
    specs = svc.plan_gap_fade_entries([_cand("PRICEY", 0.07, 25000.0)], [], 0, 0.0, 100000.0, _cfg())
    assert specs == []
