"""Unit tests for corp_action_trading_service.plan_corp_entries — the pure shared-pool
entry planner (shared 5-cap via channel, corp sub-cap, breaker, held-exclusion, sizing)."""
from types import SimpleNamespace

from autotrader.services import corp_action_trading_service as svc

CFG = SimpleNamespace(daily_loss_pct=0.03, daily_profit_pct=0.06, pead_max_positions=5,
                      corp_max_positions=2, corp_notional_cap_pct=0.20, corp_protective_stop_pct=0.15)
CAP = 2e5


def _cand(sym, dist=2.0, ref=100.0):
    return {"symbol": sym, "ref_close": ref, "event_type": "bonus", "meeting_date": "2026-01-10",
            "dist_low": dist, "instrument_key": f"NSE_EQ|{sym}"}


def test_plan_happy_sizes_and_stops():
    specs = svc.plan_corp_entries([_cand("A"), _cand("B")], [], 0, 0.0, CAP, CFG)
    assert len(specs) == 2
    s = specs[0]
    assert s["qty"] == 400              # 40000 notional // 100
    assert s["sl_price"] == 85.0        # 100 * (1 - 0.15) wide protective stop
    assert s["strategy"] == "CORP_ACTION"


def test_plan_shared_5cap_full_blocks_corp():
    # 5 channel positions (pead+corp) already -> shared cap full -> no corp entry
    assert svc.plan_corp_entries([_cand("A")], ["P1", "P2", "P3", "P4", "P5"], 0, 0.0, CAP, CFG) == []


def test_plan_corp_subcap_blocks():
    assert svc.plan_corp_entries([_cand("A")], ["P1"], 2, 0.0, CAP, CFG) == []   # corp already at 2


def test_plan_breaker_tripped_blocks():
    assert svc.plan_corp_entries([_cand("A")], [], 0, -7000.0, CAP, CFG) == []   # < -6000 loss limit


def test_plan_excludes_held_and_respects_room():
    specs = svc.plan_corp_entries([_cand("A"), _cand("B")], ["A"], 1, 0.0, CAP, CFG)
    assert [s["symbol"] for s in specs] == ["B"]   # A held; corp_room = 2-1 = 1 -> only B


def test_plan_disabled_when_corp_max_zero():
    cfg0 = SimpleNamespace(**{**CFG.__dict__, "corp_max_positions": 0})
    assert svc.plan_corp_entries([_cand("A")], [], 0, 0.0, CAP, cfg0) == []


def test_plan_zero_capital():
    assert svc.plan_corp_entries([_cand("A")], [], 0, 0.0, 0.0, CFG) == []
