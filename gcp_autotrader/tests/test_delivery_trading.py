"""Unit tests for the PURE decision core of delivery_trading_service.plan_delivery_entries.

The live I/O wrapper (run_delivery_scan_once) is validated in PAPER, not here — these tests
pin the entry logic: daily breaker, 5-slot cap, held-symbol exclusion, delivery-% ranking,
risk/notional sizing, and the 2%-participation capacity cap.
"""
from types import SimpleNamespace

from autotrader.services.delivery_trading_service import (
    plan_delivery_entries, _candidate_status)

# cfg mirroring StrategySettings DELIVERY fields at Rs2L (risk 0 -> 1.5% fallback = 3000)
CFG = SimpleNamespace(
    daily_loss_pct=0.03, daily_profit_pct=0.06,
    delivery_risk_per_trade=0.0, delivery_notional_cap_pct=0.20,
    delivery_max_positions=5, delivery_atr_sl_mult=2.5,
)
CAP = 2e5


def _cand(sym, deliv, atr=5.0, close=100.0, turnover_cr=35.0):
    return {"symbol": sym, "deliv_pct": deliv, "atr": atr, "reaction_close": close,
            "turnover_cr": turnover_cr, "reaction_date": "2026-07-14",
            "instrument_key": f"NSE_EQ|{sym}"}


def test_plan_basic_sizes_and_ranks():
    cands = [_cand("A", 78.0), _cand("B", 92.0), _cand("C", 85.0)]
    specs = plan_delivery_entries(cands, [], realized_today=0.0, channel_capital=CAP, cfg=CFG)
    # all 3 fit (5 slots, none held); ranked by deliv_pct desc
    assert [s["symbol"] for s in specs] == ["B", "C", "A"]
    # sizing: sl_dist = max(5*2.5, 100*0.01)=12.5; risk 3000/12.5=240; notional 40000/100=400;
    # participation cap: int(0.02*35e7/100)=70000 -> non-binding -> 240
    assert specs[0]["qty"] == 240
    assert specs[0]["sl_price"] == 87.5 and specs[0]["target"] == 225.0   # 100-12.5 ; 100+12.5*10
    assert specs[0]["strategy"] == "DELIVERY"


def test_plan_participation_cap_binds():
    # tiny turnover 0.1cr -> part_cap = int(0.02*0.1e7/100)=200 < risk-size 240 -> qty=200
    specs = plan_delivery_entries([_cand("A", 90.0, turnover_cr=0.1)], [],
                                  realized_today=0.0, channel_capital=CAP, cfg=CFG)
    assert specs[0]["qty"] == 200


def test_plan_participation_cap_to_zero_skips():
    # turnover so small the 2% cap floors qty to 0 -> name skipped entirely
    specs = plan_delivery_entries([_cand("A", 90.0, turnover_cr=0.0001)], [],
                                  realized_today=0.0, channel_capital=CAP, cfg=CFG)
    assert specs == []


def test_plan_daily_loss_breaker_blocks_all():
    specs = plan_delivery_entries([_cand("A", 90.0)], [], realized_today=-6001.0,
                                  channel_capital=CAP, cfg=CFG)
    assert specs == []   # -6001 <= -3% of 2L (-6000)


def test_plan_daily_profit_breaker_blocks_all():
    specs = plan_delivery_entries([_cand("A", 90.0)], [], realized_today=12000.0,
                                  channel_capital=CAP, cfg=CFG)
    assert specs == []   # +12000 >= +6% of 2L


def test_plan_nan_realized_fails_closed():
    specs = plan_delivery_entries([_cand("A", 90.0)], [], realized_today=float("nan"),
                                  channel_capital=CAP, cfg=CFG)
    assert specs == []   # unreadable ledger -> breaker trips -> no entries


def test_plan_excludes_held_symbols():
    cands = [_cand("A", 78.0), _cand("B", 92.0)]
    specs = plan_delivery_entries(cands, ["B"], realized_today=0.0, channel_capital=CAP, cfg=CFG)
    assert [s["symbol"] for s in specs] == ["A"]   # B already held


def test_plan_respects_remaining_slots():
    cands = [_cand("A", 78.0), _cand("B", 92.0), _cand("C", 85.0)]
    # 4 open -> 1 free slot -> best deliv (B)
    specs = plan_delivery_entries(cands, ["X", "Y", "Z", "W"], realized_today=0.0,
                                  channel_capital=CAP, cfg=CFG)
    assert [s["symbol"] for s in specs] == ["B"]


def test_plan_full_book_no_entries():
    specs = plan_delivery_entries([_cand("A", 99.0)], ["1", "2", "3", "4", "5"],
                                  realized_today=0.0, channel_capital=CAP, cfg=CFG)
    assert specs == []


def test_plan_zero_capital_no_entries():
    specs = plan_delivery_entries([_cand("A", 90.0)], [], realized_today=0.0,
                                  channel_capital=0.0, cfg=CFG)
    assert specs == []


def test_plan_explicit_risk_overrides_fallback():
    cfg = SimpleNamespace(**{**CFG.__dict__, "delivery_risk_per_trade": 1500.0})   # half risk
    specs = plan_delivery_entries([_cand("A", 90.0)], [], realized_today=0.0,
                                  channel_capital=CAP, cfg=cfg)
    # sl_dist 12.5; risk 1500/12.5 = 120 (vs 240 at full risk)
    assert specs[0]["qty"] == 120


# ── _candidate_status (watchlist annotation for the dashboard) ─────────────────
def test_candidate_status_entered():
    assert _candidate_status("A", {"A"}, {"A"}, set(), False) == "ENTERED"


def test_candidate_status_already_held():
    assert _candidate_status("C", set(), set(), {"C"}, False) == "ALREADY_HELD"


def test_candidate_status_breaker_halt():
    assert _candidate_status("D", set(), set(), set(), True) == "BREAKER_HALT"


def test_candidate_status_planned_not_filled():
    assert _candidate_status("B", {"B"}, set(), set(), False) == "PLANNED_NOT_FILLED"


def test_candidate_status_not_selected():
    assert _candidate_status("E", set(), set(), set(), False) == "NOT_SELECTED"
