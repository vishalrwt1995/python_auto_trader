"""Unit tests for the PURE decision core of pead_trading_service.plan_pead_entries.

The live I/O wrapper (run_pead_scan_once) is validated in PAPER, not here — these
tests pin the entry logic: daily breaker, 5-slot cap, held-symbol exclusion,
surprise ranking, and sizing.
"""
from types import SimpleNamespace

from autotrader.services.pead_trading_service import plan_pead_entries

# cfg mirroring StrategySettings PEAD fields at Rs2L (risk 0 -> 1.5% fallback = 3000)
CFG = SimpleNamespace(
    daily_loss_pct=0.03, daily_profit_pct=0.06,
    pead_risk_per_trade=0.0, pead_notional_cap_pct=0.20,
    pead_max_positions=5, pead_atr_sl_mult=2.5,
)
CAP = 2e5


def _cand(sym, surprise, atr=5.0, close=100.0):
    return {"symbol": sym, "surprise": surprise, "atr": atr, "reaction_close": close,
            "reaction_date": "2026-06-18", "instrument_key": f"NSE_EQ|{sym}"}


def test_plan_basic_sizes_and_ranks():
    cands = [_cand("A", 0.06), _cand("B", 0.12), _cand("C", 0.08)]
    specs = plan_pead_entries(cands, [], realized_today=0.0, channel_capital=CAP, cfg=CFG)
    # all 3 fit (5 slots, none held); ranked by surprise desc
    assert [s["symbol"] for s in specs] == ["B", "C", "A"]
    # sizing: sl_dist = max(5*2.5, 100*0.01)=12.5; risk 3000/12.5=240; notional 40000/100=400 -> 240
    assert specs[0]["qty"] == 240
    assert specs[0]["sl_price"] == 87.5 and specs[0]["target"] == 225.0  # 100-12.5 ; 100+12.5*10


def test_plan_daily_loss_breaker_blocks_all():
    cands = [_cand("A", 0.10)]
    specs = plan_pead_entries(cands, [], realized_today=-6001.0, channel_capital=CAP, cfg=CFG)
    assert specs == []   # -6001 <= -3% of 2L (-6000)


def test_plan_daily_profit_breaker_blocks_all():
    cands = [_cand("A", 0.10)]
    specs = plan_pead_entries(cands, [], realized_today=12000.0, channel_capital=CAP, cfg=CFG)
    assert specs == []   # +12000 >= +6% of 2L


def test_plan_excludes_held_symbols():
    cands = [_cand("A", 0.06), _cand("B", 0.12)]
    specs = plan_pead_entries(cands, ["B"], realized_today=0.0, channel_capital=CAP, cfg=CFG)
    assert [s["symbol"] for s in specs] == ["A"]   # B already held


def test_plan_respects_remaining_slots():
    cands = [_cand("A", 0.06), _cand("B", 0.12), _cand("C", 0.08)]
    # 4 already open -> only 1 free slot -> best surprise (B)
    specs = plan_pead_entries(cands, ["X", "Y", "Z", "W"], realized_today=0.0, channel_capital=CAP, cfg=CFG)
    assert [s["symbol"] for s in specs] == ["B"]


def test_plan_full_book_no_entries():
    cands = [_cand("A", 0.20)]
    specs = plan_pead_entries(cands, ["1", "2", "3", "4", "5"], realized_today=0.0, channel_capital=CAP, cfg=CFG)
    assert specs == []


def test_plan_zero_capital_no_entries():
    specs = plan_pead_entries([_cand("A", 0.10)], [], realized_today=0.0, channel_capital=0.0, cfg=CFG)
    assert specs == []


def test_plan_nan_realized_fails_closed():
    specs = plan_pead_entries([_cand("A", 0.10)], [], realized_today=float("nan"), channel_capital=CAP, cfg=CFG)
    assert specs == []   # unreadable ledger -> breaker trips -> no entries


def test_plan_explicit_risk_overrides_fallback():
    cfg = SimpleNamespace(**{**CFG.__dict__, "pead_risk_per_trade": 1500.0})  # half risk
    specs = plan_pead_entries([_cand("A", 0.10)], [], realized_today=0.0, channel_capital=CAP, cfg=cfg)
    # sl_dist 12.5; risk 1500/12.5 = 120 (vs 240 at full risk)
    assert specs[0]["qty"] == 120
