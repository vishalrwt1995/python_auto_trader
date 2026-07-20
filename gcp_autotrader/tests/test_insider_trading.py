"""Unit tests for the PURE decision core of insider_trading_service.plan_insider_entries.

The live I/O wrapper (run_insider_scan_once) + the double macro-gate live reads are validated in
PAPER, not here — these tests pin the entry logic: daily breaker, 10-slot cap, held exclusion,
cluster-strength ranking, risk/notional sizing (notional cap = 1/slots binds below risk here),
and the 2%-participation cap.
"""
from types import SimpleNamespace

from autotrader.services.insider_trading_service import plan_insider_entries, _candidate_status

# cfg mirroring StrategySettings INSIDER fields at Rs2L (risk 0 -> 1.5% fallback = 3000;
# notional cap 0.10*2L = 20000 = capital/10 slots)
CFG = SimpleNamespace(
    daily_loss_pct=0.03, daily_profit_pct=0.06,
    insider_risk_per_trade=0.0, insider_notional_cap_pct=0.10,
    insider_max_positions=10, insider_atr_sl_mult=2.5,
)
CAP = 2e5


def _cand(sym, n_buyers, total_val=1e7, atr=5.0, close=100.0, turnover_cr=35.0):
    return {"symbol": sym, "n_buyers": n_buyers, "total_val": total_val, "atr": atr,
            "reaction_close": close, "turnover_cr": turnover_cr, "reaction_date": "2026-07-20",
            "instrument_key": f"NSE_EQ|{sym}"}


def test_plan_sizes_and_ranks_by_cluster():
    cands = [_cand("A", 2, 1e7), _cand("B", 4, 5e6), _cand("C", 2, 2e7)]
    specs = plan_insider_entries(cands, [], realized_today=0.0, channel_capital=CAP, cfg=CFG)
    assert [s["symbol"] for s in specs] == ["B", "C", "A"]          # buyers desc, then val desc
    # sl_dist = max(5*2.5, 100*0.01)=12.5; risk 3000/12.5=240; notional cap 20000/100=200 -> 200
    assert specs[0]["qty"] == 200
    assert specs[0]["sl_price"] == 87.5 and specs[0]["target"] == 350.0   # 100-12.5 ; 100+12.5*20
    assert specs[0]["strategy"] == "INSIDER" and specs[0]["n_buyers"] == 4


def test_plan_participation_cap_binds():
    # tiny turnover 0.05cr -> part_cap = int(0.02*0.05e7/100)=100 < notional-size 200 -> qty=100
    specs = plan_insider_entries([_cand("A", 3, turnover_cr=0.05)], [], realized_today=0.0,
                                 channel_capital=CAP, cfg=CFG)
    assert specs[0]["qty"] == 100


def test_plan_participation_cap_to_zero_skips():
    specs = plan_insider_entries([_cand("A", 3, turnover_cr=0.0001)], [], realized_today=0.0,
                                 channel_capital=CAP, cfg=CFG)
    assert specs == []


def test_plan_daily_loss_breaker_blocks_all():
    specs = plan_insider_entries([_cand("A", 3)], [], realized_today=-6001.0,
                                 channel_capital=CAP, cfg=CFG)
    assert specs == []                                             # <= -3% of 2L


def test_plan_daily_profit_breaker_blocks_all():
    specs = plan_insider_entries([_cand("A", 3)], [], realized_today=12000.0,
                                 channel_capital=CAP, cfg=CFG)
    assert specs == []                                             # >= +6% of 2L


def test_plan_nan_realized_fails_closed():
    specs = plan_insider_entries([_cand("A", 3)], [], realized_today=float("nan"),
                                 channel_capital=CAP, cfg=CFG)
    assert specs == []                                             # unreadable ledger -> no entries


def test_plan_excludes_held_symbols():
    cands = [_cand("A", 2), _cand("B", 4)]
    specs = plan_insider_entries(cands, ["B"], realized_today=0.0, channel_capital=CAP, cfg=CFG)
    assert [s["symbol"] for s in specs] == ["A"]


def test_plan_respects_remaining_slots():
    cands = [_cand("A", 2), _cand("B", 4), _cand("C", 3)]
    held = [f"H{i}" for i in range(9)]                             # 9 open -> 1 free
    specs = plan_insider_entries(cands, held, realized_today=0.0, channel_capital=CAP, cfg=CFG)
    assert [s["symbol"] for s in specs] == ["B"]                   # strongest cluster


def test_plan_full_book_no_entries():
    specs = plan_insider_entries([_cand("A", 5)], [f"H{i}" for i in range(10)],
                                 realized_today=0.0, channel_capital=CAP, cfg=CFG)
    assert specs == []


def test_plan_zero_capital_no_entries():
    assert plan_insider_entries([_cand("A", 3)], [], realized_today=0.0,
                                channel_capital=0.0, cfg=CFG) == []


def test_plan_explicit_risk_overrides_fallback():
    # explicit small risk 500 -> 500/12.5 = 40 shares (below notional cap 200) -> 40
    cfg = SimpleNamespace(**{**CFG.__dict__, "insider_risk_per_trade": 500.0})
    specs = plan_insider_entries([_cand("A", 3)], [], realized_today=0.0,
                                 channel_capital=CAP, cfg=cfg)
    assert specs[0]["qty"] == 40


# ── _candidate_status (watchlist annotation) ───────────────────────────────────
def test_status_macro_off_dominates():
    assert _candidate_status("A", {"A"}, {"A"}, set(), False, macro_ok=False) == "MACRO_GATE_OFF"


def test_status_entered():
    assert _candidate_status("A", {"A"}, {"A"}, set(), False, macro_ok=True) == "ENTERED"


def test_status_already_held():
    assert _candidate_status("C", set(), set(), {"C"}, False, macro_ok=True) == "ALREADY_HELD"


def test_status_breaker_halt():
    assert _candidate_status("D", set(), set(), set(), True, macro_ok=True) == "BREAKER_HALT"


def test_status_not_selected():
    assert _candidate_status("E", set(), set(), set(), False, macro_ok=True) == "NOT_SELECTED"
