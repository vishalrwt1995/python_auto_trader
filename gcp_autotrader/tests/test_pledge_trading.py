"""Unit tests for the PURE decision core of pledge_trading_service.plan_pledge_entries.

The live I/O wrapper (run_pledge_scan_once) + the double macro-gate live reads are validated in
PAPER, not here — these tests pin the entry logic: daily breaker, 10-slot cap, held exclusion,
LIQUIDITY ranking (by turnover, not magnitude — magnitude was killed as OOS-invalid), risk/notional
(cap10%) sizing, the 2%-participation cap, and the nested-brain b200 read.
"""
from types import SimpleNamespace

from autotrader.services.pledge_trading_service import plan_pledge_entries, _candidate_status, _read_b200

# cfg mirroring StrategySettings PLEDGE fields at Rs2L (risk 0 -> 1.5% fallback = 3000;
# notional cap 0.10*2L = 20000 = capital/10 slots; stop 2.0xATR)
CFG = SimpleNamespace(
    daily_loss_pct=0.03, daily_profit_pct=0.06,
    pledge_risk_per_trade=0.0, pledge_notional_cap_pct=0.10,
    pledge_max_positions=10, pledge_atr_sl_mult=2.0,
)
CAP = 2e5


def _cand(sym, turnover_cr=35.0, n_revokes=1, atr=5.0, close=100.0):
    return {"symbol": sym, "n_revokes": n_revokes, "atr": atr, "reaction_close": close,
            "turnover_cr": turnover_cr, "reaction_date": "2026-07-21", "instrument_key": f"NSE_EQ|{sym}"}


def test_plan_sizes_and_ranks_by_turnover():
    cands = [_cand("A", 30.0), _cand("B", 90.0), _cand("C", 50.0)]
    specs = plan_pledge_entries(cands, [], realized_today=0.0, channel_capital=CAP, cfg=CFG)
    assert [s["symbol"] for s in specs] == ["B", "C", "A"]          # liquidity desc
    # sl_dist = max(5*2.0, 100*0.01)=10; risk 3000/10=300; notional cap 20000/100=200 -> 200
    assert specs[0]["qty"] == 200
    assert specs[0]["sl_price"] == 90.0 and specs[0]["target"] == 300.0   # 100-10 ; 100+10*20
    assert specs[0]["strategy"] == "PLEDGE"


def test_plan_participation_cap_binds():
    # tiny turnover 0.05cr -> part_cap = int(0.02*0.05e7/100)=100 < notional-size 200 -> qty=100
    specs = plan_pledge_entries([_cand("A", turnover_cr=0.05)], [], realized_today=0.0,
                                channel_capital=CAP, cfg=CFG)
    assert specs[0]["qty"] == 100


def test_plan_participation_cap_to_zero_skips():
    specs = plan_pledge_entries([_cand("A", turnover_cr=0.0001)], [], realized_today=0.0,
                                channel_capital=CAP, cfg=CFG)
    assert specs == []


def test_plan_daily_loss_breaker_blocks_all():
    specs = plan_pledge_entries([_cand("A")], [], realized_today=-6001.0,
                                channel_capital=CAP, cfg=CFG)
    assert specs == []                                             # <= -3% of 2L


def test_plan_daily_profit_breaker_blocks_all():
    specs = plan_pledge_entries([_cand("A")], [], realized_today=12000.0,
                                channel_capital=CAP, cfg=CFG)
    assert specs == []                                             # >= +6% of 2L


def test_plan_nan_realized_fails_closed():
    specs = plan_pledge_entries([_cand("A")], [], realized_today=float("nan"),
                                channel_capital=CAP, cfg=CFG)
    assert specs == []                                             # unreadable ledger -> no entries


def test_plan_excludes_held_symbols():
    cands = [_cand("A", 30.0), _cand("B", 90.0)]
    specs = plan_pledge_entries(cands, ["B"], realized_today=0.0, channel_capital=CAP, cfg=CFG)
    assert [s["symbol"] for s in specs] == ["A"]


def test_plan_respects_remaining_slots():
    cands = [_cand("A", 30.0), _cand("B", 90.0), _cand("C", 50.0)]
    held = [f"H{i}" for i in range(9)]                             # 9 open -> 1 free
    specs = plan_pledge_entries(cands, held, realized_today=0.0, channel_capital=CAP, cfg=CFG)
    assert [s["symbol"] for s in specs] == ["B"]                   # most liquid


def test_plan_full_book_no_entries():
    specs = plan_pledge_entries([_cand("A")], [f"H{i}" for i in range(10)],
                                realized_today=0.0, channel_capital=CAP, cfg=CFG)
    assert specs == []


def test_plan_zero_capital_no_entries():
    assert plan_pledge_entries([_cand("A")], [], realized_today=0.0,
                               channel_capital=0.0, cfg=CFG) == []


def test_plan_explicit_risk_overrides_fallback():
    # explicit small risk 500 -> 500/10 = 50 shares (below notional cap 200) -> 50
    cfg = SimpleNamespace(**{**CFG.__dict__, "pledge_risk_per_trade": 500.0})
    specs = plan_pledge_entries([_cand("A")], [], realized_today=0.0, channel_capital=CAP, cfg=cfg)
    assert specs[0]["qty"] == 50


# ── _candidate_status (watchlist annotation) ───────────────────────────────────
def test_status_macro_off_dominates():
    assert _candidate_status("A", {"A"}, {"A"}, set(), False, macro_ok=False) == "MACRO_GATE_OFF"


def test_status_entered_held_breaker_notselected():
    assert _candidate_status("A", {"A"}, {"A"}, set(), False, macro_ok=True) == "ENTERED"
    assert _candidate_status("C", set(), set(), {"C"}, False, macro_ok=True) == "ALREADY_HELD"
    assert _candidate_status("D", set(), set(), set(), True, macro_ok=True) == "BREAKER_HALT"
    assert _candidate_status("E", set(), set(), set(), False, macro_ok=True) == "NOT_SELECTED"


# ── _read_b200 (breadth read from the nested brain Firestore doc) ───────────────
def test_read_b200_nested_and_fallback():
    class _State:
        def __init__(self, doc): self._doc = doc
        def get_market_brain(self): return self._doc

    assert _read_b200(_State({"context": {"breadthSnapshot": {"aboveEma200Pct": 63.03}}})) == 63.03
    assert _read_b200(_State({"breadth_ema200_pct": 55.0})) == 55.0
    assert _read_b200(_State({"context": {}})) is None             # fail-closed
    assert _read_b200(_State({})) is None
