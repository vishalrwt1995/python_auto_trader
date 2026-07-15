"""Unit tests for domain/delivery_signals.py + services/delivery_signal_service.build_candidates.

Covers the delivery entry gate boundaries (deliv-%, 20d-mean turnover band, price floor),
the CRITICAL ETF exclusion, delivery-%-ranked slot selection, and the pure candidate builder
(reaction-bar location + turnover/ATR + gate + ranking).

Validated config (2026-07-14, STOCKS-ONLY): deliv>=75, 25-50cr band, price>=30, hold20, 5 slots.
"""
from autotrader.domain.delivery_signals import (
    passes_delivery_gates,
    is_etf,
    turnover_20d_cr,
    atr14,
    select_for_slots,
    DELIV_MIN,
    TURNOVER_MIN_CR,
    TURNOVER_MAX_CR,
    PRICE_MIN,
    MIN_BARS,
)
from autotrader.services import delivery_signal_service as svc


# ── passes_delivery_gates — boundary conditions ───────────────────────────────
def test_gate_passes_clean():
    # deliv 80 >= 75, turnover 35 in [25,50), price 100 >= 30, not ETF
    assert passes_delivery_gates(80.0, 35.0, 100.0, "TATASTEEL") is True


def test_gate_deliv_boundary():
    assert passes_delivery_gates(74.9, 35.0, 100.0, "X") is False   # just below floor
    assert passes_delivery_gates(75.0, 35.0, 100.0, "X") is True    # exactly the floor


def test_gate_turnover_low_boundary():
    assert passes_delivery_gates(80.0, 24.9, 100.0, "X") is False   # below band
    assert passes_delivery_gates(80.0, 25.0, 100.0, "X") is True    # band low inclusive


def test_gate_turnover_high_boundary():
    assert passes_delivery_gates(80.0, 49.9, 100.0, "X") is True    # inside band
    assert passes_delivery_gates(80.0, 50.0, 100.0, "X") is False   # band high EXCLUSIVE


def test_gate_price_boundary():
    assert passes_delivery_gates(80.0, 35.0, 29.9, "X") is False    # below price floor
    assert passes_delivery_gates(80.0, 35.0, 30.0, "X") is True     # exactly the floor


def test_gate_excludes_etf():
    # even a perfect deliv/turnover/price ETF is rejected (the decisive stock-only filter)
    assert passes_delivery_gates(99.0, 35.0, 100.0, "NIFTYBEES") is False


# ── is_etf ────────────────────────────────────────────────────────────────────
def test_is_etf_bees_suffix():
    assert is_etf("NIFTYBEES") is True
    assert is_etf("GOLDBEES") is True


def test_is_etf_curated():
    assert is_etf("MON100") is True


def test_is_etf_substring():
    assert is_etf("SOMEETF") is True


def test_is_etf_false_for_stock():
    assert is_etf("JSWCEMENT") is False
    assert is_etf("TATASTEEL") is False


# ── turnover_20d_cr ───────────────────────────────────────────────────────────
def test_turnover_20d_excludes_day_i():
    # 20 prior days at close*vol = 100*3e6 = 3e8 -> /1e7 = 30cr; day i itself excluded
    closes = [100.0] * 25
    vols = [3_000_000.0] * 25
    assert abs(turnover_20d_cr(closes, vols, 21) - 30.0) < 1e-9


def test_turnover_20d_short_history():
    assert turnover_20d_cr([100.0] * 10, [1e6] * 10, 5) == 0.0   # i < 20


# ── atr14 ─────────────────────────────────────────────────────────────────────
def test_atr14_constant_tr():
    # high=10, low=8, close=9 every day -> TR=2 -> ATR14=2 once seeded (i>=13)
    bars = [["d", 9.0, 10.0, 8.0, 9.0, 1e6] for _ in range(20)]
    out = atr14(bars)
    assert out[13] is not None and abs(out[13] - 2.0) < 1e-9
    assert out[12] is None                                        # not yet seeded


# ── select_for_slots — ranking + free-slot cap ────────────────────────────────
def _c(sym, deliv):
    return {"symbol": sym, "deliv_pct": deliv}


def test_select_ranks_by_deliv_desc():
    cands = [_c("A", 78.0), _c("B", 92.0), _c("C", 85.0)]
    got = select_for_slots(cands, open_count=0, max_slots=5)
    assert [c["symbol"] for c in got] == ["B", "C", "A"]


def test_select_free_slot_cap():
    cands = [_c("A", 78.0), _c("B", 92.0), _c("C", 85.0)]
    # 4 open, 5 slots -> 1 free -> only best deliv (B)
    got = select_for_slots(cands, open_count=4, max_slots=5)
    assert [c["symbol"] for c in got] == ["B"]


def test_select_full_book_empty():
    assert select_for_slots([_c("A", 99.0)], open_count=5, max_slots=5) == []


# ── build_candidates (pure) ───────────────────────────────────────────────────
def _bars(closes, high_mult=1.01, low_mult=0.99, vol=3_000_000.0):
    """OHLCV bars: high=close*high_mult, low=close*low_mult, sortable dates."""
    return [[f"2026-{1 + i // 28:02d}-{1 + i % 28:02d}", c, c * high_mult, c * low_mult, c, vol]
            for i, c in enumerate(closes)]


def test_build_candidates_emits_gate_passers_ranked():
    # 22 flat sessions at 100; turnover 100*3e6/1e7 = 30cr (in band); price 100 >= 30
    barsA = _bars([100.0] * MIN_BARS)
    barsB = _bars([100.0] * MIN_BARS)
    rd = barsA[-1][0]
    rows = {"A": {"deliv_pct": 80.0}, "B": {"deliv_pct": 92.0}}
    cands = svc.build_candidates(rd, rows, {"A": barsA, "B": barsB})
    assert [c["symbol"] for c in cands] == ["B", "A"]            # deliv-% ranked
    assert cands[0]["channel"] == "delivery"
    assert abs(cands[0]["turnover_cr"] - 30.0) < 1e-6
    assert cands[0]["atr"] > 0 and cands[0]["reaction_close"] == 100.0


def test_build_candidates_rejects_below_deliv():
    bars = _bars([100.0] * MIN_BARS)
    rows = {"A": {"deliv_pct": 60.0}}                            # < 75
    assert svc.build_candidates(bars[-1][0], rows, {"A": bars}) == []


def test_build_candidates_rejects_out_of_band_turnover():
    # vol 1e7 -> turnover 100*1e7/1e7 = 100cr (> 50) -> rejected
    bars = _bars([100.0] * MIN_BARS, vol=10_000_000.0)
    rows = {"A": {"deliv_pct": 90.0}}
    assert svc.build_candidates(bars[-1][0], rows, {"A": bars}) == []


def test_build_candidates_excludes_etf():
    bars = _bars([100.0] * MIN_BARS)
    rows = {"NIFTYBEES": {"deliv_pct": 99.0}}
    assert svc.build_candidates(bars[-1][0], rows, {"NIFTYBEES": bars}) == []


def test_build_candidates_short_history_skipped():
    bars = _bars([100.0] * 10)                                   # < MIN_BARS
    rows = {"A": {"deliv_pct": 90.0}}
    assert svc.build_candidates(bars[-1][0], rows, {"A": bars}) == []
