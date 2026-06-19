"""Unit tests for domain/pead_book.py — PEAD sizing, slots, daily breaker."""
from autotrader.domain import pead_book as pb


# ── sl_distance ───────────────────────────────────────────────────────────────
def test_sl_distance_atr_dominates():
    # ATR×2.5 = 5.0 > entry×1% = 1.0 -> ATR term wins
    assert abs(pb.sl_distance(2.0, 100.0, 2.5) - 5.0) < 1e-9


def test_sl_distance_floor_dominates():
    # tiny ATR -> entry×1% floor (0.01*500=5.0) protects against absurd size
    assert abs(pb.sl_distance(0.1, 500.0, 2.5) - 5.0) < 1e-9


# ── position_size ─────────────────────────────────────────────────────────────
def test_position_size_risk_bound():
    # risk 3000 / sl 5 = 600 shares; notional 40000/100 = 400 -> notional binds
    assert pb.position_size(entry=100.0, sl_dist=5.0, risk=3000.0, notional_cap=40000.0) == 400


def test_position_size_risk_binds_when_cheap_stop():
    # risk 3000 / sl 2 = 1500; notional 40000/10 = 4000 -> risk binds
    assert pb.position_size(entry=10.0, sl_dist=2.0, risk=3000.0, notional_cap=40000.0) == 1500


def test_position_size_zero_when_nothing_fits():
    assert pb.position_size(entry=100000.0, sl_dist=5.0, risk=3000.0, notional_cap=40000.0) == 0
    assert pb.position_size(entry=0.0, sl_dist=5.0, risk=3000.0, notional_cap=40000.0) == 0
    assert pb.position_size(entry=100.0, sl_dist=0.0, risk=3000.0, notional_cap=40000.0) == 0


# ── daily_breaker_tripped ─────────────────────────────────────────────────────
def test_breaker_loss_trips():
    assert pb.daily_breaker_tripped(-6001.0, loss_limit=-6000.0, profit_limit=12000.0) is True


def test_breaker_profit_trips():
    assert pb.daily_breaker_tripped(12000.0, loss_limit=-6000.0, profit_limit=12000.0) is True


def test_breaker_within_band_ok():
    assert pb.daily_breaker_tripped(500.0, loss_limit=-6000.0, profit_limit=12000.0) is False
    assert pb.daily_breaker_tripped(-5999.0, loss_limit=-6000.0, profit_limit=12000.0) is False


def test_breaker_fail_closed_on_none():
    assert pb.daily_breaker_tripped(None, loss_limit=-6000.0, profit_limit=12000.0) is True


# ── select_for_slots ──────────────────────────────────────────────────────────
def _c(sym, sur):
    return {"symbol": sym, "surprise": sur}


def test_select_takes_best_surprise_first():
    cands = [_c("A", 0.06), _c("B", 0.12), _c("C", 0.08)]
    picked = pb.select_for_slots(cands, open_count=0, max_slots=2)
    assert [c["symbol"] for c in picked] == ["B", "C"]   # top-2 by surprise


def test_select_respects_open_slots():
    cands = [_c("A", 0.06), _c("B", 0.12), _c("C", 0.08)]
    picked = pb.select_for_slots(cands, open_count=4, max_slots=5)  # only 1 free
    assert [c["symbol"] for c in picked] == ["B"]


def test_select_full_book_takes_none():
    cands = [_c("A", 0.06), _c("B", 0.12)]
    assert pb.select_for_slots(cands, open_count=5, max_slots=5) == []


def test_select_empty_candidates():
    assert pb.select_for_slots([], open_count=0, max_slots=5) == []
