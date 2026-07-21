"""Unit tests for the PURE exit core of pledge_reconciliation_service.evaluate_pledge_position.

Pledge is a FIXED-hold edge (NO trail): only a max-hold exit (60d) + a daily-close SL-breach
backstop. The I/O service is validated in PAPER.
"""
from autotrader.services.pledge_reconciliation_service import evaluate_pledge_position

ENTRY = 100.0


def _mk(closes, highs=None):
    highs = highs or [c * 1.02 for c in closes]
    return [[f"2026-{1 + i // 28:02d}-{1 + i % 28:02d}", closes[i], highs[i],
             min(closes[i], highs[i]) * 0.98, closes[i], 1e6] for i in range(len(closes))]


def _ev(candles, sl_price=90.0, max_hold=5):
    return evaluate_pledge_position(entry_price=ENTRY, side="BUY", sl_price=sl_price,
                                    entry_ts=candles[0][0], candles=candles, max_hold_days=max_hold)


def test_max_hold_exit():
    ev = _ev(_mk([105] * 5), max_hold=5)
    assert ev["exit_reason"].startswith("MAX_HOLD") and ev["days_held"] == 5


def test_below_max_hold_stays_open():
    assert _ev(_mk([110] * 4), max_hold=5)["exit_reason"] is None


def test_sl_breach_daily_backstop():
    assert _ev(_mk([105, 106, 104, 85]), max_hold=60)["exit_reason"] == "SL_BREACH_DAILY"


def test_no_exit_when_above_sl_and_within_hold():
    assert _ev(_mk([120, 130, 125]), max_hold=60)["exit_reason"] is None    # fixed hold, no trail


def test_max_hold_precedes_sl_check():
    ev = _ev(_mk([105, 106, 104, 103, 80]), sl_price=90.0, max_hold=5)
    assert ev["exit_reason"].startswith("MAX_HOLD")
