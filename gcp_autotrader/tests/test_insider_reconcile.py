"""Unit tests for the PURE exit core of insider_reconciliation_service.evaluate_insider_position.

Insider is a FIXED-hold edge (NO trail — deliberately simpler than delivery): only a max-hold
exit + a daily-close SL-breach backstop. The I/O service is validated in PAPER.
"""
from autotrader.services.insider_reconciliation_service import evaluate_insider_position

ENTRY = 100.0


def _mk(closes, highs=None):
    """Daily bars [date,o,h,l,c,v] with sortable dates (month rolls every 28 days)."""
    highs = highs or [c * 1.02 for c in closes]
    return [[f"2026-{1 + i // 28:02d}-{1 + i % 28:02d}", closes[i], highs[i],
             min(closes[i], highs[i]) * 0.98, closes[i], 1e6] for i in range(len(closes))]


def _ev(candles, sl_price=90.0, max_hold=5):
    return evaluate_insider_position(entry_price=ENTRY, side="BUY", sl_price=sl_price,
                                     entry_ts=candles[0][0], candles=candles, max_hold_days=max_hold)


def test_max_hold_exit():
    ev = _ev(_mk([105] * 5), max_hold=5)                # 5 bars since entry >= 5d hold
    assert ev["exit_reason"].startswith("MAX_HOLD") and ev["days_held"] == 5


def test_below_max_hold_stays_open():
    ev = _ev(_mk([110] * 4), max_hold=5)                # 4 < 5 -> open
    assert ev["exit_reason"] is None


def test_sl_breach_daily_backstop():
    ev = _ev(_mk([105, 106, 104, 85]), max_hold=90)     # last close 85 < sl 90
    assert ev["exit_reason"] == "SL_BREACH_DAILY"


def test_no_exit_when_above_sl_and_within_hold():
    ev = _ev(_mk([120, 130, 125]), max_hold=90)         # profitable, no trail -> stays open
    assert ev["exit_reason"] is None                    # (fixed hold: no trail action returned)


def test_max_hold_precedes_sl_check():
    # both conditions true: hold reached AND last close below sl -> max-hold wins (checked first)
    ev = _ev(_mk([105, 106, 104, 103, 80]), sl_price=90.0, max_hold=5)
    assert ev["exit_reason"].startswith("MAX_HOLD")
