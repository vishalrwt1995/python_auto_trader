"""Unit tests for the PURE exit/trail core of delivery_reconciliation_service.

evaluate_delivery_position mirrors swing/pead's daily 1R-trail (arm 1.75R) for DELIVERY's
20-day hold, reusing the shared swing_exit.trailed_stop. The I/O service is validated in PAPER.
"""
from autotrader.services.delivery_reconciliation_service import evaluate_delivery_position

ENTRY, SL_DIST = 100.0, 10.0          # base_sl = 90; 1R = 10; arm at +1.75R = peak>=117.5


def _mk(highs, closes):
    """Daily bars [date,o,h,l,c,v] with sortable dates (month rolls every 28 days)."""
    return [[f"2026-{1 + i // 28:02d}-{1 + i % 28:02d}", closes[i], highs[i],
             min(closes[i], highs[i]) * 0.98, closes[i], 1e6] for i in range(len(highs))]


def _ev(candles, sl_price=90.0, max_hold=20, sl_moved=False):
    return evaluate_delivery_position(entry_price=ENTRY, side="BUY", sl_price=sl_price,
                                      sl_dist=SL_DIST, entry_ts=candles[0][0], candles=candles,
                                      max_hold_days=max_hold, trail_r=1.0, activate_r=1.75,
                                      sl_moved=sl_moved)


def test_max_hold_exit_at_20d():
    ev = _ev(_mk([105] * 20, [104] * 20))      # 20 bars since entry >= 20d hold
    assert ev["exit_reason"].startswith("MAX_HOLD") and ev["days_held"] == 20


def test_below_max_hold_stays_open():
    ev = _ev(_mk([110] * 19, [108] * 19))      # 19 < 20 -> not a max-hold exit
    assert ev["exit_reason"] is None


def test_sl_breach_daily_backstop():
    c = _mk([110] * 10, [105] * 9 + [85])      # last close 85 < sl 90
    ev = _ev(c)
    assert ev["exit_reason"] == "SL_BREACH_DAILY"


def test_trail_not_armed_below_threshold():
    # peak high 110 < 117.5 -> not armed -> stop stays at base_sl (90)
    ev = _ev(_mk([110] * 10, [108] * 10))
    assert ev["exit_reason"] is None and ev["new_sl"] == 90.0 and ev["sl_moved"] is False


def test_trail_arms_at_1_75r_and_ratchets_up():
    # peak high 120 >= 117.5 -> armed -> stop = 120 - 1R(10) = 110
    c = _mk([112] * 5 + [120] + [115] * 4, [114] * 10)
    ev = _ev(c)
    assert ev["exit_reason"] is None and ev["new_sl"] == 110.0 and ev["sl_moved"] is True


def test_trail_ratchets_only_up_never_down():
    # already trailed to 110; peak 118 -> stop 108, but new_sl must stay 110 (no down-ratchet)
    c = _mk([118] * 10, [116] * 10)
    ev = _ev(c, sl_price=110.0, sl_moved=True)
    assert ev["exit_reason"] is None and ev["new_sl"] == 110.0


def test_no_sl_dist_skips_trail_safely():
    ev = evaluate_delivery_position(entry_price=ENTRY, side="BUY", sl_price=90.0, sl_dist=0.0,
                                    entry_ts="2026-01-01", candles=_mk([130] * 10, [125] * 10),
                                    max_hold_days=20, trail_r=1.0, activate_r=1.75)
    assert ev["exit_reason"] is None and ev["new_sl"] == 90.0   # unchanged, no crash
