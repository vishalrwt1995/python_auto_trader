from __future__ import annotations

from autotrader.time_utils import (
    NSE_TRADING_HOLIDAYS,
    is_trading_holiday_ist,
    now_ist_str,
    parse_any_ts,
)


def test_now_ist_str_is_iso8601_with_offset():
    ts = now_ist_str()
    assert "T" in ts
    assert ts.endswith("+05:30")
    parsed = parse_any_ts(ts)
    assert parsed is not None


# ── NSE holiday awareness (added 2026-05-28 after Bakri Eid gap) ────────────


def test_bakri_eid_2026_is_a_holiday():
    """The catalyst for this fix — 2026-05-28 must be in the holiday set."""
    assert "2026-05-28" in NSE_TRADING_HOLIDAYS
    assert is_trading_holiday_ist("2026-05-28") is True


def test_normal_weekday_is_not_a_holiday():
    """A regular Wednesday must NOT be flagged."""
    assert "2026-05-27" not in NSE_TRADING_HOLIDAYS  # Wed before Bakri Eid
    assert is_trading_holiday_ist("2026-05-27") is False


def test_holiday_set_has_full_2026_calendar():
    """Sanity: 2026 has 16 weekday holidays per Zerodha/NSE circular."""
    holidays_2026 = {d for d in NSE_TRADING_HOLIDAYS if d.startswith("2026-")}
    assert len(holidays_2026) == 16, (
        f"expected 16 holidays for 2026, got {len(holidays_2026)}: {sorted(holidays_2026)}"
    )


def test_key_2026_holidays_present():
    """Spot-check the well-known recurring ones."""
    for d in ("2026-01-26", "2026-04-03", "2026-05-01", "2026-05-28",
              "2026-10-02", "2026-12-25"):
        assert d in NSE_TRADING_HOLIDAYS, f"{d} should be a 2026 NSE holiday"
