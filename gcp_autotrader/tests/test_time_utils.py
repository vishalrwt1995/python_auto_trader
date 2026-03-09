from __future__ import annotations

from autotrader.time_utils import now_ist_str, parse_any_ts


def test_now_ist_str_is_iso8601_with_offset():
    ts = now_ist_str()
    assert "T" in ts
    assert ts.endswith("+05:30")
    parsed = parse_any_ts(ts)
    assert parsed is not None
