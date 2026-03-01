from __future__ import annotations

from datetime import date, timedelta


def synthetic_instrument_snapshot() -> list[dict[str, object]]:
    return [
        {
            "exchange": "NSE",
            "segment": "NSE_EQ",
            "trading_symbol": "ABC",
            "instrument_key": "NSE_EQ|ABC",
            "isin": "INE000A01001",
            "instrument_type": "EQ",
            "security_type": "SM",
            "is_enabled": True,
            "is_delisted": False,
            "is_suspended": False,
            "name": "ABC LTD",
        },
        {
            "exchange": "BSE",
            "segment": "BSE_EQ",
            "trading_symbol": "ABC",
            "instrument_key": "BSE_EQ|ABC",
            "isin": "INE000A01001",
            "instrument_type": "EQ",
            "security_type": "SM",
            "is_enabled": True,
            "is_delisted": False,
            "is_suspended": False,
            "name": "ABC LTD",
        },
        {
            "exchange": "NSE",
            "segment": "NSE_EQ",
            "trading_symbol": "DEF",
            "instrument_key": "NSE_EQ|DEF",
            "isin": "",
            "instrument_type": "EQ",
            "security_type": "SM",
            "is_enabled": True,
            "is_delisted": False,
            "is_suspended": False,
            "name": "DEF LTD",
        },
    ]


def synthetic_candles_linear_volume(*, n: int = 120, close: float = 100.0, start: date = date(2025, 1, 1)) -> list[list[object]]:
    rows: list[list[object]] = []
    for i in range(n):
        d = start + timedelta(days=i)
        o = close - 0.5
        h = close + 0.5
        l = close - 1.0
        v = i + 1
        rows.append([f"{d.isoformat()}T00:00:00+05:30", o, h, l, close, float(v)])
    return rows


def synthetic_candles_constant_tr(*, n: int = 40, close: float = 100.0, start: date = date(2025, 1, 1)) -> list[list[object]]:
    rows: list[list[object]] = []
    for i in range(n):
        d = start + timedelta(days=i)
        o = close
        h = close + 1.0
        l = close - 1.0
        rows.append([f"{d.isoformat()}T00:00:00+05:30", o, h, l, close, 10000.0])
    return rows


def synthetic_candles_fixed_gap(*, n: int = 80, close: float = 100.0, gap: float = 0.01, start: date = date(2025, 1, 1)) -> list[list[object]]:
    rows: list[list[object]] = []
    prev_close = close
    for i in range(n):
        d = start + timedelta(days=i)
        if i == 0:
            o = close
        else:
            o = prev_close * (1.0 + gap)
        h = max(o, close) + 1.0
        l = min(o, close) - 1.0
        rows.append([f"{d.isoformat()}T00:00:00+05:30", float(o), float(h), float(l), float(close), 10000.0])
        prev_close = close
    return rows
