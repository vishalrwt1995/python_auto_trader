from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone


IST = timezone(timedelta(hours=5, minutes=30))


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def now_ist() -> datetime:
    return now_utc().astimezone(IST)


def now_ist_str() -> str:
    # Operational logs/sheets must use ISO-8601 IST to avoid locale/date-serial ambiguity.
    return now_ist().isoformat(timespec="seconds")


def now_utc_iso() -> str:
    return now_utc().isoformat(timespec="seconds")


def today_ist() -> str:
    return now_ist().strftime("%Y-%m-%d")


def ist_minutes() -> int:
    n = now_ist()
    return n.hour * 60 + n.minute


def is_weekday_ist() -> bool:
    return now_ist().weekday() < 5


def is_market_open_ist() -> bool:
    m = ist_minutes()
    return is_weekday_ist() and 555 <= m <= 930


def is_entry_window_open_ist() -> bool:
    # Cut-off at 15:00 (900 min): new positions need at least 25 min to reach
    # 2:1 R:R target before the EOD force-close at 15:25. Entries at 15:19
    # gave only 6 minutes — almost always closed at EOD for a net loss.
    return is_market_open_ist() and ist_minutes() <= 900


def parse_any_ts(value: str | int | float | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        if value > 10_000_000_000:
            return datetime.fromtimestamp(value / 1000.0, tz=timezone.utc)
        return datetime.fromtimestamp(float(value), tz=timezone.utc)
    s = str(value).strip()
    if not s:
        return None
    if s.isdigit():
        n = int(s)
        return parse_any_ts(n)
    try:
        if "T" in s:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        # Apps Script format: dd-mm-yyyy HH:MM:SS (IST)
        dt = datetime.strptime(s, "%d-%m-%Y %H:%M:%S")
        return dt.replace(tzinfo=IST)
    except ValueError:
        return None


@dataclass(frozen=True)
class MarketWindow:
    start_minutes: int
    end_minutes: int

    def contains_now_ist(self) -> bool:
        m = ist_minutes()
        if self.start_minutes <= self.end_minutes:
            return self.start_minutes <= m <= self.end_minutes
        return m >= self.start_minutes or m <= self.end_minutes
