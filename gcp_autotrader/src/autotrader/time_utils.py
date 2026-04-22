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
    # 2026-04-21 post-mortem: Cut-off tightened from 15:00 → 14:00 (840 min).
    # With FLAT_TIMEOUT reverted to 120 min, entries after 13:25 cannot complete
    # their timeout before EOD force-close at 15:25, guaranteeing a premature
    # exit. 04-16 had multiple entries at 14:29 IST; 04-20 had 4 entries at
    # ~14:30 — all exited FLAT_TIMEOUT or EOD_CLOSE with poor PnL.
    #
    # Batch 2.2 (2026-04-22): Tightened 14:00 → 13:30 (810 min). At 14:00
    # entry only 85 min remain before EOD force-close — less than the 120-min
    # FLAT_TIMEOUT, so every 14:00 entry that doesn't hit SL or target in
    # 85 min is pre-committed to EOD_CLOSE exits at whatever price the market
    # gives. 13:30 gives 115 min, which is effectively the full timeout
    # window and leaves room for intraday continuation/reversal. Post-mortem
    # showed 04-16/04-20/04-21 had multiple late-afternoon entries exiting
    # EOD_CLOSE flat-to-losing (see trades table exit_reason distribution).
    return is_market_open_ist() and ist_minutes() <= 810


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
