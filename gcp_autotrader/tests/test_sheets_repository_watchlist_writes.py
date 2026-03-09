from __future__ import annotations

from autotrader.adapters.sheets_repository import GoogleSheetsRepository, SheetNames


def _repo_with_spies() -> tuple[GoogleSheetsRepository, list[tuple[str, str, str]]]:
    repo = object.__new__(GoogleSheetsRepository)
    calls: list[tuple[str, str, str]] = []

    def _clear_range(a1_range: str) -> None:
        calls.append(("clear", a1_range, ""))

    def _update_values(a1_range: str, values: list[list[object]], *, value_input_option: str = "USER_ENTERED") -> None:
        del values
        calls.append(("update", a1_range, value_input_option))

    repo.clear_range = _clear_range  # type: ignore[method-assign]
    repo.update_values = _update_values  # type: ignore[method-assign]
    return repo, calls


def test_replace_watchlist_swing_v2_uses_raw_value_input_option():
    repo, calls = _repo_with_spies()
    repo.replace_watchlist_swing_v2([["x"]])
    assert calls[0] == ("clear", f"'{SheetNames.WATCHLIST_SWING_V2}'!A4:ZZ", "")
    assert calls[1] == ("update", f"'{SheetNames.WATCHLIST_SWING_V2}'!A4", "RAW")


def test_replace_watchlist_intraday_v2_uses_raw_value_input_option():
    repo, calls = _repo_with_spies()
    repo.replace_watchlist_intraday_v2([["x"]])
    assert calls[0] == ("clear", f"'{SheetNames.WATCHLIST_INTRADAY_V2}'!A4:ZZ", "")
    assert calls[1] == ("update", f"'{SheetNames.WATCHLIST_INTRADAY_V2}'!A4", "RAW")


def test_replace_watchlist_legacy_alias_uses_raw_value_input_option():
    repo, calls = _repo_with_spies()
    repo.replace_watchlist([["x"]])
    assert calls[0] == ("clear", f"'{SheetNames.WATCHLIST_INTRADAY_V2}'!A4:ZZ", "")
    assert calls[1] == ("update", f"'{SheetNames.WATCHLIST_INTRADAY_V2}'!A4", "RAW")

