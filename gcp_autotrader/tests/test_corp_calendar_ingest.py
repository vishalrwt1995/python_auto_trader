"""Tests for the pure parser in corp_calendar_ingest_service -- no live NSE/BQ, same convention
as test_delivery_ingest.py. Fixtures are real rows captured from the live endpoint spike."""
from autotrader.services.corp_calendar_ingest_service import _parse_nse_date, parse_corp_actions

HEG_RAW = {
    "bcEndDate": "-", "bcStartDate": "-", "caBroadcastDate": None,
    "comp": "HEG Limited", "exDate": "07-Sep-2026", "faceVal": "2", "ind": "-",
    "isin": "INE545A01016", "ndEndDate": "-", "ndStartDate": "-",
    "recDate": "07-Sep-2026", "series": "EQ", "subject": "Demerger", "symbol": "HEG",
}
DIVIDEND_RAW = {
    "bcEndDate": "-", "bcStartDate": "-", "caBroadcastDate": None,
    "comp": "Globus Spirits Limited", "exDate": "08-Sep-2026", "faceVal": "10", "ind": "-",
    "isin": "INE615I01010", "ndEndDate": "-", "ndStartDate": "-",
    "recDate": "08-Sep-2026", "series": "EQ", "subject": "Dividend - Rs 6.53 Per Share",
    "symbol": "GLOBUSSPR",
}


def test_parse_nse_date_converts_dd_mon_yyyy_to_iso():
    assert _parse_nse_date("07-Sep-2026") == "2026-09-07"


def test_parse_nse_date_treats_dash_placeholder_as_missing():
    assert _parse_nse_date("-") is None


def test_parse_nse_date_handles_none_and_empty():
    assert _parse_nse_date(None) is None
    assert _parse_nse_date("") is None


def test_parse_nse_date_fails_closed_on_garbage():
    assert _parse_nse_date("not a date") is None


def test_parses_the_real_heg_demerger_row():
    rows = parse_corp_actions([HEG_RAW])
    assert len(rows) == 1
    assert rows[0] == {
        "symbol": "HEG", "isin": "INE545A01016", "subject": "Demerger",
        "ex_date": "2026-09-07", "rec_date": "2026-09-07",
    }


def test_parses_multiple_rows_independently():
    rows = parse_corp_actions([HEG_RAW, DIVIDEND_RAW])
    assert len(rows) == 2
    assert {r["symbol"] for r in rows} == {"HEG", "GLOBUSSPR"}


def test_drops_rows_with_no_symbol():
    bad = {**HEG_RAW, "symbol": ""}
    assert parse_corp_actions([bad]) == []


def test_drops_rows_where_neither_date_parses():
    bad = {**HEG_RAW, "exDate": "-", "recDate": "-"}
    assert parse_corp_actions([bad]) == []


def test_keeps_a_row_with_only_ex_date():
    row = {**HEG_RAW, "recDate": "-"}
    parsed = parse_corp_actions([row])
    assert len(parsed) == 1
    assert parsed[0]["ex_date"] == "2026-09-07"
    assert parsed[0]["rec_date"] is None


def test_empty_input_is_empty_output():
    assert parse_corp_actions([]) == []


def test_symbol_is_upper_cased_for_reliable_matching():
    row = {**HEG_RAW, "symbol": "heg"}
    assert parse_corp_actions([row])[0]["symbol"] == "HEG"
