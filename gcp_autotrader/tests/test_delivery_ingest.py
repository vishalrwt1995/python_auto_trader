"""Unit tests for services/delivery_ingest_service.parse_bhavdata.

The pure NSE sec_bhavdata_full CSV parser: EQ-series only, "-" DELIV_PER skipped,
turnover_cr = TURNOVER_LACS / 100, caller-supplied date. No live NSE/BQ in tests
(the fetch + BQ write are validated in PAPER).
"""
from autotrader.services.delivery_ingest_service import parse_bhavdata

# Real NSE sec_bhavdata_full shape: header carries a LEADING SPACE on every field
# after SYMBOL. Columns: SYMBOL, SERIES, DATE1, PREV_CLOSE, OPEN_PRICE, HIGH_PRICE,
# LOW_PRICE, LAST_PRICE, CLOSE_PRICE, AVG_PRICE, TTL_TRD_QNTY, TURNOVER_LACS,
# NO_OF_TRADES, DELIV_QTY, DELIV_PER.
_HEADER = ("SYMBOL, SERIES, DATE1, PREV_CLOSE, OPEN_PRICE, HIGH_PRICE, LOW_PRICE, "
           "LAST_PRICE, CLOSE_PRICE, AVG_PRICE, TTL_TRD_QNTY, TURNOVER_LACS, "
           "NO_OF_TRADES, DELIV_QTY, DELIV_PER")


def _row(sym, series, close, ttl, turnover_lacs, deliv_qty, deliv_per):
    # positional: SYMBOL,SERIES,DATE1,PREV_CLOSE,OPEN,HIGH,LOW,LAST,CLOSE,AVG,TTL,TURN,NOT,DQTY,DPER
    return (f"{sym}, {series}, 14-JUL-2026, 99, 100, 101, 98, {close}, {close}, 99.5, "
            f"{ttl}, {turnover_lacs}, 500, {deliv_qty}, {deliv_per}")


def _csv(*rows):
    return ("\n".join([_HEADER, *rows])).encode("utf-8")


def test_parse_keeps_eq_and_computes_fields():
    raw = _csv(_row("TATASTEEL", "EQ", 150.5, 1_000_000, 15000.0, 800_000, 80.0))
    out = parse_bhavdata(raw, "2026-07-14")
    assert len(out) == 1
    r = out[0]
    assert r["symbol"] == "TATASTEEL"
    assert r["date"] == "2026-07-14"
    assert r["deliv_pct"] == 80.0
    assert r["deliv_qty"] == 800_000
    assert r["ttl_trd_qty"] == 1_000_000
    assert r["close_price"] == 150.5
    # turnover_cr = TURNOVER_LACS / 100 = 15000 / 100 = 150.0
    assert r["turnover_cr"] == 150.0


def test_parse_drops_non_eq_series():
    raw = _csv(
        _row("TATASTEEL", "EQ", 150.5, 1_000_000, 15000.0, 800_000, 80.0),
        _row("SOMEBOND", "N2", 1000.0, 5000, 50.0, 100, 2.0),   # non-EQ -> dropped
        _row("SGBSERIES", "GB", 5000.0, 200, 100.0, 50, 25.0),  # non-EQ -> dropped
    )
    out = parse_bhavdata(raw, "2026-07-14")
    assert [r["symbol"] for r in out] == ["TATASTEEL"]


def test_parse_skips_unparseable_deliv_per():
    # DELIV_PER "-" (NSE's placeholder for series with no delivery data) -> skipped
    raw = _csv(
        _row("GOODNAME", "EQ", 100.0, 1_000_000, 10000.0, 700_000, 70.0),
        _row("NODELIV", "EQ", 50.0, 5000, 25.0, 0, "-"),        # "-" DELIV_PER -> skipped
    )
    out = parse_bhavdata(raw, "2026-07-14")
    assert [r["symbol"] for r in out] == ["GOODNAME"]


def test_parse_turnover_lacs_to_crore():
    # 2500 lacs = 25 crore (a mid-cap in the delivery band)
    raw = _csv(_row("MIDCAP", "EQ", 200.0, 500_000, 2500.0, 400_000, 88.0))
    out = parse_bhavdata(raw, "2026-07-14")
    assert out[0]["turnover_cr"] == 25.0


def test_parse_empty_or_bad_header():
    assert parse_bhavdata(b"", "2026-07-14") == []
    assert parse_bhavdata(b"WRONG,HEADER,COLS\nA,B,C", "2026-07-14") == []


def test_parse_multiple_eq_symbols_uppercased():
    raw = _csv(
        _row("infy", "EQ", 100.0, 1_000_000, 3000.0, 900_000, 90.0),
        _row("wipro", "EQ", 50.0, 2_000_000, 4000.0, 1_500_000, 75.0),
    )
    out = parse_bhavdata(raw, "2026-07-14")
    assert {r["symbol"] for r in out} == {"INFY", "WIPRO"}       # symbols upper-cased
