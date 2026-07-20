"""Unit tests for services/insider_ingest_service.parse_pit.

The pure NSE corporates-pit JSON parser: dissemination-date -> YYYY-MM-DD, symbol upper-cased,
sec_val fallback to buyValue, ALL categories preserved (filtering is downstream in the domain).
Records with no parseable date or no symbol are skipped. No live NSE/BQ in tests.
"""
from autotrader.services.insider_ingest_service import parse_pit


def _rec(sym="ACME", date="30-Jan-2026 17:08", cat="Promoters", txn="Buy",
         mode="Market Purchase", secval="2000000", bef="40", aft="40.5", buyval=None):
    r = {"symbol": sym, "date": date, "acqName": "Jane Doe", "personCategory": cat,
         "tdpTransactionType": txn, "acqMode": mode, "secVal": secval,
         "befAcqSharesPer": bef, "afterAcqSharesPer": aft}
    if buyval is not None:
        r["buyValue"] = buyval
    return r


def test_parse_basic_fields():
    out = parse_pit([_rec()])
    assert len(out) == 1
    r = out[0]
    assert r["date"] == "2026-01-30"                       # "30-Jan-2026 17:08" -> ISO
    assert r["symbol"] == "ACME"
    assert r["person_category"] == "Promoters"
    assert r["transaction_type"] == "Buy"
    assert r["acq_mode"] == "Market Purchase"
    assert r["sec_val"] == 2_000_000.0
    assert r["bef_pct"] == 40.0 and r["after_pct"] == 40.5
    assert r["disseminated_ts"] == "30-Jan-2026 17:08"


def test_parse_uppercases_symbol():
    assert parse_pit([_rec(sym="infy")])[0]["symbol"] == "INFY"


def test_parse_secval_fallback_to_buyvalue():
    r = _rec(secval="", buyval="750000")
    assert parse_pit([r])[0]["sec_val"] == 750_000.0


def test_parse_skips_unparseable_date():
    assert parse_pit([_rec(date="")]) == []
    assert parse_pit([_rec(date="garbage")]) == []


def test_parse_skips_missing_symbol():
    assert parse_pit([_rec(sym="")]) == []


def test_parse_preserves_all_categories():
    # parser keeps sells / ESOP / employees too — the cluster gate filters later
    recs = [_rec(cat="Employees/Designated Employees", txn="Sell", mode="ESOP"),
            _rec(cat="Promoters")]
    out = parse_pit(recs)
    assert len(out) == 2                                   # nothing filtered at parse time
    assert {r["person_category"] for r in out} == {"Employees/Designated Employees", "Promoters"}


def test_parse_bad_numeric_defaults_zero():
    r = _rec(secval="N/A", bef="-", aft="")
    out = parse_pit([r])
    assert out[0]["sec_val"] == 0.0 and out[0]["bef_pct"] == 0.0 and out[0]["after_pct"] == 0.0


def test_parse_empty_input():
    assert parse_pit([]) == []
    assert parse_pit(None) == []
