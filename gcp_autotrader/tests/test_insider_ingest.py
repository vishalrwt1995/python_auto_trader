"""Unit tests for insider_ingest_service: parse_insider_xbrl (BSE in-bse-co PIT XBRL) + _index_filings.

NSE moved insider detail into per-filing XBRL (corporates-pit-gg is index-only). These pin the
XBRL parse (one row per Disclosure leg; symbol+date from the index record; holding-% fraction→
percent; filer value kept but untrusted) + the index-window filter. No live NSE/BQ in tests.
"""
from autotrader.services.insider_ingest_service import parse_insider_xbrl, _index_filings, _bcast_date

# Minimal but realistic in-bse-co PIT XBRL: company context (MainI) + two transaction legs
# (Disclosure1 = a promoter BUY, Disclosure2 = a director SELL). Filtering to informed/buy/market
# happens later in the domain — the parser emits every leg.
_XBRL = """<?xml version="1.0" encoding="UTF-8"?>
<xbrli:xbrl xmlns:in-bse-co="http://www.bseindia.com/xbrl/co/2017-09-15/in-bse-co" xmlns:xbrli="http://www.xbrl.org/2003/instance">
<xbrli:context id="MainI"><xbrli:entity><xbrli:identifier scheme="x">500001</xbrli:identifier></xbrli:entity><xbrli:period><xbrli:instant>2026-07-20</xbrli:instant></xbrli:period></xbrli:context>
<xbrli:context id="Disclosure1"><xbrli:entity><xbrli:identifier scheme="x">500001</xbrli:identifier></xbrli:entity><xbrli:period><xbrli:instant>2026-07-20</xbrli:instant></xbrli:period></xbrli:context>
<xbrli:context id="Disclosure2"><xbrli:entity><xbrli:identifier scheme="x">500001</xbrli:identifier></xbrli:entity><xbrli:period><xbrli:instant>2026-07-20</xbrli:instant></xbrli:period></xbrli:context>
<in-bse-co:Symbol contextRef="MainI">ACME</in-bse-co:Symbol>
<in-bse-co:NameOfTheCompany contextRef="MainI">Acme Industries Limited</in-bse-co:NameOfTheCompany>
<in-bse-co:CategoryOfPerson contextRef="Disclosure1">Promoter</in-bse-co:CategoryOfPerson>
<in-bse-co:NameOfThePerson contextRef="Disclosure1">JOHN PROMOTER</in-bse-co:NameOfThePerson>
<in-bse-co:SecuritiesAcquiredOrDisposedNumberOfSecurity contextRef="Disclosure1" unitRef="shares">20000</in-bse-co:SecuritiesAcquiredOrDisposedNumberOfSecurity>
<in-bse-co:SecuritiesAcquiredOrDisposedValueOfSecurity contextRef="Disclosure1" unitRef="INR">1</in-bse-co:SecuritiesAcquiredOrDisposedValueOfSecurity>
<in-bse-co:SecuritiesAcquiredOrDisposedTransactionType contextRef="Disclosure1">Buy</in-bse-co:SecuritiesAcquiredOrDisposedTransactionType>
<in-bse-co:ModeOfAcquisitionOrDisposal contextRef="Disclosure1">Market Purchase</in-bse-co:ModeOfAcquisitionOrDisposal>
<in-bse-co:SecuritiesHeldPriorToAcquisitionOrDisposalPercentageOfShareholding contextRef="Disclosure1" unitRef="pure">0.10</in-bse-co:SecuritiesHeldPriorToAcquisitionOrDisposalPercentageOfShareholding>
<in-bse-co:SecuritiesHeldPostAcquistionOrDisposalPercentageOfShareholding contextRef="Disclosure1" unitRef="pure">0.12</in-bse-co:SecuritiesHeldPostAcquistionOrDisposalPercentageOfShareholding>
<in-bse-co:CategoryOfPerson contextRef="Disclosure2">Director</in-bse-co:CategoryOfPerson>
<in-bse-co:NameOfThePerson contextRef="Disclosure2">JANE DIRECTOR</in-bse-co:NameOfThePerson>
<in-bse-co:SecuritiesAcquiredOrDisposedNumberOfSecurity contextRef="Disclosure2" unitRef="shares">5000</in-bse-co:SecuritiesAcquiredOrDisposedNumberOfSecurity>
<in-bse-co:SecuritiesAcquiredOrDisposedTransactionType contextRef="Disclosure2">Sell</in-bse-co:SecuritiesAcquiredOrDisposedTransactionType>
<in-bse-co:ModeOfAcquisitionOrDisposal contextRef="Disclosure2">Market Sale</in-bse-co:ModeOfAcquisitionOrDisposal>
</xbrli:xbrl>"""

_IDX = {"appId": "1882", "broadcastDateTime": "20-Jul-2026 16:33:41", "symbol": "ACME",
        "xmlFileName": "https://nsearchives.nseindia.com/corporate/xbrl/PIT_x.xml"}


def test_bcast_date():
    assert _bcast_date("20-Jul-2026 16:33:41") == "2026-07-20"
    assert _bcast_date("") is None and _bcast_date("garbage") is None


def test_parse_emits_one_row_per_disclosure_leg():
    rows = parse_insider_xbrl(_XBRL, _IDX)
    assert len(rows) == 2                                           # two Disclosure contexts
    by_name = {r["acq_name"]: r for r in rows}
    buy = by_name["JOHN PROMOTER"]
    assert buy["symbol"] == "ACME" and buy["date"] == "2026-07-20"  # from the index record
    assert buy["person_category"] == "Promoter"
    assert buy["transaction_type"] == "Buy"
    assert buy["acq_mode"] == "Market Purchase"
    assert buy["shares"] == 20000.0
    assert buy["bef_pct"] == 10.0 and buy["after_pct"] == 12.0      # fraction 0.10 -> 10.0 percent
    assert buy["app_id"] == "1882"


def test_parse_keeps_sells_too_domain_filters_later():
    rows = parse_insider_xbrl(_XBRL, _IDX)
    sell = next(r for r in rows if r["acq_name"] == "JANE DIRECTOR")
    assert sell["transaction_type"] == "Sell"                      # parser is not a filter


def test_parse_skips_leg_without_shares():
    xbrl = _XBRL.replace(">20000<", ">0<")                          # zero shares on leg 1
    rows = parse_insider_xbrl(_XBRL if False else xbrl, _IDX)
    assert {r["acq_name"] for r in rows} == {"JANE DIRECTOR"}       # only the 5000-share leg


def test_parse_bad_xml_returns_empty():
    assert parse_insider_xbrl("not xml", _IDX) == []
    assert parse_insider_xbrl(_XBRL, {"broadcastDateTime": "", "symbol": ""}) == []   # no date/symbol


# ── _index_filings (window filter + dedupe) ────────────────────────────────────
def _rec(appid, dt, sym="AAA", xml="http://x/1.xml"):
    return {"appId": appid, "broadcastDateTime": dt, "symbol": sym, "xmlFileName": xml}


def test_index_filings_window_and_dedupe():
    recs = [
        _rec("1", "20-Jul-2026 10:00:00"),        # in window
        _rec("2", "19-Jul-2026 10:00:00"),        # in window
        _rec("3", "01-May-2026 10:00:00"),        # before window -> dropped
        _rec("1", "20-Jul-2026 11:00:00"),        # dup appId -> dropped
        {"appId": "4", "broadcastDateTime": "20-Jul-2026 12:00:00", "symbol": "B"},  # no xmlFileName -> dropped
    ]
    out = _index_filings(recs, window_start="2026-07-18")
    assert sorted(r["appId"] for r in out) == ["1", "2"]
