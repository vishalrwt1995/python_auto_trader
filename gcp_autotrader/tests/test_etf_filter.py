"""Tests for the consolidated STOCK-ONLY filter (domain/etf_filter, added 2026-08-28).

Two live defects motivated this module, and each gets a test that would have caught it:

  1. THREE DRIFTED COPIES. `delivery_signals.is_etf` carried 29 curated names while
     `insider_signals` / `pledge_signals` carried 13. The 08-07 ETF fix (§8 ㉔) reached delivery
     and universe but never insider or pledge, so those two were missing 20 names — including
     `LIQUIDCASE`, which ㉔ records as having actually reached the SIGNAL stage. They would have
     traded it. `test_all_three_channels_agree` fails if the copies ever diverge again.

  2. NAME MATCHING CANNOT BE COMPLETE. `NSE_EQ|INF740KA1SW3` passed both name checks in delivery
     on 2026-08-28 and reached the bar fetch, stopped only because Upstox returned HTTP 400 — luck,
     not design. `test_the_live_leak_is_closed` pins that exact key.

The safety property that matters most is ONE-DIRECTIONAL: consolidating may only ever ADD
exclusions. A stock-only mandate can tolerate over-exclusion; it cannot tolerate letting a fund
through. `test_no_regression_against_any_old_copy` asserts that directly.
"""
from __future__ import annotations

from autotrader.domain import delivery_signals, etf_filter, insider_signals, pledge_signals

# real held positions as of 2026-08-28 — none may ever be flagged non-equity
HELD = ("HEG", "THANGAMAYL", "TRUALT", "SILVERTUC", "APOLLOHOSP", "PARADEEP",
        "ANURAS", "AYE", "CYIENT", "HONAUT", "MEDANTA")


# --------------------------------------------------------------- the live leak
def test_the_live_leak_is_closed():
    """NSE_EQ|INF740KA1SW3 reached delivery's fetch on 2026-08-28. Name alone cannot catch it."""
    key = "NSE_EQ|INF740KA1SW3"
    assert etf_filter.is_fund_instrument_key(key) is True
    # a plausible ordinary-looking ticker + a fund ISIN must still be excluded
    assert etf_filter.is_non_equity("SOMEFUND", key) is True


def test_equity_isin_is_not_a_fund():
    assert etf_filter.is_fund_instrument_key("NSE_EQ|INE002A01018") is False
    assert etf_filter.is_non_equity("RELIANCE", "NSE_EQ|INE002A01018") is False


def test_bare_isin_accepted_without_the_exchange_prefix():
    assert etf_filter.is_fund_instrument_key("INF204KB14I2") is True


def test_missing_key_never_drops_a_symbol():
    """Fail-SAFE direction: absent data must not cause exclusion, or a resolver outage would
    silently empty the book."""
    for k in (None, "", "NSE_EQ|", "   "):
        assert etf_filter.is_fund_instrument_key(k) is False
    assert etf_filter.is_non_equity("RELIANCE", None) is False


# --------------------------------------------------------------- drift
def test_all_three_channels_agree():
    """The drift that caused defect 1. Fails the moment a copy diverges again."""
    probe = sorted(etf_filter.ETF_CURATED) + list(HELD) + ["RELIANCE", "TCS", "FOOBEES", "XETFY"]
    for sym in probe:
        d = delivery_signals.is_etf(sym)
        i = insider_signals.is_etf(sym)
        p = pledge_signals.is_etf(sym)
        assert d == i == p, f"{sym}: delivery={d} insider={i} pledge={p}"


def test_the_names_insider_and_pledge_used_to_miss():
    """These matched no pattern and were absent from the 13-name lists, so insider/pledge would
    have traded them. LIQUIDCASE is the one ㉔ observed reaching the SIGNAL stage."""
    for sym in ("GOLDSHARE", "LIQUIDCASE", "LIQUIDADD", "MAFANG", "N100", "QGOLDHALF",
                "KOTAKGOLD", "TATAGOLD", "MOM50", "ICICIB22"):
        assert insider_signals.is_etf(sym) is True, sym
        assert pledge_signals.is_etf(sym) is True, sym
        assert delivery_signals.is_etf(sym) is True, sym


def test_no_regression_against_any_old_copy():
    """THE one-directional safety property: consolidation may only ADD exclusions.

    The pre-consolidation union is hardcoded because the old lists no longer exist in source —
    this is the record of what each copy used to exclude.
    """
    old_union = {
        "NASDAQ", "N100", "MASPTOP50", "MAFANG", "SETFNIF50", "SETFNIFBK", "SETFNIFTY",
        "SETFGOLD", "QGOLDHALF", "QNIFTY", "GOLDSHARE", "HDFCLIQUID", "KOTAKNIFTY", "KOTAKGOLD",
        "AXISNIFTY", "AXISGOLD", "UTINIFTETF", "TATAGOLD", "GROWWGOLD", "CPSEETF", "PSUBANK",
        "LIQUID", "LIQUIDADD", "LIQUIDCASE", "MOM30", "MON100", "MOM50", "MOM100", "ICICIB22",
        "NIFTYBEES", "BANKBEES", "GOLDBEES", "LIQUIDBEES",
    }
    for sym in old_union:
        assert etf_filter.is_etf_symbol(sym) is True, f"{sym} was excluded before, now allowed"


# --------------------------------------------------------------- patterns + held names
def test_pattern_layer_still_works():
    """Use names NOT in the curated list, or the curated set masks the pattern and the test is
    vacuous. A mutation run caught exactly that: breaking endswith("BEES") changed nothing,
    because every BEES name we happen to list is also curated. A NEWLY-listed BEES ETF would
    depend on the pattern alone."""
    assert etf_filter.is_etf_symbol("FOOBEES") is True       # endswith BEES, NOT curated
    assert etf_filter.is_etf_symbol("SOMETHINGETF") is True  # contains ETF, NOT curated
    assert etf_filter.is_etf_symbol("XIETFY") is True        # contains IETF, NOT curated
    assert "FOOBEES" not in etf_filter.ETF_CURATED           # proves the pattern did the work


def test_no_held_position_is_excluded():
    """A false positive here would silently stop a real channel from re-entering a good name."""
    for sym in HELD:
        assert etf_filter.is_non_equity(sym) is False, sym
        assert etf_filter.is_etf_symbol(sym) is False, sym


def test_empty_symbol_is_not_an_etf():
    for s in ("", "   ", None):
        assert etf_filter.is_etf_symbol(s) is False
