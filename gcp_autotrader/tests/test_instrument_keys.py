"""Tests for the shared ``symbol -> instrument_key`` resolver (extracted 2026-08-24).

WHY THIS MODULE EXISTS AND WHY IT IS TESTED THIS WAY.

Five call sites (pead / delivery / insider / pledge, plus corp_action importing pead's private
copy) each resolved keys from BQ ``candles_daily``, a table last written **2026-06-07**. Any
symbol listing -- or entering a channel's universe -- after that date could never resolve, so it
could never trade, and nothing logged it. On pead this measured ``no_key`` **15% -> 78% -> 64%**
across 08-20/21/24, which in the logs is indistinguishable from a quiet market.

The tempting fix was to swap the table. A coverage query killed that:

    candles_daily 2,638 syms | candles_5m(30d) 2,440 | gained 224 | LOST 422 if replaced

So the shipped behaviour is a UNION -- fresh wins on conflict, deep fills the gaps. The tests
below pin the three properties that make that correct, because each has a plausible-looking
wrong version:

  * BOTH tables are queried (a "simplification" to one table silently loses 224 or 422 symbols);
  * FRESH WINS on conflict (deep is stale, so preferring it re-introduces the frozen-table bug
    for any symbol present in both);
  * FAIL-CLOSED per query (a fresh-probe outage must degrade to the deep table, not to nothing;
    and a total outage must yield {} so the caller books no positions rather than a partial set).

The log lines are asserted verbatim: they are the only live evidence that key resolution is
healthy, and PROJECT_KNOWLEDGE tells future sessions to grep for exactly these strings.
"""
from __future__ import annotations

import logging

from autotrader.adapters.instrument_keys import resolve_instrument_keys


class _BQ:
    """Records SQL; returns a scripted result per call (deep first, then fresh)."""

    def __init__(self, *result_sets):
        self.sql: list[str] = []
        self._results = list(result_sets)

    def query(self, q):
        self.sql.append(q)
        return self._results[len(self.sql) - 1]


class _BrokenBQ:
    def query(self, q):
        raise RuntimeError("bq unavailable")


class _HalfBrokenBQ:
    """Deep succeeds, fresh raises — the fresh-probe-outage case."""

    def __init__(self, deep):
        self._deep = deep
        self.calls = 0

    def query(self, q):
        self.calls += 1
        if self.calls == 1:
            return self._deep
        raise RuntimeError("fresh probe down")


DEEP = [{"symbol": "ACME", "ik": "NSE_EQ|DEEP1"}, {"symbol": "OLDCO", "ik": "NSE_EQ|DEEP2"}]
FRESH = [{"symbol": "ACME", "ik": "NSE_EQ|FRESH1"}, {"symbol": "NEWCO", "ik": "NSE_EQ|FRESH3"}]


# --------------------------------------------------------------------------- union behaviour
def test_queries_both_tables_not_just_one():
    """A one-table 'simplification' loses 224 symbols (fresh) or 422 (deep). Pin both."""
    bq = _BQ(DEEP, FRESH)
    resolve_instrument_keys(["ACME"], bq, "pead")
    assert len(bq.sql) == 2
    assert "candles_daily" in bq.sql[0]
    assert "candles_5m" in bq.sql[1]


def test_fresh_wins_on_conflict():
    """ACME is in both. Deep is frozen since 2026-06-07, so fresh MUST win."""
    got = resolve_instrument_keys(["ACME"], _BQ(DEEP, FRESH), "pead")
    assert got["ACME"] == "NSE_EQ|FRESH1"


def test_deep_fills_gaps_fresh_cannot_cover():
    """candles_5m carries only actively-traded names; deep still supplies the other 422."""
    got = resolve_instrument_keys(["OLDCO"], _BQ(DEEP, FRESH), "pead")
    assert got["OLDCO"] == "NSE_EQ|DEEP2"


def test_union_is_the_full_set_not_either_table():
    got = resolve_instrument_keys(["ACME", "OLDCO", "NEWCO"], _BQ(DEEP, FRESH), "pead")
    assert got == {"ACME": "NSE_EQ|FRESH1", "OLDCO": "NSE_EQ|DEEP2", "NEWCO": "NSE_EQ|FRESH3"}


def test_fresh_only_symbol_is_the_whole_point_of_the_fix():
    """NEWCO exists ONLY in the fresh table — a post-06-07 listing. Before the union it was
    unresolvable, therefore untradeable, silently."""
    assert "NEWCO" in resolve_instrument_keys(["NEWCO"], _BQ(DEEP, FRESH), "pead")


# --------------------------------------------------------------------------- fail-closed
def test_total_outage_returns_empty_not_partial():
    """Caller must book nothing rather than a partial book."""
    assert resolve_instrument_keys(["ACME"], _BrokenBQ(), "pledge") == {}


def test_fresh_probe_outage_degrades_to_deep_table():
    """The reason this is two queries and not one clever UNION: losing fresh must not lose all."""
    got = resolve_instrument_keys(["ACME", "OLDCO"], _HalfBrokenBQ(DEEP), "insider")
    assert got == {"ACME": "NSE_EQ|DEEP1", "OLDCO": "NSE_EQ|DEEP2"}


def test_bq_none_does_not_raise_into_the_scan():
    assert resolve_instrument_keys(["ACME"], None, "delivery") == {}


def test_empty_symbols_issues_no_query_at_all():
    bq = _BQ()
    assert resolve_instrument_keys([], bq, "pead") == {}
    assert bq.sql == []


# --------------------------------------------------------------------------- input handling
def test_symbols_are_upper_cased_and_trimmed():
    bq = _BQ(DEEP, FRESH)
    resolve_instrument_keys([" acme ", "oldco"], bq, "pead")
    assert "'ACME'" in bq.sql[0] and "'OLDCO'" in bq.sql[0]


def test_single_quote_in_symbol_cannot_break_the_query():
    bq = _BQ([], [])
    resolve_instrument_keys(["TICK'S"], bq, "pead")
    assert "'TICKS'" in bq.sql[0]


# --------------------------------------------------------------------------- observability
def test_log_prefix_is_the_channel_verbatim(caplog):
    """PROJECT_KNOWLEDGE ㉚ tells future sessions to grep these exact strings."""
    with caplog.at_level(logging.INFO, logger="autotrader.adapters.instrument_keys"):
        resolve_instrument_keys(["ACME", "OLDCO", "NEWCO"], _BQ(DEEP, FRESH), "delivery")
    assert "delivery_resolve_keys asked=3 deep=2 fresh=2 merged=3 fresh_only=1" in caplog.text


def test_each_channel_logs_under_its_own_name():
    """corp_action used to log as 'pead' because it called pead's private copy — actively
    misleading, since it made one channel's resolution look like another's."""
    for ch in ("pead", "delivery", "insider", "pledge", "corp_action"):
        recs: list[str] = []

        class _H(logging.Handler):
            def emit(self, r):
                recs.append(r.getMessage())

        lg = logging.getLogger("autotrader.adapters.instrument_keys")
        lg.handlers, lg.propagate, prev = [_H()], False, lg.level
        lg.setLevel(logging.INFO)
        try:
            resolve_instrument_keys(["ACME"], _BQ(DEEP, FRESH), ch)
        finally:
            lg.handlers, lg.propagate = [], True
            lg.setLevel(prev)
        assert recs and recs[0].startswith(f"{ch}_resolve_keys asked=")


def test_both_failure_paths_log_which_source_died(caplog):
    with caplog.at_level(logging.ERROR, logger="autotrader.adapters.instrument_keys"):
        resolve_instrument_keys(["ACME"], _BrokenBQ(), "pledge")
    assert "pledge_resolve_keys_deep_failed" in caplog.text
    assert "pledge_resolve_keys_fresh_failed" in caplog.text
