"""Unit tests for domain/insider_signals.py + services/insider_signal_service.build_candidates.

Covers informed-buy classification, the TWO-PASS cluster logic (aggregate_legs pre-price +
finalize_clusters with value = shares × reaction-close, since the NSE filer value is unreliable),
liquidity/price gates, the double macro-gate helper, cluster-ranked selection, and the pure
candidate builder.

Validated GOD-MODE config (2026-07-20): cluster>=2 + b200>50 + Nifty>100DMA, turnover>=10cr,
price>=30, hold 90d, 10 slots.
"""
from autotrader.domain.insider_signals import (
    is_informed, is_open_market_buy, qualifies_leg, aggregate_legs, finalize_clusters,
    passes_insider_gates, macro_gate_ok, is_etf, turnover_20d_cr, atr14, select_for_slots, MIN_BARS,
)
from autotrader.services import insider_signal_service as svc


# ── classification ────────────────────────────────────────────────────────────
def test_is_informed():
    for c in ("Promoters", "Promoter Group", "Director", "Key Managerial Personnel", "Immediate relative"):
        assert is_informed(c) is True
    for c in ("Employees/Designated Employees", "Other", "-", ""):
        assert is_informed(c) is False


def test_is_open_market_buy():
    assert is_open_market_buy("Buy", "Market Purchase") is True
    assert is_open_market_buy("Sell", "Market Sale") is False
    assert is_open_market_buy("Buy", "ESOP") is False
    assert is_open_market_buy("Buy", "Off Market") is False
    assert is_open_market_buy("Buy", "Conversion of security") is False


def _leg(sym, cat="Promoters", txn="Buy", mode="Market Purchase", shares="20000", bef="10", aft="10.5"):
    return {"symbol": sym, "person_category": cat, "transaction_type": txn,
            "acq_mode": mode, "shares": shares, "bef_pct": bef, "after_pct": aft}


def test_qualifies_leg():
    assert qualifies_leg(_leg("A")) is True
    assert qualifies_leg(_leg("A", cat="Employees")) is False       # not informed
    assert qualifies_leg(_leg("A", txn="Sell")) is False            # sell
    assert qualifies_leg(_leg("A", mode="ESOP")) is False           # not market
    assert qualifies_leg(_leg("A", shares="0")) is False            # no shares


# ── aggregate_legs (pass 1, pre-price) ─────────────────────────────────────────
def test_aggregate_legs_two_legs_pass():
    rows = [_leg("ACME", cat="Promoters", shares="20000", aft="40.5", bef="40"),
            _leg("ACME", cat="Director", shares="15000", aft="1.2", bef="1")]
    legs = aggregate_legs(rows)
    assert "ACME" in legs and len(legs["ACME"]) == 2
    assert {lg["shares"] for lg in legs["ACME"]} == {20000.0, 15000.0}


def test_aggregate_legs_single_buyer_dropped():
    assert "SOLO" not in aggregate_legs([_leg("SOLO")])


def test_aggregate_legs_drops_sells_esop_and_excludes_etf():
    rows = [_leg("X", txn="Buy", mode="Market Purchase"),
            _leg("X", txn="Sell", mode="Market Sale"),
            _leg("X", mode="ESOP")]
    assert "X" not in aggregate_legs(rows)                          # only 1 qualifying leg
    assert "NIFTYBEES" not in aggregate_legs([_leg("NIFTYBEES"), _leg("NIFTYBEES")])


def test_aggregate_legs_min_buyers_param():
    rows = [_leg("Y"), _leg("Y")]
    assert "Y" in aggregate_legs(rows, min_buyers=2)
    assert "Y" not in aggregate_legs(rows, min_buyers=3)


# ── finalize_clusters (pass 2, value = shares × price) ─────────────────────────
def test_finalize_value_from_shares_times_price():
    legs = {"A": [{"shares": 20000.0, "category": "Promoters", "dpct": 0.5},
                  {"shares": 15000.0, "category": "Director", "dpct": 0.2}]}
    # price 100 -> values 2.0M & 1.5M, both >= Rs5L -> cluster of 2
    cl = finalize_clusters(legs, {"A": 100.0})
    assert cl["A"]["n_buyers"] == 2
    assert cl["A"]["total_val"] == 3_500_000.0
    assert cl["A"]["category"] == "promoter"                        # best category
    assert abs(cl["A"]["dpct"] - 0.5) < 1e-9


def test_finalize_drops_legs_below_value_then_cluster_fails():
    legs = {"A": [{"shares": 2000.0, "category": "Promoters", "dpct": 0.1},
                  {"shares": 1000.0, "category": "Director", "dpct": 0.1}]}
    # price 100 -> values 200k & 100k, both < Rs5L -> 0 kept -> no cluster
    assert finalize_clusters(legs, {"A": 100.0}) == {}


def test_finalize_partial_drop_keeps_only_if_still_two():
    legs = {"A": [{"shares": 20000.0, "category": "Promoters", "dpct": 0.3},   # 2.0M ok
                  {"shares": 1000.0, "category": "Director", "dpct": 0.1}]}     # 100k dropped
    assert finalize_clusters(legs, {"A": 100.0}) == {}              # only 1 leg survives -> not a cluster


def test_finalize_skips_symbol_without_price():
    legs = {"A": [{"shares": 20000.0, "category": "Promoters", "dpct": 0.3},
                  {"shares": 15000.0, "category": "Director", "dpct": 0.2}]}
    assert finalize_clusters(legs, {}) == {}                        # no price -> skip (fail-closed)


# ── passes_insider_gates ───────────────────────────────────────────────────────
def test_gate_turnover_floor():
    assert passes_insider_gates(9.9, 100.0, "X") is False
    assert passes_insider_gates(10.0, 100.0, "X") is True


def test_gate_no_upper_turnover_cap():
    assert passes_insider_gates(200.0, 100.0, "X") is True


def test_gate_price_floor_and_etf():
    assert passes_insider_gates(35.0, 29.9, "X") is False
    assert passes_insider_gates(35.0, 30.0, "X") is True
    assert passes_insider_gates(50.0, 100.0, "GOLDBEES") is False


# ── macro_gate_ok (double gate) ────────────────────────────────────────────────
def test_macro_gate():
    assert macro_gate_ok(55.0, 100.0, 98.0) is True
    assert macro_gate_ok(50.0, 100.0, 98.0) is False               # b200 not > 50 (strict)
    assert macro_gate_ok(55.0, 98.0, 100.0) is False               # nifty below MA
    assert macro_gate_ok(None, 100.0, 98.0) is False               # fail-closed


# ── shared math ────────────────────────────────────────────────────────────────
def test_turnover_and_atr():
    assert abs(turnover_20d_cr([100.0] * 25, [3_000_000.0] * 25, 21) - 30.0) < 1e-9
    bars = [["d", 9.0, 10.0, 8.0, 9.0, 1e6] for _ in range(20)]
    out = atr14(bars)
    assert out[12] is None and abs(out[13] - 2.0) < 1e-9


# ── select_for_slots ────────────────────────────────────────────────────────────
def test_select_ranks_by_buyers_then_value():
    cands = [{"symbol": "A", "n_buyers": 2, "total_val": 1e7},
             {"symbol": "B", "n_buyers": 3, "total_val": 5e6},
             {"symbol": "C", "n_buyers": 2, "total_val": 2e7}]
    assert [c["symbol"] for c in select_for_slots(cands, 0, 5)] == ["B", "C", "A"]


def test_select_free_slot_cap_and_full_book():
    cands = [{"symbol": s, "n_buyers": 2, "total_val": v} for s, v in [("A", 1e6), ("B", 3e6)]]
    assert [c["symbol"] for c in select_for_slots(cands, 9, 10)] == ["B"]
    assert select_for_slots(cands, 10, 10) == []


# ── build_candidates (pure) ────────────────────────────────────────────────────
def _bars(closes, vol=3_000_000.0):
    return [[f"2026-{1 + i // 28:02d}-{1 + i % 28:02d}", c, c * 1.01, c * 0.99, c, vol]
            for i, c in enumerate(closes)]


def test_build_candidates_ranked_by_cluster():
    barsA = _bars([100.0] * MIN_BARS); barsB = _bars([100.0] * MIN_BARS)
    rd = barsA[-1][0]
    clusters = {"A": {"n_buyers": 2, "total_val": 1e7, "dpct": 0.3, "category": "promoter"},
                "B": {"n_buyers": 4, "total_val": 5e6, "dpct": 0.1, "category": "director"}}
    cands = svc.build_candidates(rd, clusters, {"A": barsA, "B": barsB})
    assert [c["symbol"] for c in cands] == ["B", "A"]               # more buyers first
    assert cands[0]["channel"] == "insider" and cands[0]["max_hold_days"] == 90
    assert abs(cands[0]["turnover_cr"] - 30.0) < 1e-6 and cands[0]["atr"] > 0


def test_build_candidates_rejects_below_turnover():
    bars = _bars([100.0] * MIN_BARS, vol=500_000.0)                 # 5cr < 10
    clusters = {"A": {"n_buyers": 3, "total_val": 1e7, "dpct": 0.2, "category": "promoter"}}
    assert svc.build_candidates(bars[-1][0], clusters, {"A": bars}) == []
