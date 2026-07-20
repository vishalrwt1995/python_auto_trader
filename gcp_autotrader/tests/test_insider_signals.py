"""Unit tests for domain/insider_signals.py + services/insider_signal_service.build_candidates.

Covers the informed-buy classification, the CRITICAL cluster aggregation (>=2 informed open-
market buy legs >=Rs5L, same symbol+day), liquidity/price gates (no upper turnover cap), the
double macro-gate helper, cluster-ranked slot selection, and the pure candidate builder.

Validated GOD-MODE config (2026-07-20): cluster>=2 + b200>50 + Nifty>100DMA, turnover>=10cr,
price>=30, hold 90d, 10 slots.
"""
from autotrader.domain.insider_signals import (
    is_informed, is_open_market_buy, qualifies_leg, aggregate_clusters,
    passes_insider_gates, macro_gate_ok, is_etf, turnover_20d_cr, atr14,
    select_for_slots, MIN_BARS,
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
    assert is_open_market_buy("Sell", "Market Sale") is False       # sell
    assert is_open_market_buy("Buy", "ESOP") is False               # not market
    assert is_open_market_buy("Buy", "Off Market") is False         # off-market excluded
    assert is_open_market_buy("Buy", "Gift") is False


def _leg(sym, cat="Promoters", txn="Buy", mode="Market Purchase", val="2000000", bef="10", aft="10.5"):
    return {"symbol": sym, "person_category": cat, "transaction_type": txn,
            "acq_mode": mode, "sec_val": val, "bef_pct": bef, "after_pct": aft}


def test_qualifies_leg():
    assert qualifies_leg(_leg("A")) is True
    assert qualifies_leg(_leg("A", cat="Employees")) is False       # not informed
    assert qualifies_leg(_leg("A", txn="Sell")) is False            # sell
    assert qualifies_leg(_leg("A", mode="ESOP")) is False           # not market
    assert qualifies_leg(_leg("A", val="400000")) is False          # < Rs 5 lakh


# ── aggregate_clusters — the cluster gate ──────────────────────────────────────
def test_cluster_two_legs_pass():
    rows = [_leg("ACME", cat="Promoters", val="2000000", aft="40.5", bef="40"),
            _leg("ACME", cat="Director", val="1500000", aft="1.2", bef="1")]
    cl = aggregate_clusters(rows)
    assert "ACME" in cl
    assert cl["ACME"]["n_buyers"] == 2
    assert cl["ACME"]["total_val"] == 3_500_000.0
    assert cl["ACME"]["category"] == "promoter"                     # best category present
    assert abs(cl["ACME"]["dpct"] - 0.5) < 1e-9                     # max holding delta


def test_single_buyer_not_a_cluster():
    assert "SOLO" not in aggregate_clusters([_leg("SOLO")])


def test_cluster_drops_sells_and_esop_and_small():
    rows = [_leg("X", txn="Buy", mode="Market Purchase", val="2000000"),   # qualifies
            _leg("X", txn="Sell", mode="Market Sale", val="9000000"),       # sell dropped
            _leg("X", mode="ESOP", val="9000000"),                          # esop dropped
            _leg("X", val="100000")]                                        # <5L dropped
    # only 1 qualifying leg -> no cluster
    assert "X" not in aggregate_clusters(rows)


def test_cluster_excludes_etf():
    rows = [_leg("NIFTYBEES"), _leg("NIFTYBEES")]
    assert "NIFTYBEES" not in aggregate_clusters(rows)


def test_cluster_min_buyers_param():
    rows = [_leg("Y"), _leg("Y")]
    assert "Y" in aggregate_clusters(rows, min_buyers=2)
    assert "Y" not in aggregate_clusters(rows, min_buyers=3)        # needs 3


# ── passes_insider_gates ───────────────────────────────────────────────────────
def test_gate_turnover_floor():
    assert passes_insider_gates(9.9, 100.0, "X") is False           # below 10cr
    assert passes_insider_gates(10.0, 100.0, "X") is True           # exactly the floor


def test_gate_no_upper_turnover_cap():
    assert passes_insider_gates(200.0, 100.0, "X") is True          # large-caps allowed (no cap)


def test_gate_price_floor():
    assert passes_insider_gates(35.0, 29.9, "X") is False
    assert passes_insider_gates(35.0, 30.0, "X") is True


def test_gate_excludes_etf():
    assert passes_insider_gates(50.0, 100.0, "GOLDBEES") is False


# ── macro_gate_ok (double gate) ────────────────────────────────────────────────
def test_macro_gate_both_conditions():
    assert macro_gate_ok(55.0, 100.0, 98.0) is True                 # b200>50 & nifty>ma
    assert macro_gate_ok(50.0, 100.0, 98.0) is False               # b200 not > 50 (strict)
    assert macro_gate_ok(55.0, 98.0, 100.0) is False               # nifty below ma


def test_macro_gate_fail_closed_on_missing():
    assert macro_gate_ok(None, 100.0, 98.0) is False
    assert macro_gate_ok(55.0, None, 98.0) is False
    assert macro_gate_ok(55.0, 100.0, None) is False


# ── shared math (turnover / atr) ───────────────────────────────────────────────
def test_turnover_20d_excludes_day_i():
    assert abs(turnover_20d_cr([100.0] * 25, [3_000_000.0] * 25, 21) - 30.0) < 1e-9


def test_atr14_seeds_at_i13():
    bars = [["d", 9.0, 10.0, 8.0, 9.0, 1e6] for _ in range(20)]
    out = atr14(bars)
    assert out[12] is None and out[13] is not None and abs(out[13] - 2.0) < 1e-9


# ── select_for_slots — cluster-strength ranking ────────────────────────────────
def test_select_ranks_by_buyers_then_value():
    cands = [{"symbol": "A", "n_buyers": 2, "total_val": 1e7},
             {"symbol": "B", "n_buyers": 3, "total_val": 5e6},
             {"symbol": "C", "n_buyers": 2, "total_val": 2e7}]
    got = select_for_slots(cands, open_count=0, max_slots=5)
    assert [c["symbol"] for c in got] == ["B", "C", "A"]            # B(3) > C(2,20m) > A(2,10m)


def test_select_free_slot_cap():
    cands = [{"symbol": s, "n_buyers": 2, "total_val": v} for s, v in [("A", 1e6), ("B", 3e6)]]
    assert [c["symbol"] for c in select_for_slots(cands, open_count=9, max_slots=10)] == ["B"]


def test_select_full_book_empty():
    assert select_for_slots([{"symbol": "A", "n_buyers": 5, "total_val": 1e8}], 10, 10) == []


# ── build_candidates (pure) ────────────────────────────────────────────────────
def _bars(closes, vol=3_000_000.0):
    return [[f"2026-{1 + i // 28:02d}-{1 + i % 28:02d}", c, c * 1.01, c * 0.99, c, vol]
            for i, c in enumerate(closes)]


def test_build_candidates_emits_ranked_by_cluster():
    barsA = _bars([100.0] * MIN_BARS)
    barsB = _bars([100.0] * MIN_BARS)
    rd = barsA[-1][0]
    clusters = {"A": {"n_buyers": 2, "total_val": 1e7, "dpct": 0.3, "category": "promoter"},
                "B": {"n_buyers": 4, "total_val": 5e6, "dpct": 0.1, "category": "director"}}
    cands = svc.build_candidates(rd, clusters, {"A": barsA, "B": barsB})
    assert [c["symbol"] for c in cands] == ["B", "A"]               # more buyers first
    assert cands[0]["channel"] == "insider"
    assert abs(cands[0]["turnover_cr"] - 30.0) < 1e-6               # in-band, >=10cr
    assert cands[0]["atr"] > 0 and cands[0]["reaction_close"] == 100.0
    assert cands[0]["max_hold_days"] == 90


def test_build_candidates_rejects_below_turnover():
    bars = _bars([100.0] * MIN_BARS, vol=500_000.0)                 # 100*5e5/1e7 = 5cr < 10
    clusters = {"A": {"n_buyers": 3, "total_val": 1e7, "dpct": 0.2, "category": "promoter"}}
    assert svc.build_candidates(bars[-1][0], clusters, {"A": bars}) == []


def test_build_candidates_short_history_skipped():
    bars = _bars([100.0] * 10)
    clusters = {"A": {"n_buyers": 3, "total_val": 1e7, "dpct": 0.2, "category": "promoter"}}
    assert svc.build_candidates(bars[-1][0], clusters, {"A": bars}) == []
