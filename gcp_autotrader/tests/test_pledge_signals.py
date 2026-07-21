"""Unit tests for domain/pledge_signals.py + services/pledge_signal_service.build_candidates.

Covers promoter pledge-revoke classification, single-leg aggregation (a lone promoter revoke IS the
signal — no cluster), the liquidity + price + px>200DMA (falling-knife) gate, the double macro-gate
helper, liquidity-ranked slot selection, and the pure candidate builder.

Validated FINAL config (2026-07-21): promoter revoke + px>200DMA + turnover>=25cr + price>=30,
hold 60d, 10 slots, ATR14x2.0.
"""
from autotrader.domain.pledge_signals import (
    is_promoter, is_pledge_revoke, qualifies_revoke, aggregate_revokes, passes_pledge_gates,
    macro_gate_ok, is_etf, turnover_20d_cr, atr14, sma, select_for_slots, MIN_BARS, MA_DAYS,
)
from autotrader.services import pledge_signal_service as svc


# ── classification ────────────────────────────────────────────────────────────
def test_is_promoter():
    for c in ("Promoters", "Promoter Group", "PROMOTER GROUP"):
        assert is_promoter(c) is True
    for c in ("Director", "Key Managerial Personnel", "Employees", "Other", "-", ""):
        assert is_promoter(c) is False


def test_is_pledge_revoke():
    assert is_pledge_revoke("Pledge Revoke") is True            # live pit-gg feed value
    assert is_pledge_revoke("Revokation of Pledge") is True     # legacy value
    assert is_pledge_revoke("Pledge") is False                  # creation (bearish)
    assert is_pledge_revoke("Invocation of pledge") is False    # forced sale (bearish)
    assert is_pledge_revoke("Buy") is False


def _row(sym, cat="Promoters", txn="Pledge Revoke", shares="100000"):
    return {"symbol": sym, "person_category": cat, "transaction_type": txn, "shares": shares}


def test_qualifies_revoke():
    assert qualifies_revoke(_row("A")) is True
    assert qualifies_revoke(_row("A", cat="Director")) is False   # not promoter
    assert qualifies_revoke(_row("A", txn="Pledge")) is False     # creation, not revoke
    assert qualifies_revoke(_row("A", shares="0")) is False       # no shares


# ── aggregate_revokes (single leg IS the signal — no cluster) ──────────────────
def test_aggregate_single_revoke_qualifies():
    out = aggregate_revokes([_row("ACME")])
    assert "ACME" in out and out["ACME"]["n_revokes"] == 1


def test_aggregate_counts_multiple_and_picks_category():
    out = aggregate_revokes([_row("X", cat="Promoter Group"), _row("X", cat="Promoter Group")])
    assert out["X"]["n_revokes"] == 2 and out["X"]["category"] == "promoter group"


def test_aggregate_mixed_category_prefers_individual_promoter():
    out = aggregate_revokes([_row("X", cat="Promoters"), _row("X", cat="Promoter Group")])
    assert out["X"]["category"] == "promoter"      # an individual promoter revoke present


def test_aggregate_drops_non_revoke_non_promoter_and_etf():
    rows = [_row("Y", txn="Pledge"), _row("Y", cat="Director"), _row("NIFTYBEES")]
    assert aggregate_revokes(rows) == {}


# ── passes_pledge_gates (liquidity + price + 200DMA) ───────────────────────────
def test_gate_turnover_floor_25cr():
    assert passes_pledge_gates(24.9, 100.0, True, "X") is False
    assert passes_pledge_gates(25.0, 100.0, True, "X") is True


def test_gate_requires_above_200dma():
    assert passes_pledge_gates(50.0, 100.0, False, "X") is False   # below 200DMA -> falling knife, rejected
    assert passes_pledge_gates(50.0, 100.0, True, "X") is True


def test_gate_price_floor_and_etf():
    assert passes_pledge_gates(50.0, 29.9, True, "X") is False
    assert passes_pledge_gates(50.0, 100.0, True, "GOLDBEES") is False


# ── macro_gate_ok (double gate) ────────────────────────────────────────────────
def test_macro_gate():
    assert macro_gate_ok(55.0, 100.0, 98.0) is True
    assert macro_gate_ok(50.0, 100.0, 98.0) is False               # b200 not > 50 (strict)
    assert macro_gate_ok(55.0, 98.0, 100.0) is False               # nifty below MA
    assert macro_gate_ok(None, 100.0, 98.0) is False               # fail-closed


# ── shared math ────────────────────────────────────────────────────────────────
def test_turnover_atr_and_sma():
    assert abs(turnover_20d_cr([100.0] * 25, [3_000_000.0] * 25, 21) - 30.0) < 1e-9
    bars = [["d", 9.0, 10.0, 8.0, 9.0, 1e6] for _ in range(20)]
    assert atr14(bars)[13] == 2.0
    assert sma([100.0] * 200, 200, 199) == 100.0                    # full 200-window -> mean
    assert sma([100.0] * 199, 200, 198) is None                     # only 199 bars -> can't fill window
    assert sma([1.0, 2.0, 3.0], 2, 0) is None                       # i < n-1 -> not enough history
    assert sma([1.0, 2.0, 3.0], 2, 1) == 1.5                        # window [1,2]


# ── select_for_slots (ranked by liquidity) ─────────────────────────────────────
def test_select_ranks_by_turnover():
    cands = [{"symbol": "A", "turnover_cr": 30.0}, {"symbol": "B", "turnover_cr": 90.0},
             {"symbol": "C", "turnover_cr": 50.0}]
    assert [c["symbol"] for c in select_for_slots(cands, 0, 5)] == ["B", "C", "A"]


def test_select_free_slot_cap_and_full_book():
    cands = [{"symbol": s, "turnover_cr": v} for s, v in [("A", 30.0), ("B", 90.0)]]
    assert [c["symbol"] for c in select_for_slots(cands, 9, 10)] == ["B"]
    assert select_for_slots(cands, 10, 10) == []


# ── build_candidates (pure) ────────────────────────────────────────────────────
def _bars(closes, vol=3_000_000.0):
    return [[f"20{16 + i // 300:02d}-{1 + (i // 28) % 12:02d}-{1 + i % 28:02d}", c, c * 1.01, c * 0.99, c, vol]
            for i, c in enumerate(closes)]


def test_build_candidates_passes_uptrend_liquid():
    closes = [100.0] * 200 + [130.0]                               # last close well above 200d mean
    bars = _bars(closes)
    rd = bars[-1][0]
    out = svc.build_candidates(rd, {"A": {"n_revokes": 1, "category": "promoter"}}, {"A": bars})
    assert len(out) == 1
    assert out[0]["symbol"] == "A" and out[0]["channel"] == "pledge" and out[0]["max_hold_days"] == 60
    assert out[0]["atr"] > 0 and out[0]["turnover_cr"] >= 25.0


def test_build_candidates_rejects_below_200dma():
    bars = _bars([100.0] * (MIN_BARS))                             # flat -> close == 200dma, not above
    rd = bars[-1][0]
    assert svc.build_candidates(rd, {"A": {"n_revokes": 1, "category": "promoter"}}, {"A": bars}) == []


def test_build_candidates_rejects_below_turnover():
    bars = _bars([100.0] * 200 + [130.0], vol=500_000.0)           # 5cr < 25cr
    rd = bars[-1][0]
    assert svc.build_candidates(rd, {"A": {"n_revokes": 1, "category": "promoter"}}, {"A": bars}) == []


def test_build_candidates_ranks_by_turnover():
    closes = [100.0] * 200 + [130.0]
    barsA = _bars(closes, vol=3_000_000.0)      # ~30cr
    barsB = _bars(closes, vol=9_000_000.0)      # ~90cr
    rd = barsA[-1][0]
    out = svc.build_candidates(rd, {"A": {"n_revokes": 1, "category": "promoter"},
                                    "B": {"n_revokes": 1, "category": "promoter"}},
                               {"A": barsA, "B": barsB})
    assert [c["symbol"] for c in out] == ["B", "A"]                # most liquid first
