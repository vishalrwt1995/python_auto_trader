"""Phase 0 dashboard per-channel endpoints — unit tests for the pure rollup.

Covers `build_channel_overview` (the cockpit data) + `_position_channel`
routing. The endpoints themselves are thin I/O wrappers (smoke-verified live).
"""
from __future__ import annotations

from autotrader.web.dashboard_api import (
    build_channel_overview,
    _position_channel,
    _CHANNELS,
)

CAP = {"swing": 500000.0, "intraday": 100000.0, "pead": 200000.0,
       "gap_fade": 100000.0, "core": 300000.0}
MAXP = {"swing": 5, "intraday": 3, "pead": 5, "gap_fade": 3, "core": None}


def _cap(ch): return CAP.get(ch, 0.0)
def _maxp(ch): return MAXP.get(ch)


def test_position_channel_routing():
    assert _position_channel({"channel": "core"}) == "core"
    assert _position_channel({"channel": "GAP_FADE"}) == "gap_fade"   # normalized
    assert _position_channel({"wl_type": "swing"}) == "swing"          # falls back to wl_type
    assert _position_channel({"channel": "pead", "wl_type": "corp_action"}) == "pead"  # channel wins
    assert _position_channel({}) == "intraday"                         # legacy default


def test_overview_rollup_and_totals():
    positions = ([{"channel": "core", "symbol": f"C{i}"} for i in range(30)]
                 + [{"channel": "swing", "symbol": s} for s in ("SAIL", "CROMPTON", "JAYNECOIND")])
    pnl = {"swing": -200.0}
    risk = {"swing": 4500.0, "core": 0.0}
    out = build_channel_overview(_CHANNELS, positions, pnl, risk, _cap, _maxp, 0.03, 0.06)
    rows = {r["channel"]: r for r in out["channels"]}

    assert set(rows) == set(_CHANNELS)
    assert rows["core"]["open_positions"] == 30
    assert rows["core"]["capital"] == 300000.0 and rows["core"]["enabled"] is True
    assert rows["core"]["max_positions"] is None              # CORE is a basket, not slot-capped
    assert rows["swing"]["open_positions"] == 3
    assert rows["swing"]["today_pnl"] == -200.0 and rows["swing"]["open_risk"] == 4500.0
    assert rows["swing"]["max_positions"] == 5
    assert "SAIL" in rows["swing"]["open_symbols"]
    # totals
    assert out["totals"]["capital"] == 1200000.0             # Rs12L across 5 funded channels
    assert out["totals"]["open_positions"] == 33


def test_breaker_loss_and_profit_limits():
    # swing capital 500000 -> loss_limit -3% = -15000, profit_limit 6% = 30000
    loss = build_channel_overview(["swing"], [], {"swing": -15000.0}, {}, _cap, _maxp, 0.03, 0.06)["channels"][0]
    assert loss["daily_loss_limit"] == -15000.0 and loss["daily_profit_limit"] == 30000.0
    assert loss["breaker_tripped"] is True and loss["breaker_reason"] == "daily_loss_limit_hit"

    profit = build_channel_overview(["swing"], [], {"swing": 30000.0}, {}, _cap, _maxp, 0.03, 0.06)["channels"][0]
    assert profit["breaker_tripped"] is True and profit["breaker_reason"] == "daily_profit_target_hit"

    calm = build_channel_overview(["swing"], [], {"swing": -5000.0}, {}, _cap, _maxp, 0.03, 0.06)["channels"][0]
    assert calm["breaker_tripped"] is False and calm["breaker_reason"] is None


def test_unfunded_channel_disabled_and_never_trips():
    out = build_channel_overview(["corp_action"], [], {"corp_action": -99999.0}, {},
                                 lambda ch: 0.0, _maxp, 0.03, 0.06)["channels"][0]
    assert out["enabled"] is False
    assert out["breaker_tripped"] is False         # capital 0 -> no breaker
    assert out["daily_loss_limit"] == 0.0
