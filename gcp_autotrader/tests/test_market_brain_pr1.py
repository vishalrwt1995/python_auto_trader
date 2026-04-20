"""PR-1 Tier-0 regression tests.

Locks behaviour for:
  - PCR → options_positioning_score mapping
  - FII+DII → flow_score mapping
  - breadth rate-of-change scoring
  - signal-age decay penalty
  - _map_regime table-driven regime selection (PANIC exit guard, TREND_UP
    hysteresis, CHOP entry, RECOVERY gate) against default RegimeThresholds
  - _map_risk_mode transitions (LOCKDOWN / DEFENSIVE / AGGRESSIVE / NORMAL)
  - pubsub gating: emits on first persist, on transition, and on heartbeat;
    otherwise suppresses

These tests are the quality gate before externalising thresholds for
tuning — if anyone changes the defaults in `RegimeThresholds`, this file
is the canary that says "this is intentional, please update the tests."
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

import pytest

from autotrader.domain.models import (
    FiiDiiSnapshot,
    FreshnessSnapshot,
    MarketBrainState,
    PcrSnapshot,
    RegimeSnapshot,
)
from autotrader.services.market_brain_service import MarketBrainService
from autotrader.settings import RegimeThresholds
from autotrader.time_utils import IST, now_ist


# --------------------------------------------------------------------- #
# Minimal service factory — just enough to call _map_regime / _map_risk_mode
# without wiring up BQ/GCS/Firestore/regime_service/universe.
# --------------------------------------------------------------------- #


class _Stub:
    def __getattr__(self, _name: str) -> Any:
        return _Stub()

    def __call__(self, *_a: Any, **_kw: Any) -> Any:
        return _Stub()


def _svc(thresholds: RegimeThresholds | None = None) -> MarketBrainService:
    return MarketBrainService(
        regime_service=_Stub(),     # type: ignore[arg-type]
        universe_service=_Stub(),   # type: ignore[arg-type]
        gcs=_Stub(),                # type: ignore[arg-type]
        state=_Stub(),              # type: ignore[arg-type]
        thresholds=thresholds or RegimeThresholds(),
    )


def _prev(regime: str, *, asof: datetime | None = None, breadth_score: float = 50.0) -> MarketBrainState:
    asof_ts = (asof or now_ist()).astimezone(IST).isoformat()
    return MarketBrainState(
        asof_ts=asof_ts,
        regime=regime,  # type: ignore[arg-type]
        breadth_score=breadth_score,
    )


# --------------------------------------------------------------------- #
# Pure helpers
# --------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "pcr,confidence,expected_low,expected_high",
    [
        (0.3,  100.0, 0.0,  1.0),     # saturation bear
        (0.7,  100.0, 29.5, 30.5),    # exactly 30
        (1.0,  100.0, 49.5, 50.5),    # neutral
        (1.3,  100.0, 69.5, 70.5),    # bullish
        (2.5,  100.0, 99.5, 100.0),   # saturation bull
        (3.5,  100.0, 99.5, 100.0),   # super-saturation clamps to 100
        # Low confidence dilutes toward 50 (neutral)
        (2.5,  0.0,   49.5, 50.5),
        (0.3,  50.0,  24.5, 25.5),    # halfway between 0 and 50
        # Invalid PCR falls back to neutral
        (0.0,  100.0, 49.5, 50.5),
        (-1.0, 100.0, 49.5, 50.5),
    ],
)
def test_pcr_to_positioning_score(pcr: float, confidence: float, expected_low: float, expected_high: float) -> None:
    got = MarketBrainService._pcr_to_positioning_score(pcr, confidence=confidence)
    assert expected_low <= got <= expected_high, f"pcr={pcr} conf={confidence} got={got}"


@pytest.mark.parametrize(
    "fii,dii,freshness,expected_low,expected_high",
    [
        (-10000.0,     0.0, 100.0,  0.0,  0.5),    # heavy outflow
        ( -2500.0,     0.0, 100.0, 24.5, 25.5),    # half bearish
        (     0.0,     0.0, 100.0, 49.5, 50.5),    # neutral
        (  2500.0,     0.0, 100.0, 74.5, 75.5),    # half bullish
        ( 10000.0,     0.0, 100.0, 99.5, 100.0),   # saturation
        # DII cushions FII selling
        ( -3000.0,  3000.0, 100.0, 49.5, 50.5),
        # Stale data → pull toward neutral
        ( 10000.0,     0.0,   0.0, 49.5, 50.5),
    ],
)
def test_fii_dii_to_flow_score(fii: float, dii: float, freshness: float, expected_low: float, expected_high: float) -> None:
    got = MarketBrainService._fii_dii_to_flow_score(fii, dii, freshness=freshness)
    assert expected_low <= got <= expected_high, f"fii={fii} dii={dii} fresh={freshness} got={got}"


def test_breadth_roc_no_prior_is_neutral() -> None:
    svc = _svc()
    assert svc._breadth_roc_score(75.0, None) == 50.0


def test_breadth_roc_expansion_and_contraction() -> None:
    svc = _svc()
    prior = _prev("RANGE", breadth_score=50.0)
    assert svc._breadth_roc_score(60.0, prior) == 100.0   # saturation up
    assert svc._breadth_roc_score(40.0, prior) == 0.0     # saturation down
    assert svc._breadth_roc_score(55.0, prior) == 75.0    # halfway up
    assert svc._breadth_roc_score(50.0, prior) == 50.0    # no change


def test_signal_age_penalty_fresh_and_stale() -> None:
    svc = _svc()
    # None live_regime → 0
    assert svc._signal_age_penalty(None) == 0.0
    # Fresh: all ages < 120s → 0
    fresh_snap = RegimeSnapshot(freshness=FreshnessSnapshot(nifty_age_sec=30.0, vix_age_sec=60.0, pcr_age_sec=90.0))
    assert svc._signal_age_penalty(fresh_snap) == 0.0
    # Fully stale: worst age > 900s → 40
    stale_snap = RegimeSnapshot(freshness=FreshnessSnapshot(nifty_age_sec=30.0, vix_age_sec=60.0, pcr_age_sec=1500.0))
    assert svc._signal_age_penalty(stale_snap) == 40.0
    # Half-stale: worst age at midpoint of [120, 900] ≈ 510s → ~20
    mid_snap = RegimeSnapshot(freshness=FreshnessSnapshot(nifty_age_sec=30.0, vix_age_sec=60.0, pcr_age_sec=510.0))
    got = svc._signal_age_penalty(mid_snap)
    assert 19.0 <= got <= 21.0, got


def test_transitions_today_resets_across_midnight() -> None:
    now_i = now_ist()
    yesterday = now_i - timedelta(days=1)
    prev = _prev("RANGE", asof=yesterday)
    prev.regime_transitions_today = 5
    # Crossing midnight resets, and this IS a transition → 1
    got = MarketBrainService._count_transitions_today(prev, is_transition=True, now_i=now_i)
    assert got == 1


def test_transitions_today_increments_same_day() -> None:
    now_i = now_ist()
    prev = _prev("RANGE", asof=now_i - timedelta(minutes=5))
    prev.regime_transitions_today = 2
    assert MarketBrainService._count_transitions_today(prev, is_transition=True, now_i=now_i) == 3
    assert MarketBrainService._count_transitions_today(prev, is_transition=False, now_i=now_i) == 2


# --------------------------------------------------------------------- #
# Table-driven regime tests — lock default RegimeThresholds behaviour
# --------------------------------------------------------------------- #


def _call(svc: MarketBrainService, *, prev: MarketBrainState | None = None, **scores: float) -> str:
    defaults = dict(
        trend_score=50.0,
        breadth_score=50.0,
        leadership_score=50.0,
        volatility_stress_score=50.0,
        data_quality_score=80.0,
        risk_appetite=55.0,
    )
    defaults.update(scores)
    return svc._map_regime(prev=prev, **defaults)


def test_regime_panic_on_extreme_stress() -> None:
    svc = _svc()
    assert _call(svc, volatility_stress_score=82.0) == "PANIC"
    # Just below threshold no longer PANIC
    assert _call(svc, volatility_stress_score=81.9, breadth_score=60.0, leadership_score=55.0) != "PANIC"


def test_regime_panic_on_breadth_capitulation() -> None:
    svc = _svc()
    assert _call(svc, breadth_score=10.0, trend_score=60.0, leadership_score=60.0) == "PANIC"
    # breadth 15 (> 12) is NOT PANIC
    assert _call(svc, breadth_score=15.0, trend_score=30.0, leadership_score=40.0) != "PANIC"


def test_regime_panic_on_broken_data_pipeline() -> None:
    svc = _svc()
    assert _call(svc, data_quality_score=25.0) == "PANIC"


def test_regime_trend_up_standard_entry() -> None:
    svc = _svc()
    # All four conditions met
    r = _call(svc, trend_score=72.0, breadth_score=64.0, leadership_score=58.0, volatility_stress_score=40.0)
    assert r == "TREND_UP"


def test_regime_trend_up_highbreadth_entry() -> None:
    svc = _svc()
    # Index trend weak, but breadth + leadership + low stress qualifies
    r = _call(svc, trend_score=55.0, breadth_score=82.0, leadership_score=62.0, volatility_stress_score=40.0)
    assert r == "TREND_UP"


def test_regime_trend_down_entry() -> None:
    svc = _svc()
    r = _call(svc, trend_score=30.0, breadth_score=35.0, leadership_score=40.0, volatility_stress_score=55.0)
    assert r == "TREND_DOWN"


def test_regime_chop_entry() -> None:
    svc = _svc()
    r = _call(
        svc,
        trend_score=45.0,
        breadth_score=50.0,
        leadership_score=40.0,
        volatility_stress_score=65.0,
        risk_appetite=44.0,
    )
    assert r == "CHOP"


def test_regime_recovery_only_from_stressed_prior() -> None:
    svc = _svc()
    prev = _prev("PANIC", asof=now_ist() - timedelta(minutes=10))
    r = _call(svc, prev=prev, trend_score=45.0, breadth_score=40.0, leadership_score=45.0, volatility_stress_score=55.0)
    assert r == "RECOVERY"
    # From a healthy prior, same scores just map to RANGE
    prev_range = _prev("RANGE", asof=now_ist() - timedelta(minutes=10))
    r2 = _call(svc, prev=prev_range, trend_score=45.0, breadth_score=40.0, leadership_score=45.0, volatility_stress_score=55.0)
    assert r2 != "RECOVERY"


def test_regime_panic_exit_guard_breadth_below() -> None:
    """Coming out of PANIC, breadth < 22 keeps us in PANIC even if other scores recover."""
    svc = _svc()
    prev = _prev("PANIC", asof=now_ist() - timedelta(minutes=10))
    # Would otherwise classify as RANGE; breadth=20 forces stay-in-PANIC
    r = _call(
        svc,
        prev=prev,
        trend_score=50.0,
        breadth_score=20.0,  # below exit threshold
        leadership_score=50.0,
        volatility_stress_score=55.0,  # below panic_stress_min 82
    )
    assert r == "PANIC"


def test_regime_trend_up_hysteresis_holds_on_dip() -> None:
    """From TREND_UP, a small pullback should not immediately demote to RANGE."""
    svc = _svc()
    prev = _prev("TREND_UP", asof=now_ist() - timedelta(minutes=10))
    r = _call(
        svc,
        prev=prev,
        trend_score=62.0,        # below strict TREND_UP entry but above hold
        breadth_score=56.0,
        leadership_score=52.0,
        volatility_stress_score=45.0,
    )
    assert r == "TREND_UP"


def test_regime_trend_up_reenter_requires_stronger_proof() -> None:
    svc = _svc()
    prev = _prev("RANGE", asof=now_ist() - timedelta(minutes=10))
    # Exactly at standard TREND_UP entry — qualifies (≥70/62/56/≤48) for first classifier, but re-entry
    # demands 74/66/58. Scores 72/64/57 should NOT re-enter.
    r = _call(
        svc,
        prev=prev,
        trend_score=72.0,
        breadth_score=64.0,
        leadership_score=57.0,
        volatility_stress_score=40.0,
    )
    assert r == prev.regime  # blocked by re-entry guard


def test_regime_transition_damper_blocks_fast_flip() -> None:
    """Non-PANIC transitions under 240s revert to prior regime."""
    svc = _svc()
    prev = _prev("RANGE", asof=now_ist() - timedelta(seconds=60))
    r = _call(
        svc,
        prev=prev,
        trend_score=30.0,
        breadth_score=35.0,
        leadership_score=40.0,
    )
    # Would be TREND_DOWN, but age < 240 → reverts
    assert r == "RANGE"


# --------------------------------------------------------------------- #
# Risk mode
# --------------------------------------------------------------------- #


def test_risk_mode_lockdown_on_extreme_stress() -> None:
    svc = _svc()
    r = svc._map_risk_mode(regime="PANIC", risk_appetite=10.0, volatility_stress_score=86.0, data_quality_score=70.0)
    assert r == "LOCKDOWN"


def test_risk_mode_lockdown_on_broken_dq() -> None:
    svc = _svc()
    r = svc._map_risk_mode(regime="RANGE", risk_appetite=55.0, volatility_stress_score=40.0, data_quality_score=30.0)
    assert r == "LOCKDOWN"


def test_risk_mode_defensive_in_panic_regime() -> None:
    svc = _svc()
    r = svc._map_risk_mode(regime="PANIC", risk_appetite=40.0, volatility_stress_score=70.0, data_quality_score=70.0)
    assert r == "DEFENSIVE"


def test_risk_mode_aggressive_needs_all_four() -> None:
    svc = _svc()
    r = svc._map_risk_mode(regime="TREND_UP", risk_appetite=70.0, volatility_stress_score=40.0, data_quality_score=70.0)
    assert r == "AGGRESSIVE"
    # One condition fails → fallback to NORMAL
    r2 = svc._map_risk_mode(regime="TREND_UP", risk_appetite=70.0, volatility_stress_score=52.0, data_quality_score=70.0)
    assert r2 == "NORMAL"


def test_risk_mode_normal_fallback() -> None:
    svc = _svc()
    r = svc._map_risk_mode(regime="RANGE", risk_appetite=55.0, volatility_stress_score=45.0, data_quality_score=70.0)
    assert r == "NORMAL"


# --------------------------------------------------------------------- #
# Pubsub gating
# --------------------------------------------------------------------- #


def test_should_emit_pubsub_first_time() -> None:
    svc = _svc()
    state = MarketBrainState(asof_ts=now_ist().isoformat(), regime="RANGE", risk_mode="NORMAL")
    assert svc._should_emit_pubsub(state) is True


def test_should_emit_pubsub_on_regime_change() -> None:
    svc = _svc()
    svc._last_pubsub_state = {"regime": "RANGE", "risk_mode": "NORMAL", "ts": now_ist()}
    state = MarketBrainState(asof_ts=now_ist().isoformat(), regime="TREND_UP", risk_mode="NORMAL")
    assert svc._should_emit_pubsub(state) is True


def test_should_emit_pubsub_on_risk_mode_change() -> None:
    svc = _svc()
    svc._last_pubsub_state = {"regime": "RANGE", "risk_mode": "NORMAL", "ts": now_ist()}
    state = MarketBrainState(asof_ts=now_ist().isoformat(), regime="RANGE", risk_mode="DEFENSIVE")
    assert svc._should_emit_pubsub(state) is True


def test_should_emit_pubsub_suppresses_duplicate_within_heartbeat() -> None:
    svc = _svc()
    svc._last_pubsub_state = {"regime": "RANGE", "risk_mode": "NORMAL", "ts": now_ist()}
    state = MarketBrainState(asof_ts=now_ist().isoformat(), regime="RANGE", risk_mode="NORMAL")
    assert svc._should_emit_pubsub(state) is False


def test_should_emit_pubsub_heartbeat_after_silence() -> None:
    svc = _svc()
    svc._last_pubsub_state = {
        "regime": "RANGE",
        "risk_mode": "NORMAL",
        "ts": now_ist() - timedelta(seconds=400),  # > 300s heartbeat
    }
    state = MarketBrainState(asof_ts=now_ist().isoformat(), regime="RANGE", risk_mode="NORMAL")
    assert svc._should_emit_pubsub(state) is True
