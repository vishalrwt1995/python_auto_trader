"""M7 — Full-stack replay / integration tests for the M0→M6 pipeline.

Goal: with every redesign flag ON, prove a synthetic trade flows through
the gate chain (playbook → expected_edge → portfolio_book → thesis →
FSM → close → attribution → rollup) and produces the expected
downstream rows. And with flags OFF, prove the legacy path is unchanged.

These tests stitch pure-domain modules plus `OrderService` together
using in-memory fakes for Firestore/BigQuery/Upstox/PubSub. No GCP
credentials required, no network.

Scenarios covered:
  S1. Happy path — playbook allows, expected_edge positive, portfolio
      has budget, thesis builds, FSM walks INITIAL→CONFIRMED→RUNNER,
      close writes both `trades` and `attribution`, rollup aggregates.
  S2. Playbook hard-block — unknown regime × setup combo returns
      disallowed; no downstream work happens.
  S3. Expected-edge blocker — negative EV prior denies entry.
  S4. Portfolio daily-halt — DD over 3% blocks.
  S5. Portfolio daily-throttle — DD over 1.5% allows at 0.5× size.
  S6. Attribution flag OFF — close writes `trades` only, NOT
      `attribution`. Regression guard on the flag-gate.
  S7. FSM replay — SL-hit from INITIAL produces TERMINAL + SL_HIT.
  S8. FSM replay — runner graduation across two ticks.

Design:
  * Tests talk to the REAL OrderService, not a seam. The seam is at the
    adapter boundary (Firestore/BQ/Upstox fakes).
  * Every scenario asserts the SHAPE of what the operator sees after
    the flow — either a position doc in Firestore, a row in a fake BQ
    table, or a gate decision. No internal-private-method assertions.
"""
from __future__ import annotations

from typing import Any

import pytest

from autotrader.domain.attribution import build_row_from_position, rollup
from autotrader.domain.exit_fsm import (
    ExitState,
    FsmConfig,
    PositionView,
    TickEvent,
    transition,
)
from autotrader.domain.expected_edge import evaluate as evaluate_edge
from autotrader.domain.playbook import check_playbook
from autotrader.domain.portfolio_book import build_book, check_can_open
from autotrader.domain.thesis import build_thesis


# ──────────────────────────────────────────────────────────────────────────
# In-memory fakes (adapter seam)
# ──────────────────────────────────────────────────────────────────────────


class FakeState:
    """Minimal Firestore stand-in — enough for OrderService.close path."""

    def __init__(self) -> None:
        self.positions: dict[str, dict[str, Any]] = {}
        self.paper_gtts: dict[str, dict[str, Any]] = {}

    # OrderService close-path surface
    def get_position(self, tag: str) -> dict[str, Any] | None:
        doc = self.positions.get(tag)
        return dict(doc) if doc else None

    def update_position(self, tag: str, updates: dict[str, Any]) -> None:
        self.positions.setdefault(tag, {}).update(updates)

    def save_position(self, tag: str, payload: dict[str, Any]) -> None:
        self.positions[tag] = dict(payload)

    def delete_paper_gtt(self, tag: str) -> None:
        self.paper_gtts.pop(tag, None)

    def save_paper_gtt(self, tag: str, payload: dict[str, Any]) -> None:
        self.paper_gtts[tag] = dict(payload)


class FakeBq:
    """Capture every insert so tests can assert what landed where."""

    def __init__(self) -> None:
        self.trades: list[dict[str, Any]] = []
        self.attribution: list[dict[str, Any]] = []
        self.daily_metrics: list[dict[str, Any]] = []

    def insert_trade(self, row: dict[str, Any]) -> None:
        self.trades.append(dict(row))

    def insert_attribution(self, row: dict[str, Any]) -> None:
        self.attribution.append(dict(row))

    def insert_daily_metrics(self, row: dict[str, Any]) -> None:
        self.daily_metrics.append(dict(row))


class FakePubSub:
    def __init__(self) -> None:
        self.opened: list[dict[str, Any]] = []
        self.closed: list[dict[str, Any]] = []

    def publish_position_opened(self, doc: dict[str, Any]) -> None:
        self.opened.append(dict(doc))

    def publish_position_closed(self, doc: dict[str, Any]) -> None:
        self.closed.append(dict(doc))


class FakeUpstox:
    # OrderService close-path never touches Upstox, but the constructor
    # still requires the field. Keep it minimal.
    pass


def _make_settings(*, use_attribution_log_v1: bool = True):
    from autotrader.settings import (
        AppSettings,
        GcpSettings,
        RuntimeSettings,
        StrategySettings,
        UpstoxSettings,
    )
    return AppSettings(
        gcp=GcpSettings(project_id="test", region="asia-south1", bucket_name="b"),
        upstox=UpstoxSettings(
            api_v2_host="", api_v3_host="",
            client_id_secret_name="",
            client_secret_secret_name="",
            access_token_secret_name="",
            access_token_expiry_secret_name="",
        ),
        runtime=RuntimeSettings(
            paper_trade=True,
            job_trigger_token="t",
            log_level="INFO",
            use_attribution_log_v1=use_attribution_log_v1,
        ),
        strategy=StrategySettings(),
    )


def _make_order_service(*, use_attribution_log_v1: bool = True):
    from autotrader.services.order_service import OrderService
    state = FakeState()
    bq = FakeBq()
    pubsub = FakePubSub()
    svc = OrderService(
        settings=_make_settings(use_attribution_log_v1=use_attribution_log_v1),
        state=state,  # type: ignore[arg-type]
        upstox=FakeUpstox(),  # type: ignore[arg-type]
        bq=bq,  # type: ignore[arg-type]
        pubsub=pubsub,  # type: ignore[arg-type]
    )
    return svc, state, bq, pubsub


def _seed_position(state: FakeState, **overrides: Any) -> str:
    """Seed an OPEN position doc exactly as the real entry path would."""
    tag = overrides.get("position_tag", "AT-TEST-1")
    base: dict[str, Any] = {
        "position_tag": tag,
        "symbol": "INFY",
        "instrument_key": "NSE_EQ|INE009A01021",
        "side": "BUY",
        "strategy": "BREAKOUT",
        "qty": 10,
        "entry_price": 1000.0,
        "sl_price": 990.0,       # sl_dist = 10
        "target": 1020.0,
        "entry_ts": "2026-04-23 09:30:00",
        "signal_score": 78,
        "regime": "TREND_UP",
        "risk_mode": "NORMAL",
        "status": "OPEN",
        "paper": True,
        "channel": "intraday",
        "is_swing": False,
        "thesis": {
            "edge_name": "BREAKOUT_TREND_UP",
            "edge_version": "v1",
            "setup": "BREAKOUT",
            "direction": "BUY",
            "entry_price": 1000.0,
            "expected_r": 1.25,
            "expected_hold_minutes": 90,
            "invalidation_price": 990.0,
            "regime_at_entry": "TREND_UP",
            "risk_mode_at_entry": "NORMAL",
            "ts_epoch": 1_700_000_000.0,
        },
    }
    base.update(overrides)
    state.save_position(tag, base)
    return tag


# ──────────────────────────────────────────────────────────────────────────
# S1 — Happy path: gate chain admits + close emits both BQ rows.
# ──────────────────────────────────────────────────────────────────────────


def test_s1_happy_path_gate_chain_and_close_writes_attribution():
    # Gate 1: Playbook — TREND_UP × BREAKOUT × BUY is registered.
    allowed, reason = check_playbook("BREAKOUT", "BUY", "TREND_UP", "NORMAL")
    assert allowed, f"playbook denied happy path: {reason}"

    # Gate 2: Expected-edge — seed priors ship with positive EV for
    # TREND_UP:BREAKOUT:LONG but n=0 → stale-guard allows.
    er = evaluate_edge("TREND_UP", "BREAKOUT", "BUY")
    assert er.allowed

    # Gate 3: PortfolioBook — 50k capital, no open risk, no DD.
    book = build_book(capital=50_000.0, open_risk_by_channel={}, daily_pnl=0.0)
    decision = check_can_open(book, "intraday", risk_amount=250.0)
    assert decision.allowed and decision.size_multiplier == 1.0

    # Thesis — snapshot at entry.
    th = build_thesis(
        setup="BREAKOUT", direction="BUY",
        entry_price=1000.0, sl_price=990.0,
        regime="TREND_UP", risk_mode="NORMAL",
        ts_epoch=1_700_000_000.0,
        edge_name="BREAKOUT_TREND_UP",
        expected_r=er.expected_edge_r or 1.25,
    )
    assert th.regime_at_entry == "TREND_UP"
    assert th.invalidation_price == 990.0

    # ── Close → both trades and attribution rows emitted.
    svc, state, bq, _pubsub = _make_order_service(use_attribution_log_v1=True)
    tag = _seed_position(state)
    svc._close_position_firestore(
        position_tag=tag, exit_price=1020.0, exit_reason="TARGET_HIT",
    )

    assert len(bq.trades) == 1
    assert len(bq.attribution) == 1

    trade = bq.trades[0]
    attr = bq.attribution[0]

    # trade_row keeps the legacy shape.
    assert trade["position_tag"] == tag
    assert trade["exit_reason"] == "TARGET_HIT"
    assert trade["symbol"] == "INFY"

    # attribution row carries the thesis-derived fields.
    assert attr["edge_name"] == "BREAKOUT_TREND_UP"
    assert attr["expected_r"] == 1.25
    # realized_r = (1020-1000)/10 = +2R; r_delta = 2-1.25 = 0.75
    assert attr["realized_r"] == 2.0
    assert attr["r_delta"] == 0.75
    assert attr["regime_at_entry"] == "TREND_UP"
    assert attr["paper"] is True
    assert attr["channel"] == "intraday"


# ──────────────────────────────────────────────────────────────────────────
# S2 — Playbook hard-block: unknown regime × setup denies.
# ──────────────────────────────────────────────────────────────────────────


def test_s2_playbook_hard_blocks_unknown_combo():
    allowed, reason = check_playbook("BREAKOUT", "BUY", "PANIC", "DEFENSIVE")
    assert not allowed
    assert reason  # non-empty "why blocked"


# ──────────────────────────────────────────────────────────────────────────
# S3 — Expected-edge blocks on negative EV with sufficient sample.
# ──────────────────────────────────────────────────────────────────────────


def test_s3_expected_edge_blocks_when_prior_is_negative_and_mature(monkeypatch):
    import autotrader.domain.priors as priors_mod
    from autotrader.domain.priors import Prior

    # Stub get_prior to return a matured negative-EV prior.
    def fake_get_prior(*, regime: str, setup: str, direction: str) -> Prior:
        return Prior(win_rate=0.10, avg_win_r=1.0, avg_loss_r=-1.0, n=999)
    monkeypatch.setattr(priors_mod, "get_prior", fake_get_prior)
    # expected_edge.evaluate imports get_prior by name; monkeypatch there too.
    import autotrader.domain.expected_edge as edge_mod
    monkeypatch.setattr(edge_mod, "get_prior", fake_get_prior)

    res = evaluate_edge("TREND_UP", "BREAKOUT", "BUY")
    assert res.allowed is False
    assert res.expected_edge_r < 0
    assert "expected_edge" in res.reason


# ──────────────────────────────────────────────────────────────────────────
# S4/S5 — PortfolioBook halt vs throttle.
# ──────────────────────────────────────────────────────────────────────────


def test_s4_portfolio_daily_halt_blocks_entry():
    # 50k capital, -4% day → daily_dd_pct = 0.04 > 3% halt.
    book = build_book(capital=50_000.0, open_risk_by_channel={}, daily_pnl=-2_000.0)
    decision = check_can_open(book, "intraday", risk_amount=250.0)
    assert decision.allowed is False
    assert decision.reason == "portfolio_daily_dd_halt"


def test_s5_portfolio_daily_throttle_halves_size():
    # 50k capital, -2% day → above 1.5% throttle but below 3% halt.
    book = build_book(capital=50_000.0, open_risk_by_channel={}, daily_pnl=-1_000.0)
    decision = check_can_open(book, "intraday", risk_amount=250.0)
    assert decision.allowed is True
    assert decision.size_multiplier == 0.5


# ──────────────────────────────────────────────────────────────────────────
# S6 — Attribution flag OFF: trades row lands, attribution does not.
# ──────────────────────────────────────────────────────────────────────────


def test_s6_attribution_flag_off_skips_attribution_write():
    svc, state, bq, _pubsub = _make_order_service(use_attribution_log_v1=False)
    tag = _seed_position(state)
    svc._close_position_firestore(
        position_tag=tag, exit_price=1020.0, exit_reason="TARGET_HIT",
    )
    assert len(bq.trades) == 1
    assert len(bq.attribution) == 0, "attribution should be gated off when flag is false"


# ──────────────────────────────────────────────────────────────────────────
# S7 — FSM replay: SL hit from INITIAL.
# ──────────────────────────────────────────────────────────────────────────


def test_s7_fsm_sl_hit_from_initial_goes_terminal():
    pos = PositionView(
        tag="X", side="BUY", entry_price=100.0, atr=1.0, sl_dist=2.0,
        is_swing=False, entry_epoch=0.0, state=ExitState.INITIAL,
        current_sl=98.0,
    )
    tick = TickEvent(ltp=97.5, ts=30.0)  # crossed SL
    out = transition(pos, tick, FsmConfig())
    assert out.next_state == ExitState.TERMINAL
    assert out.exit_reason == "SL_HIT"


# ──────────────────────────────────────────────────────────────────────────
# S8 — FSM replay: INITIAL → CONFIRMED → RUNNER across a tick stream.
# ──────────────────────────────────────────────────────────────────────────


def test_s8_fsm_runner_graduation_across_ticks():
    cfg = FsmConfig(confirm_debounce_s=15.0, confirm_mfe_r=0.8, runner_mfe_r=2.0)
    pos = PositionView(
        tag="Y", side="BUY", entry_price=100.0, atr=1.0, sl_dist=2.0,
        is_swing=False, entry_epoch=0.0, state=ExitState.INITIAL,
        current_sl=98.0, best_price=100.0,
    )

    # Tick 1: 0.8R breach → confirm arms.
    out1 = transition(pos, TickEvent(ltp=101.6, ts=0.0), cfg)
    assert out1.next_state == ExitState.INITIAL
    pos.confirm_started_epoch = 0.1   # caller stamps this
    pos.best_price = 101.6
    pos.peak_mfe_r = 0.8

    # Tick 2: still ≥0.8R, 30s later → debounce elapses, moves to CONFIRMED.
    out2 = transition(pos, TickEvent(ltp=101.7, ts=30.0), cfg)
    assert out2.next_state == ExitState.CONFIRMED
    assert out2.sl_changed
    # Apply SL update and move pos state forward.
    pos.state = ExitState.CONFIRMED
    pos.current_sl = out2.new_sl
    pos.best_price = 101.7
    pos.peak_mfe_r = max(pos.peak_mfe_r, out2.mfe_r_now)

    # Tick 3: 2R reached → RUNNER graduation.
    out3 = transition(pos, TickEvent(ltp=104.0, ts=120.0), cfg)
    assert out3.next_state == ExitState.RUNNER


# ──────────────────────────────────────────────────────────────────────────
# S9 — End-to-end: close path + rollup produces sane DailyMetrics.
# ──────────────────────────────────────────────────────────────────────────


def test_s9_close_then_rollup_produces_daily_metrics():
    svc, state, bq, _ = _make_order_service(use_attribution_log_v1=True)

    # Two winners and one loser on the same day.
    tag1 = _seed_position(state, position_tag="AT-A",
                          entry_price=100.0, sl_price=98.0, symbol="A")
    tag2 = _seed_position(state, position_tag="AT-B",
                          entry_price=100.0, sl_price=98.0, symbol="B")
    tag3 = _seed_position(state, position_tag="AT-C",
                          entry_price=100.0, sl_price=98.0, symbol="C")

    svc._close_position_firestore(position_tag=tag1, exit_price=104.0, exit_reason="TARGET_HIT")
    svc._close_position_firestore(position_tag=tag2, exit_price=103.0, exit_reason="TARGET_HIT")
    svc._close_position_firestore(position_tag=tag3, exit_price=98.0,  exit_reason="SL_HIT")

    assert len(bq.attribution) == 3

    metrics = rollup(bq.attribution)
    assert metrics.n_trades == 3
    assert metrics.n_wins == 2
    assert metrics.win_rate == round(2 / 3, 4)
    # Realized Rs: +2, +1.5, -1 → mean 0.833...
    assert abs(metrics.mean_realized_r - round((2.0 + 1.5 - 1.0) / 3, 4)) < 1e-4


# ──────────────────────────────────────────────────────────────────────────
# S10 — Legacy-regression: attribution builder works without a thesis.
# ──────────────────────────────────────────────────────────────────────────


def test_s10_attribution_degrades_without_thesis_dict():
    # Legacy position doc (pre-M2 entry — no `thesis` key).
    pos = {
        "position_tag": "LEGACY-1",
        "symbol": "OLD",
        "side": "BUY",
        "strategy": "BREAKOUT",
        "entry_price": 100.0,
        "exit_price": 105.0,
        "sl_price": 98.0,
        "entry_ts": "2026-04-23 09:30:00",
        "exit_ts": "2026-04-23 10:15:00",
        "hold_minutes": 45,
        "regime": "TREND_UP",
        "risk_mode": "NORMAL",
        "exit_reason": "TARGET_HIT",
    }
    row = build_row_from_position(pos)
    d = row.to_bq_row()
    assert d["edge_name"] == ""           # no thesis → blank
    assert d["expected_r"] == 0.0          # no priors on legacy row
    assert d["realized_r"] == 2.5          # (105-100)/2
    # regime_at_entry falls back to pos.regime when thesis is missing.
    assert d["regime_at_entry"] == "TREND_UP"


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
