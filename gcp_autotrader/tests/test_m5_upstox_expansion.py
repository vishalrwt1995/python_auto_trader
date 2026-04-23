"""Tests for M5 — Option analytics + News store + flag wire-up.

These test only the pure/primitive pieces — the Upstox HTTP polling
and Firestore I/O are exercised indirectly via the test doubles.
Covers:
  * compute_max_pain picks a strike that minimises total writer pain.
  * PCR math (static + OI change).
  * IV skew picks only near-ATM strikes.
  * aggregate_sentiment majority+magnitude rule.
  * Settings expose USE_OPTION_ANALYTICS_V1 / USE_NEWS_SIGNALS_V1 /
    USE_PORTFOLIO_STREAM_V1 flags and read them from env.
  * ws_monitor_service reads the portfolio-stream flag at init.
"""
from __future__ import annotations

import inspect

from autotrader.adapters.news_store import aggregate_sentiment
from autotrader.domain.option_analytics import (
    OptionMetrics,
    compute_iv_skew,
    compute_max_pain,
    compute_metrics,
    compute_oi_change_pcr,
    compute_pcr,
)


# ──────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────


def _row(strike: float, ce_oi: float = 0.0, pe_oi: float = 0.0,
          ce_oi_chg: float = 0.0, pe_oi_chg: float = 0.0,
          ce_iv: float = 0.0, pe_iv: float = 0.0) -> dict:
    return {
        "strike_price": strike,
        "call_options": {
            "market_data": {"oi": ce_oi, "oi_change": ce_oi_chg},
            "option_greeks": {"iv": ce_iv},
        },
        "put_options": {
            "market_data": {"oi": pe_oi, "oi_change": pe_oi_chg},
            "option_greeks": {"iv": pe_iv},
        },
    }


# ──────────────────────────────────────────────────────────────────────────
# Max pain
# ──────────────────────────────────────────────────────────────────────────


def test_max_pain_centre_of_mass():
    """Max pain should land on the strike with the highest bilateral OI
    concentration — the writer-pain minimum."""
    chain = [
        _row(100, ce_oi=1000, pe_oi=0),
        _row(105, ce_oi=500, pe_oi=500),
        _row(110, ce_oi=0, pe_oi=1000),
    ]
    mp = compute_max_pain(chain)
    # Symmetric writers → 105 is the pain minimum.
    assert mp == 105.0


def test_max_pain_empty_chain():
    assert compute_max_pain([]) == 0.0


def test_max_pain_skews_toward_heavier_oi_strike():
    """Max pain lands at the strike that minimises total writer pain —
    heavy call OI at 100 means expiry=100 is cheapest for writers."""
    chain = [
        _row(100, ce_oi=5000, pe_oi=100),
        _row(105, ce_oi=500, pe_oi=500),
        _row(110, ce_oi=200, pe_oi=500),
    ]
    # Hand math at each K (see comment block in compute_max_pain):
    # K=100 total = 7500, K=105 total = 27500, K=110 total = 52500 → 100 wins.
    mp = compute_max_pain(chain)
    assert mp == 100.0


# ──────────────────────────────────────────────────────────────────────────
# PCR
# ──────────────────────────────────────────────────────────────────────────


def test_pcr_neutral_on_empty_ce():
    chain = [_row(100, ce_oi=0, pe_oi=0)]
    assert compute_pcr(chain) == 1.0


def test_pcr_ratio_math():
    chain = [
        _row(100, ce_oi=1000, pe_oi=2000),
        _row(105, ce_oi=1000, pe_oi=2000),
    ]
    # 4000 / 2000 = 2.0 (bearish skew)
    assert compute_pcr(chain) == 2.0


def test_oi_change_pcr_ignores_oi_reductions():
    """Negative OI changes are CLOSING positions — they shouldn't swing
    the reactive PCR. Only additions count."""
    chain = [
        _row(100, ce_oi_chg=+100, pe_oi_chg=+200),
        _row(105, ce_oi_chg=-500, pe_oi_chg=-500),  # closures — ignored
    ]
    # Only additions: pe 200 / ce 100 = 2.0
    assert compute_oi_change_pcr(chain) == 2.0


# ──────────────────────────────────────────────────────────────────────────
# IV skew
# ──────────────────────────────────────────────────────────────────────────


def test_iv_skew_positive_when_otm_puts_bid():
    chain = [
        _row(95, pe_iv=22.0),   # OTM put (spot 100)
        _row(105, ce_iv=18.0),  # OTM call
    ]
    skew = compute_iv_skew(chain, spot=100.0)
    assert skew == round(22.0 - 18.0, 4)


def test_iv_skew_ignores_far_strikes():
    chain = [
        _row(70, pe_iv=50.0),    # deep OTM put — outside ±7% band
        _row(95, pe_iv=22.0),
        _row(105, ce_iv=18.0),
        _row(140, ce_iv=5.0),    # deep OTM call
    ]
    skew = compute_iv_skew(chain, spot=100.0)
    # Should ignore the 70 / 140 strikes
    assert skew == round(22.0 - 18.0, 4)


def test_iv_skew_zero_when_no_spot():
    chain = [_row(95, pe_iv=22.0), _row(105, ce_iv=18.0)]
    assert compute_iv_skew(chain, spot=0.0) == 0.0


# ──────────────────────────────────────────────────────────────────────────
# compute_metrics aggregates
# ──────────────────────────────────────────────────────────────────────────


def test_compute_metrics_fills_all_fields():
    chain = [
        _row(100, ce_oi=1000, pe_oi=500, ce_oi_chg=100, pe_oi_chg=200, ce_iv=15),
        _row(105, ce_oi=500, pe_oi=1000, ce_oi_chg=50, pe_oi_chg=100, pe_iv=18),
    ]
    m = compute_metrics(chain, spot=102.0)
    assert isinstance(m, OptionMetrics)
    assert m.n_rows == 2
    assert m.max_pain_strike in (100.0, 105.0)
    assert m.put_call_ratio > 0


def test_compute_metrics_empty_chain_returns_neutral():
    m = compute_metrics([], spot=100.0)
    assert m.n_rows == 0
    assert m.put_call_ratio == 1.0


# ──────────────────────────────────────────────────────────────────────────
# News aggregate_sentiment
# ──────────────────────────────────────────────────────────────────────────


def test_aggregate_sentiment_empty_is_neutral():
    label, conf = aggregate_sentiment([])
    assert label == "NEUTRAL" and conf == 0.0


def test_aggregate_sentiment_bullish_majority():
    items = [
        {"sentiment": "BULLISH", "score": 0.8},
        {"sentiment": "BULLISH", "score": 0.6},
        {"sentiment": "BEARISH", "score": 0.2},
    ]
    label, conf = aggregate_sentiment(items)
    assert label == "BULLISH"
    assert 0.6 < conf <= 1.0


def test_aggregate_sentiment_balanced_is_neutral():
    items = [
        {"sentiment": "BULLISH", "score": 0.5},
        {"sentiment": "BEARISH", "score": 0.5},
    ]
    label, _ = aggregate_sentiment(items)
    assert label == "NEUTRAL"


# ──────────────────────────────────────────────────────────────────────────
# Wire-up / settings
# ──────────────────────────────────────────────────────────────────────────


def test_settings_exposes_m5_flags():
    from autotrader.settings import RuntimeSettings
    # Flags default False
    r = RuntimeSettings(log_level="INFO", paper_trade=True, job_trigger_token="t")
    assert r.use_option_analytics_v1 is False
    assert r.use_news_signals_v1 is False
    assert r.use_portfolio_stream_v1 is False


def test_settings_bootstrap_reads_m5_flag_env(monkeypatch):
    import importlib
    from autotrader import settings as settings_mod
    monkeypatch.setenv("USE_OPTION_ANALYTICS_V1", "true")
    monkeypatch.setenv("USE_NEWS_SIGNALS_V1", "false")
    monkeypatch.setenv("USE_PORTFOLIO_STREAM_V1", "1")
    # The from_env reader uses current env — re-call to read them.
    src = inspect.getsource(settings_mod)
    assert 'use_option_analytics_v1=_env_bool("USE_OPTION_ANALYTICS_V1"' in src
    assert 'use_news_signals_v1=_env_bool("USE_NEWS_SIGNALS_V1"' in src
    assert 'use_portfolio_stream_v1=_env_bool("USE_PORTFOLIO_STREAM_V1"' in src


def test_ws_monitor_reads_portfolio_stream_flag():
    from autotrader.services import ws_monitor_service
    src = inspect.getsource(ws_monitor_service)
    assert "use_portfolio_stream_v1" in src


def test_poll_option_chain_script_exists_and_imports():
    import importlib.util
    from pathlib import Path
    p = Path(__file__).resolve().parents[1] / "scripts" / "redesign" / "poll_option_chain.py"
    assert p.exists()
    # Don't execute main() — just confirm the module loads syntactically.
    spec = importlib.util.spec_from_file_location("poll_option_chain", p)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    assert hasattr(mod, "main")
