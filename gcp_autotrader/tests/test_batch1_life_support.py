"""Tests for Batch 1 — Life Support (2026-04-22).

Covers the four "dead setting / silent failure" fixes that bring the
system to a baseline of behavioural consistency:

1.1 max_trades_day enforcement — previously defined in StrategySettings
    but never read by the trading path. Trading could blow past the
    intended cap indefinitely, burning fixed per-trade costs.

1.2 Swing watchlist reporting outside premarket — prior `swing_written =
    bool(premarket)` caused every intraday rebuild to report 0 swing
    selected rows even though the Firestore watchlist was being rewritten
    with the full swing set. Log noise and incorrect telemetry.

1.3 SWING_MIN_SIGNAL_SCORE source-of-truth — dataclass default was 70
    (P1 calibration) but from_env() default was still 75, so production
    silently used 75 while unit tests constructing StrategySettings()
    directly saw 70.

1.4 Silent except-pass sites replaced with DEBUG logs so stale-brain-cap
    and earnings-blackout failures are diagnosable.
"""
from __future__ import annotations

import inspect
import os
from pathlib import Path
from unittest.mock import patch

from autotrader.services import trading_service as ts_mod
from autotrader.services import universe_service as us_mod
from autotrader.settings import AppSettings, StrategySettings


# ─── 1.1: max_trades_day enforcement ────────────────────────────────────


def test_max_trades_day_enforcement_present_in_scan_loop():
    """run_scan_once must SKIP when today's trade count hits max_trades_day.

    We validate this structurally (source contains the check + the skip
    reason string) rather than running the whole scanner with a mocked
    Firestore — the scan loop has too much fixture surface to exercise
    end-to-end in a unit test. This guards against silent revert.
    """
    src = inspect.getsource(ts_mod)
    assert "get_today_trade_count" in src, (
        "max_trades_day enforcement missing — expected call to "
        "state.get_today_trade_count(today_iso) in run_scan_once. "
        "Batch 1.1 (2026-04-22)."
    )
    assert "max_trades_day_hit" in src, (
        "max_trades_day circuit-breaker reason string missing — we need a "
        "distinct skip reason so dashboards can surface the cap being hit."
    )
    assert "cfg.max_trades_day" in src, (
        "max_trades_day check must read from cfg (so runtime overrides work), "
        "not from settings.strategy.max_trades_day (which bypasses Firestore "
        "config/{key} overrides)."
    )


def test_firestore_state_has_get_today_trade_count():
    """The Firestore adapter must expose a counter used by the scanner."""
    from autotrader.adapters import firestore_state as fs_mod
    src = inspect.getsource(fs_mod)
    assert "def get_today_trade_count" in src, (
        "firestore_state.get_today_trade_count() missing — scanner relies on it "
        "to count today's positions (OPEN + CLOSED)."
    )
    # Must count ALL positions entered today (not just CLOSED) — a position
    # that was opened and stopped-out earlier today still consumes a slot.
    assert "entry_ts" in src, (
        "get_today_trade_count must filter by entry_ts, not exit_ts — positions "
        "entered today that are still OPEN must count toward the daily cap."
    )


# ─── 1.2: swing watchlist persistence outside premarket ────────────────


def test_swing_written_not_gated_by_premarket():
    """Intraday rebuilds must report swing count correctly.

    Previously `swing_written = bool(premarket)` caused `swingSelected` to
    be reported as 0 on every intraday rebuild even though the Firestore
    watchlist document was being rewritten with the full swing set.
    """
    src = inspect.getsource(us_mod)
    # We only care about the executable assignment, not a historical reference
    # in a comment. Look for the pattern at the start of a line (i.e. real
    # code, not inside a `# ... prior logic ...` comment).
    import re
    bad = re.search(r"^\s{4,8}swing_written = bool\(premarket\)", src, re.MULTILINE)
    assert bad is None, (
        "swing_written still gated by premarket — intraday rebuilds under-report "
        "swingSelected=0 even though Firestore watchlist does include swing rows. "
        "Batch 1.2 (2026-04-22)."
    )
    # The new pattern separates BQ audit persistence (premarket-only) from
    # the reporting flag (always true).
    assert "swing_bq_persisted" in src or "swing_written = True" in src, (
        "expected either an always-true swing_written flag or an explicit "
        "swing_bq_persisted split so the BQ audit write can stay premarket-gated "
        "without the reporting counter going stale."
    )


# ─── 1.3: SWING_MIN_SIGNAL_SCORE source-of-truth ───────────────────────


def test_swing_min_signal_score_env_default_matches_dataclass():
    """from_env() must use the same default as the dataclass.

    Divergence previously meant prod (uses from_env) saw 75 while tests
    (construct StrategySettings() directly) saw 70 — the P1 calibration
    silently never took effect in prod because no SWING_MIN_SIGNAL_SCORE
    env var is set in Cloud Run.
    """
    # Dataclass default
    assert StrategySettings().swing_min_signal_score == 70
    # from_env() default must match
    settings_src = Path(__file__).parent.parent / "src" / "autotrader" / "settings.py"
    text = settings_src.read_text()
    # The env-loader line must reference the same literal 70
    assert '_env_int("SWING_MIN_SIGNAL_SCORE", 70)' in text, (
        "settings.from_env() SWING_MIN_SIGNAL_SCORE env default must be 70 to "
        "match the dataclass default. Mismatch silently overrides the P1 "
        "calibration in production. Batch 1.3 (2026-04-22)."
    )
    assert '_env_int("SWING_MIN_SIGNAL_SCORE", 75)' not in text, (
        "from_env() still has the old pre-P1 env default (75) — revert?"
    )


def test_from_env_produces_swing_threshold_70_when_env_unset():
    """End-to-end: with no SWING_MIN_SIGNAL_SCORE env var, from_env → 70."""
    # Save/restore env so the test is hermetic
    saved = os.environ.pop("SWING_MIN_SIGNAL_SCORE", None)
    try:
        # Need all required env vars to build AppSettings; just assert the
        # env loader we care about. Build a minimal env so from_env works.
        required = {
            "GCP_PROJECT_ID": "test",
            "GCS_BUCKET": "test-bucket",
            "UPSTOX_CLIENT_ID_SECRET_NAME": "n",
            "UPSTOX_CLIENT_SECRET_SECRET_NAME": "n",
            "UPSTOX_ACCESS_TOKEN_SECRET_NAME": "n",
            "UPSTOX_ACCESS_TOKEN_EXPIRY_SECRET_NAME": "n",
            "JOB_TRIGGER_TOKEN": "t",
        }
        with patch.dict(os.environ, required, clear=False):
            app = AppSettings.from_env()
            assert app.strategy.swing_min_signal_score == 70, (
                f"from_env produced {app.strategy.swing_min_signal_score} — "
                "expected 70 (P1 calibration). If this is 75 the env-default "
                "alignment regressed."
            )
    finally:
        if saved is not None:
            os.environ["SWING_MIN_SIGNAL_SCORE"] = saved


# ─── 1.4: silent except-pass sites replaced with DEBUG logs ────────────


def test_stale_brain_cap_site_logs_on_failure():
    """The stale-brain size cap except-handler must emit a debug log.

    Previously `except Exception: pass` hid parse_any_ts failures that
    left stale brain state uncapped — the opposite of defensive.
    """
    src = inspect.getsource(ts_mod)
    assert "stale_brain_state_cap_failed" in src, (
        "stale-brain cap except-handler no longer logs — silent except-pass "
        "regressed. Batch 1.4 (2026-04-22)."
    )


def test_earnings_blackout_site_logs_on_failure():
    """The earnings-blackout read except-handler must emit a debug log."""
    src = inspect.getsource(ts_mod)
    assert "earnings_blackout_read_failed" in src, (
        "earnings-blackout read except-handler no longer logs — silent "
        "except-pass regressed. Batch 1.4 (2026-04-22)."
    )


def test_no_bare_pass_in_critical_except_handlers():
    """Scan trading_service for newly-introduced `except Exception: pass`.

    This is a weak structural guard — we can't banish bare-pass entirely
    because some handlers intentionally swallow (e.g. malformed earnings
    date). But the two sites we fixed in 1.4 must not regress to pass.
    """
    src = inspect.getsource(ts_mod)
    # The stale-brain block: the `except Exception: pass` immediately
    # following the size-cap try-block
    # Count total `except Exception:\n                        pass` occurrences
    # at the deeply-nested indentation used by those two sites.
    import re
    bad_pattern = re.compile(
        r"except Exception:\s*\n\s*pass\s*#\s*best-effort;?\s*don't break the scan",
        re.MULTILINE,
    )
    assert not bad_pattern.search(src), (
        "stale-brain cap still uses silent `except Exception: pass # best-effort` "
        "— Batch 1.4 expected this replaced with logger.debug(..., exc_info=True)."
    )
    bad_pattern_earnings = re.compile(
        r"except Exception:\s*\n\s*pass\s*#\s*earnings blackout is best-effort",
        re.MULTILINE,
    )
    assert not bad_pattern_earnings.search(src), (
        "earnings-blackout read still uses silent `except Exception: pass # "
        "earnings blackout is best-effort` — Batch 1.4 expected logger.debug."
    )
