"""Tests for the 2026-05-08 mid-session pipeline fixes.

These guard the changes made during the live trading session after
identifying issues from real production behavior:

  Fix 2a: Swing channel scans full watchlist (no rotational batching)
  Fix 8 : Phase 2 eligibility lowered today_bars < 4 → < 3
"""
from __future__ import annotations

import inspect

from autotrader.services import trading_service as ts_mod
from autotrader.services import universe_service as us_mod


# ─── Fix 2a: swing full-batch ───────────────────────────────────────────


def test_swing_full_batch_no_rotation():
    """For wl_type='swing', _slice_watchlist_for_scan must return ALL rows
    regardless of total size. No rotation, no cursor advancement.

    Background: with 1-4 swing scans per day and the previous rotation
    of 35/150 rows per tick, each row was evaluated only once every 4-5
    days. Today's 5 MEAN_REVERSION rows had only 1 evaluated; the other
    4 weren't touched.
    """
    src = inspect.getsource(ts_mod.TradingService._slice_watchlist_for_scan)
    # The swing branch must exist and short-circuit before the rotation logic
    assert 'wl_type or ""' in src or 'wl_type:' in src, (
        "_slice_watchlist_for_scan must accept wl_type to differentiate swing"
    )
    assert '"swing"' in src, (
        "_slice_watchlist_for_scan must check for swing wl_type to skip rotation"
    )


def test_swing_slice_returns_full_watchlist():
    """Functional check: a 200-row watchlist with wl_type='swing' returns
    all 200 rows. The rotation cursor mechanism is bypassed."""

    # Build a stub trading service exposing only the slice function. The
    # function only touches `self.state` to manage the cursor, which we
    # don't need for swing (no rotation).
    class _StubState:
        def get_runtime_prop(self, k, default=""):
            return default
        def set_runtime_prop(self, k, v):
            pass

    class _Stub:
        state = _StubState()
        _slice_watchlist_for_scan = ts_mod.TradingService._slice_watchlist_for_scan

    # Fake watchlist of 200 rows (more than core+batch=10+25=35)
    watchlist = list(range(200))
    subset, meta = _Stub._slice_watchlist_for_scan(_Stub(), watchlist, wl_type="swing")
    assert len(subset) == 200, f"swing must scan all 200 rows, got {len(subset)}"
    assert meta["total"] == 200
    assert meta["scanned"] == 200
    assert meta["wrapped"] is True


def test_intraday_slice_still_rotates():
    """Sanity: intraday watchlist with rows > core+batch still uses rotational
    batching. We don't want the swing fix to accidentally break intraday."""
    class _StubState:
        _cursor = "0"
        def get_runtime_prop(self, k, default=""):
            return self._cursor
        def set_runtime_prop(self, k, v):
            self._cursor = v

    class _Stub:
        state = _StubState()
        _slice_watchlist_for_scan = ts_mod.TradingService._slice_watchlist_for_scan

    # 200 rows; intraday should return only 35 (10 core + 25 rotated)
    watchlist = list(range(200))
    subset, meta = _Stub._slice_watchlist_for_scan(_Stub(), watchlist, wl_type="intraday")
    assert len(subset) == 35, f"intraday batch should be 35, got {len(subset)}"
    assert meta["scanned"] == 35
    assert meta["core"] == 10
    assert meta["rotated"] == 25
    assert meta["wrapped"] is False  # 200 > 35, not wrapped


def test_default_wl_type_treated_as_intraday():
    """If wl_type is empty/missing, behaviour matches intraday (rotation)."""
    class _StubState:
        def get_runtime_prop(self, k, default=""):
            return "0"
        def set_runtime_prop(self, k, v):
            pass

    class _Stub:
        state = _StubState()
        _slice_watchlist_for_scan = ts_mod.TradingService._slice_watchlist_for_scan

    watchlist = list(range(200))
    # No wl_type kwarg
    subset_default, meta_default = _Stub._slice_watchlist_for_scan(_Stub(), watchlist)
    # Empty wl_type
    subset_empty, meta_empty = _Stub._slice_watchlist_for_scan(_Stub(), watchlist, wl_type="")
    assert len(subset_default) == 35
    assert len(subset_empty) == 35


# ─── Fix 8: Phase 2 bar threshold 4 → 3 ─────────────────────────────────


def test_phase2_bar_threshold_lowered_to_three():
    """Phase 2 eligibility now passes with 3 today bars (was 4). This
    means Phase 2 fires from the first ~09:25 IST watchlist build instead
    of waiting until 09:30. Recovers ~5 min of intraday-momentum coverage
    at market open.

    The first 3 bars cover 09:15-09:25 = 15 min = full ORB window which
    is what the momentum-scoring formula actually needs."""
    src = inspect.getsource(us_mod.UniverseService._phase2_eligibility)
    # The threshold check must use < 3, not < 4
    assert "if len(today_bars) < 3:" in src, (
        "Phase 2 today_bars threshold must be 3 (was 4) per 2026-05-08 "
        "mid-session fix. Lower threshold lets Phase 2 fire ~5 min earlier."
    )
    # The previous threshold should NOT be present anymore
    assert "if len(today_bars) < 4:" not in src, (
        "Old `today_bars < 4` threshold still present — fix didn't take"
    )


def test_phase2_required_slots_unchanged():
    """The required-slots logic (ORB anchors + last 4) hasn't changed.
    Only the bar-count gate was relaxed."""
    src = inspect.getsource(us_mod.UniverseService._phase2_required_slots)
    # ORB anchor slots still required for 5m
    for slot in ("09:15", "09:20", "09:25"):
        assert f'"{slot}"' in src, (
            f"ORB anchor slot {slot} must still be required by Phase 2"
        )
