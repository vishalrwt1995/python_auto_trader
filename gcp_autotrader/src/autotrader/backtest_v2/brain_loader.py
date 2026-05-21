"""Brain snapshot loader — reads stored production brain state from GCS.

For the 10-week window (March 7 – May 21, 2026) production wrote brain
snapshots to GCS at `state/market_brain/history/{YYYY-MM-DD}/{HHMMSS}.json`.

Each snapshot contains:
  - `state.*`        — computed regime, risk_mode, scores
  - `context.*`      — raw inputs used: VIX, PCR, FII/DII, NIFTY, breadth,
                       leadership, stress, liquidity, data quality
  - `policy.*`       — MarketPolicy (allowed_strategies, multipliers, etc.)
                       at that moment
  - `narrative.*`    — human-readable explanation

This loader:
  - Lists/parses snapshot files for any historical day
  - Converts to `MarketBrainState` (production's brain model)
  - Converts to `RegimeSnapshot` (what scoring functions consume)
  - Returns `BrainSnapshot` container holding state + context + policy

Key invariant: the data we pass to `score_signal()` matches what production
saw at that moment — no approximation, no recompute.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from autotrader.adapters.gcs_store import GoogleCloudStorageStore
from autotrader.domain.models import (
    FiiDiiSnapshot,
    FreshnessSnapshot,
    MarketBrainState,
    NiftySnapshot,
    NiftyStructureSnapshot,
    PcrSnapshot,
    RegimeSnapshot,
)


SNAPSHOT_PREFIX = "state/market_brain/history"
NIFTY_INTRADAY_5M_PATH = "cache/watchlist_v2/index_intraday/5m/NSE_INDEX_NIFTY_50.json"

# 1m backfill from Upstox v3 (populated by backfill_intraday_1m.py)
NIFTY_1M_LOCAL = "cache__backfill__1m__NSE_INDEX_Nifty_50.json"
VIX_1M_LOCAL = "cache__backfill__1m__NSE_INDEX_India_VIX.json"


@dataclass
class BrainSnapshot:
    """Full container holding everything from a single snapshot file."""
    asof_ts: str
    state: MarketBrainState
    raw_state: dict[str, Any]          # full state.* dict
    raw_context: dict[str, Any]        # full context.* dict
    raw_policy: dict[str, Any]         # full policy.* dict
    raw_narrative: dict[str, Any] = field(default_factory=dict)

    def to_regime_snapshot(self) -> RegimeSnapshot:
        """Build the RegimeSnapshot that scoring functions consume."""
        ctx = self.raw_context
        # VIX from stressSnapshot
        stress = ctx.get("stressSnapshot", {})
        vix = float(stress.get("vix") or 0.0)

        # PCR — pull from optionsPositioning + map to PcrSnapshot fields
        opt = ctx.get("optionsPositioning", {})
        pcr = PcrSnapshot(
            pcr=float(opt.get("pcrWeighted") or 1.0),
            pcr_near=float(opt.get("pcrNear") or 1.0),
            pcr_weighted=float(opt.get("pcrWeighted") or 1.0),
            expiries_used=int(opt.get("expiriesUsed") or 0),
            confidence=float(opt.get("confidence") or 0.0),
        )

        # FII/DII
        flow = ctx.get("flowSnapshot", {})
        fii = FiiDiiSnapshot(
            fii=float(flow.get("fiiNet") or 0.0),
            dii=float(flow.get("diiNet") or 0.0),
            as_of_date=str(flow.get("asOfDate") or ""),
            freshness_score=float(flow.get("freshness") or 0.0),
        )

        # NIFTY — derive from regimeContext.daily
        rc_daily = ctx.get("regimeContext", {}).get("daily", {})
        nifty = NiftySnapshot(
            ltp=float(rc_daily.get("close") or 0.0),
            close=float(rc_daily.get("close") or 0.0),
            change_pct=0.0,  # not directly in snapshot; would need prev close
        )

        # NIFTY structure (intraday)
        rc_intra = ctx.get("regimeContext", {}).get("intraday", {})
        nifty_structure = NiftyStructureSnapshot(
            bars=int(rc_intra.get("bars") or 0),
            chop_risk=float(stress.get("chopRisk") or 0.0),
            atr_pct=float(rc_daily.get("atrPct") or 0.0),
        )

        # Freshness
        freshness = FreshnessSnapshot(
            generated_at=self.asof_ts,
            session_phase=self.state.phase,
        )

        regime = self.state.regime
        bias = _regime_to_bias(regime)

        return RegimeSnapshot(
            regime=regime,
            bias=bias,
            vix=vix,
            pcr=pcr,
            fii=fii,
            nifty=nifty,
            nifty_structure=nifty_structure,
            freshness=freshness,
            confidence=float(self.state.market_confidence),
            data_health=float(self.state.data_quality_score),
            source_quality=float(self.state.run_integrity_confidence),
            sub_regime=str(self.raw_state.get("sub_regime_v2") or "UNKNOWN"),
            rationale=" ; ".join(self.raw_state.get("reasons") or []),
            source="brain_snapshot_replay",
        )


def _regime_to_bias(regime: str) -> str:
    """Map regime to bias (mirrors production logic)."""
    r = (regime or "").upper()
    if r in ("TREND_UP", "RECOVERY"):
        return "BULLISH"
    if r in ("TREND_DOWN", "PANIC"):
        return "BEARISH"
    return "NEUTRAL"


def _build_state(raw_state: dict[str, Any]) -> MarketBrainState:
    """Reconstruct MarketBrainState from JSON dict (mirrors production's
    _state_from_dict in market_brain_service.py:67-114)."""
    def f(key: str, default: float = 50.0) -> float:
        v = raw_state.get(key)
        try:
            return float(v) if v is not None else default
        except Exception:
            return default

    def s(key: str, default: str) -> str:
        v = raw_state.get(key)
        return str(v) if v is not None else default

    return MarketBrainState(
        asof_ts=s("asof_ts", ""),
        phase=s("phase", "PREMARKET"),
        regime=s("regime", "RANGE"),
        participation=s("participation", "MODERATE"),
        risk_mode=s("risk_mode", "NORMAL"),
        trend_score=f("trend_score"),
        breadth_score=f("breadth_score"),
        leadership_score=f("leadership_score"),
        volatility_stress_score=f("volatility_stress_score"),
        liquidity_health_score=f("liquidity_health_score"),
        data_quality_score=f("data_quality_score"),
        market_confidence=f("market_confidence"),
        breadth_confidence=f("breadth_confidence"),
        leadership_confidence=f("leadership_confidence"),
        phase2_confidence=f("phase2_confidence"),
        policy_confidence=f("policy_confidence"),
        run_integrity_confidence=f("run_integrity_confidence"),
        options_positioning_score=f("options_positioning_score"),
        flow_score=f("flow_score"),
        breadth_roc_score=f("breadth_roc_score"),
        prev_regime=s("prev_regime", "RANGE"),
        regime_age_seconds=f("regime_age_seconds", 0.0),
        regime_transitions_today=int(raw_state.get("regime_transitions_today") or 0),
        signal_age_penalty=f("signal_age_penalty", 0.0),
    )


@dataclass
class BrainSnapshotLoader:
    """Loads brain snapshots from GCS with local-disk caching for speed."""
    bucket_name: str = "grow-profit-machine-autotrader-data"
    local_cache_dir: str | None = None

    def __post_init__(self) -> None:
        self.gcs = GoogleCloudStorageStore(self.bucket_name)
        if self.local_cache_dir is None:
            self.local_cache_dir = str(Path.home() / ".autotrader_backtest_cache" / "brain_snapshots")
        Path(self.local_cache_dir).mkdir(parents=True, exist_ok=True)
        self._dates_with_snapshots: list[str] | None = None
        self._snapshots_by_date: dict[str, list[str]] = {}  # date -> list of paths
        self._nifty_intraday_5m: list[list[Any]] | None = None  # NIFTY 5m cache for change_pct calc
        self._nifty_1m: list[list[Any]] | None = None  # 1m backfill cache
        self._vix_1m: list[list[Any]] | None = None
        self._nifty_daily_close_by_date: dict[str, float] | None = None  # date -> close for change_pct ref

    # ---------- discovery ----------

    def list_dates_with_snapshots(self) -> list[str]:
        """Return sorted list of YYYY-MM-DD dates that have brain snapshots.

        Derived from listing files under the SNAPSHOT_PREFIX and extracting
        the date directory from each path.
        """
        if self._dates_with_snapshots is not None:
            return self._dates_with_snapshots
        all_paths = self.gcs.list_paths(SNAPSHOT_PREFIX + "/")
        dates: set[str] = set()
        for p in all_paths:
            # Path: state/market_brain/history/YYYY-MM-DD/HHMMSS.json
            parts = p.split("/")
            if len(parts) >= 5 and parts[0] == "state" and parts[1] == "market_brain" and parts[2] == "history":
                d = parts[3]
                if len(d) == 10 and d[4] == "-" and d[7] == "-":
                    dates.add(d)
        self._dates_with_snapshots = sorted(dates)
        return self._dates_with_snapshots

    def list_snapshots_for_date(self, date: str) -> list[str]:
        """Return sorted list of snapshot paths for a date (sorted by time)."""
        if date in self._snapshots_by_date:
            return self._snapshots_by_date[date]
        prefix = f"{SNAPSHOT_PREFIX}/{date}/"
        paths = self.gcs.list_paths(prefix)
        # Sort by filename (HHMMSS.json) — chronological
        paths = sorted([p for p in paths if p.endswith(".json")])
        self._snapshots_by_date[date] = paths
        return paths

    # ---------- loading ----------

    def load_snapshot_file(self, path: str) -> BrainSnapshot:
        """Load a single snapshot JSON file from GCS (with local cache)."""
        local_fname = path.replace("/", "__")
        local_path = Path(self.local_cache_dir) / local_fname
        if local_path.exists():
            try:
                with open(local_path) as fh:
                    raw = json.load(fh)
            except Exception:
                raw = None
        else:
            raw = None
        if raw is None:
            raw = self.gcs.read_json(path, default={}) or {}
            try:
                with open(local_path, "w") as fh:
                    json.dump(raw, fh)
            except Exception:
                pass

        raw_state = raw.get("state") or {}
        return BrainSnapshot(
            asof_ts=str(raw_state.get("asof_ts") or ""),
            state=_build_state(raw_state),
            raw_state=raw_state,
            raw_context=raw.get("context") or {},
            raw_policy=raw.get("policy") or {},
            raw_narrative=raw.get("narrative") or {},
        )

    def find_snapshot_before(self, target_ts: str) -> BrainSnapshot | None:
        """Find the most recent snapshot at or before `target_ts`.

        `target_ts` is an ISO-8601 timestamp like '2026-05-20T11:23:00+05:30'.
        Returns None if no snapshot exists on or before that timestamp.
        """
        target_date = target_ts[:10]
        # Search this date's snapshots first (most likely hit)
        candidates = self.list_snapshots_for_date(target_date)
        # Path format: state/market_brain/history/YYYY-MM-DD/HHMMSS.json
        # Build a (path_ts, path) list and binary-search
        best: tuple[str, str] | None = None
        for p in candidates:
            fname = p.rsplit("/", 1)[-1].replace(".json", "")  # HHMMSS
            if len(fname) != 6:
                continue
            hh, mm, ss = fname[:2], fname[2:4], fname[4:6]
            path_ts = f"{target_date}T{hh}:{mm}:{ss}"
            # Compare ignoring timezone for simplicity (both are IST)
            if path_ts <= target_ts[:19]:
                if best is None or path_ts > best[0]:
                    best = (path_ts, p)
        if best:
            return self.load_snapshot_file(best[1])

        # Fall back to previous day's last snapshot if no match today
        prev_dates = [d for d in self.list_dates_with_snapshots() if d < target_date]
        if not prev_dates:
            return None
        last_date = prev_dates[-1]
        last_snapshots = self.list_snapshots_for_date(last_date)
        if not last_snapshots:
            return None
        return self.load_snapshot_file(last_snapshots[-1])

    # ---------- bulk operations ----------

    def load_all_snapshots_for_date(self, date: str) -> list[BrainSnapshot]:
        """Load every snapshot for a single date, sorted chronologically."""
        paths = self.list_snapshots_for_date(date)
        return [self.load_snapshot_file(p) for p in paths]

    # ---------- NIFTY change_pct + VIX helpers (1m precision) ----------

    def _load_nifty_5m(self) -> list[list[Any]]:
        if self._nifty_intraday_5m is None:
            data = self.gcs.read_json(NIFTY_INTRADAY_5M_PATH, default=[]) or []
            self._nifty_intraday_5m = data
        return self._nifty_intraday_5m

    def _load_nifty_1m(self) -> list[list[Any]]:
        if self._nifty_1m is None:
            local_path = Path(self.local_cache_dir).parent / NIFTY_1M_LOCAL
            if local_path.exists():
                try:
                    with open(local_path) as fh:
                        self._nifty_1m = json.load(fh)
                except Exception:
                    self._nifty_1m = []
            else:
                self._nifty_1m = []
        return self._nifty_1m or []

    def _load_vix_1m(self) -> list[list[Any]]:
        if self._vix_1m is None:
            local_path = Path(self.local_cache_dir).parent / VIX_1M_LOCAL
            if local_path.exists():
                try:
                    with open(local_path) as fh:
                        self._vix_1m = json.load(fh)
                except Exception:
                    self._vix_1m = []
            else:
                self._vix_1m = []
        return self._vix_1m or []

    def _ltp_at(self, series: list[list[Any]], scan_ts: str) -> float | None:
        """Find the close of the most recent bar at or before scan_ts."""
        if not series:
            return None
        scan_short = scan_ts[:19]
        # Linear scan; series is sorted ascending by ts
        best_ltp = None
        for c in series:
            ts = str(c[0])[:19]
            if ts <= scan_short:
                best_ltp = float(c[4])
            else:
                break
        return best_ltp

    def nifty_ltp_at(self, scan_ts: str) -> float | None:
        """Precise NIFTY LTP at scan_ts using 1m backfill (falls back to 5m)."""
        ltp = self._ltp_at(self._load_nifty_1m(), scan_ts)
        if ltp is not None:
            return ltp
        return self._ltp_at(self._load_nifty_5m(), scan_ts)

    def vix_at(self, scan_ts: str) -> float | None:
        """Precise VIX value at scan_ts using 1m backfill."""
        return self._ltp_at(self._load_vix_1m(), scan_ts)

    def nifty_change_pct_at(self, scan_ts: str) -> float:
        """Compute NIFTY change_pct at scan_ts using:
            LTP from 1m backfill (or 5m fallback)
            prev_close from yesterday's last 1m bar (or daily close fallback)
        """
        ltp = self.nifty_ltp_at(scan_ts)
        if ltp is None:
            return 0.0
        # Find yesterday's last close from 1m series
        scan_date = scan_ts[:10]
        yest_close = None
        for c in reversed(self._load_nifty_1m()):
            ts = str(c[0])[:10]
            if ts < scan_date:
                yest_close = float(c[4])
                break
        if yest_close is None:
            for c in reversed(self._load_nifty_5m()):
                ts = str(c[0])[:10]
                if ts < scan_date:
                    yest_close = float(c[4])
                    break
        if yest_close is None or yest_close <= 0:
            return 0.0
        return (ltp - yest_close) / yest_close * 100.0
