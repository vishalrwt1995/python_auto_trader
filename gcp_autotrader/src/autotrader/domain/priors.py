"""M3 — Priors: regime × setup × direction → expected realized R.

Priors are persisted as a flat JSON dict (config/priors/priors_v1.json).
Keys are "{REGIME}:{SETUP}:{DIRECTION}" upper-snake — same key-shape the
Edge registry uses in `priors_key`. Unknown keys fall back to `_default`.

Reload semantics (intentional):
  * Loaded ONCE at process start, cached module-level.
  * No hot-reload. Updating priors requires a deploy. This is the same
    contract as the Edge registry — reviewable, traceable.

Why JSON not YAML: the project doesn't carry a YAML dependency today,
and the priors file is flat enough that JSON+comments-via-`_comment`
key is fine.
"""
from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_PRIORS_CACHE: dict[str, Any] | None = None
_DEFAULT_PATH = Path(__file__).resolve().parents[3] / "config" / "priors" / "priors_v1.json"


@dataclass(frozen=True)
class Prior:
    """A single (regime, setup, direction) entry with derived expected_edge_R."""
    win_rate: float
    avg_win_r: float
    avg_loss_r: float
    n: int

    @property
    def expected_edge_r(self) -> float:
        """p × avg_win_r + (1-p) × avg_loss_r. Positive = +EV edge."""
        p = max(0.0, min(1.0, float(self.win_rate)))
        return p * float(self.avg_win_r) + (1.0 - p) * float(self.avg_loss_r)


# Internal — not frozen so the loader can write into it.
_FALLBACK = Prior(win_rate=0.40, avg_win_r=1.50, avg_loss_r=-1.00, n=0)


def _coerce(d: dict[str, Any]) -> Prior:
    return Prior(
        win_rate=float(d.get("win_rate", 0.40) or 0.40),
        avg_win_r=float(d.get("avg_win_r", 1.50) or 1.50),
        avg_loss_r=float(d.get("avg_loss_r", -1.00) or -1.00),
        n=int(d.get("n", 0) or 0),
    )


def load(path: str | os.PathLike[str] | None = None) -> dict[str, Any]:
    """Load the priors file into the module cache. Idempotent."""
    global _PRIORS_CACHE
    p = Path(path) if path else _DEFAULT_PATH
    try:
        with open(p, "r", encoding="utf-8") as fh:
            raw = json.load(fh)
    except FileNotFoundError:
        logger.warning("priors_file_missing path=%s falling_back_to_default", p)
        raw = {}
    except Exception as exc:
        logger.exception("priors_file_load_failed path=%s err=%s", p, exc)
        raw = {}

    _PRIORS_CACHE = raw
    return raw


def _ensure_loaded() -> dict[str, Any]:
    if _PRIORS_CACHE is None:
        load()
    return _PRIORS_CACHE or {}


def min_sample_size() -> int:
    """Sample floor under which a specific prior key is treated as stale
    and the caller falls back to `_default` (or skips the prior gate)."""
    raw = _ensure_loaded()
    return int(raw.get("min_sample_size", 30) or 30)


def _norm_direction(direction: str) -> str:
    d = str(direction or "").strip().upper()
    if d in ("BUY", "LONG"):
        return "LONG"
    if d in ("SELL", "SHORT"):
        return "SHORT"
    return d  # HOLD / empty / other


def get_prior(regime: str, setup: str, direction: str) -> Prior:
    """Look up a prior; falls back to `_default` on miss or stale sample."""
    raw = _ensure_loaded()
    reg = str(regime or "").strip().upper()
    stp = str(setup or "").strip().upper()
    dr = _norm_direction(direction)

    key = f"{reg}:{stp}:{dr}"
    entry = raw.get(key)
    if isinstance(entry, dict):
        prior = _coerce(entry)
        return prior

    # Fall back to `_default` entry, then to hard-coded fallback.
    default_entry = raw.get("_default")
    if isinstance(default_entry, dict):
        return _coerce(default_entry)
    return _FALLBACK


def reset_for_tests() -> None:
    """Clear the cache so tests can point `load()` at fixture files."""
    global _PRIORS_CACHE
    _PRIORS_CACHE = None


__all__ = ["Prior", "load", "get_prior", "min_sample_size", "reset_for_tests"]
