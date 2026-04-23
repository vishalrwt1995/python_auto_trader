"""M3 — Expected-edge-R gate.

Composes signal_score + priors into a single scalar (expected_edge_R)
and a hard-block rule: "if expected_edge_R ≤ 0, DO NOT enter."

This is the new top-layer gate in the entry pipeline (DESIGN.md §6).
Sits behind USE_EXPECTED_EDGE_R_V1 flag; when OFF, it's a no-op.

Design:
  * Pure function — reads priors via `priors.get_prior`, no I/O beyond
    that.
  * Stale-prior guard: if `n < min_sample_size`, we do NOT block purely
    on expected_edge_R (we don't have enough evidence). We only block
    once the sample count crosses the threshold AND the edge is ≤ 0.
    This prevents the seed-priors-only state from over-blocking.
  * The signal_score still matters — this gate is additive, not a
    replacement. A signal needs BOTH a high score AND a +EV prior.
"""
from __future__ import annotations

from dataclasses import dataclass

from autotrader.domain.priors import Prior, get_prior, min_sample_size


@dataclass(frozen=True)
class ExpectedEdgeResult:
    allowed: bool
    reason: str                  # empty when allowed, else e.g. "expected_edge_non_positive"
    expected_edge_r: float
    prior: Prior


def evaluate(
    regime: str,
    setup: str,
    direction: str,
    *,
    min_expected_edge_r: float = 0.0,
) -> ExpectedEdgeResult:
    """Return (allowed, reason, expected_edge_r, prior)."""
    p = get_prior(regime=regime, setup=setup, direction=direction)
    edge = p.expected_edge_r

    # Stale guard: if we don't have enough samples, don't block on EV.
    n_floor = min_sample_size()
    if p.n < n_floor:
        return ExpectedEdgeResult(
            allowed=True,
            reason="",
            expected_edge_r=edge,
            prior=p,
        )

    if edge <= float(min_expected_edge_r):
        return ExpectedEdgeResult(
            allowed=False,
            reason="expected_edge_non_positive",
            expected_edge_r=edge,
            prior=p,
        )
    return ExpectedEdgeResult(allowed=True, reason="", expected_edge_r=edge, prior=p)


__all__ = ["ExpectedEdgeResult", "evaluate"]
