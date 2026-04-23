"""M2 — Playbook: regime × edge hard-block gate.

Replaces the old "pass-through on unknown regime" behaviour
(see AUDIT.md §5) with a hard-block: a (setup, direction, regime,
risk_mode) tuple that doesn't match any registered Edge's allowed
set is DENIED. Fail-closed — unknown → blocked.

Wire-up: `trading_service.py` calls `check_playbook(...)` after the
existing strategy-entry gate, gated behind `settings.runtime.use_playbook_v1`.
When the flag is off the call is skipped and the old behaviour
(pass-through) is preserved — this is the ROLLOUT contract from
DESIGN.md §9.

Design notes:
  * Pure function — no I/O, no Firestore. Same inputs → same output.
  * Reads from the Edge registry (module-level dict populated at
    import time by `edge._register_defaults`). No DB, no hot-reload.
  * `setup` is compared case-insensitively to match what the scanner
    emits (BREAKOUT, breakout, Breakout are all equal).
  * Direction "HOLD" short-circuits to allowed — nothing to block.
  * Unknown setups or direction "BOTH" fall through to DENIED so that
    shipping a new scanner setup without registering an Edge triggers
    a clear block, not a silent admission.
"""
from __future__ import annotations

from autotrader.domain.edge import Edge, all_edges


def _normalize_setup(setup: str) -> str:
    # Scanner emits e.g. "SHORT_BREAKDOWN" while the edge registry uses
    # "BREAKOUT" for both sides (direction discriminates). Collapse the
    # known scanner aliases to their registry counterpart.
    s = str(setup or "").strip().upper()
    aliases = {
        "SHORT_BREAKDOWN": "BREAKOUT",
        "SHORT_PULLBACK": "PULLBACK",
        "VWAP_REVERSAL": "MEAN_REVERSION",
        "VWAP_TREND": "MOMENTUM",
        "PHASE1_MOMENTUM": "MOMENTUM",
        "PHASE1_REVERSAL": "MEAN_REVERSION",
    }
    return aliases.get(s, s)


def _direction_allowed(edge: Edge, direction: str) -> bool:
    d = str(direction or "").strip().upper()
    if edge.direction == "BOTH":
        return d in ("BUY", "SELL", "LONG", "SHORT")
    want_long = edge.direction == "LONG"
    if want_long:
        return d in ("BUY", "LONG")
    return d in ("SELL", "SHORT")


def matching_edges(setup: str, direction: str) -> list[Edge]:
    """Return edges from the registry that match (setup, direction).

    Exposed for attribution/logging — the Playbook itself uses it
    internally but callers may want to surface "which edge fired".
    """
    setup_n = _normalize_setup(setup)
    out: list[Edge] = []
    for e in all_edges():
        if _normalize_setup(e.setup) != setup_n:
            continue
        if not _direction_allowed(e, direction):
            continue
        out.append(e)
    return out


def check_playbook(
    setup: str,
    direction: str,
    regime: str,
    risk_mode: str = "NORMAL",
) -> tuple[bool, str]:
    """Hard-block gate — return (allowed, reason_if_blocked).

    Reason strings are namespaced with 'playbook_' so log-grep can
    distinguish playbook blocks from the older strategy_-prefixed
    ones emitted by `check_strategy_entry`.

    Blocks (fail-closed):
      * playbook_no_edge_registered — no Edge matches (setup, direction)
      * playbook_regime_not_allowed — matching Edge(s) exist but none
        list this regime in allowed_regimes
      * playbook_risk_mode_not_allowed — Edge allows regime but not
        this risk_mode (e.g. LOCKDOWN blocks aggressive edges)
    """
    d = str(direction or "").strip().upper()
    if d in ("HOLD", ""):
        return True, ""
    r = str(regime or "").strip().upper()
    rm = str(risk_mode or "").strip().upper() or "NORMAL"

    candidates = matching_edges(setup, direction)
    if not candidates:
        return False, "playbook_no_edge_registered"

    # First check any edge allows this regime; THEN check risk_mode on
    # the regime-allowed subset. Reporting "regime not allowed" when the
    # user is actually tripping the risk_mode gate was misleading.
    regime_ok = [e for e in candidates if r in e.allowed_regimes]
    if not regime_ok:
        return False, "playbook_regime_not_allowed"

    risk_ok = [e for e in regime_ok if rm in e.allowed_risk_modes]
    if not risk_ok:
        return False, "playbook_risk_mode_not_allowed"

    return True, ""


__all__ = ["check_playbook", "matching_edges"]
