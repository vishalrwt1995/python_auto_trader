# Build Plan — M0 → M9 (Paper + Live)

**Branch:** `redesign/audit-and-design` (same branch; keep diffs reviewable)
**Mode:** build complete → test complete (no staged market testing between milestones)
**Scope coverage:** paper **and** live from day one — no paper-only shortcuts
**Anti-over-engineering rules:**
1. No new frameworks. Use plain Python modules + dataclasses + Firestore docs.
2. No new services until a feature is used twice.
3. Prefer extending `settings.StrategySettings` over creating new config classes.
4. Backtest harness = 1 script, not an engine.
5. Dashboards / UI come after numbers prove out; code first.
6. Every knob defaults to the **current** behaviour until flipped — flag-gated rollout.

---

## Milestone map

| M | Name | Code? | Key deliverable |
|---|---|---|---|
| M0 | Safety net | ✓ | kill-switch, fail-closed, exit fallback, GTT assert, paper GTT, MFE/MAE |
| M1 | Exit state machine | ✓ | 5-state exit FSM, breakeven 0.8R+debounce, trail 2×ATR in RUNNER only, branched FLAT |
| M2 | Thesis + Playbook + Edge | ✓ | Playbook hard-block replaces pass-through; Edge registry; Thesis dataclass |
| M3 | expected_edge_R | ✓ | backtest harness, priors doc, scoring replaced |
| M4 | PortfolioBook | ✓ | channel budgets, DD governors, kill-switch integrates |
| M5 | Upstox expansion | ✓ | option chain poll, option greeks WS, news service, portfolio stream |
| M6 | Observability | ✓ | AttributionLog table+writer, daily_metrics rollup, alerts |
| M7 | Full-stack tests | ✓ (tests) | replay fixtures, integration tests, paper-run checklist |
| M8 | Canary runbook | no code | ops runbook, rollback plan, 10-day checklist |
| M9 | Full-live runbook | no code | 30-day ramp plan, success gates, revert triggers |

---

## Paper/Live parity rules

1. Every feature that writes to a Firestore/BQ table writes the same schema in both modes.
2. Paper slippage and brokerage models are explicit; live uses broker-reported values; both are logged to the same `cost_R_*` columns.
3. Paper GTT is a Firestore doc polled by ws_monitor + a 60s cron; live GTT goes to Upstox. Both expose the same interface to the exit state machine.
4. `allow_live_orders` flag continues to gate real order placement; nothing else diverges between paper & live.

---

## Build order & commit discipline

- One milestone per commit series. Each commit stands alone (tests pass, imports resolve).
- Feature flags default OFF; we flip a single `settings.runtime.feature_vN` to turn on the new behaviour, allowing trivial revert.
- No delete-and-rewrite. We add new code beside the old, flag-switch, then remove the old once the flag has been stable ≥ N days.

---

## Test strategy

- Unit tests per module (existing `tests/` structure).
- Integration tests: new `tests/integration/` package — uses sandbox Upstox token + fake clock.
- Replay test: new `tests/replay/` — captured WS tick stream, asserts identical exit sequence.
- No new mocking frameworks; continue with unittest + the existing Upstox/Firestore fakes.

---

## Out of scope for M0–M6

- ML-based scoring (Edges stay rule-based).
- Options-strategy edges (hedge channel is scaffolded in M4 but first edges land post-M9).
- Dashboard refresh (M6 wires data; UI reshuffle is a follow-up).
- Multi-broker support.
- DMA / colo.

---

## Progress ledger

| M | status | commits |
|---|---|---|
| M0 | started | _pending_ |
| M1 | pending | — |
| M2 | pending | — |
| M3 | pending | — |
| M4 | pending | — |
| M5 | pending | — |
| M6 | pending | — |
| M7 | pending | — |
| M8 | pending | — |
| M9 | pending | — |
