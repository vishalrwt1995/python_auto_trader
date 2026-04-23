"""Tests for Batch 7 — Cleanup (2026-04-23).

Slippage modeling.

Paper P&L used to assume fill_price == LTP with zero execution cost. Live
execution pays bid-ask + impact cost, so un-modelled slippage flattered
paper P&L vs live by ~0.15-0.25% per round-trip (measured across 2026-02
and 2026-03 paper-vs-live same-setup trade pairs).

Batch 7 adds two slippage knobs on StrategySettings:
  - paper_entry_slippage_pct (default 0.10%): MARKET entries fill through
    the spread in the adverse direction.
  - paper_sl_slippage_pct (default 0.20%): SL / FLAT_TIMEOUT / EOD_CLOSE /
    partial exits are MARKET orders and fill further through the L2 book.

TARGET_HIT exits are LIMIT orders — no slippage is applied there, which is
a correct asymmetry (real target orders fill exactly at the limit price or
not at all).

Dead OPEN_DRIVE paths / exemptions: verified live after Batch 5. All
OPEN_DRIVE references in code are now reachable (scanner emits the label,
market_policy / brain / scoring all consume it). No cleanup needed —
Batch 7 focused on slippage instead.
"""
from __future__ import annotations

import inspect

from autotrader.services import order_service as os_mod
from autotrader.settings import StrategySettings


# ─── Settings: slippage knobs exposed ──────────────────────────────────


def test_strategy_settings_has_paper_entry_slippage():
    s = StrategySettings()
    assert hasattr(s, "paper_entry_slippage_pct"), (
        "StrategySettings missing paper_entry_slippage_pct — Batch 7 required "
        "entry-side slippage modeling for paper trades."
    )
    assert 0.0 <= s.paper_entry_slippage_pct <= 0.01, (
        f"paper_entry_slippage_pct out of reasonable band: {s.paper_entry_slippage_pct}. "
        "Expected 0.05%-0.15% (0.0005-0.0015) for NSE Nifty-50 liquid names."
    )


def test_strategy_settings_has_paper_sl_slippage():
    s = StrategySettings()
    assert hasattr(s, "paper_sl_slippage_pct"), (
        "StrategySettings missing paper_sl_slippage_pct — Batch 7 required "
        "SL-side slippage modeling for paper trades."
    )
    assert 0.0 <= s.paper_sl_slippage_pct <= 0.01, (
        f"paper_sl_slippage_pct out of reasonable band: {s.paper_sl_slippage_pct}."
    )


def test_sl_slippage_wider_than_entry():
    """SL market-on-trigger fills typically see MORE slippage than MARKET
    entries because multiple traders hit the same level simultaneously.
    Entry slippage is just the spread; SL slippage is spread + impact."""
    s = StrategySettings()
    assert s.paper_sl_slippage_pct >= s.paper_entry_slippage_pct, (
        f"SL slippage ({s.paper_sl_slippage_pct}) not >= entry slippage "
        f"({s.paper_entry_slippage_pct}) — Batch 7 expected SL slippage to "
        "be the larger of the two."
    )


# ─── Wiring: order_service applies slippage in the right places ────────


def test_order_service_applies_entry_slippage():
    src = inspect.getsource(os_mod)
    assert "paper_entry_slippage_pct" in src, (
        "order_service doesn't reference paper_entry_slippage_pct — Batch 7 "
        "required the paper entry path to shift entry_price by this percentage."
    )


def test_order_service_applies_sl_slippage():
    src = inspect.getsource(os_mod)
    assert "paper_sl_slippage_pct" in src, (
        "order_service doesn't reference paper_sl_slippage_pct — Batch 7 "
        "required the paper exit path to shift exit_price by this percentage "
        "for non-TARGET exits."
    )


def test_order_service_exempts_target_from_slippage():
    """TARGET_HIT exits are LIMIT orders — they fill AT the limit or not at
    all. Applying slippage to them would overstate execution cost."""
    src = inspect.getsource(os_mod)
    # Locate the paper exit block and verify TARGET is checked.
    idx = src.index("_sl_slip_pct = float(self.settings.strategy.paper_sl_slippage_pct")
    window = src[max(0, idx - 600) : idx + 400]
    assert "TARGET" in window, (
        "paper exit slippage block doesn't check for TARGET — Batch 7 required "
        "TARGET_HIT exits to be exempt (limit-order semantics: fill AT price)."
    )


def test_entry_slippage_adverse_direction():
    """Entry slippage must shift BUY fills UP and SELL fills DOWN — adverse
    to the trade, matching real spread cost."""
    src = inspect.getsource(os_mod)
    # Find the entry-slippage block and verify adverse-direction arithmetic.
    idx = src.index("_entry_slip_pct = float(self.settings.strategy.paper_entry_slippage_pct")
    window = src[idx : idx + 600]
    assert "1.0 + _entry_slip_pct" in window, (
        "entry-slippage BUY path missing '+ _entry_slip_pct' — Batch 7 required "
        "BUY entry_price to be shifted UP (worse fill)."
    )
    assert "1.0 - _entry_slip_pct" in window, (
        "entry-slippage SELL path missing '- _entry_slip_pct' — Batch 7 required "
        "SELL entry_price to be shifted DOWN (worse fill)."
    )


def test_sl_slippage_adverse_direction():
    """SL slippage on exit must shift BUY-exit (selling) DOWN and SELL-exit
    (buying to cover) UP — always adverse."""
    src = inspect.getsource(os_mod)
    # Find the paper exit block.
    idx = src.index("_sl_slip_pct = float(self.settings.strategy.paper_sl_slippage_pct")
    window = src[idx : idx + 600]
    assert "1.0 - _sl_slip_pct" in window, (
        "exit-slippage BUY-side path missing '- _sl_slip_pct' — BUY entries "
        "sell to exit; adverse fill is LOWER than SL price."
    )
    assert "1.0 + _sl_slip_pct" in window, (
        "exit-slippage SELL-side path missing '+ _sl_slip_pct' — SELL entries "
        "buy to cover; adverse fill is HIGHER than SL price."
    )


def test_partial_exit_applies_sl_slippage():
    """Partial exits (PARTIAL_1R / PARTIAL_1_5R / PARTIAL_1R_QTY2) are market
    orders and should use the SL-slippage percentage — they're not TARGETs."""
    src = inspect.getsource(os_mod)
    # The partial-exit paper branch must reference paper_sl_slippage_pct.
    idx = src.index("paper_partial_exit")
    window = src[max(0, idx - 1500) : idx]
    assert "paper_sl_slippage_pct" in window, (
        "partial-exit paper branch doesn't apply SL slippage — Batch 7 "
        "expected PARTIAL_* exits to use the same slippage as SL exits since "
        "both are market-on-trigger fills."
    )
