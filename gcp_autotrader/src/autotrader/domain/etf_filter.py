"""Shared non-equity (ETF / mutual-fund unit) exclusion — the STOCK-ONLY rule in one place.

WHY THIS MODULE EXISTS (2026-08-28). Two real defects, both live, found from one ERROR log:

1. **THREE drifted copies of `is_etf`** — `delivery_signals` (29 curated names),
   `insider_signals` (13) and `pledge_signals` (13, identical to insider). The 08-07 ETF fix
   (§8 ㉔) landed in delivery/universe but NEVER reached insider or pledge, so they were missing
   **20 names** including `GOLDSHARE`, `LIQUIDCASE` and `LIQUIDADD` — and `LIQUIDCASE` is one of
   the exact ETFs ㉔ records as having reached the SIGNAL stage. Those two channels would trade
   them today. (delivery lacked 4 that the others listed, but all 4 end in ``BEES`` and were
   already caught by the pattern, so delivery loses nothing here.)

2. **Name matching cannot be complete.** `NSE_EQ|INF740KA1SW3` reached delivery's bar fetch on
   2026-08-28 and was stopped only because Upstox returned HTTP 400 — luck, not design. Its ticker
   matches no pattern and was in no curated list. The ISIN, however, is unambiguous: NSE fund
   units carry an **INF** prefix where company equity carries **INE**.

So: curated names are the UNION of all three lists, and the ISIN prefix is added as a second,
independent layer. Filtering can only get MORE restrictive than any previous copy, never less —
which is the only safe direction for a stock-only mandate.

Note on scope: an ``INF`` ISIN is a *positive* indicator of a fund, not a complete test — the name
layer stays because a fund could in principle carry another prefix. The two layers are additive.
"""
from __future__ import annotations

# Union of the three previously-drifted lists (delivery 29 | insider 13 | pledge 13 = 33).
# Names that do NOT match any pattern below must be listed here explicitly.
ETF_CURATED: frozenset[str] = frozenset({
    # broad-market / index funds
    "NASDAQ", "N100", "MASPTOP50", "MAFANG", "QNIFTY", "MOM30", "MON100",
    "MOM50", "MOM100", "ICICIB22",
    "SETFNIF50", "SETFNIFBK", "SETFNIFTY", "KOTAKNIFTY", "AXISNIFTY", "UTINIFTETF",
    "NIFTYBEES", "BANKBEES", "CPSEETF", "PSUBANK",
    # gold / commodity
    "SETFGOLD", "QGOLDHALF", "GOLDSHARE", "GOLDBEES", "KOTAKGOLD", "AXISGOLD",
    "TATAGOLD", "GROWWGOLD",
    # liquid / cash-equivalent
    "LIQUID", "LIQUIDADD", "LIQUIDCASE", "LIQUIDBEES", "HDFCLIQUID",
})

# NSE ISIN prefixes. Company equity is INE...; mutual-fund units (which is what an ETF is) are
# INF... Anything INF is not a stock.
_FUND_ISIN_PREFIXES: tuple[str, ...] = ("INF",)


def is_etf_symbol(symbol: str) -> bool:
    """Name-based layer: True for a ticker that looks like an ETF / fund unit."""
    s = str(symbol or "").strip().upper()
    if not s:
        return False
    return (s.endswith("BEES")
            or "ETF" in s
            or "IETF" in s
            or s in ETF_CURATED)


def is_fund_instrument_key(instrument_key: str | None) -> bool:
    """ISIN-based layer: True when the instrument key carries a fund ISIN prefix.

    Accepts ``"NSE_EQ|INF740KA1SW3"`` or a bare ISIN. Unknown/empty input returns False — this
    layer only ever ADDS exclusions, so it must never guess a symbol is a fund on missing data.
    """
    k = str(instrument_key or "").strip().upper()
    if not k:
        return False
    isin = k.rsplit("|", 1)[-1]
    return isin.startswith(_FUND_ISIN_PREFIXES)


def is_non_equity(symbol: str, instrument_key: str | None = None) -> bool:
    """Combined STOCK-ONLY test. True if either layer says this is not a company share.

    Pass ``instrument_key`` wherever it is known (every channel resolves one before fetching
    bars) — that is the layer that catches funds whose ticker looks like an ordinary stock.
    """
    return is_etf_symbol(symbol) or is_fund_instrument_key(instrument_key)
