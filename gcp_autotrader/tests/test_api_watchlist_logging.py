from __future__ import annotations

from autotrader.web.api import _watchlist_done_log_fields


def test_watchlist_done_log_fields_include_required_premarket_audit_keys():
    wl_out = {
        "selected": 150,
        "intradaySelected": 150,
        "coverage": {
            "expectedLCD": "2026-03-04",
            "runTimeBlock": "PREMARKET",
            "phase2Candidates": 0,
        },
        "regimeV2": {
            "regimeDaily": "RANGE",
            "regimeIntraday": "CHOPPY",
            "source": {
                "dailyKey": "NSE_INDEX|Nifty 50",
                "dailySource": "upstox_api_expectedlcd_sync",
            },
        },
        "intradayPhaseStats": {
            "phase2UsedCount": 0,
            "phase1FallbackCount": 150,
            "phase2EligibleCount": 0,
            "phase2EligiblePct": 0.0,
            "intradaySelectedCount": 150,
        },
    }

    ctx = _watchlist_done_log_fields(wl_out, is_premarket=True)
    assert ctx["expectedLCD"] == "2026-03-04"
    assert ctx["runTimeBlock"] == "PREMARKET"
    assert ctx["isPremarket"] is True
    assert ctx["indexDailyKeyChosen"] == "NSE_INDEX|Nifty 50"
    assert ctx["indexDailySource"] == "expectedlcd_sync_api"
    assert ctx["regimeDaily"] == "RANGE"
    assert ctx["regimeIntraday"] == "NA"
    assert ctx["phase2_used_count"] + ctx["phase1_fallback_count"] == ctx["intraday_selected_count"]
