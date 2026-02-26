from autotrader.domain.indicators import compute_indicators
from autotrader.domain.models import RegimeSnapshot
from autotrader.domain.scoring import determine_direction, score_signal
from autotrader.settings import StrategySettings


def _candles():
    rows = []
    px = 200.0
    for i in range(130):
        px += 0.5
        rows.append((f"2025-01-{(i%28)+1:02d}T10:{i%60:02d}:00+05:30", px - 0.2, px + 1.0, px - 0.8, px, 5000 + i * 20))
    return rows


def test_direction_and_score_run():
    cfg = StrategySettings()
    ind = compute_indicators(_candles(), cfg)
    assert ind is not None
    regime = RegimeSnapshot(regime="TREND", bias="BULLISH", vix=12.0)
    d = determine_direction(ind, regime)
    s = score_signal("TEST", d, ind, regime, cfg)
    assert s.score >= 0
    assert s.direction in {"BUY", "SELL", "HOLD"}

