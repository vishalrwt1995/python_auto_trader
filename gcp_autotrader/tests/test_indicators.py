from autotrader.domain.indicators import compute_indicators
from autotrader.settings import StrategySettings


def _make_candles(n: int = 120):
    rows = []
    px = 100.0
    for i in range(n):
        px += 0.4 if i % 3 else -0.1
        o = px - 0.2
        h = px + 0.8
        l = px - 0.9
        c = px
        v = 10000 + i * 10
        rows.append((f"2025-01-{(i%28)+1:02d}T09:{i%60:02d}:00+05:30", o, h, l, c, v))
    return rows


def test_compute_indicators_returns_snapshot():
    out = compute_indicators(_make_candles(), StrategySettings())
    assert out is not None
    assert out.atr > 0
    assert 0 <= out.rsi.curr <= 100
    assert out.volume.curr > 0

