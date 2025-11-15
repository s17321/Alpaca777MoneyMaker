from typing import List
from src.domain.interfaces import Strategy
from src.domain.dto import Bar

def _ema(series: list[float], period: int) -> float:
    if len(series) < period:
        return sum(series)/len(series)
    k = 2 / (period + 1)
    ema = series[0]
    for x in series[1:]:
        ema = x * k + ema * (1 - k)
    return ema

def _rsi(closes: list[float], period: int = 14) -> float:
    if len(closes) <= period:
        return 50.0
    gains, losses = 0.0, 0.0
    for i in range(-period, -1):
        d = closes[i+1] - closes[i]
        if d >= 0: gains += d
        else: losses -= d
    avg_gain = gains / period
    avg_loss = losses / period if losses > 0 else 1e-9
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))

class EmaRsiTrend(Strategy):
    """
    Long-only: w=target_weight (0..1)
    - Trend: EMA_fast > EMA_slow
    - Filtr RSI: wchodzimy tylko gdy rsi_min <= RSI <= rsi_max (unikamy skrajnych stanów)
    """
    def __init__(self, fast: int = 20, slow: int = 50, rsi_min: int = 35, rsi_max: int = 65, target_w: float = 0.2):
        assert fast < slow
        self.fast, self.slow = fast, slow
        self.rsi_min, self.rsi_max = rsi_min, rsi_max
        self.target_w = target_w

    def target_weight(self, symbol: str, bars: List[Bar]) -> float:
        closes = [b.close for b in bars]
        if len(closes) < max(self.slow, 14):
            return 0.0
        ema_f = _ema(closes[-self.slow:], self.fast)  # liczymy na ostatnim oknie
        ema_s = _ema(closes[-self.slow:], self.slow)
        rsi = _rsi(closes, 14)
        if ema_f > ema_s and self.rsi_min <= rsi <= self.rsi_max:
            return self.target_w   # np. 20% kapitału
        return 0.0
