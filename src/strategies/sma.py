from typing import List
from src.domain.interfaces import Strategy
from src.domain.dto import Bar


class SmaCross(Strategy):
    def __init__(self, fast: int = 20, slow: int = 50):
        assert fast < slow, "fast < slow"
        self.fast, self.slow = fast, slow


    def target_weight(self, symbol: str, bars: List[Bar]) -> float:
        closes = [b.close for b in bars]
        if len(closes) < self.slow:
            return 0.0
        f = sum(closes[-self.fast:]) / self.fast
        s = sum(closes[-self.slow:]) / self.slow
        if f > s:
            return +1.0
        if f < s:
            return 0.0 # na starcie bez shortów
        return 0.0