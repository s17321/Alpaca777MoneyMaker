from __future__ import annotations
from typing import List, Dict, Tuple
from src.domain.interfaces import RiskManager
from src.domain.dto import Bar

# === Pomocnicze wskaźniki ===

def ema(values: List[float], period: int) -> float:
    if not values:
        return 0.0
    k = 2 / (period + 1)
    e = values[0]
    for v in values[1:]:
        e = v * k + e * (1 - k)
    return e

def atr(bars: List[Bar], period: int = 14) -> float:
    if len(bars) < period + 1:
        return 0.0
    trs = []
    for i in range(1, len(bars)):
        h, l, pc = bars[i].high, bars[i].low, bars[i-1].close
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    # prosty EMA na TR
    return ema(trs[-period:], period)

class SimpleRisk(RiskManager):
    """Zachowana stara implementacja (clamp 0..max_pct)."""
    def __init__(self, max_position_pct: float = 0.2):
        self.max_pct = max_position_pct

    def adjust_weight(self, bot_id: str, symbol: str, raw_weight: float, bars: List[Bar]) -> float:
        return max(min(raw_weight, self.max_pct), 0.0)

class AtrStopsVolRisk(RiskManager):
    """
    - Regime filter: gramy tylko, gdy EMA(100) rośnie i close > EMA(100)
    - Volatility targeting: skaluje wagę ~  k / ATR% (cap do max_pct)
    - Stop-Loss / Take-Profit: na podstawie % od ceny wejścia albo ATR wielokrotności
    
    Stan (na poziomie risk) trzymamy per (bot_id, symbol): entry_price, in_pos.
    """
    def __init__(
        self,
        max_position_pct: float = 0.2,
        vol_k: float = 0.02,        # docelowa zmienność udziału (im większy ATR%, tym mniejsza waga)
        atr_period: int = 14,
        regime_ema: int = 100,
        sl_atr_mult: float = 2.0,
        tp_atr_mult: float = 3.0,
        use_pct_stops: bool = False,
        sl_pct: float = 0.03,
        tp_pct: float = 0.06,
    ):
        self.max_pct = max_position_pct
        self.vol_k = vol_k
        self.atr_p = atr_period
        self.reg_ema = regime_ema
        self.sl_atr = sl_atr_mult
        self.tp_atr = tp_atr_mult
        self.use_pct_stops = use_pct_stops
        self.sl_pct = sl_pct
        self.tp_pct = tp_pct
        self._state: Dict[Tuple[str,str], Dict[str, float]] = {}

    def _get_state(self, bot_id: str, symbol: str) -> Dict[str, float]:
        key = (bot_id, symbol)
        if key not in self._state:
            self._state[key] = {"in_pos": 0.0, "entry": 0.0}
        return self._state[key]

    def adjust_weight(self, bot_id: str, symbol: str, raw_weight: float, bars: List[Bar]) -> float:
        if not bars:
            return 0.0
        st = self._get_state(bot_id, symbol)
        closes = [b.close for b in bars]
        price = closes[-1]

        # 1) Regime filter (EMA100 rośnie i cena nad EMA100)
        if len(closes) >= self.reg_ema:
            ema100_hist = closes[-self.reg_ema:]
            ema100_now = ema(ema100_hist, self.reg_ema)
            ema100_prev = ema(ema100_hist[:-1], self.reg_ema) if len(ema100_hist) > 1 else ema100_now
            regime_ok = (ema100_now > ema100_prev) and (price > ema100_now)
            if not regime_ok:
                raw_weight = 0.0
        else:
            raw_weight = 0.0

        # 2) Vol targeting przez ATR%
        a = atr(bars, self.atr_p)
        atr_pct = (a / price) if price > 0 else 0.0
        if atr_pct > 0:
            vt_cap = min(self.max_pct, self.vol_k / atr_pct)
        else:
            vt_cap = self.max_pct
        w = max(min(raw_weight, vt_cap), 0.0)

        # 3) Stop-loss / Take-profit
        in_pos = st["in_pos"] > 0
        if in_pos and st["entry"] > 0:
            if self.use_pct_stops:
                sl_hit = price <= st["entry"] * (1 - self.sl_pct)
                tp_hit = price >= st["entry"] * (1 + self.tp_pct)
            else:
                sl_hit = a > 0 and price <= st["entry"] - self.sl_atr * a
                tp_hit = a > 0 and price >= st["entry"] + self.tp_atr * a
            if sl_hit or tp_hit:
                w = 0.0  # wyjście

        # 4) Aktualizacja stanu wejścia/wyjścia na podstawie wagi
        if (not in_pos) and w > 0:
            st["in_pos"] = 1.0
            st["entry"] = price
        elif in_pos and w == 0.0:
            st["in_pos"] = 0.0
            st["entry"] = 0.0

        return w