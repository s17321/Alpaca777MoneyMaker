from dataclasses import dataclass, field
from typing import Dict, List


@dataclass
class BotConfig:
    bot_id: str
    initial_cash: float
    symbols: List[str]
    timeframe: str = "1Day"
    strategy_name: str = "sma"
    max_position_pct: float = 0.2
    rebalance_interval_sec: int = 60


@dataclass
class PortfolioState:
    cash: float
    positions: Dict[str, float] = field(default_factory=dict)
    equity: float = 0.0