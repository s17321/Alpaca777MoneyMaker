from abc import ABC, abstractmethod
from typing import List, Optional
from .dto import OrderRequest, OrderFill, PositionSnapshot, Bar

class BrokerPort(ABC):
    @abstractmethod
    def get_positions(self) -> List[PositionSnapshot]: ...

    @abstractmethod
    def place_order(self, order: OrderRequest) -> str: ...  # returns client_order_id

    @abstractmethod
    def get_fills(self, since_ts: Optional[int] = None) -> List[OrderFill]: ...

class DataFeedPort(ABC):
    @abstractmethod
    def get_history(self, symbol: str, timeframe: str, start: str, end: str) -> List[Bar]: ...

    @abstractmethod
    def get_last_bar(self, symbol: str, timeframe: str) -> Bar: ...

class Strategy(ABC):
    @abstractmethod
    def target_weight(self, symbol: str, bars: List[Bar]) -> float: ...

class RiskManager(ABC):
    @abstractmethod
    def adjust_weight(self, bot_id: str, symbol: str, raw_weight: float, bars: List[Bar]) -> float: ...