from dataclasses import dataclass


@dataclass
class Bar:
    ts: int # epoch ms
    open: float
    high: float
    low: float
    close: float
    volume: int


@dataclass
class OrderRequest:
    client_id: str
    symbol: str
    side: str # 'buy' | 'sell'
    qty: float
    type: str = "market"
    tif: str = "day"


@dataclass
class OrderFill:
    client_id: str
    symbol: str
    filled_qty: float
    avg_price: float
    ts: int


@dataclass
class PositionSnapshot:
    symbol: str
    qty: float
    avg_price: float
    market_value: float