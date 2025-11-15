from typing import List, Optional
from time import time
from src.config import settings
from src.domain.dto import OrderRequest, OrderFill, PositionSnapshot
from src.domain.interfaces import BrokerPort


class DryRunBroker(BrokerPort):
    """Prosty symulator: trzyma pozycje w pamięci, filluje po cenie 'last' (nie idealnie, ale OK na start)."""
    def __init__(self):
        self.positions: dict[str, PositionSnapshot] = {}
        self.fills: list[OrderFill] = []

    def get_positions(self) -> List[PositionSnapshot]:
        return list(self.positions.values())

    def place_order(self, order: OrderRequest) -> str:
        # Fill natychmiastowy po 'cenie rynkowej' = 100.0 (placeholder) –
        # możesz podać realną cenę z DataFeed, jeśli zgłosisz ją w OrderRequest.
        price = 100.0
        sign = 1 if order.side == "buy" else -1
        pos = self.positions.get(order.symbol)
        new_qty = (pos.qty if pos else 0.0) + sign * order.qty
        avg_price = price if not pos else price # uproszczenie
        self.positions[order.symbol] = PositionSnapshot(
            symbol=order.symbol, qty=new_qty, avg_price=avg_price, market_value=new_qty * price
        )
        fill = OrderFill(order.client_id, order.symbol, order.qty, price, int(time() * 1000))
        self.fills.append(fill)
        return order.client_id


    def get_fills(self, since_ts: Optional[int] = None) -> List[OrderFill]:
        if since_ts is None:
            return list(self.fills)
        return [f for f in self.fills if f.ts >= since_ts]


# Placeholder – kiedy ustawisz klucze, możesz rozwinąć ten adapter do prawdziwego Alpaca.
AlpacaBroker = DryRunBroker