import time
from src.domain.dto import OrderRequest

def make_order(client_prefix: str, bot_id: str, symbol: str, side: str, qty: float, tif: str) -> OrderRequest:
    # unikalny client_id (pomaga w śledzeniu, nawet w DRY_RUN)
    client_id = f"{client_prefix}-{bot_id}-{symbol}-{int(time.time())}"
    return OrderRequest(client_id=client_id, symbol=symbol, side=side, qty=abs(qty), tif=tif)