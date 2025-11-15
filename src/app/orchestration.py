from time import time
from loguru import logger
from src.config import settings
from src.domain.models import BotConfig, PortfolioState
from src.domain.interfaces import BrokerPort, DataFeedPort, Strategy, RiskManager
from src.app.portfolio_service import compute_target_qty, delta_qty
from src.app.order_service import make_order
from src.infra import persistence

EPSILON = 1e-3   # tolerancja float dla różnicy ilości
MIN_QTY = 0.01   # minimalna ilość transakcyjna (dostosuj do instrumentu)

class SingleBotOrchestrator:
    def __init__(self, broker: BrokerPort, data: DataFeedPort, strategy: Strategy, risk: RiskManager, bot: BotConfig):
        self.broker = broker
        self.data = data
        self.strategy = strategy
        self.risk = risk
        self.bot = bot
        self.state = PortfolioState(cash=bot.initial_cash, positions={}, equity=bot.initial_cash)
        self.conn = persistence.get_conn()
        self._last_ts = None  # pamiętamy timestamp ostatniego przetworzonego bara

    def step(self):
        symbol = self.bot.symbols[0]
        bars = self.data.get_history(symbol, self.bot.timeframe, start="2000-01-01", end="2100-01-01")
        last = bars[-1]

        # 1) działaj tylko przy NOWYM barze
        if self._last_ts is not None and last.ts == self._last_ts:
            logger.info(f"[{self.bot.bot_id}] Brak nowego baru — pomijam krok.")
            return
        self._last_ts = last.ts

        # 2) policz target i przytnij ryzykiem
        w_raw = self.strategy.target_weight(symbol, bars)
        w = self.risk.adjust_weight(self.bot.bot_id, symbol, w_raw, bars)

        price = last.close
        ts_ms = int(time() * 1000)

        target_qty = compute_target_qty(w, self.state.equity, price)
        current_qty = self.state.positions.get(symbol, 0.0)
        dq = delta_qty(current_qty, target_qty)

        # 3) zablokuj mikroruchy (epsilon + minimalna ilość)
        if abs(dq) < max(EPSILON, MIN_QTY):
            logger.info(f"[{self.bot.bot_id}] Zmiana < min trade size — pomijam.")
            persistence.insert_equity(self.conn, ts_ms, self.bot.bot_id, self.state.equity)
            return ts_ms, self.state.equity

        side = "buy" if dq > 0 else "sell"
        order = make_order(settings.ORDER_CLIENT_PREFIX, self.bot.bot_id, symbol, side, abs(dq), settings.DEFAULT_TIF)
        oid = self.broker.place_order(order)
        logger.info(f"[{self.bot.bot_id}] {side.upper()} {abs(dq):.4f} {symbol} @~{price:.2f} (order_id={oid})")

        # 4) Aktualizacja stanu portfela (cash/qty, mark-to-market)
        if dq > 0:  # kupno
            self.state.cash -= abs(dq) * price * (1 + settings.COMMISSION_PCT)
        else:       # sprzedaż
            self.state.cash += abs(dq) * price * (1 - settings.COMMISSION_PCT)

        new_qty = current_qty + dq
        self.state.positions[symbol] = new_qty
        self.state.equity = self.state.cash + new_qty * price

        # 5) Zapis do SQLite
        persistence.insert_trades(
            self.conn,
            [(ts_ms, self.bot.bot_id, symbol, side, float(abs(dq)), float(price), order.client_id)],
        )
        persistence.insert_equity(self.conn, ts_ms, self.bot.bot_id, float(self.state.equity))

        return ts_ms, self.state.equity