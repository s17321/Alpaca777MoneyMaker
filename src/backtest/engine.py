from typing import Sequence, Callable
from src.domain.dto import Bar


class BacktestResult:
    def __init__(self, equity_curve: list[float]):
        self.equity_curve = equity_curve


class Backtester:
    def __init__(self, commission_pct: float = 0.0005):
        self.commission = commission_pct

    def run(
        self,
        bars: Sequence[Bar],
        signal_fn: Callable[[Sequence[Bar]], float],
        initial_cash: float = 1000.0,
    ) -> BacktestResult:
        cash = initial_cash
        position = 0.0
        curve = []
        for i in range(50, len(bars)):
            window = bars[:i]
            price = window[-1].close

            # docelowa waga -> docelowa ilość
            w = signal_fn(window)
            target_value = w * (cash + position * price)
            target_qty = target_value / price if price > 0 else 0.0
            dq = target_qty - position

            # transakcja z prowizją
            trade_value = dq * price
            if dq > 0:  # kupno
                cost = trade_value * (1 + self.commission)
                cash -= cost
            else:  # sprzedaż
                proceeds = (-dq) * price * (1 - self.commission)
                cash += proceeds

            position += dq
            equity = cash + position * price
            curve.append(equity)
        return BacktestResult(curve)