import argparse
import time
from loguru import logger
from src.config import settings
from src.infra.logging import setup_logging
from src.infra.alpaca_broker import AlpacaBroker
from src.infra.data_feed import CsvDataFeed, DATA_DIR
from src.strategies.ema_rsi import EmaRsiTrend
from src.risk.core import AtrStopsVolRisk
from src.domain.models import BotConfig
from src.app.orchestration import SingleBotOrchestrator
from src.backtest.engine import Backtester
import pandas as pd
from src.domain.dto import Bar


def main():
    setup_logging()

    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd", required=True)

    live = sub.add_parser("live")
    live.add_argument("symbol", type=str)
    live.add_argument("timeframe", type=str, nargs="?", default="1Day")

    loop = sub.add_parser("live-loop")
    loop.add_argument("symbol", type=str)
    loop.add_argument("timeframe", type=str, nargs="?", default="1Day")
    loop.add_argument("--interval", type=int, default=60, help="Co ile sekund wykonywać krok")

    bt = sub.add_parser("backtest")
    bt.add_argument("symbol", type=str)
    bt.add_argument("timeframe", type=str, nargs="?", default="1Day")

    args = parser.parse_args()

    if args.cmd in {"live", "live-loop"}:
        broker = AlpacaBroker()
        data = CsvDataFeed()
        strat = EmaRsiTrend(12, 50, 35, 65, 0.2)
        risk = AtrStopsVolRisk(max_position_pct=settings.RISK_MAX_POSITION_PCT)
        bot = BotConfig(bot_id="adam", initial_cash=1000.0, symbols=[args.symbol], timeframe=args.timeframe)
        orch = SingleBotOrchestrator(broker, data, strat, risk, bot)

        if args.cmd == "live":
            orch.step()
            logger.info("Live step wykonany (DRY_RUN).")
        else:
            logger.info(f"Start live-loop co {args.interval}s. Przerwij Ctrl+C")
            try:
                while True:
                    orch.step()
                    time.sleep(args.interval)
            except KeyboardInterrupt:
                logger.info("Zatrzymano live-loop.")

    elif args.cmd == "backtest":
        fp = DATA_DIR / f"{args.symbol}_{args.timeframe}.csv"
        df = pd.read_csv(fp)
        bars = [
            Bar(ts=int(pd.to_datetime(r.timestamp).value // 10**6), open=r.open, high=r.high, low=r.low, close=r.close, volume=int(r.volume))
            for r in df.itertuples(index=False)
        ]
        bt = Backtester(settings.COMMISSION_PCT)
        strat = EmaRsiTrend(12, 50, 35, 65, 0.2)
        risk = AtrStopsVolRisk(max_position_pct=settings.RISK_MAX_POSITION_PCT)
        def sig(window):
            raw = strat.target_weight(args.symbol, list(window))
            return risk.adjust_weight("adam", args.symbol, raw, list(window))
        res = bt.run(bars, sig)
        eq = res.equity_curve
        rets = [0.0] + [ (eq[i]-eq[i-1])/eq[i-1] for i in range(1, len(eq)) ]
        from src.backtest.report import max_drawdown, sharpe_ratio
        print("Equity (last):", round(eq[-1], 2) if eq else "-" )
        print("MDD:", round(max_drawdown(eq)*100, 2), "%")
        print("Sharpe:", round(sharpe_ratio(rets), 2))

if __name__ == "__main__":
    main()