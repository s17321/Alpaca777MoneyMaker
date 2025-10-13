# src/app/__main__.py
from __future__ import annotations

import argparse
import logging
import sys
from typing import Literal

from src.backend.broker.alpaca_client import (
    AlpacaBroker,
    BrokerConfig,
    BrokerAuthError,
    BrokerError,
    BrokerOrderRejected,
    BrokerNetworkError,
    BrokerRateLimitError,
)

Action = Literal["info", "order", "positions", "cancel"]

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="alpaca-app",
        description="Runner do testowania warstwy brokera (paper/live z DRY_RUN).",
    )
    p.add_argument(
        "action",
        choices=["info", "order", "positions", "cancel"],
        help="Co zrobić: info (konto+clock), order (market), positions (otwarte), cancel (anuluj oczekujące).",
    )
    p.add_argument("--symbol", default="AAPL", help="Ticker dla akcji 'order' (domyślnie AAPL).")
    p.add_argument("--qty", type=int, default=1, help="Ilość sztuk dla akcji 'order' (domyślnie 1).")
    p.add_argument("--side", choices=["buy", "sell"], default="buy", help="Strona transakcji (buy/sell).")
    p.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Poziom logowania (domyślnie INFO).",
    )
    return p

def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    broker = AlpacaBroker(BrokerConfig.from_env())

    try:
        if args.action == "info":
            account = broker.get_account()
            clock = broker.get_clock()
            print("\n=== ACCOUNT ===")
            print(f"Equity       : {account.get('equity')}")
            print(f"Cash         : {account.get('cash')}")
            print(f"Buying Power : {account.get('buying_power')}")
            print(f"Status       : {account.get('status')}")
            print("\n=== CLOCK ===")
            print(f"Is Open      : {clock.get('is_open')}")
            print(f"Next Open    : {clock.get('next_open')}")
            print(f"Next Close   : {clock.get('next_close')}")
            print()
            return 0

        if args.action == "positions":
            pos = broker.get_open_positions()
            print("\n=== OPEN POSITIONS ===")
            if not pos:
                print("(brak)")
            else:
                for p in pos:
                    print(
                        f"{p['symbol']:>6}  qty={p['qty']:<6}  avg={p['avg_price']:<10}  "
                        f"mkt_val={p['market_value']:<10}  uPL={p['unrealized_pl']}"
                    )
            print()
            return 0

        if args.action == "cancel":
            res = broker.cancel_all()
            print("\n=== CANCEL ALL ===")
            print(res)
            print()
            return 0

        if args.action == "order":
            res = broker.submit_market_order(symbol=args.symbol, qty=args.qty, side=args.side)
            print("\n=== SUBMIT MARKET ORDER ===")
            print(res)
            print()
            return 0

        print("Nieznana akcja.")
        return 2

    except (BrokerAuthError, BrokerOrderRejected, BrokerRateLimitError, BrokerNetworkError, BrokerError) as e:
        logging.getLogger("runner").error("Błąd: %s", e)
        return 1
    except KeyboardInterrupt:
        print("\nPrzerwano.")
        return 130

if __name__ == "__main__":
    sys.exit(main())
