# src/app/__main__.py
import logging
from backend.broker.alpaca_client import AlpacaBroker, BrokerConfig

def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    broker = AlpacaBroker(BrokerConfig.from_env())

    # Placeholdery: pokaż co mamy, ale to jeszcze nie są realne dane z API
    account = broker.get_account()
    clock = broker.get_clock()
    print("Account (placeholder):", account)
    print("Clock   (placeholder):", clock)

    # Test symulowanego zlecenia:
    res = broker.submit_market_order(symbol="AAPL", qty=1, side="buy")
    print("Order result:", res)

if __name__ == "__main__":
    main()
