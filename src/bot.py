# src/bot.py
import alpaca_trade_api as tradeapi
from config.settings import settings

def main() -> None:
    api = tradeapi.REST(
        key_id=settings.API_KEY,
        secret_key=settings.SECRET_KEY,
        base_url=settings.BASE_URL,
    )
    account = api.get_account()
    print(f"Saldo konta demo: ${account.cash}")

if __name__ == "__main__":
    main()
