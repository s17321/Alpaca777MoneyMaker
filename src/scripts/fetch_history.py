import argparse
import pandas as pd
from pathlib import Path
from alpaca.data import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from datetime import datetime
from src.config import settings


OUT_DIR = Path("data/history")


TF_MAP = {
    "1Day": TimeFrame.Day,
    "1Min": TimeFrame.Minute,
    "5Min": TimeFrame(5, "Min"),
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--start", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="YYYY-MM-DD")
    parser.add_argument("--timeframe", default="1Day", choices=list(TF_MAP.keys()))
    args = parser.parse_args()


    client = StockHistoricalDataClient(
        api_key=settings.APCA_API_KEY_ID,
        secret_key=settings.APCA_API_SECRET_KEY,
    )


    req = StockBarsRequest(
        symbol_or_symbols=args.symbol,
        timeframe=TF_MAP[args.timeframe],
        start=datetime.fromisoformat(args.start),
        end=datetime.fromisoformat(args.end),
        feed=settings.APCA_DATA_FEED,
        limit=10000,
        adjustment="raw",
    )


    bars = client.get_stock_bars(req).df
    if bars.empty:
        raise SystemExit("Brak danych z Alpaca (sprawdź symbol, zakres dat lub klucze API)")


    # Jeśli wielosymbolowe, ramka ma MultiIndex; tu zakładamy 1 symbol
    bars = bars.reset_index()
    bars = bars.rename(columns={
        "timestamp": "timestamp",
        "open": "open",
        "high": "high",
        "low": "low",
        "close": "close",
        "volume": "volume",
    })
    # Upewnij się, że timestamp jest w ISO dla spójności z loaderem CSV
    bars["timestamp"] = pd.to_datetime(bars["timestamp"]).dt.strftime("%Y-%m-%d")
    out = OUT_DIR / f"{args.symbol}_{args.timeframe}.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    bars[["timestamp","open","high","low","close","volume"]].to_csv(out, index=False)
    print(f"Zapisano: {out}")


if __name__ == "__main__":
    main()