import argparse
from pathlib import Path
import pandas as pd
import yfinance as yf

RAW_DIR = Path("data/raw")
OUT_DIR = Path("data/history")

YF_INTERVAL = {
    "1Day": ("1d", None),      # (interval, default period)
    "15Min": ("15m", "60d"),   # Yahoo zwykle max ~60 dni intraday
}

def _flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        # weź ostatni poziom (OHLCV)
        df.columns = df.columns.get_level_values(-1)
    return df

def _normalize_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    # mapuj case-insensitive + fallback na Adj Close
    lowermap = {str(c).lower(): c for c in df.columns}
    out = pd.DataFrame(index=df.index)

    def pick(name: str, alt: list[str] = None, allow_zero=False):
        if alt is None:
            alt = []
        keys = [name] + alt
        for k in keys:
            if k in lowermap:
                return pd.to_numeric(df[lowermap[k]], errors="coerce")
            if k.title() in df.columns:
                return pd.to_numeric(df[k.title()], errors="coerce")
        if allow_zero:
            return pd.Series(0, index=df.index)
        return None

    o = pick("open")
    h = pick("high")
    l = pick("low")
    c = pick("close", alt=["adj close"])  # dopuszczamy Adj Close
    v = pick("volume", allow_zero=True)

    if any(x is None for x in [o, h, l, c]):
        # nie panikuj – zwróć dostępne kolumny bez normalizacji
        return pd.DataFrame({k: df[k] for k in df.columns})

    out["open"] = o
    out["high"] = h
    out["low"] = l
    out["close"] = c
    out["volume"] = pd.to_numeric(v, errors="coerce").fillna(0).astype("int64")
    out = out.dropna(subset=["open", "high", "low", "close"], how="any")
    return out

def fetch(symbol: str, timeframe: str, start: str | None, end: str | None, period: str | None):
    interval, default_period = YF_INTERVAL[timeframe]

    # 1) Pobierz – preferuj period dla 15m, dla 1d użyj start/end
    if timeframe == "15Min":
        use_period = period or default_period or "60d"
        df = yf.Ticker(symbol).history(period=use_period, interval=interval, auto_adjust=False, actions=False)
    else:
        df = yf.Ticker(symbol).history(start=start, end=end, interval=interval, auto_adjust=False, actions=False)

    if df is None or df.empty:
        raise SystemExit("Brak danych z Yahoo Finance (sprawdź zakres/interval/period)")

    df = _flatten_columns(df)

    # Zapisz RAW (dokładnie to, co przyszło), z indeksem jako kolumną Timestamp
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    raw_out = RAW_DIR / f"NG_F_{timeframe}_raw.csv"
    df_raw = df.copy()
    df_raw.insert(0, "Timestamp", pd.to_datetime(df_raw.index))
    df_raw.to_csv(raw_out, index=False)

    # Spróbuj znormalizować do OHLCV – jeśli się nie uda, zapisz co jest i poinformuj
    norm = _normalize_ohlcv(df)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out = OUT_DIR / f"NG_F_{timeframe}.csv"

    if set(["open","high","low","close"]).issubset(set([c.lower() for c in norm.columns])):
        # mamy OHLCV → budujemy timestamp jako str
        idx = pd.to_datetime(norm.index)
        ts = idx.strftime("%Y-%m-%d" if timeframe == "1Day" else "%Y-%m-%d %H:%M:%S")
        pd.DataFrame({
            "timestamp": ts.to_numpy(),
            "open": norm["open"].to_numpy(),
            "high": norm["high"].to_numpy(),
            "low": norm["low"].to_numpy(),
            "close": norm["close"].to_numpy(),
            "volume": (norm["volume"] if "volume" in norm.columns else 0).to_numpy(),
        }).to_csv(out, index=False)
        print(f"Zapisano znormalizowany OHLCV: {out}")
    else:
        # Brak pełnego OHLCV – zapisz przycięte dane z informacją
        df_raw.to_csv(out, index=False)
        print("Brak pełnych kolumn OHLCV – zapisano dane RAW jako fallback:", out)

    # Raport diagnostyczny
    print("=== PODSUMOWANIE POBIERANIA ===")
    print(f"Symbol: {symbol} | Timeframe: {timeframe} | Interval: {interval}")
    if timeframe == "15Min":
        print(f"Zakres: period={period or default_period or '60d'} (Yahoo ignoruje start/end dla 15m)")
    else:
        print(f"Zakres: start={start} end={end}")
    print(f"RAW -> {raw_out}")
    print(f"OUT -> {out}")
    print("Kolumny RAW:", list(df.columns))
    if set(["open","high","low","close"]).issubset(set([c.lower() for c in norm.columns])):
        print("Kolumny OUT (znormalizowane):", list(norm.columns))
    else:
        print("Kolumny OUT (RAW fallback):", list(df_raw.columns))
    print("Wierszy RAW:", len(df_raw))

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--timeframe", choices=["1Day", "15Min"], default="1Day")
    p.add_argument("--start", default=None, help="YYYY-MM-DD")
    p.add_argument("--end", default=None, help="YYYY-MM-DD")
    p.add_argument("--period", default=None, help="np. 60d (dla 15Min)")
    args = p.parse_args()
    fetch(symbol="NG=F", timeframe=args.timeframe, start=args.start, end=args.end, period=args.period)