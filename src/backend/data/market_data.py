# src/backend/data/market_data.py
from __future__ import annotations

import os
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional, Literal

import pandas as pd

# === alpaca-py ===
from alpaca.data import StockHistoricalDataClient, CryptoHistoricalDataClient
from alpaca.data.requests import StockBarsRequest, CryptoBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.data.enums import Adjustment, DataFeed

AssetClassLiteral = Literal["US_EQUITY", "CRYPTO"]


def _parse_timeframe(tf: str) -> TimeFrame:
    tf = tf.strip().lower()
    if tf.endswith("day"):
        n = int(tf[:-3]); return TimeFrame(n, TimeFrameUnit.Day)
    if tf.endswith("hour"):
        n = int(tf[:-4]); return TimeFrame(n, TimeFrameUnit.Hour)
    if tf.endswith("min"):
        n = int(tf[:-3]); return TimeFrame(n, TimeFrameUnit.Minute)
    raise ValueError(f"Nieznany timeframe: {tf}")

def _ensure_utc(dt: Optional[datetime]) -> datetime:
    if dt is None:
        return datetime.now(timezone.utc)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def _looks_like_crypto(symbol: str) -> bool:
    s = symbol.upper()
    return ("/" in s) or s.endswith("USD") or s.endswith("USDT") or s.endswith("USDC")


@dataclass
class MarketDataConfig:
    api_key_id: str
    api_secret_key: str
    feed: DataFeed = DataFeed.IEX            # dla akcji/ETF
    adjustment: Adjustment = Adjustment.SPLIT
    cache_dir: str = "data_cache"

    @staticmethod
    def from_env() -> "MarketDataConfig":
        from dotenv import load_dotenv
        load_dotenv()
        key = os.getenv("APCA_API_KEY_ID", "")
        sec = os.getenv("APCA_API_SECRET_KEY", "")
        cache_dir = os.getenv("DATA_CACHE_DIR", "data_cache")
        if not os.path.isdir(cache_dir):
            os.makedirs(cache_dir, exist_ok=True)
        return MarketDataConfig(api_key_id=key, api_secret_key=sec, cache_dir=cache_dir)


class MarketDataService:
    """
    Serwis świec dla akcji/ETF **i** krypto + prosty cache CSV.
    """

    def __init__(self, cfg: Optional[MarketDataConfig] = None, logger: Optional[logging.Logger] = None) -> None:
        self.cfg = cfg or MarketDataConfig.from_env()
        self.log = logger or logging.getLogger(self.__class__.__name__)
        if not self.log.handlers:
            h = logging.StreamHandler()
            self.log.addHandler(h); self.log.setLevel(logging.INFO)
        self.log.propagate = False

        self.stock_client = StockHistoricalDataClient(self.cfg.api_key_id, self.cfg.api_secret_key)
        self.crypto_client = CryptoHistoricalDataClient(self.cfg.api_key_id, self.cfg.api_secret_key)

        os.makedirs(self.cfg.cache_dir, exist_ok=True)

    def _cache_path(self, symbol: str, tf: str, cls: AssetClassLiteral) -> str:
        sym = symbol.upper().replace("/", "_")
        tf_norm = tf.replace(" ", "")
        return os.path.join(self.cfg.cache_dir, f"{cls.lower()}_{sym}_{tf_norm}.csv")

    def load_cache(self, symbol: str, tf: str, cls: AssetClassLiteral) -> Optional[pd.DataFrame]:
        path = self._cache_path(symbol, tf, cls)
        if not os.path.exists(path):
            return None
        df = pd.read_csv(path, parse_dates=["timestamp"])
        if df.empty:
            return df
        # UTC-aware
        try:
            if df["timestamp"].dt.tz is None:
                df["timestamp"] = df["timestamp"].dt.tz_localize("UTC")
            else:
                df["timestamp"] = df["timestamp"].dt.tz_convert("UTC")
        except Exception:
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        return df.sort_values("timestamp").reset_index(drop=True)

    def save_cache(self, symbol: str, tf: str, cls: AssetClassLiteral, df: pd.DataFrame) -> None:
        path = self._cache_path(symbol, tf, cls)
        df.sort_values("timestamp").to_csv(path, index=False)

    def get_bars(
        self,
        symbol: str,
        timeframe: str = "1Day",
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        use_cache: bool = True,
        asset_class: Optional[AssetClassLiteral] = None,  # <-- NOWE: wymuś klasę lub auto
    ) -> pd.DataFrame:
        """
        Zwraca DataFrame: timestamp, open, high, low, close, volume.
        Obsługuje US_EQUITY (akcje/ETF) i CRYPTO (np. BTC/USD).
        """
        symbol = symbol.upper()
        tf = timeframe
        tf_obj = _parse_timeframe(tf)

        # 1) domyślny zakres
        now_utc = datetime.now(timezone.utc)
        if end is None:
            end = now_utc
        if start is None:
            default_days = 365 if tf_obj.unit == TimeFrameUnit.Day else 30
            start = end - timedelta(days=default_days)

        # 2) UTC-aware
        end = _ensure_utc(end)
        start = _ensure_utc(start)

        # 3) rozpoznaj klasę aktywa
        cls: AssetClassLiteral = asset_class or ("CRYPTO" if _looks_like_crypto(symbol) else "US_EQUITY")

        # 4) cache
        cached = self.load_cache(symbol, tf, cls) if use_cache else None
        need_from = start
        if cached is not None and not cached.empty:
            last_ts = cached["timestamp"].max()
            if last_ts >= end:
                df_cached = cached[(cached["timestamp"] >= start) & (cached["timestamp"] <= end)].copy()
                return df_cached.reset_index(drop=True)
            need_from = max(last_ts + timedelta(seconds=1), start)

        # 5) API call
        if cls == "US_EQUITY":
            req = StockBarsRequest(
                symbol_or_symbols=symbol,
                timeframe=tf_obj,
                start=need_from,
                end=end,
                adjustment=self.cfg.adjustment,
                feed=self.cfg.feed,  # IEX (darmowy) lub SIP (płatny)
                limit=10_000,
            )
            bars = self.stock_client.get_stock_bars(req)
            data = bars.data.get(symbol, [])
        else:
            # CRYPTO
            # Uwaga: symbole krypto zwykle w formacie 'BTC/USD' (z ukośnikiem)
            req = CryptoBarsRequest(
                symbol_or_symbols=symbol,
                timeframe=tf_obj,
                start=need_from,
                end=end,
                limit=10_000,
            )
            bars = self.crypto_client.get_crypto_bars(req)
            data = bars.data.get(symbol, [])

        # 6) do DF
        if not data:
            new_df = pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])
        else:
            new_df = pd.DataFrame({
                "timestamp": [b.timestamp for b in data],
                "open": [float(b.open) for b in data],
                "high": [float(b.high) for b in data],
                "low": [float(b.low) for b in data],
                "close": [float(b.close) for b in data],
                "volume": [int(getattr(b, "volume", 0) or 0) for b in data],
            })
            # UTC
            new_df["timestamp"] = pd.to_datetime(new_df["timestamp"], utc=True)

        # 7) łącz z cache
        frames = []
        if cached is not None and not cached.empty:
            frames.append(cached)
        if new_df is not None and not new_df.empty:
            frames.append(new_df)

        if frames:
            df = pd.concat(frames, ignore_index=True)
            df = df.drop_duplicates(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
        else:
            # pusta ramka z docelowymi typami (żeby nie było niespodzianek)
            df = pd.DataFrame({
                "timestamp": pd.to_datetime([], utc=True),
                "open": pd.Series(dtype="float64"),
                "high": pd.Series(dtype="float64"),
                "low": pd.Series(dtype="float64"),
                "close": pd.Series(dtype="float64"),
                "volume": pd.Series(dtype="int64"),
            })

        # 8) finalny filtr
        if not df.empty:
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
            df = df[(df["timestamp"] >= start) & (df["timestamp"] <= end)].copy().reset_index(drop=True)
        return df
