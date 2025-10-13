# src/backend/data/market_data.py
from __future__ import annotations

import os
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

import pandas as pd

from alpaca.data import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.data.enums import Adjustment, DataFeed


def _parse_timeframe(tf: str) -> TimeFrame:
    """
    Akceptuje: '1Day', '1Hour', '15Min', '5Min', '1Min'
    Zwraca alpaca.data.timeframe.TimeFrame.
    """
    tf = tf.strip().lower()
    if tf.endswith("day"):
        n = int(tf[:-3])
        return TimeFrame(n, TimeFrameUnit.Day)
    if tf.endswith("hour"):
        n = int(tf[:-4])
        return TimeFrame(n, TimeFrameUnit.Hour)
    if tf.endswith("min"):
        n = int(tf[:-3])
        return TimeFrame(n, TimeFrameUnit.Minute)
    raise ValueError(f"Nieznany timeframe: {tf}")

def _ensure_utc(dt: Optional[datetime]) -> datetime:
    """
    Zwraca datetime w UTC (tz-aware).
    - Jeśli dt jest None -> zwraca "teraz" w UTC (tylko awaryjnie; zwykle nie używamy tej ścieżki).
    - Jeśli dt jest naive -> dołącz UTC.
    - Jeśli dt ma strefę -> konwertuje do UTC.
    """
    if dt is None:
        return datetime.now(timezone.utc)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


@dataclass
class MarketDataConfig:
    api_key_id: str
    api_secret_key: str
    feed: DataFeed = DataFeed.IEX    # IEX = darmowy dla akcji US
    adjustment: Adjustment = Adjustment.SPLIT
    cache_dir: str = "data_cache"

    @staticmethod
    def from_env() -> "MarketDataConfig":
        from dotenv import load_dotenv
        load_dotenv()
        import os
        key = os.getenv("APCA_API_KEY_ID", "")
        sec = os.getenv("APCA_API_SECRET_KEY", "")
        cache_dir = os.getenv("DATA_CACHE_DIR", "data_cache")
        if not os.path.isdir(cache_dir):
            os.makedirs(cache_dir, exist_ok=True)
        return MarketDataConfig(api_key_id=key, api_secret_key=sec, cache_dir=cache_dir)


class MarketDataService:
    """
    Prosty serwis do pobierania świec + cache CSV.
    Klient alpaca-py: StockHistoricalDataClient (Market Data).
    """

    def __init__(self, cfg: Optional[MarketDataConfig] = None, logger: Optional[logging.Logger] = None) -> None:
        self.cfg = cfg or MarketDataConfig.from_env()
        self.log = logger or logging.getLogger(self.__class__.__name__)
        if not self.log.handlers:
            h = logging.StreamHandler()
            self.log.addHandler(h)
            self.log.setLevel(logging.INFO)

        self.client = StockHistoricalDataClient(self.cfg.api_key_id, self.cfg.api_secret_key)

        os.makedirs(self.cfg.cache_dir, exist_ok=True)

    def _cache_path(self, symbol: str, tf: str) -> str:
        sym = symbol.upper()
        tf_norm = tf.replace(" ", "")
        return os.path.join(self.cfg.cache_dir, f"{sym}_{tf_norm}.csv")

    def load_cache(self, symbol: str, tf: str) -> Optional[pd.DataFrame]:
        path = self._cache_path(symbol, tf)
        if not os.path.exists(path):
            return None
        df = pd.read_csv(path, parse_dates=["timestamp"])
        # upewnij się, że kolumna jest UTC-aware
        if df["timestamp"].dt.tz is None:
            df["timestamp"] = df["timestamp"].dt.tz_localize("UTC")
        else:
            df["timestamp"] = df["timestamp"].dt.tz_convert("UTC")
        df = df.sort_values("timestamp").reset_index(drop=True)
        return df

    def save_cache(self, symbol: str, tf: str, df: pd.DataFrame) -> None:
        path = self._cache_path(symbol, tf)
        df.sort_values("timestamp").to_csv(path, index=False)

    def get_bars(
        self,
        symbol: str,
        timeframe: str = "1Day",
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        use_cache: bool = True,
    ) -> pd.DataFrame:
        """
        Zwraca DataFrame z kolumnami: timestamp, open, high, low, close, volume.
        - Używa cache CSV per (symbol, timeframe).
        - Jeśli zakres wykracza poza cache, dociąga brakujące świece i łączy.
        """
        end = _ensure_utc(end)
        start = _ensure_utc(start)
        symbol = symbol.upper()
        tf = timeframe
        tf_obj = _parse_timeframe(tf)

        # zakres domyślny: ostatnie 365 dni (dla 1Day) lub 30 dni (inny tf)
        now_utc = datetime.now(timezone.utc)
        if end is None:
            end = now_utc
        if start is None:
            default_days = 365 if tf_obj.unit == TimeFrameUnit.Day else 30
            start = end - timedelta(days=default_days)

        # wczytaj cache
        cached = self.load_cache(symbol, tf) if use_cache else None

        need_from = start
        if cached is not None and not cached.empty:
            last_ts = cached["timestamp"].max()
            # jeśli cache już pokrywa zakres — przefiltruj i zwróć
            if last_ts >= end:
                df = cached[(cached["timestamp"] >= start) & (cached["timestamp"] <= end)].copy()
                return df.reset_index(drop=True)
            # w przeciwnym razie dociągamy od ostatniej + epsilon
            need_from = max(last_ts + timedelta(seconds=1), start)

        # pobierz brakujące świece z API
        req = StockBarsRequest(
            symbol_or_symbols=symbol,
            timeframe=tf_obj,
            start=need_from,
            end=end,
            adjustment=self.cfg.adjustment,
            feed=self.cfg.feed,
            limit=10_000,
        )
        bars = self.client.get_stock_bars(req)
        # wynik może być „multi symbol” — my mamy 1
        if symbol not in bars.data:
            new_df = pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])
        else:
            got = bars.data[symbol]
            new_df = pd.DataFrame(
                {
                    "timestamp": [b.timestamp for b in got],
                    "open": [float(b.open) for b in got],
                    "high": [float(b.high) for b in got],
                    "low": [float(b.low) for b in got],
                    "close": [float(b.close) for b in got],
                    "volume": [int(b.volume) for b in got],
                }
            )

        if not new_df.empty:
            if pd.api.types.is_datetime64_any_dtype(new_df["timestamp"]):
            # nadaj/konwertuj do UTC
                try:
                    new_df["timestamp"] = new_df["timestamp"].dt.tz_convert("UTC")
                except TypeError:
                    new_df["timestamp"] = new_df["timestamp"].dt.tz_localize("UTC")

        # złącz z cache i zapisz
        if cached is not None and not cached.empty:
            df = pd.concat([cached, new_df], ignore_index=True)
            df = df.drop_duplicates(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
        else:
            df = new_df.sort_values("timestamp").reset_index(drop=True)

        if use_cache:
            self.save_cache(symbol, tf, df)

        # przefiltruj finalnie do żądanego [start, end]
        # gwarantuj UTC na finalnym df
        if df["timestamp"].dt.tz is None:
            df["timestamp"] = df["timestamp"].dt.tz_localize("UTC")
        else:
            df["timestamp"] = df["timestamp"].dt.tz_convert("UTC")

        df = df[(df["timestamp"] >= start) & (df["timestamp"] <= end)].copy().reset_index(drop=True)
        return df
