# src/backend/data/assets.py
from __future__ import annotations
import os
import logging
from dataclasses import dataclass
from typing import Optional, Literal, Dict, List

import pandas as pd
from dotenv import load_dotenv
import yaml

from alpaca.trading.client import TradingClient

AssetClassLiteral = Literal["US_EQUITY", "CRYPTO"]


@dataclass
class AssetsConfig:
    api_key_id: str
    api_secret_key: str
    cache_dir: str = "data_cache/assets"

    @staticmethod
    def from_env() -> "AssetsConfig":
        load_dotenv()
        key = os.getenv("APCA_API_KEY_ID", "")
        sec = os.getenv("APCA_API_SECRET_KEY", "")
        cache = os.getenv("ASSETS_CACHE_DIR", "data_cache/assets")
        os.makedirs(cache, exist_ok=True)
        return AssetsConfig(api_key_id=key, api_secret_key=sec, cache_dir=cache)


class AssetsService:
    """
    Serwis pobierania listy instrumentów z Alpaca Trading API + cache CSV.
    - fetch_assets(cls): pobiera z API (ACTIVE) i zwraca DataFrame (filtr lokalny)
    - save_cache/load_cache/ensure_cached: prosty cache per klasa aktywów
    - search(cls, query): filtr po symbolu/nazwie
    - load_universe(): czyta 'data/universe.yaml' (indeksy/surowce -> ETF proxies)
    - symbols_from_proxies(proxies): zwraca detale proxies dostępnych w Alpaca
    - list_crypto(): szybki dostęp do cached listy krypto
    """

    def __init__(self, cfg: Optional[AssetsConfig] = None, logger: Optional[logging.Logger] = None) -> None:
        self.cfg = cfg or AssetsConfig.from_env()
        self.log = logger or logging.getLogger(self.__class__.__name__)
        if not self.log.handlers:
            h = logging.StreamHandler()
            self.log.addHandler(h)
            self.log.setLevel(logging.INFO)
        self.log.propagate = False  # uniknij podwójnych logów

        # /assets działa tak samo dla live/paper — paper=True OK
        self.client = TradingClient(self.cfg.api_key_id, self.cfg.api_secret_key, paper=True)

    # ---------- Cache paths ----------

    def _cache_path(self, cls: AssetClassLiteral) -> str:
        return os.path.join(self.cfg.cache_dir, f"{cls.lower()}.csv")

    # ---------- Core fetch (wstecznie zgodny) ----------

    def fetch_assets(self, cls: AssetClassLiteral = "US_EQUITY", only_tradable: bool = True) -> pd.DataFrame:
        """
        Wersja kompatybilna ze starszymi alpaca-py: pobiera wszystkie assety bez kwargs,
        następnie filtruje lokalnie po klasie i statusie ACTIVE oraz tradowalności.
        """
        assets = self.client.get_all_assets()  # bez kwargs w starszych wersjach

        target_cls = "US_EQUITY" if cls == "US_EQUITY" else "CRYPTO"

        rows = []
        for a in assets:
            status_str = str(getattr(a, "status", "")).upper()
            cls_str = str(getattr(a, "asset_class", "")).upper()

            if "ACTIVE" not in status_str:
                continue
            if target_cls not in cls_str:
                continue
            if only_tradable and not bool(getattr(a, "tradable", False)):
                continue

            rows.append({
                "symbol": a.symbol,
                "name": a.name,
                "class": cls_str,
                "exchange": getattr(a, "exchange", None),
                "tradable": bool(getattr(a, "tradable", False)),
                "shortable": bool(getattr(a, "shortable", False)),
                "marginable": bool(getattr(a, "marginable", False)),
                "fractionable": bool(getattr(a, "fractionable", False)),
                "easy_to_borrow": bool(getattr(a, "easy_to_borrow", False)),
            })

        df = pd.DataFrame(rows).sort_values(["class", "symbol"]).reset_index(drop=True)
        return df

    # ---------- Cache helpers ----------

    def save_cache(self, df: pd.DataFrame, cls: AssetClassLiteral) -> str:
        path = self._cache_path(cls)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        df.to_csv(path, index=False)
        self.log.info("Zapisano cache: %s (%d wierszy)", path, len(df))
        return path

    def load_cache(self, cls: AssetClassLiteral) -> Optional[pd.DataFrame]:
        path = self._cache_path(cls)
        if os.path.exists(path):
            return pd.read_csv(path)
        return None

    def ensure_cached(self, cls: AssetClassLiteral, refresh: bool = False) -> pd.DataFrame:
        if not refresh:
            cached = self.load_cache(cls)
            if cached is not None:
                return cached
        df = self.fetch_assets(cls)
        self.save_cache(df, cls)
        return df

    # ---------- Search ----------

    def search(self, cls: AssetClassLiteral, query: str) -> pd.DataFrame:
        df = self.ensure_cached(cls, refresh=False)
        q = (query or "").strip().lower()
        if not q:
            return df
        mask = df["symbol"].str.lower().str.contains(q) | df["name"].str.lower().str.contains(q)
        return df[mask].copy().reset_index(drop=True)

    # ---------- Curated universe (indeksy/surowce -> proxies) ----------

    def load_universe(self, path: str = "data/universe.yaml") -> Dict[str, List[dict]]:
        """
        Wczytuje 'data/universe.yaml'. Zwraca dict z kluczami:
        indices: [{name, proxies: [...]}],
        commodities: [{name, proxies: [...]}],
        crypto_buckets: [{name, symbols: [...]}],
        notes: [str, ...]
        """
        if not os.path.exists(path):
            return {"indices": [], "commodities": [], "crypto_buckets": [], "notes": []}
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        data.setdefault("indices", [])
        data.setdefault("commodities", [])
        data.setdefault("crypto_buckets", [])
        data.setdefault("notes", [])
        return data

    def symbols_from_proxies(self, proxies: List[str]) -> pd.DataFrame:
        """
        Zwraca wiersze z cached US_EQUITY odpowiadające podanym proxy (np. ['QQQ','GLD']).
        """
        df = self.ensure_cached("US_EQUITY", refresh=False)
        if df.empty or not proxies:
            return pd.DataFrame(columns=df.columns if not df.empty else ["symbol", "name"])
        symset = [p.upper() for p in proxies]
        return df[df["symbol"].isin(symset)].copy().reset_index(drop=True)

    def list_crypto(self) -> pd.DataFrame:
        """Zwraca cached listę CRYPTO (ACTIVE, tradable)."""
        return self.ensure_cached("CRYPTO", refresh=False)
