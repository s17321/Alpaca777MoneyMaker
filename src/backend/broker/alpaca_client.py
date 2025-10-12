# src/backend/broker/alpaca_client.py
from __future__ import annotations

import os
import time
import uuid
import logging
from dataclasses import dataclass
from typing import Optional, Literal, Dict, Any

from dotenv import load_dotenv

# UWAGA: na razie nie używamy faktycznie SDK; podłączymy je w kolejnym kroku.
# from alpaca_trade_api import REST, RESTError  # noqa: F401

Side = Literal["buy", "sell"]
TimeInForce = Literal["day"]  # rozszerzymy później

# ===== Wyjątki specyficzne dla naszej warstwy brokera =====
class BrokerError(Exception): ...
class BrokerValidationError(BrokerError): ...
class BrokerAuthError(BrokerError): ...
class BrokerRateLimitError(BrokerError): ...
class BrokerNetworkError(BrokerError): ...
class BrokerOrderRejected(BrokerError): ...

# ===== Struktury wyników (minimalne) =====
Account = Dict[str, Any]
Clock = Dict[str, Any]
OrderResult = Dict[str, Any]
Position = Dict[str, Any]


@dataclass
class BrokerConfig:
    api_key_id: str
    api_secret_key: str
    base_url: str = "https://paper-api.alpaca.markets"
    dry_run: bool = True
    default_tif: TimeInForce = "day"
    client_prefix: str = "alpaca777"

    @staticmethod
    def from_env() -> "BrokerConfig":
        load_dotenv()
        key = os.getenv("APCA_API_KEY_ID", "")
        secret = os.getenv("APCA_API_SECRET_KEY", "")
        base = os.getenv("APCA_API_BASE_URL", "https://paper-api.alpaca.markets")
        dry = os.getenv("DRY_RUN", "true").lower() == "true"
        tif = os.getenv("DEFAULT_TIF", "day")
        prefix = os.getenv("ORDER_CLIENT_PREFIX", "alpaca777")

        if not key or not secret:
            # W DRY_RUN pozwalamy działać bez kluczy, żeby szybciej ruszyć.
            if not dry:
                raise BrokerAuthError("Brakuje APCA_API_KEY_ID / APCA_API_SECRET_KEY w .env")
        return BrokerConfig(
            api_key_id=key,
            api_secret_key=secret,
            base_url=base,
            dry_run=dry,
            default_tif=tif,  # type: ignore
            client_prefix=prefix,
        )


class AlpacaBroker:
    """
    Hermetyzacja komunikacji z brokerem (Alpaca).
    Publiczne metody zwracają nasze proste dict-y, nie obiekty SDK.

    TODO (kolejny krok): wpiąć prawdziwe wywołania SDK dla metod READ/WRITE, obsłużyć retry/backoff.
    """

    def __init__(self, cfg: Optional[BrokerConfig] = None, logger: Optional[logging.Logger] = None) -> None:
        self.cfg = cfg or BrokerConfig.from_env()
        self.log = logger or logging.getLogger(self.__class__.__name__)
        self._setup_logger()

        # TODO (kolejny krok): utworzyć klienta REST Alpaca tylko jeśli not dry_run
        # self._rest = REST(self.cfg.api_key_id, self.cfg.api_secret_key, base_url=self.cfg.base_url)

        self.log.info(
            "AlpacaBroker zainicjalizowany (dry_run=%s, base_url=%s)", self.cfg.dry_run, self.cfg.base_url
        )

    # ---------- public API ----------

    def get_account(self) -> Account:
        """Zwraca podstawowe informacje o koncie. (READ)"""
        # TODO: implementacja z SDK; na teraz placeholder przydatny do uruchomienia UI/logów
        return {
            "equity": None,
            "cash": None,
            "buying_power": None,
            "status": "unknown (placeholder)",
        }

    def get_clock(self) -> Clock:
        """Zwraca status rynku i granice sesji. (READ)"""
        # TODO: implementacja z SDK; teraz tylko placeholder
        return {
            "is_open": None,
            "next_open": None,
            "next_close": None,
        }

    def get_open_positions(self) -> list[Position]:
        """Lista otwartych pozycji. (READ)"""
        # TODO: implementacja z SDK
        return []

    def cancel_all(self) -> dict:
        """Anuluje wszystkie oczekujące zlecenia. (WRITE lub NOP w DRY_RUN)"""
        if self.cfg.dry_run:
            self.log.info("[DRY_RUN] cancel_all() — symulacja, nic nie wysyłam.")
            return {"cancelled": 0, "dry_run": True}
        # TODO: prawdziwe wywołanie SDK
        raise NotImplementedError("cancel_all() prawdziwe wywołanie — w kolejnym kroku")

    def submit_market_order(
        self,
        symbol: str,
        qty: int,
        side: Side,
        tif: TimeInForce | None = None,
        client_order_id: Optional[str] = None,
    ) -> OrderResult:
        """
        Składa lub symuluje market order.
        Waliduje parametry i buduje client_order_id (idempotencja).
        """
        self._validate_order(symbol, qty, side, tif)
        coid = client_order_id or self._make_client_order_id(symbol, side)

        if self.cfg.dry_run:
            # Symulacja: tylko log + pseudo-wynik
            now = time.time()
            fake_id = f"SIM-{uuid.uuid4().hex[:12]}"
            self.log.info(
                "[DRY_RUN] Market %s %s x%d (tif=%s, coid=%s) — symuluję wysłanie.",
                side.upper(),
                symbol,
                qty,
                tif or self.cfg.default_tif,
                coid,
            )
            return {
                "id": fake_id,
                "client_order_id": coid,
                "status": "accepted (simulated)",
                "symbol": symbol,
                "qty": qty,
                "side": side,
                "submitted_at": now,
                "dry_run": True,
            }

        # TODO: prawdziwe wywołanie SDK + mapowanie błędów
        raise NotImplementedError("submit_market_order() prawdziwe wywołanie — w kolejnym kroku")

    # ---------- helpers ----------

    def _setup_logger(self) -> None:
        if not self.log.handlers:
            handler = logging.StreamHandler()
            fmt = logging.Formatter("%(asctime)s | %(name)s | %(levelname)s | %(message)s")
            handler.setFormatter(fmt)
            self.log.addHandler(handler)
            self.log.setLevel(logging.INFO)

    def _validate_order(self, symbol: str, qty: int, side: Side, tif: Optional[TimeInForce]) -> None:
        if not symbol or not symbol.strip():
            raise BrokerValidationError("symbol nie może być pusty")
        if qty <= 0:
            raise BrokerValidationError("qty musi być > 0")
        if side not in ("buy", "sell"):
            raise BrokerValidationError("side musi być 'buy' lub 'sell'")
        if tif is not None and tif not in ("day",):
            raise BrokerValidationError("tif=day jest jedyną dozwoloną opcją na start")

    def _make_client_order_id(self, symbol: str, side: Side) -> str:
        # Format: <prefix>-<symbol>-<side>-<losowy>
        return f"{self.cfg.client_prefix}-{symbol}-{side}-{uuid.uuid4().hex[:8]}"
