# src/backend/broker/alpaca_client.py
from __future__ import annotations

import logging
import os
import time
import uuid
import requests
from dataclasses import dataclass
from typing import Any, Dict, Literal, Optional

from dotenv import load_dotenv

# alpaca-py (TRADING)
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderSide, TimeInForce
from alpaca.trading.requests import MarketOrderRequest
from alpaca.common.exceptions import APIError

Side = Literal["buy", "sell"]
TimeInForceLiteral = Literal["day"]  # rozszerzymy później

# ===== Wyjątki specyficzne dla naszej warstwy brokera =====
class BrokerError(Exception):
    """Ogólny błąd warstwy brokera."""


class BrokerValidationError(BrokerError):
    """Nieprawidłowe dane wejściowe (symbol/qty/side/tif)."""


class BrokerAuthError(BrokerError):
    """Błędne klucze/sekrety lub brak uprawnień."""


class BrokerRateLimitError(BrokerError):
    """Przekroczony limit zapytań API."""


class BrokerNetworkError(BrokerError):
    """Błąd sieci/połączenia."""


class BrokerOrderRejected(BrokerError):
    """Zlecenie odrzucone przez brokera."""


# ===== Struktury wyników (minimalne, proste dict-y) =====
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
    default_tif: TimeInForceLiteral = "day"
    client_prefix: str = "alpaca777"
    allow_extended_hours: bool = False

    @staticmethod
    def from_env() -> "BrokerConfig":
        """
        Ładuje konfigurację z .env. W DRY_RUN pozwalamy na puste klucze,
        żeby móc uruchomić symulację bez dostępu do API.
        """
        load_dotenv()
        key = os.getenv("APCA_API_KEY_ID", "")
        secret = os.getenv("APCA_API_SECRET_KEY", "")
        base = os.getenv("APCA_API_BASE_URL", "https://paper-api.alpaca.markets")
        dry = os.getenv("DRY_RUN", "true").lower() == "true"
        tif = os.getenv("DEFAULT_TIF", "day")
        prefix = os.getenv("ORDER_CLIENT_PREFIX", "alpaca777")
        allow_ext = os.getenv("ALLOW_EXTENDED_HOURS", "false").lower() == "true"

        if (not key or not secret) and not dry:
            raise BrokerAuthError("Brakuje APCA_API_KEY_ID / APCA_API_SECRET_KEY w .env (DRY_RUN=false).")

        return BrokerConfig(
            api_key_id=key,
            api_secret_key=secret,
            base_url=base,
            dry_run=dry,
            default_tif=tif,  # type: ignore
            client_prefix=prefix,
            allow_extended_hours=allow_ext,
        )


class AlpacaBroker:
    """
    Hermetyzacja komunikacji z brokerem (Alpaca) na bazie alpaca-py.

    Publiczne metody zwracają nasze lekkie dict-y zamiast obiektów SDK,
    żeby warstwa strategii/frontendu była odporna na zmiany biblioteki.
    """

    def __init__(self, cfg: Optional[BrokerConfig] = None, logger: Optional[logging.Logger] = None) -> None:
        self.cfg = cfg or BrokerConfig.from_env()
        self.log = logger or logging.getLogger(self.__class__.__name__)
        self._setup_logger()

        # TradingClient tworzymy tylko jeśli mamy klucze (w DRY_RUN bez kluczy jedziemy symulację)
        self._client: Optional[TradingClient] = None
        if self.cfg.api_key_id and self.cfg.api_secret_key:
            self._client = TradingClient(
                self.cfg.api_key_id,
                self.cfg.api_secret_key,
                paper=self.cfg.base_url.endswith("paper-api.alpaca.markets"),
            )

        self.log.info(
            "AlpacaBroker zainicjalizowany (dry_run=%s, base_url=%s)", self.cfg.dry_run, self.cfg.base_url
        )

    # ---------- PUBLIC API: READ ----------

    def get_account(self) -> Account:
        """Zwraca podstawowe informacje o koncie (equity, cash, buying_power, status)."""
        client = self._ensure_client()
        try:
            acc = self._with_retry(lambda: client.get_account())
            return {
                "equity": float(acc.equity) if acc.equity is not None else None,
                "cash": float(acc.cash) if acc.cash is not None else None,
                "buying_power": float(acc.buying_power) if acc.buying_power is not None else None,
                "status": getattr(acc, "status", None),
            }
        except APIError as e:
            msg = str(e)
            if "401" in msg or "403" in msg:
                raise BrokerAuthError(f"Auth error: {msg}") from e
            raise BrokerError(f"API error w get_account(): {msg}") from e

    def get_clock(self) -> Clock:
        """Zwraca status rynku oraz następne otwarcie/zamknięcie (datetime)."""
        client = self._ensure_client()
        try:
            clk = self._with_retry(lambda: client.get_clock())
            return {
                "is_open": bool(getattr(clk, "is_open", False)),
                "next_open": getattr(clk, "next_open", None),
                "next_close": getattr(clk, "next_close", None),
            }
        except APIError as e:
            msg = str(e)
            if "401" in msg or "403" in msg:
                raise BrokerAuthError(f"Auth error: {msg}") from e
            raise BrokerError(f"API error w get_clock(): {msg}") from e

    def get_open_positions(self) -> list[Position]:
        """Lista aktualnie otwartych pozycji."""
        client = self._ensure_client()
        try:
            pos = self._with_retry(lambda: client.get_all_positions())
            out: list[Position] = []
            for p in pos:
                out.append(
                    {
                        "symbol": p.symbol,
                        "qty": float(p.qty),
                        "avg_price": float(p.avg_entry_price) if p.avg_entry_price is not None else None,
                        "market_value": float(p.market_value) if p.market_value is not None else None,
                        "unrealized_pl": float(p.unrealized_pl) if p.unrealized_pl is not None else None,
                    }
                )
            return out
        except APIError as e:
            raise BrokerError(f"API error w get_open_positions(): {e}") from e

    # ---------- PUBLIC API: WRITE ----------

    def cancel_all(self) -> dict:
        """
        Anuluje wszystkie oczekujące zlecenia.
        W DRY_RUN to tylko bezpieczna symulacja.
        """
        if self.cfg.dry_run:
            self.log.info("[DRY_RUN] cancel_all() — symulacja, nic nie wysyłam.")
            return {"cancelled": 0, "dry_run": True}

        client = self._ensure_client()
        try:
            res = self._with_retry(lambda: client.cancel_orders())
            return {"cancelled": len(res) if hasattr(res, "__len__") else None, "dry_run": False}
        except APIError as e:
            raise BrokerError(f"API error w cancel_all(): {e}") from e

    def submit_market_order(
        self,
        symbol: str,
        qty: int,
        side: Side,
        tif: Optional[TimeInForceLiteral] = None,
        client_order_id: Optional[str] = None,
    ) -> OrderResult:
        """
        Składa lub symuluje market order (z idempotency client_order_id).
        Waliduje parametry i loguje szczegóły.
        """
        self._validate_order(symbol, qty, side, tif)
        self._preflight_market_open() 
        coid = client_order_id or self._make_client_order_id(symbol, side)

        # DRY_RUN: tylko log + pseudo-wynik
        if self.cfg.dry_run:
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

        # REAL call
        client = self._ensure_client()
        try:
            order_req = MarketOrderRequest(
                symbol=symbol,
                qty=qty,
                side=OrderSide.BUY if side == "buy" else OrderSide.SELL,
                time_in_force=TimeInForce.DAY,
                client_order_id=coid,
            )

            placed = self._with_retry(lambda: client.submit_order(order_data=order_req))

            return {
                "id": placed.id,
                "client_order_id": placed.client_order_id,
                "status": placed.status,
                "symbol": placed.symbol,
                "qty": int(placed.qty) if getattr(placed, "qty", None) is not None else qty,
                "side": side,
                "submitted_at": getattr(placed, "submitted_at", None),
                "dry_run": False,
            }

        except APIError as e:
            msg = str(e)
            low = msg.lower()
            if "insufficient" in low or "rejected" in low:
                raise BrokerOrderRejected(f"Order rejected: {msg}") from e
            if "429" in low or "rate limit" in low:
                raise BrokerRateLimitError(f"Rate limit: {msg}") from e
            if "401" in low or "403" in low:
                raise BrokerAuthError(f"Auth error: {msg}") from e
            raise BrokerError(f"API error w submit_market_order(): {msg}") from e

    # ---------- HELPERS ----------

    def _setup_logger(self) -> None:
        if not self.log.handlers:
            handler = logging.StreamHandler()
            fmt = logging.Formatter("%(asctime)s | %(name)s | %(levelname)s | %(message)s")
            handler.setFormatter(fmt)
            self.log.addHandler(handler)
        # Domyślnie INFO; możesz podnieść do DEBUG w .env i tu odczytać.
        self.log.setLevel(logging.INFO)

    def _ensure_client(self) -> TradingClient:
        if not self._client:
            # W DRY_RUN pozwalaliśmy działać bez kluczy — ale do READ/WRITE potrzebny klient.
            raise BrokerAuthError("Brak klienta TradingClient: uzupełnij APCA_API_* w .env lub włącz DRY_RUN.")
        return self._client

    def _with_retry(self, fn, *, tries: int = 3, base_delay: float = 0.5):
        """
        Lekki exponential backoff na błędy API i sieci.
        - APIError: retry (czasem chwilowe 5xx/429)
        - requests.ConnectionError/Timeout: retry, a po wyczerpaniu prób -> BrokerNetworkError
        """
        attempt = 0
        while True:
            try:
                return fn()
            except APIError as e:
                # czasem chwilowe problemy po stronie Alpaca – spróbujmy ponownie
                if attempt < tries - 1:
                    delay = base_delay * (2 ** attempt)
                    self.log.warning("APIError: %s — retry za %.1fs (attempt %d/%d)...", e, delay, attempt + 1, tries)
                    time.sleep(delay)
                    attempt += 1
                    continue
                # wyczerpane próby – przepuść dalej, górne warstwy zmapują na BrokerError/Auth/RateLimit
                raise
            except (requests.ConnectionError, requests.Timeout) as e:
                if attempt < tries - 1:
                    delay = base_delay * (2 ** attempt)
                    self.log.warning("Network error: %s — retry za %.1fs (attempt %d/%d)...",
                                    e.__class__.__name__, delay, attempt + 1, tries)
                    time.sleep(delay)
                    attempt += 1
                    continue
                # po próbach: rzuć nasz kontrolowany wyjątek, który front już łapie
                raise BrokerNetworkError(f"Network error: {e}") from e

    def _validate_order(self, symbol: str, qty: int, side: Side, tif: Optional[TimeInForceLiteral]) -> None:
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
    
    def _preflight_market_open(self) -> None:
        """
        Blokuje market order poza regularną sesją gdy allow_extended_hours=False.
        Działa także w DRY_RUN (symulacje są realistyczne).
        """
        # jeśli świadomie pozwalasz na extended hours – przepuszczamy
        if self.cfg.allow_extended_hours:
            return
        # bez klienta (DRY_RUN bez kluczy) – nie wiemy, czy otwarte: pozwólmy, ale poinformujmy
        if self._client is None:
            self.log.info("[PRE-FLIGHT] Brak klienta/kluczy – nie mogę sprawdzić clock. Kontynuuję.")
            return

        clk = self.get_clock()
        if not clk.get("is_open", False):
            raise BrokerValidationError(
                "Rynek jest zamknięty (is_open=False). Ustaw ALLOW_EXTENDED_HOURS=true jeśli chcesz zlecać poza sesją."
            )
