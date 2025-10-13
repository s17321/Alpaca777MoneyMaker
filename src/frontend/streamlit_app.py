# src/frontend/streamlit_app.py

from __future__ import annotations

import os
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import streamlit as st
import altair as alt
from datetime import datetime, timedelta, timezone

# --- Streamlit page config MUST be first Streamlit call ---
st.set_page_config(page_title="Alpaca777 — Dashboard", layout="wide")
# ----------------------------------------------------------

# --- add project root to sys.path so "src" is importable when running via `streamlit run` ---
import os, sys
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
# -------------------------------------------------------------------------------------------

from src.backend.data.assets import AssetsService
from src.backend.data.market_data import MarketDataService
from src.backend.broker.alpaca_client import (
    AlpacaBroker,
    BrokerConfig,
    BrokerAuthError,
    BrokerError,
    BrokerOrderRejected,
    BrokerNetworkError,
    BrokerRateLimitError,
)

WARSAW = ZoneInfo("Europe/Warsaw")

st.sidebar.title("Nawigacja")
st.sidebar.markdown("Wybierz stronę w menu poniżej (Analizy, RL Bot).")

@st.cache_resource
def get_broker() -> AlpacaBroker:
    # cache_resource: jedna instancja brokera na sesję
    cfg = BrokerConfig.from_env()
    return AlpacaBroker(cfg)


def to_warsaw(dt) -> str:
    if dt is None:
        return "-"
    if isinstance(dt, str):
        # alpaca-py zwykle daje datetime, ale gdyby był string ISO:
        dt = datetime.fromisoformat(dt.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        # traktuj jako UTC
        dt = dt.replace(tzinfo=ZoneInfo("UTC"))
    return dt.astimezone(WARSAW).strftime("%Y-%m-%d %H:%M:%S %Z")


def render_header(broker: AlpacaBroker):
    st.title("🐮 Alpaca777 — Trading Dashboard")
    st.caption(
        f"DRY_RUN: **{broker.cfg.dry_run}** · Endpoint: `{broker.cfg.base_url}` · Extended hours allowed: **{broker.cfg.allow_extended_hours}**"
    )
    st.divider()


def render_account_and_clock(broker: AlpacaBroker):
    st.subheader("Konto i rynek")
    cols = st.columns(2)

    with cols[0]:
        st.markdown("**Account**")
        try:
            acc = broker.get_account()
            st.metric("Equity", f"{acc.get('equity')}")
            st.metric("Cash", f"{acc.get('cash')}")
            st.metric("Buying Power", f"{acc.get('buying_power')}")
            st.text(f"Status: {acc.get('status')}")
        except (BrokerError, BrokerAuthError) as e:
            st.error(f"Account error: {e}")

    with cols[1]:
        st.markdown("**Clock**")
        try:
            clk = broker.get_clock()
            is_open = clk.get("is_open")
            st.metric("Is open", "Yes" if is_open else "No")
            st.text(f"Next Open (NY): {clk.get('next_open')}")
            st.text(f"Next Close (NY): {clk.get('next_close')}")
            # Lokalnie (Warszawa)
            st.text(f"Next Open (WAW): {to_warsaw(clk.get('next_open'))}")
            st.text(f"Next Close (WAW): {to_warsaw(clk.get('next_close'))}")
        except (BrokerError, BrokerAuthError) as e:
            st.error(f"Clock error: {e}")

    st.divider()


def render_positions(broker: AlpacaBroker):
    st.subheader("Otwarte pozycje")
    try:
        pos = broker.get_open_positions()
        if not pos:
            st.info("Brak otwartych pozycji.")
            return
        df = pd.DataFrame(pos)
        st.dataframe(df, use_container_width=True)
    except (BrokerError, BrokerAuthError) as e:
        st.error(f"Positions error: {e}")


def render_order_form(broker: AlpacaBroker):
    st.subheader("Złóż market order")
    with st.form("order_form"):
        c1, c2, c3, c4 = st.columns([2, 1, 1, 1])
        symbol = c1.text_input("Symbol", value="AAPL")
        qty = c2.number_input("Qty", min_value=1, step=1, value=1)
        side = c3.selectbox("Side", ["buy", "sell"], index=0)
        submit = st.form_submit_button(
            "Submit order" + (" (SIMULATED — DRY_RUN)" if broker.cfg.dry_run else "")
        )

        if submit:
            try:
                res = broker.submit_market_order(symbol=symbol.strip().upper(), qty=int(qty), side=side)
                if broker.cfg.dry_run:
                    st.success(f"✅ [SIM] Order accepted: {res}")
                else:
                    st.success(f"✅ Order sent: {res}")
            except (BrokerOrderRejected, BrokerRateLimitError, BrokerNetworkError, BrokerAuthError, BrokerError) as e:
                st.error(f"❌ Order error: {e}")

    st.caption(
        "Uwaga: przy **DRY_RUN=true** zlecenia są tylko symulowane. Aby wysyłać realne zlecenia na PAPER, ustaw `DRY_RUN=false` w `.env`."
    )
    st.divider()


def render_cancel_all(broker: AlpacaBroker):
    st.subheader("Zarządzanie zleceniami")
    if st.button("Cancel all (pending orders)"):
        try:
            res = broker.cancel_all()
            st.success(f"OK: {res}")
        except (BrokerError, BrokerAuthError) as e:
            st.error(f"Cancel error: {e}")

def render_candles(df: pd.DataFrame, title: str = "Candles"):
    if df.empty:
        st.warning("Brak danych do wyświetlenia.")
        return
    base = alt.Chart(df).encode(x="timestamp:T")
    rule = base.mark_rule().encode(
        y="low:Q",
        y2="high:Q",
        tooltip=["timestamp:T", "open:Q", "high:Q", "low:Q", "close:Q", "volume:Q"],
    )
    bar = base.mark_bar().encode(
        y="open:Q",
        y2="close:Q",
        color=alt.condition("datum.close >= datum.open", alt.value("#4caf50"), alt.value("#f44336")),
    )
    chart = (rule + bar).properties(width="container", height=380, title=title)
    st.altair_chart(chart, use_container_width=True)

def render_market_data_section():
    st.subheader("Dane rynkowe & wykres")
    mds = MarketDataService()  # korzysta z tych samych kluczy z .env

    c1, c2, c3, c4 = st.columns([2, 1, 1, 1])
    symbol = c1.text_input("Symbol", value="AAPL").upper()
    timeframe = c2.selectbox("Timeframe", ["1Day", "1Hour", "15Min", "5Min"], index=0)

    # domyślnie 180 dni dla 1Day, 14 dni dla intra
    default_days = 180 if timeframe.endswith("Day") else 14
    days_back = c3.number_input("Ile dni wstecz", min_value=1, max_value=1500, value=default_days, step=1)
    action = c4.button("Pobierz & pokaż")

    if action:
        try:
            end = datetime.now(timezone.utc)                    # <-- UTC-aware
            start = end - timedelta(days=int(days_back))        # <-- też UTC-aware
            with st.spinner("Pobieram dane..."):
                df = mds.get_bars(symbol, timeframe=timeframe, start=start, end=end, use_cache=True)
            st.success(f"Pobrano {len(df)} świec.")
            render_candles(df, title=f"{symbol} — {timeframe}")
            st.caption("Dane: Alpaca Market Data (IEX), cache: ./data_cache/*.csv")
        except Exception as e:
            st.error(f"Market data error: {e}")
    st.divider()

@st.cache_resource
def get_assets_service():
    from src.backend.data.assets import AssetsService
    return AssetsService()

def render_assets_section():
    st.subheader("Instrumenty (Alpaca Assets)")
    svc = get_assets_service()

    tab1, tab2 = st.tabs(["US Equities", "Crypto"])

    with tab1:
        c1, c2 = st.columns([1, 1])
        # klucze przycisków MUSZĄ być unikalne
        if c1.button("Pobierz/odśwież listę akcji (cache)", key="refresh_equities"):
            st.session_state["_do_refresh_equities"] = True
        query = c2.text_input("Szukaj (symbol/nazwa)", value="", key="search_equities")

        # edge trigger – wykonaj fetch raz, potem skasuj flagę
        if st.session_state.get("_do_refresh_equities"):
            with st.spinner("Pobieram aktywne akcje..."):
                df_eq = svc.fetch_assets("US_EQUITY")
                svc.save_cache(df_eq, "US_EQUITY")
                st.success(f"Pobrano {len(df_eq)} symboli. Zapisano do cache.")
            st.session_state["_do_refresh_equities"] = False

        df = svc.search("US_EQUITY", query)
        st.dataframe(df, use_container_width=True, height=300)

    with tab2:
        c1, c2 = st.columns([1, 1])
        if c1.button("Pobierz/odśwież listę krypto (cache)", key="refresh_crypto"):
            st.session_state["_do_refresh_crypto"] = True
        query_c = c2.text_input("Szukaj (crypto)", value="", key="search_crypto")

        if st.session_state.get("_do_refresh_crypto"):
            with st.spinner("Pobieram aktywne crypto..."):
                df_c = svc.fetch_assets("CRYPTO")
                svc.save_cache(df_c, "CRYPTO")
                st.success(f"Pobrano {len(df_c)} symboli. Zapisano do cache.")
            st.session_state["_do_refresh_crypto"] = False

        dfc = svc.search("CRYPTO", query_c)
        st.dataframe(dfc, use_container_width=True, height=300)

    st.caption("Źródło: Trading API → /assets (ACTIVE, tradable). Indeksy: używaj ETF (np. QQQ dla NASDAQ-100).")
    st.divider()


def main():
    broker = get_broker()
    render_header(broker)
    render_account_and_clock(broker)
    render_assets_section()

    c1, c2 = st.columns([2, 1])
    with c1:
        render_positions(broker)
        render_market_data_section()
    with c2:
        render_order_form(broker)
        render_cancel_all(broker)

    st.info(
        "Plan na później: wykres świec + sygnały strategii, widok backtestów, sentyment/news feed i panel konfiguracji botów."
    )


if __name__ == "__main__":
    main()
