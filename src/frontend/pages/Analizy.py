# --- make 'src' importable ---
import os, sys
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
# -----------------------------

from datetime import datetime, timedelta, timezone
from typing import List, Dict

import pandas as pd
import streamlit as st
import altair as alt

from src.backend.data.market_data import MarketDataService
from src.backend.data.assets import AssetsService
from src.backend.rl.features.pipeline import FeaturePipeline, FeaturePipelineConfig
from src.backend.rl.environment.basic_env import TradingEnv, EnvConfig


st.set_page_config(page_title="Analizy — Alpaca777", layout="wide")
st.title("📈 Analizy i strategie")
st.caption("Wybierz instrument → pobierz dane → zobacz wykres świec, SMA i mini-metryki.")

# ---------- Services ----------
@st.cache_resource
def get_mds():
    return MarketDataService()

@st.cache_resource
def get_assets():
    return AssetsService()

mds = get_mds()
assets = get_assets()

# ---------- Helpers ----------
INDEX_ALIAS_MAP: Dict[str, List[str]] = {
    # popularne aliasy ⇒ ETF-proxies (możesz rozszerzać)
    "US100": ["QQQ", "QQQM"],     # NASDAQ-100
    "NASDAQ100": ["QQQ", "QQQM"],
    "NDX": ["QQQ", "QQQM"],
    "SP500": ["SPY", "VOO", "IVV"],
    "DE40": ["DAX", "EWG"],       # DAX / Niemcy proxy (ETF na Niemcy)
    "WIG20": ["EPOL"],            # najbliższy proxy na PL (MSCI Poland)
    "GOLD": ["GLD", "IAU"],
    "OIL": ["USO"],
}

def plot_candles(df: pd.DataFrame, overlays: dict[str, pd.Series] | None = None, title="Candles"):
    if df.empty:
        st.warning("Brak danych.")
        return
    base = alt.Chart(df).encode(x="timestamp:T")
    rule = base.mark_rule().encode(
        y="low:Q", y2="high:Q",
        tooltip=["timestamp:T","open:Q","high:Q","low:Q","close:Q","volume:Q"]
    )
    bar = base.mark_bar().encode(
        y="open:Q", y2="close:Q",
        color=alt.condition("datum.close >= datum.open", alt.value("#4caf50"), alt.value("#f44336"))
    )
    chart = (rule + bar).properties(height=380, title=title)
    if overlays:
        for name, s in overlays.items():
            odf = pd.DataFrame({"timestamp": df["timestamp"], name: s})
            line = alt.Chart(odf).mark_line().encode(
                x="timestamp:T", y=alt.Y(f"{name}:Q", title="")
            )
            chart = chart + line
    st.altair_chart(chart, use_container_width=True)

def perf_metrics(prices: pd.Series) -> dict:
    ret = prices.pct_change().fillna(0.0)
    cum = (1 + ret).cumprod()
    peak = cum.cummax()
    dd = (cum / peak - 1.0)
    mdd = dd.min() if len(dd) else 0.0
    sharpe = (ret.mean() / (ret.std() + 1e-9)) * (252 ** 0.5)
    return {"CAGR-ish": cum.iloc[-1] - 1 if len(cum) else 0.0, "MaxDD": mdd, "Sharpe~": sharpe}

# ---------- Instrument picker (TOP) ----------
st.subheader("Wybór instrumentu")

colA, colB, colC = st.columns([1.2, 1, 2])
category = colA.selectbox(
    "Kategoria",
    ["Akcje/ETF", "Krypto", "Indeksy (ETF-proxy)", "Surowce (ETF-proxy)"],
    index=0,
)

search_q = colB.text_input("Szukaj symbol/nazwa", value="").strip()
timeframe = colC.selectbox("Timeframe", ["1Day","1Hour","15Min","5Min"], index=0)

# wczytaj listy wg kategorii
symbol_options: List[str] = []
symbol_label_map: Dict[str, str] = {}

if category == "Akcje/ETF":
    df = assets.search("US_EQUITY", search_q)
    # pokaż top 30 dla przejrzystości
    df = df.iloc[:30] if len(df) > 30 else df
    for _, r in df.iterrows():
        sym = str(r["symbol"])
        label = f"{sym} — {r.get('name','')}"
        symbol_options.append(sym)
        symbol_label_map[sym] = label

elif category == "Krypto":
    dfc = assets.search("CRYPTO", search_q)
    dfc = dfc.iloc[:50] if len(dfc) > 50 else dfc
    for _, r in dfc.iterrows():
        sym = str(r["symbol"])
        label = f"{sym} — {r.get('name','')}"
        symbol_options.append(sym)
        symbol_label_map[sym] = label

else:
    # Indeksy / Surowce – z curated universe + aliasy
    uni = assets.load_universe()
    pool = []
    if category.startswith("Indeksy"):
        for item in uni.get("indices", []):
            pool.extend(item.get("proxies", []))
    else:  # Surowce
        for item in uni.get("commodities", []):
            pool.extend(item.get("proxies", []))
    # dodaj aliasy z mapy (np. US100 -> QQQ/QQQM)
    if search_q:
        alias_key = search_q.upper().replace(" ", "")
        pool.extend(INDEX_ALIAS_MAP.get(alias_key, []))
    # unikalne i dostępne w Alpaca (US_EQUITY)
    proxies_df = assets.symbols_from_proxies(sorted(set(pool)))
    if search_q:
        # dodatkowy filtr po wpisanym tekście również po symbolu/nazwie
        mask = proxies_df["symbol"].str.contains(search_q, case=False) | proxies_df["name"].str.contains(search_q, case=False)
        proxies_df = proxies_df[mask].copy()
    for _, r in proxies_df.iterrows():
        sym = str(r["symbol"])
        label = f"{sym} — {r.get('name','')}"
        symbol_options.append(sym)
        symbol_label_map[sym] = label

# Fallback gdy lista pusta
if not symbol_options:
    symbol_options = ["AAPL"] if category == "Akcje/ETF" else (["BTCUSD"] if category == "Krypto" else ["QQQ"])
    for s in symbol_options:
        symbol_label_map[s] = s

selected = st.selectbox(
    "Wybierz instrument",
    options=symbol_options,
    format_func=lambda s: symbol_label_map.get(s, s),
    index=0,
    key="instrument_select",
)

# parametry analizy
c1, c2, c3, c4 = st.columns([1,1,1,1])
days = c1.number_input("Ile dni wstecz", min_value=30, max_value=2000, value=365, step=10)
fast = c2.slider("SMA fast", 5, 100, 20, step=1)
slow = c3.slider("SMA slow", 20, 300, 50, step=1)
go = c4.button("Pobierz dane i policz", type="primary")

st.divider()

# ---------- Analysis ----------
if go:
    with st.spinner("Pobieram dane…"):
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=int(days))
        df = mds.get_bars(selected, timeframe=timeframe, start=start, end=end, use_cache=True)

    cfg = FeaturePipelineConfig()
    pipe = FeaturePipeline(cfg)

    bm_df = None
    feats = pipe.compute_from_df(df, benchmark_df=bm_df)

    st.subheader("Wektor cech — ostatnie 10 wierszy")
    st.dataframe(feats.tail(10), use_container_width=True, height=280)

    if df.empty:
        st.error("Brak danych dla tego symbolu/timeframe.")
    else:
        # policz SMA PRZED zapisem do session_state
        df["sma_fast"] = df["close"].rolling(fast).mean()
        df["sma_slow"] = df["close"].rolling(slow).mean()

        # wykres
        plot_candles(
            df,
            overlays={"SMA fast": df["sma_fast"], "SMA slow": df["sma_slow"]},
            title=f"{selected} — {timeframe}"
        )

        # prościutki long-only (fast>slow)
        df["signal"] = (df["sma_fast"] > df["sma_slow"]).astype(int)
        ret = df["close"].pct_change().fillna(0.0)
        strat_ret = ret * df["signal"].shift(1).fillna(0.0)
        equity = (1 + strat_ret).cumprod()

        st.subheader("Mini-metryki")
        left, right = st.columns(2)
        m_bh = perf_metrics(df["close"])
        m_sma = perf_metrics(equity)
        with left:
            st.markdown("**Buy & Hold (Close)**")
            st.json({k: round(float(v),4) for k,v in m_bh.items()})
        with right:
            st.markdown("**SMA crossover (long-only)**")
            st.json({k: round(float(v),4) for k,v in m_sma.items()})

        st.line_chart(
            pd.DataFrame({
                "Close (norm)": (df["close"] / df["close"].iloc[0]),
                "Strategy": equity
            }).set_index(df["timestamp"])
        )

        # ← ZAPIS do session_state na końcu (df już ma SMA)
        st.session_state["analysis_df"] = df
        st.session_state["analysis_feats"] = feats
    
# ---------- Symulacja (poza if go:) ----------
st.subheader("Symulacja polityki SMA (proxy 'agenta')")
run_sim = st.button("Uruchom symulację (SMA policy)")

if run_sim:
    df_cached = st.session_state.get("analysis_df")
    feats_cached = st.session_state.get("analysis_feats")

    if df_cached is None or feats_cached is None or df_cached.empty:
        st.warning("Najpierw kliknij „Pobierz dane i policz”, żeby mieć świeże dane i cechy.")
    else:
        # asekuracja: gdyby SMA nie było (np. stary stan)
        if "sma_fast" not in df_cached.columns or "sma_slow" not in df_cached.columns:
            df_cached = df_cached.copy()
            df_cached["sma_fast"] = df_cached["close"].rolling(fast).mean()
            df_cached["sma_slow"] = df_cached["close"].rolling(slow).mean()
            df_cached["timestamp"] = pd.to_datetime(df_cached["timestamp"], utc=True)
            feats_cached["timestamp"] = pd.to_datetime(feats_cached["timestamp"], utc=True)

        # --- ENV ---
        env = TradingEnv(
            bars=df_cached[["timestamp", "close"]],
            feats=feats_cached,
            cfg=EnvConfig(costs_bp=3.0)
        )
        _ = env.reset()

        # --- POLICY ZGRANA DO CZASU ENV (ELIMINUJE konflikt typów czasu) ---
        # sygnał SMA po danych źródłowych
        sig_df = pd.DataFrame({
            "timestamp": pd.to_datetime(df_cached["timestamp"], utc=True),
            "sig": (df_cached["sma_fast"] > df_cached["sma_slow"]).astype(int).values,
        }).sort_values("timestamp")

        # oś czasu środowiska (to już są tylko timestampy "ready")
        env_times = pd.DataFrame({
            "timestamp": pd.to_datetime(pd.Series(env.times), utc=True)
        }).sort_values("timestamp")

        # dla każdego czasu w env weź ostatni dostępny sygnał (asof, backward)
        M = pd.merge_asof(env_times, sig_df, on="timestamp", direction="backward")
        policy_arr = M["sig"].fillna(0).astype(int).to_numpy()
        # --- KONIEC POLICY ---

        # pętla symulacji
        equity_curve = []
        step_idx = 0
        while not env.done:
            a = int(policy_arr[min(step_idx, len(policy_arr) - 1)])
            _, _, _, info = env.step(a)
            equity_curve.append(info.get("eq", env.equity))
            step_idx += 1

        # wykres equity
        eq_index = pd.to_datetime(env.times[:len(equity_curve)], utc=True)
        eq_series = pd.Series(equity_curve, index=eq_index, name="equity")
        st.line_chart(eq_series)
        st.caption(f"Equity końcowe: {eq_series.iloc[-1]:.3f}  | kroki: {len(eq_series)}")

# ---------- Opisy – co to jest i po co ----------
st.divider()
with st.expander("🧠 Co oglądamy na tej stronie? (kliknij)"):
    st.markdown("""
**Wykres świec (candlestick)** — każda świeca pokazuje zakres ceny w interwale:
*open/high/low/close*. Pozwala szybko złapać trend, zmienność i poziomy wsparcia/oporu.

**SMA (Simple Moving Average)** — prosta średnia krocząca:
- *SMA fast* (krótka) reaguje szybciej na zmiany,
- *SMA slow* (długa) jest „gładsza”, pokazuje trend bazowy.

**SMA crossover** — kiedy SMA fast > SMA slow, traktujemy to jako sygnał „trend wzrostowy”
(long). Gdy fast < slow — brak pozycji/wyjście. To bardzo prosty filtr trendu.

**Mini-metryki** (szybkie spojrzenie na jakość):
- **CAGR-ish**: przybliżony wzrost wartości portfela za cały okres (nie uwzględnia prowizji, slippage).
- **Max Drawdown (MaxDD)**: największe obsunięcie kapitału względem wcześniejszego szczytu — im mniejsze, tym „spokojniej”.
- **Sharpe~**: prosty, zgrubny Sharpe (średni zwrot / odchylenie std * sqrt(252)) — im wyżej, tym lepszy stosunek zysku do ryzyka.

> Docelowo dorzucimy: prowizje, slippage, dokładniejszy backtest (otwieranie/zamykanie pozycji),
> oraz zakładkę **RL Bot**, gdzie agent RL będzie uczył się na tych danych podejmować decyzje (z limitem budżetu, kontrolą ryzyka).
""")
