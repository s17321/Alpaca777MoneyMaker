# --- make 'src' importable ---
import os, sys
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
# -----------------------------

from datetime import datetime, timedelta, timezone
import numpy as np
import pandas as pd
import streamlit as st

from src.backend.data.market_data import MarketDataService

st.set_page_config(page_title="RL Bot — Alpaca777", layout="wide")
st.title("🤖 RL Bot — sandbox")
st.caption("Tu zbudujemy środowisko i agenta RL. Na razie prosty placeholder pętli decyzyjnej na danych historycznych.")

@st.cache_resource
def get_mds():
    return MarketDataService()

def simulate_random_policy(df: pd.DataFrame, init_cash: float = 10_000.0):
    """
    Bardzo prosty symulator: co bar wybiera losowo 1 (long) albo 0 (flat),
    liczy equity przy full allocation. Placeholder pod RL.
    """
    if df.empty:
        return pd.Series(dtype=float)
    ret = df["close"].pct_change().fillna(0.0)
    policy = np.random.randint(0, 2, size=len(df))  # 0/1
    strat = (1 + ret * np.roll(policy, 1)).cumprod() * (init_cash / 1.0)
    equity = pd.Series(strat, index=df["timestamp"], name="equity")
    return equity

mds = get_mds()

left, right = st.columns([2,1])
with right:
    symbol = st.text_input("Symbol", value="QQQ").upper()
    timeframe = st.selectbox("Timeframe", ["1Day","1Hour","15Min","5Min"], index=0)
    days = st.number_input("Ile dni wstecz", min_value=60, max_value=2000, value=365, step=10)
    init_cash = st.number_input("Budżet początkowy (USD)", min_value=100.0, value=10_000.0, step=100.0)
    runs = st.number_input("Ile przebiegów (dla rozkładu wyników)", min_value=1, max_value=100, value=10, step=1)
    start_btn = st.button("Start symulacji (placeholder)")

with left:
    if start_btn:
        with st.spinner("Pobieram dane..."):
            end = datetime.now(timezone.utc); start = end - timedelta(days=int(days))
            df = mds.get_bars(symbol, timeframe=timeframe, start=start, end=end, use_cache=True)
        if df.empty:
            st.error("Brak danych.")
        else:
            st.success(f"Pobrano {len(df)} świec. Uruchamiam {runs} przebiegów…")
            curves = []
            for _ in range(int(runs)):
                equity = simulate_random_policy(df, init_cash=float(init_cash))
                curves.append(equity)
            eq_df = pd.concat(curves, axis=1)
            st.line_chart(eq_df)

            final_vals = eq_df.iloc[-1, :].to_numpy()
            st.write(f"Średni wynik końcowy: {final_vals.mean():.2f} USD  |  min: {final_vals.min():.2f}  |  max: {final_vals.max():.2f}")
            st.caption("To *losowy* baseline. Następnie podmienimy na prawdziwego agenta RL.")
