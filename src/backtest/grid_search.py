import itertools
import pandas as pd
import math
from src.domain.dto import Bar
from src.strategies.ema_rsi import EmaRsiTrend
from src.risk.core import AtrStopsVolRisk
from src.backtest.engine import Backtester
from src.backtest.report import max_drawdown, sharpe_ratio

PATH = "data/history/NG_F_15Min.csv"

def load_bars(path: str):
    df = pd.read_csv(path)
    return [
        Bar(ts=int(pd.to_datetime(r.timestamp).value//10**6), open=r.open, high=r.high, low=r.low, close=r.close, volume=int(r.volume))
        for r in df.itertuples(index=False)
    ]

def evaluate(bars, strat, risk):
    bt = Backtester(commission_pct=0.0005)
    def sig(seq):
        window = list(seq)
        raw = strat.target_weight("NG_F", window)
        return risk.adjust_weight("adam", "NG_F", raw, window)
    res = bt.run(bars, sig, initial_cash=1000.0)
    eq = res.equity_curve
    if not eq:
        return None
    rets = [0.0] + [(eq[i]-eq[i-1])/eq[i-1] for i in range(1, len(eq))]
    return {
        "equity_last": round(eq[-1],2),
        "mdd": round(max_drawdown(eq)*100,2),
        "sharpe": round(sharpe_ratio(rets),2),
    }

if __name__ == "__main__":
    bars = load_bars(PATH)
    best = None
    grid = {
        "fast": [8, 12, 20],
        "slow": [30, 50, 80],
        "vol_k": [0.01, 0.02, 0.03],
        "sl_atr": [1.5, 2.0, 2.5],
        "tp_atr": [2.5, 3.0, 4.0],
    }
    for fast in grid["fast"]:
        for slow in grid["slow"]:
            if fast >= slow: continue
            for vol_k in grid["vol_k"]:
                for sl_a in grid["sl_atr"]:
                    for tp_a in grid["tp_atr"]:
                        strat = EmaRsiTrend(fast=fast, slow=slow, rsi_min=35, rsi_max=65, target_w=1.0)
                        risk = AtrStopsVolRisk(max_position_pct=0.3, vol_k=vol_k, sl_atr_mult=sl_a, tp_atr_mult=tp_a)
                        m = evaluate(bars, strat, risk)
                        if not m: continue
                        row = {"fast":fast,"slow":slow,"vol_k":vol_k,"sl_atr":sl_a,"tp_atr":tp_a, **m}
                        print(row)
                        if (best is None) or (row["sharpe"] > best["sharpe"]):
                            best = row
    print("\nBEST:", best)

def max_drawdown(equity):
    peak, mdd = -math.inf, 0.0
    for x in equity:
        peak = max(peak, x)
        mdd = max(mdd, (peak - x) / peak if peak > 0 else 0.0)
    return mdd

def sharpe_ratio(returns, rf=0.0):
    if not returns:
        return 0.0
    import statistics as st
    mean = st.mean(returns) - rf
    std = st.pstdev(returns) or 1e-9
    return (mean / std) * (252 ** 0.5)