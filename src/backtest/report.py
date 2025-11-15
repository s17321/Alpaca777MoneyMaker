import math


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