# src/backend/rl/features/pipeline.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Dict
import numpy as np
import pandas as pd


# ====== Pomocnicze funkcje wskaźników (bez zewn. bibliotek TA) ======

def ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()

def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    up = delta.clip(lower=0.0)
    down = (-delta).clip(lower=0.0)
    roll_up = up.ewm(alpha=1/period, adjust=False).mean()
    roll_down = down.ewm(alpha=1/period, adjust=False).mean()
    rs = roll_up / (roll_down + 1e-12)
    return 100 - (100 / (1 + rs))

def true_range(high: pd.Series, low: pd.Series, close: pd.Series) -> pd.Series:
    prev_close = close.shift(1)
    tr1 = (high - low).abs()
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()
    return pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

def atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
    tr = true_range(high, low, close)
    return tr.ewm(alpha=1/period, adjust=False).mean()

def bollinger_width(close: pd.Series, period: int = 20, n_std: float = 2.0) -> pd.Series:
    ma = close.rolling(period).mean()
    sd = close.rolling(period).std(ddof=0)
    upper = ma + n_std * sd
    lower = ma - n_std * sd
    return (upper - lower) / (ma.replace(0, np.nan))

def obv(close: pd.Series, volume: pd.Series) -> pd.Series:
    sign = np.sign(close.diff().fillna(0.0))
    return (sign * volume.fillna(0)).cumsum()


# ====== Główna klasa pipeline ======

@dataclass
class FeaturePipelineConfig:
    # okna dla wskaźników
    sma_fast: int = 10
    sma_slow: int = 50
    rsi_period: int = 14
    macd_fast: int = 12
    macd_slow: int = 26
    macd_signal: int = 9
    atr_period: int = 14
    vol_window: int = 20         # realized vol + vol z-score
    bb_period: int = 20
    norm_window: int = 100       # okno normalizacji rolling z-score
    beta_window: int = 60        # okno dla rolling beta (gdy jest benchmark)

class FeaturePipeline:
    """
    Oblicza wektor cech (Core-20) z danych OHLCV.
    - Wejście: DataFrame z kolumnami ['timestamp','open','high','low','close','volume'] (UTC)
    - (opcjonalnie) DataFrame benchmarku z kolumnami ['timestamp','close'] do cech reżimu (trend/beta)
    - Wyjście: DataFrame z kolumnami 'f_*' (znormalizowane) oraz 'ready' (bool)
    Zasady:
    - rolling / ewm liczone wyłącznie na danych do t (bez wycieku przyszłości)
    - normalizacja rolling z-score w oknie `norm_window`
    """

    def __init__(self, cfg: Optional[FeaturePipelineConfig] = None) -> None:
        self.cfg = cfg or FeaturePipelineConfig()

    @staticmethod
    def _zscore(x: pd.Series, window: int) -> pd.Series:
        m = x.rolling(window).mean()
        s = x.rolling(window).std(ddof=0)
        return (x - m) / (s + 1e-9)

    def compute_from_df(
        self,
        df: pd.DataFrame,
        benchmark_df: Optional[pd.DataFrame] = None
    ) -> pd.DataFrame:
        """
        Zwraca DataFrame z kolumnami:
        - 'timestamp' (UTC)
        - 'f_*' (znormalizowane cechy)
        - 'ready' (True jeśli wszystkie okna wypełnione)
        """
        required_cols = {"timestamp", "open", "high", "low", "close", "volume"}
        if not required_cols.issubset(df.columns):
            raise ValueError(f"df musi zawierać kolumny: {sorted(required_cols)}")

        X = df.copy().sort_values("timestamp").reset_index(drop=True)

        # --- bazowe wektory ---
        close = X["close"].astype(float)
        high = X["high"].astype(float)
        low  = X["low"].astype(float)
        vol  = X["volume"].astype(float)

        # --- zwroty i momentum ---
        ret_1  = close.pct_change()
        ret_5  = close.pct_change(5)
        ret_10 = close.pct_change(10)
        mom_10 = close - close.shift(10)

        # --- średnie i MACD ---
        sma_f = close.rolling(self.cfg.sma_fast).mean()
        sma_s = close.rolling(self.cfg.sma_slow).mean()
        ema_fast = ema(close, self.cfg.macd_fast)
        ema_slow = ema(close, self.cfg.macd_slow)
        macd = ema_fast - ema_slow
        macd_sig = ema(macd, self.cfg.macd_signal)

        # --- RSI / ATR / zmienność / zakres ---
        rsi_ = rsi(close, self.cfg.rsi_period)
        atr_ = atr(high, low, close, self.cfg.atr_period)
        rv20 = close.pct_change().rolling(self.cfg.vol_window).std(ddof=0)
        range_rel = (high - low) / close.replace(0, np.nan)
        bb_w = bollinger_width(close, self.cfg.bb_period, 2.0)

        # --- wolumen / OBV / z-score wolumenu ---
        vol_z = self._zscore(vol.fillna(0.0), self.cfg.vol_window)
        obv_  = obv(close, vol)

        # --- odchylenia od trendu ---
        price_above_sma = close / (sma_s.replace(0, np.nan)) - 1.0

        # --- benchmark / reżim (opcjonalne) ---
        market_trend = pd.Series(index=X.index, dtype=float)
        market_beta  = pd.Series(index=X.index, dtype=float)
        if benchmark_df is not None and "close" in benchmark_df.columns:
            B = benchmark_df.copy().sort_values("timestamp")
            B_close = B["close"].astype(float).reindex_like(close, method=None)
            # dopasowanie po indeksie czasowym: spróbuj merge po timestamp
            try:
                M = pd.merge_asof(
                    X[["timestamp", "close"]].rename(columns={"close": "a_close"}),
                    B[["timestamp", "close"]].rename(columns={"close": "b_close"}),
                    on="timestamp"
                )
                B_close = M["b_close"].astype(float)
            except Exception:
                pass
            market_trend = B_close.rolling(self.cfg.sma_slow).mean().pct_change()  # proxy kierunku
            # rolling beta
            r_a = close.pct_change()
            r_b = B_close.pct_change()
            cov = (r_a.rolling(self.cfg.beta_window).cov(r_b))
            var = (r_b.rolling(self.cfg.beta_window).var())
            market_beta = cov / (var + 1e-12)

        # --- surowe cechy w dict ---
        raw: Dict[str, pd.Series] = {
            "ret_1": ret_1,
            "ret_5": ret_5,
            "ret_10": ret_10,
            "mom_10": mom_10,
            "sma_fast": sma_f,
            "sma_slow": sma_s,
            "macd": macd,
            "macd_signal": macd_sig,
            "rsi_14": rsi_,
            "atr_14": atr_,
            "realized_vol_20": rv20,
            "range_rel": range_rel,
            "bb_width_20": bb_w,
            "vol_z_20_raw": vol_z,   # to już z-score, ale utrzymujemy w spójności
            "obv": obv_,
            "price_above_sma": price_above_sma,
            "market_trend": market_trend,
            "market_beta": market_beta,
            # Kalendarz (day of week one-hot)
            "dow_0": (pd.to_datetime(X["timestamp"]).dt.weekday == 0).astype(float),
            "dow_1": (pd.to_datetime(X["timestamp"]).dt.weekday == 1).astype(float),
            "dow_2": (pd.to_datetime(X["timestamp"]).dt.weekday == 2).astype(float),
            "dow_3": (pd.to_datetime(X["timestamp"]).dt.weekday == 3).astype(float),
            "dow_4": (pd.to_datetime(X["timestamp"]).dt.weekday == 4).astype(float),
        }

        # --- normalizacja rolling (z-score) dla większości cech ---
        feats = {}
        for k, s in raw.items():
            s = s.astype(float)
            if k.startswith("dow_") or k == "vol_z_20_raw":
                feats[f"f_{k}"] = s.fillna(0.0)  # „kalendarz” i już z-normalizowane wolumeny
            else:
                feats[f"f_{k}"] = self._zscore(s, self.cfg.norm_window).fillna(0.0).clip(-5.0, 5.0)

        out = pd.DataFrame(feats)
        ts = pd.to_datetime(X["timestamp"], utc=True)
        out.insert(0, "timestamp", ts)

        # maska gotowości: wszystkie wymagające okna muszą być pełne
        min_warmup = max(self.cfg.norm_window, self.cfg.sma_slow, self.cfg.bb_period, self.cfg.beta_window)
        ready = pd.Series(False, index=out.index)
        if len(out) >= min_warmup:
            ready.iloc[min_warmup - 1:] = True
        out["ready"] = ready.astype(bool)

        return out
