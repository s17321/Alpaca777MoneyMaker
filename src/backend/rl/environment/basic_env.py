# src/backend/rl/environment/basic_env.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Tuple, Iterable, Dict, Any
import numpy as np
import pandas as pd

@dataclass
class EnvConfig:
    costs_bp: float = 3.0          # koszt transakcyjny w bp na zmianę pozycji (np. 3 bp = 0.03%)
    action_set: Tuple[float, ...] = (0.0, 1.0)  # 0=flat, 1=long
    max_drawdown: float = 0.20     # twarde odcięcie (opcjonalnie)

class TradingEnv:
    """
    Bardzo proste środowisko:
    - observation: wektor f_* z pipeline'u (numpy array)
    - action: indeks z EnvConfig.action_set (0 -> 0.0, 1 -> 1.0)
    - reward_t = position_{t-1} * ret_t - cost(change)
    - done: po ostatnim barze albo po przekroczeniu MDD
    Wymaga dataframe'u 'bars' z kolumną 'close' i dataframe'u 'feats' z kolumnami f_*
    oraz 'ready'==True.
    """
    def __init__(self, bars: pd.DataFrame, feats: pd.DataFrame, cfg: Optional[EnvConfig] = None):
        self.cfg = cfg or EnvConfig()

        # --- Kopie + JEDNOLITE UTC ---
        X = bars.copy()
        F = feats.copy()

        # Ujednolicenie typu czasu na obu ramkach
        X["timestamp"] = pd.to_datetime(X["timestamp"], utc=True)
        F["timestamp"] = pd.to_datetime(F["timestamp"], utc=True)

        X = X.sort_values("timestamp").reset_index(drop=True)
        F = F.sort_values("timestamp").reset_index(drop=True)

        # bierz tylko gotowe featy
        fe_cols = [c for c in F.columns if c.startswith("f_")]
        F_ready = F.loc[F["ready"], ["timestamp"] + fe_cols].copy()

        # SAFETY: jeśli nadal coś nie gra, wymuś dtype (rzadko potrzebne, ale bezpieczne)
        X["timestamp"] = pd.DatetimeIndex(X["timestamp"]).tz_convert("UTC")
        F_ready["timestamp"] = pd.DatetimeIndex(F_ready["timestamp"]).tz_convert("UTC")

        # --- merge_asof: po lewej ceny, po prawej cechy gotowe (ostatnia znana wartość) ---
        M = pd.merge_asof(
            X[["timestamp", "close"]],
            F_ready,
            on="timestamp",
            direction="backward",
        )

        # Usuń wiersze, gdzie nie było jeszcze 'ready' (NaN w pierwszej kolumnie cech)
        M = M.dropna(subset=[fe_cols[0]])

        # Wyciągnij osie
        self._times = M["timestamp"].to_numpy()
        self._close = M["close"].astype(float).to_numpy()
        self._obs = M[fe_cols].to_numpy()

        # Zwroty barowe (na nagrodę)
        self._ret = np.zeros_like(self._close, dtype=float)
        if len(self._close) > 1:
            self._ret[1:] = (self._close[1:] / self._close[:-1]) - 1.0

        # Stan
        self.t = 0
        self.position = 0.0
        self.equity = 1.0
        self.equity_peak = 1.0
        self.done = False

    @property
    def n_steps(self) -> int:
        return len(self._close)

    @property
    def obs_dim(self) -> int:
        return self._obs.shape[1]

    @property
    def times(self):
        return self._times

    def reset(self) -> np.ndarray:
        self.t = 0
        self.position = 0.0
        self.equity = 1.0
        self.equity_peak = 1.0
        self.done = False
        return self._obs[self.t].copy()

    def step(self, action_idx: int):
        if self.done:
            raise RuntimeError("Step called on done env")
        target_pos = float(self.cfg.action_set[action_idx])
        cost = 0.0
        if target_pos != self.position:
            cost = abs(target_pos - self.position) * (self.cfg.costs_bp / 10000.0)

        t_next = self.t + 1
        if t_next >= self.n_steps:
            self.done = True
            return self._obs[self.t].copy(), 0.0, True, {"info": "end"}

        reward = (self.position * self._ret[t_next]) - cost
        self.position = target_pos

        self.equity *= (1.0 + reward)
        self.equity_peak = max(self.equity_peak, self.equity)
        dd = (self.equity / (self.equity_peak + 1e-12)) - 1.0
        if dd <= -abs(self.cfg.max_drawdown):
            self.done = True
            return self._obs[t_next].copy(), reward, True, {"info": "max_drawdown"}

        self.t = t_next
        done = (self.t >= self.n_steps - 1)
        if done:
            self.done = True
        return self._obs[self.t].copy(), reward, done, {"eq": self.equity, "pos": self.position}