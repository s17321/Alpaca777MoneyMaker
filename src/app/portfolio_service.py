from typing import Dict


def compute_target_qty(target_weight: float, equity: float, price: float) -> float:
    target_value = target_weight * equity
    if price <= 0:
        return 0.0
    return target_value / price


def delta_qty(current_qty: float, target_qty: float) -> float:
    return target_qty - current_qty