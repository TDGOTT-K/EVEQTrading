from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Dict, Union


@dataclass
class FeeConfig:
    buy_broker_fee_rate: float = 0.006
    sell_broker_fee_rate: float = 0.006
    sales_tax_rate: float = 0.03375
    relist_fee_rate: float = 0.0015

    def to_dict(self) -> Dict[str, float]:
        return asdict(self)


@dataclass
class FilterConfig:
    min_avg_daily_isk_volume_30d: float = 20_000_000.0
    min_active_days_ratio_30d: float = 0.6
    min_avg_daily_units_7d: float = 50_000.0
    min_net_spread_pct: float = 0.005
    max_estimated_exit_days_small_size: float = 3.0

    def to_dict(self) -> Dict[str, float]:
        return asdict(self)


@dataclass
class ScanConfig:
    strategy_set: str = "v1-default"
    parameter_version: str = "v1"
    test_position_isk: float = 250_000_000.0
    capital_budget_isk: float = 1_000_000_000.0
    top_n: int = 3

    def to_dict(self) -> Dict[str, Union[float, int, str]]:
        return asdict(self)
