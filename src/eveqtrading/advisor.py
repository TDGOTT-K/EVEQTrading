from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from .account_budget import build_analysis_context, derive_account_budget_context, load_account_snapshot
from .report import build_market_report


def _clamp(value: float, minimum: float, maximum: float) -> float:
    return max(minimum, min(value, maximum))


def _safe_div(numerator: float, denominator: float) -> float:
    if denominator == 0:
        return 0.0
    return numerator / denominator


@dataclass(frozen=True)
class StrategyThresholds:
    min_profit_margin: float
    max_turnover_days: float
    min_daily_isk_volume: float
    max_volatility: float
    min_active_days_ratio: float
    max_price_zscore: float | None = None
    min_price_zscore: float | None = None


@dataclass(frozen=True)
class StrategyProfile:
    thresholds: StrategyThresholds
    deploy_min_profit_margin: float
    deploy_max_turnover_days: float
    deploy_min_daily_isk_volume: float
    deploy_max_volatility: float
    deploy_min_active_days_ratio: float
    deploy_max_price_zscore: float | None = None
    deploy_min_price_zscore: float | None = None
    deploy_confidence_score: int = 78
    watch_only: bool = False
    model_limitations: tuple[str, ...] = ()


@dataclass(frozen=True)
class RiskProfileConfig:
    name: str
    label: str
    budget_share_low_confidence: float
    budget_share_mid_confidence: float
    budget_share_high_confidence: float
    liquidity_divisor: float
    deployable_cap_multiplier: float
    summary: str


STRATEGY_PROFILES: dict[str, StrategyProfile] = {
    "station-flip": StrategyProfile(
        thresholds=StrategyThresholds(
            min_profit_margin=0.015,
            max_turnover_days=1.5,
            min_daily_isk_volume=100_000_000.0,
            max_volatility=0.15,
            min_active_days_ratio=0.9,
            max_price_zscore=0.75,
        ),
        deploy_min_profit_margin=0.022,
        deploy_max_turnover_days=1.0,
        deploy_min_daily_isk_volume=180_000_000.0,
        deploy_max_volatility=0.12,
        deploy_min_active_days_ratio=0.95,
        deploy_max_price_zscore=0.25,
        deploy_confidence_score=80,
        model_limitations=(
            "assumes first fill near current best ask and exit near current best bid",
            "does not model queue position or orderbook depth decay",
        ),
    ),
    "jita-passive-flip": StrategyProfile(
        thresholds=StrategyThresholds(
            min_profit_margin=0.02,
            max_turnover_days=2.0,
            min_daily_isk_volume=150_000_000.0,
            max_volatility=0.14,
            min_active_days_ratio=0.9,
            max_price_zscore=0.5,
        ),
        deploy_min_profit_margin=0.025,
        deploy_max_turnover_days=1.2,
        deploy_min_daily_isk_volume=220_000_000.0,
        deploy_max_volatility=0.12,
        deploy_min_active_days_ratio=0.95,
        deploy_max_price_zscore=0.2,
        deploy_confidence_score=84,
        model_limitations=(
            "assumes passive buy fill near current best bid and passive sell exit near current best ask",
            "uses conservative inventory cap proxy of avg_daily_isk_volume_30d / 12",
            "does not model queue position, relist cadence, or orderbook depth decay",
        ),
    ),
    "jita-passive-simple": StrategyProfile(
        thresholds=StrategyThresholds(
            min_profit_margin=0.01,
            max_turnover_days=1.0,
            min_daily_isk_volume=100_000_000.0,
            max_volatility=0.30,
            min_active_days_ratio=0.9,
        ),
        deploy_min_profit_margin=0.015,
        deploy_max_turnover_days=0.8,
        deploy_min_daily_isk_volume=150_000_000.0,
        deploy_max_volatility=0.22,
        deploy_min_active_days_ratio=0.95,
        deploy_confidence_score=76,
        model_limitations=(
            "uses a simplified two-sided turnover proxy derived from current best bid ask plus 30d history high low average",
            "does not model queue position, orderbook depth, or true historical buy-side versus sell-side fills",
        ),
    ),
    "cross-location": StrategyProfile(
        thresholds=StrategyThresholds(
            min_profit_margin=0.025,
            max_turnover_days=2.5,
            min_daily_isk_volume=80_000_000.0,
            max_volatility=0.18,
            min_active_days_ratio=0.85,
            max_price_zscore=0.5,
        ),
        deploy_min_profit_margin=0.045,
        deploy_max_turnover_days=1.8,
        deploy_min_daily_isk_volume=150_000_000.0,
        deploy_max_volatility=0.14,
        deploy_min_active_days_ratio=0.92,
        deploy_max_price_zscore=0.10,
        deploy_confidence_score=82,
        model_limitations=(
            "does not include hauling time, route safety, or cargo capacity constraints",
            "does not include travel opportunity cost between buy and sell locations",
        ),
    ),
    "reservoir": StrategyProfile(
        thresholds=StrategyThresholds(
            min_profit_margin=0.04,
            max_turnover_days=5.0,
            min_daily_isk_volume=50_000_000.0,
            max_volatility=0.22,
            min_active_days_ratio=0.7,
            min_price_zscore=-0.8,
        ),
        deploy_min_profit_margin=0.065,
        deploy_max_turnover_days=3.5,
        deploy_min_daily_isk_volume=120_000_000.0,
        deploy_max_volatility=0.15,
        deploy_min_active_days_ratio=0.80,
        deploy_min_price_zscore=-1.6,
        deploy_confidence_score=88,
        watch_only=True,
        model_limitations=(
            "does not model catalyst timing for mean reversion",
            "does not model inventory financing or long hold capital lock-up",
        ),
    ),
}

RISK_PROFILES: dict[str, RiskProfileConfig] = {
    "conservative": RiskProfileConfig(
        name="conservative",
        label="Conservative",
        budget_share_low_confidence=0.08,
        budget_share_mid_confidence=0.12,
        budget_share_high_confidence=0.15,
        liquidity_divisor=8.0,
        deployable_cap_multiplier=1.0,
        summary="Smaller single-item sizing and a tighter daily liquidity cap.",
    ),
    "standard": RiskProfileConfig(
        name="standard",
        label="Standard",
        budget_share_low_confidence=0.10,
        budget_share_mid_confidence=0.15,
        budget_share_high_confidence=0.20,
        liquidity_divisor=6.0,
        deployable_cap_multiplier=2.0,
        summary="Baseline sizing for traced trial deployment.",
    ),
    "aggressive": RiskProfileConfig(
        name="aggressive",
        label="Aggressive",
        budget_share_low_confidence=0.15,
        budget_share_mid_confidence=0.20,
        budget_share_high_confidence=0.25,
        liquidity_divisor=4.0,
        deployable_cap_multiplier=3.0,
        summary="Larger single-item sizing and a looser daily liquidity cap.",
    ),
}

_ACTION_PRIORITY = {
    "deploy-small": 0,
    "watch": 1,
    "reject": 2,
}


def _extract_metrics(candidate: dict[str, Any]) -> dict[str, float | None]:
    price_zscore = candidate.get("price_zscore_30d")
    cross_location_net_spread_pct = candidate.get("cross_location_net_spread_pct")
    flip_units_cv_30d = candidate.get("flip_units_cv_30d")
    flip_units_cv_7d = candidate.get("flip_units_cv_7d")
    estimated_capture_share = candidate.get("estimated_capture_share")
    competition_intensity = candidate.get("competition_intensity")
    return {
        "profit_margin": float(candidate.get("profit_margin") or candidate.get("screened_profit_margin") or 0.0),
        "turnover_days": float(
            candidate.get("expected_turnover_days") or candidate.get("estimated_exit_days_small_size") or 0.0
        ),
        "avg_daily_isk_volume_30d": float(candidate.get("avg_daily_isk_volume_30d") or 0.0),
        "active_days_ratio_30d": float(candidate.get("active_days_ratio_30d") or 0.0),
        "volatility_30d": float(candidate.get("volatility_30d") or 0.0),
        "price_zscore_30d": float(price_zscore) if price_zscore is not None else None,
        "cross_location_net_spread_pct": (
            float(cross_location_net_spread_pct) if cross_location_net_spread_pct is not None else None
        ),
        "flip_units_cv_30d": float(flip_units_cv_30d) if flip_units_cv_30d is not None else None,
        "flip_units_cv_7d": float(flip_units_cv_7d) if flip_units_cv_7d is not None else None,
        "estimated_capture_share": float(estimated_capture_share) if estimated_capture_share is not None else None,
        "competition_intensity": float(competition_intensity) if competition_intensity is not None else None,
    }


def _evaluate_thresholds(metrics: dict[str, float | None], thresholds: StrategyThresholds) -> tuple[list[str], list[str]]:
    passes: list[str] = []
    failures: list[str] = []

    def check(condition: bool, pass_message: str, fail_message: str) -> None:
        if condition:
            passes.append(pass_message)
        else:
            failures.append(fail_message)

    profit_margin = float(metrics["profit_margin"] or 0.0)
    turnover_days = float(metrics["turnover_days"] or 0.0)
    daily_isk_volume = float(metrics["avg_daily_isk_volume_30d"] or 0.0)
    volatility = float(metrics["volatility_30d"] or 0.0)
    active_days_ratio = float(metrics["active_days_ratio_30d"] or 0.0)
    price_zscore = metrics["price_zscore_30d"]

    check(
        profit_margin >= thresholds.min_profit_margin,
        f"profit_margin {profit_margin:.4%} >= {thresholds.min_profit_margin:.4%}",
        f"profit_margin {profit_margin:.4%} < {thresholds.min_profit_margin:.4%}",
    )
    check(
        turnover_days <= thresholds.max_turnover_days,
        f"turnover_days {turnover_days:.2f} <= {thresholds.max_turnover_days:.2f}",
        f"turnover_days {turnover_days:.2f} > {thresholds.max_turnover_days:.2f}",
    )
    check(
        daily_isk_volume >= thresholds.min_daily_isk_volume,
        f"avg_daily_isk_volume_30d {daily_isk_volume:.2f} >= {thresholds.min_daily_isk_volume:.2f}",
        f"avg_daily_isk_volume_30d {daily_isk_volume:.2f} < {thresholds.min_daily_isk_volume:.2f}",
    )
    check(
        volatility <= thresholds.max_volatility,
        f"volatility_30d {volatility:.4f} <= {thresholds.max_volatility:.4f}",
        f"volatility_30d {volatility:.4f} > {thresholds.max_volatility:.4f}",
    )
    check(
        active_days_ratio >= thresholds.min_active_days_ratio,
        f"active_days_ratio_30d {active_days_ratio:.4f} >= {thresholds.min_active_days_ratio:.4f}",
        f"active_days_ratio_30d {active_days_ratio:.4f} < {thresholds.min_active_days_ratio:.4f}",
    )

    if thresholds.max_price_zscore is not None and price_zscore is not None:
        check(
            price_zscore <= thresholds.max_price_zscore,
            f"price_zscore_30d {price_zscore:.2f} <= {thresholds.max_price_zscore:.2f}",
            f"price_zscore_30d {price_zscore:.2f} > {thresholds.max_price_zscore:.2f}",
        )
    if thresholds.min_price_zscore is not None and price_zscore is not None:
        check(
            price_zscore >= thresholds.min_price_zscore,
            f"price_zscore_30d {price_zscore:.2f} >= {thresholds.min_price_zscore:.2f}",
            f"price_zscore_30d {price_zscore:.2f} < {thresholds.min_price_zscore:.2f}",
        )

    return passes, failures


def _build_tailwinds(
    *,
    strategy_type: str,
    metrics: dict[str, float | None],
    profile: StrategyProfile,
) -> list[str]:
    tailwinds: list[str] = []
    profit_margin = float(metrics["profit_margin"] or 0.0)
    turnover_days = float(metrics["turnover_days"] or 0.0)
    daily_isk_volume = float(metrics["avg_daily_isk_volume_30d"] or 0.0)
    active_days_ratio = float(metrics["active_days_ratio_30d"] or 0.0)
    volatility = float(metrics["volatility_30d"] or 0.0)
    price_zscore = metrics["price_zscore_30d"]
    cross_location_net_spread_pct = metrics["cross_location_net_spread_pct"]
    flip_units_cv_30d = metrics["flip_units_cv_30d"]
    flip_units_cv_7d = metrics["flip_units_cv_7d"]
    estimated_capture_share = metrics["estimated_capture_share"]
    competition_intensity = metrics["competition_intensity"]

    if profit_margin >= profile.deploy_min_profit_margin:
        tailwinds.append(
            f"profit margin is in deploy range at {profit_margin:.4%}"
        )
    elif profit_margin >= profile.thresholds.min_profit_margin:
        tailwinds.append(
            f"profit margin clears the first screen at {profit_margin:.4%}"
        )

    if turnover_days <= profile.deploy_max_turnover_days:
        tailwinds.append(f"turnover is fast at {turnover_days:.2f} days")
    elif turnover_days <= profile.thresholds.max_turnover_days:
        tailwinds.append(f"turnover remains within the first-pass ceiling at {turnover_days:.2f} days")

    if daily_isk_volume >= profile.deploy_min_daily_isk_volume:
        tailwinds.append(f"30d daily ISK volume is strong at {daily_isk_volume:.2f}")

    if active_days_ratio >= profile.deploy_min_active_days_ratio:
        tailwinds.append(f"market stayed active on {active_days_ratio:.2%} of the last 30 days")

    if volatility <= profile.deploy_max_volatility:
        tailwinds.append(f"30d volatility is controlled at {volatility:.4f}")

    if (
        strategy_type == "jita-passive-simple"
        and flip_units_cv_30d is not None
        and flip_units_cv_7d is not None
    ):
        if flip_units_cv_30d <= 0.70 and flip_units_cv_7d <= 0.35:
            tailwinds.append(
                f"two-sided turnover is stable with 30d CV {flip_units_cv_30d:.2%} and 7d CV {flip_units_cv_7d:.2%}"
            )
        elif flip_units_cv_30d <= 1.00 and flip_units_cv_7d <= 0.50:
            tailwinds.append(
                f"two-sided turnover stays workable with 30d CV {flip_units_cv_30d:.2%} and 7d CV {flip_units_cv_7d:.2%}"
            )
    if strategy_type == "jita-passive-simple" and estimated_capture_share is not None:
        if estimated_capture_share >= 0.12:
            tailwinds.append(f"modeled capture share is solid at {estimated_capture_share:.2%}")
        elif estimated_capture_share >= 0.07:
            tailwinds.append(f"modeled capture share remains usable at {estimated_capture_share:.2%}")
    if strategy_type == "jita-passive-simple" and competition_intensity is not None and competition_intensity <= 0.35:
        tailwinds.append(f"top-of-book competition looks manageable at intensity {competition_intensity:.2f}")

    if strategy_type in {"station-flip", "jita-passive-flip"} and price_zscore is not None and price_zscore <= 0.10:
        tailwinds.append(f"price is not stretched for same-station exit at zscore {price_zscore:.2f}")
    if strategy_type == "cross-location" and cross_location_net_spread_pct is not None and cross_location_net_spread_pct >= 0.04:
        tailwinds.append(
            f"cross-location net spread is strong at {cross_location_net_spread_pct:.4%}"
        )
    if strategy_type == "reservoir" and price_zscore is not None and price_zscore <= -1.5:
        tailwinds.append(f"price is deeply below 30d mean at zscore {price_zscore:.2f}")

    return tailwinds


def _build_blockers(
    *,
    strategy_type: str,
    metrics: dict[str, float | None],
    failures: list[str],
    profile: StrategyProfile,
) -> tuple[list[str], list[str]]:
    hard_blockers: list[str] = []
    soft_blockers: list[str] = []
    price_zscore = metrics["price_zscore_30d"]
    estimated_capture_share = metrics["estimated_capture_share"]
    competition_intensity = metrics["competition_intensity"]

    for failure in failures:
        if "price_zscore_30d" in failure:
            soft_blockers.append(failure)
            continue
        if "volatility_30d" in failure and strategy_type == "reservoir":
            soft_blockers.append(failure)
            continue
        hard_blockers.append(failure)

    if strategy_type == "cross-location":
        soft_blockers.append("hauling risk and route friction are not yet modeled in v1")
    if strategy_type == "reservoir":
        soft_blockers.append("mean reversion trigger timing is not modeled in v1")
    if (
        strategy_type in {"station-flip", "jita-passive-flip"}
        and price_zscore is not None
        and price_zscore > profile.deploy_max_price_zscore
    ):
        soft_blockers.append(f"same-station price entry is less favorable than deploy range at zscore {price_zscore:.2f}")
    if strategy_type == "jita-passive-simple" and estimated_capture_share is not None:
        if estimated_capture_share < 0.04:
            hard_blockers.append(f"modeled capture share is too thin at {estimated_capture_share:.2%}")
        elif estimated_capture_share < 0.07:
            soft_blockers.append(f"modeled capture share is thin at {estimated_capture_share:.2%}")
    if strategy_type == "jita-passive-simple" and competition_intensity is not None and competition_intensity >= 0.80:
        soft_blockers.append(f"top-of-book competition is intense at {competition_intensity:.2f}")

    return hard_blockers, soft_blockers


def _build_confidence_score(
    *,
    strategy_type: str,
    metrics: dict[str, float | None],
    profile: StrategyProfile,
    hard_blockers: list[str],
    soft_blockers: list[str],
) -> int:
    profit_margin = float(metrics["profit_margin"] or 0.0)
    turnover_days = float(metrics["turnover_days"] or 0.0)
    daily_isk_volume = float(metrics["avg_daily_isk_volume_30d"] or 0.0)
    active_days_ratio = float(metrics["active_days_ratio_30d"] or 0.0)
    volatility = float(metrics["volatility_30d"] or 0.0)
    price_zscore = metrics["price_zscore_30d"]
    flip_units_cv_30d = metrics["flip_units_cv_30d"]
    flip_units_cv_7d = metrics["flip_units_cv_7d"]
    estimated_capture_share = metrics["estimated_capture_share"]
    competition_intensity = metrics["competition_intensity"]

    score = 0.0
    score += 28.0 * _clamp(profit_margin / max(profile.deploy_min_profit_margin, 0.0001), 0.0, 1.2)
    score += 18.0 * _clamp(profile.deploy_max_turnover_days / max(turnover_days, 0.05), 0.0, 1.2)
    score += 18.0 * _clamp(daily_isk_volume / max(profile.deploy_min_daily_isk_volume, 1.0), 0.0, 1.2)
    score += 12.0 * _clamp(active_days_ratio / max(profile.deploy_min_active_days_ratio, 0.01), 0.0, 1.1)
    score += 10.0 * _clamp(max(profile.deploy_max_volatility - volatility, 0.0) / max(profile.deploy_max_volatility, 0.001), 0.0, 1.0)

    if price_zscore is not None:
        if strategy_type in {"station-flip", "jita-passive-flip", "cross-location"} and profile.deploy_max_price_zscore is not None:
            score += 14.0 * _clamp(
                max(profile.deploy_max_price_zscore - price_zscore, 0.0)
                / max(profile.deploy_max_price_zscore + 0.5, 0.1),
                0.0,
                1.0,
            )
        elif strategy_type == "reservoir" and profile.deploy_min_price_zscore is not None:
            score += 14.0 * _clamp(
                max(profile.deploy_min_price_zscore - price_zscore, 0.0) / 1.2,
                0.0,
                1.0,
            )

    if strategy_type == "jita-passive-simple":
        if flip_units_cv_30d is not None:
            score += 8.0 * _clamp((1.10 - flip_units_cv_30d) / 0.90, 0.0, 1.0)
        if flip_units_cv_7d is not None:
            score += 6.0 * _clamp((0.60 - flip_units_cv_7d) / 0.45, 0.0, 1.0)
        if estimated_capture_share is not None:
            score += 12.0 * _clamp(_safe_div(estimated_capture_share, 0.12), 0.0, 1.0)
        if competition_intensity is not None:
            score += 8.0 * _clamp(1.0 - competition_intensity, 0.0, 1.0)

    score -= 22.0 * len(hard_blockers)
    score -= 8.0 * len(soft_blockers)

    return int(round(_clamp(score, 0.0, 100.0)))


def _thesis_strength(confidence_score: int) -> str:
    if confidence_score >= 85:
        return "strong"
    if confidence_score >= 70:
        return "workable"
    if confidence_score >= 50:
        return "fragile"
    return "weak"


def _recommend_action(
    *,
    strategy_type: str,
    profile: StrategyProfile,
    hard_blockers: list[str],
    soft_blockers: list[str],
    confidence_score: int,
) -> tuple[str, str]:
    if hard_blockers:
        return "reject", "failed one or more execution-critical checks"

    if profile.watch_only:
        return "watch", "passes the first screen but this strategy stays watch-only until hold-risk modeling exists"

    if confidence_score >= profile.deploy_confidence_score and len(soft_blockers) <= 1:
        if strategy_type == "cross-location":
            return "deploy-small", "spread, liquidity, and turnover are strong enough for a small traced trial"
        if strategy_type == "station-flip":
            return "deploy-small", "same-station edge is strong enough for a small traced trial"
        if strategy_type == "jita-passive-flip":
            return "deploy-small", "jita passive spread, turnover, and inventory cap are strong enough for a small traced trial"
        if strategy_type == "jita-passive-simple":
            return "deploy-small", "jita passive simple spread, turnover, and liquidity are strong enough for a small traced trial"

    if soft_blockers:
        return "watch", "passes the hard floor but still has model or timing caveats"

    return "watch", "passes the first fine-screen thresholds but edge is not yet strong enough for immediate deployment"


def _resolve_risk_profile_config(risk_profile: str) -> RiskProfileConfig:
    config = RISK_PROFILES.get(risk_profile)
    if config is None:
        valid = ", ".join(sorted(RISK_PROFILES))
        raise ValueError(f"Unsupported risk_profile '{risk_profile}'. Expected one of: {valid}.")
    return config


def _deploy_budget_share(confidence_score: int, risk_profile: RiskProfileConfig) -> float:
    if confidence_score >= 90:
        return risk_profile.budget_share_high_confidence
    if confidence_score >= 80:
        return risk_profile.budget_share_mid_confidence
    return risk_profile.budget_share_low_confidence


def _position_plan_row(
    *,
    advice_row: dict[str, Any],
    total_budget_isk: float,
    remaining_budget_isk: float,
    priority_rank: int,
    risk_profile: RiskProfileConfig,
) -> tuple[dict[str, Any], float]:
    action = str(advice_row.get("action") or "")
    metrics = dict(advice_row.get("metrics") or {})
    capital_required = float(advice_row.get("capital_required") or 0.0)
    tradable_units = float(advice_row.get("tradable_units") or 0.0)
    avg_daily_isk_volume_30d = float(metrics.get("avg_daily_isk_volume_30d") or 0.0)
    confidence_score = int(advice_row.get("confidence_score") or 0)
    capital_per_unit = (capital_required / tradable_units) if capital_required > 0 and tradable_units > 0 else None
    recommended_deployable_isk = float(advice_row.get("recommended_deployable_isk") or 0.0)
    theoretical_market_capacity_isk = float(
        advice_row.get("theoretical_market_capacity_isk") or advice_row.get("daily_capacity_isk_30d") or 0.0
    )
    max_position_isk = float(advice_row.get("max_position_isk") or 0.0)
    if recommended_deployable_isk > 0:
        scaled_recommended_deployable_isk = recommended_deployable_isk * risk_profile.deployable_cap_multiplier
        position_cap_isk = scaled_recommended_deployable_isk
        position_cap_name = "recommended_deployable_isk_scaled"
    elif max_position_isk > 0:
        scaled_recommended_deployable_isk = None
        position_cap_isk = max_position_isk
        position_cap_name = "max_position_isk"
    else:
        scaled_recommended_deployable_isk = None
        position_cap_isk = capital_required
        position_cap_name = "capital_required"

    if total_budget_isk <= 0:
        return (
            {
                "status": "no_budget",
                "priority_rank": priority_rank,
                "risk_profile": risk_profile.name,
                "suggested_capital_isk": 0.0,
                "suggested_units": 0,
                "budget_share_of_total": 0.0,
                "budget_share_cap_isk": 0.0,
                "budget_share_cap_pct": 0.0,
                "liquidity_cap_isk": None,
                "liquidity_cap_daily_volume_fraction": 1.0 / risk_profile.liquidity_divisor,
                "deployable_cap_multiplier": risk_profile.deployable_cap_multiplier,
                "base_recommended_deployable_isk": recommended_deployable_isk if recommended_deployable_isk > 0 else None,
                "scaled_recommended_deployable_isk": scaled_recommended_deployable_isk,
                "position_cap_isk": position_cap_isk if position_cap_isk > 0 else None,
                "position_cap_source": position_cap_name,
                "remaining_budget_after_isk": remaining_budget_isk,
                "binding_constraints": ["total_budget_zero"],
            },
            remaining_budget_isk,
        )

    if action == "reject":
        return (
            {
                "status": "rejected",
                "priority_rank": priority_rank,
                "risk_profile": risk_profile.name,
                "suggested_capital_isk": 0.0,
                "suggested_units": 0,
                "budget_share_of_total": 0.0,
                "budget_share_cap_isk": 0.0,
                "budget_share_cap_pct": 0.0,
                "liquidity_cap_isk": None,
                "liquidity_cap_daily_volume_fraction": 1.0 / risk_profile.liquidity_divisor,
                "deployable_cap_multiplier": risk_profile.deployable_cap_multiplier,
                "base_recommended_deployable_isk": recommended_deployable_isk if recommended_deployable_isk > 0 else None,
                "scaled_recommended_deployable_isk": scaled_recommended_deployable_isk,
                "position_cap_isk": position_cap_isk if position_cap_isk > 0 else None,
                "position_cap_source": position_cap_name,
                "remaining_budget_after_isk": remaining_budget_isk,
                "binding_constraints": ["rejected"],
            },
            remaining_budget_isk,
        )

    if action == "watch":
        return (
            {
                "status": "watch_only",
                "priority_rank": priority_rank,
                "risk_profile": risk_profile.name,
                "suggested_capital_isk": 0.0,
                "suggested_units": 0,
                "budget_share_of_total": 0.0,
                "budget_share_cap_isk": 0.0,
                "budget_share_cap_pct": 0.0,
                "liquidity_cap_isk": None,
                "liquidity_cap_daily_volume_fraction": 1.0 / risk_profile.liquidity_divisor,
                "deployable_cap_multiplier": risk_profile.deployable_cap_multiplier,
                "base_recommended_deployable_isk": recommended_deployable_isk if recommended_deployable_isk > 0 else None,
                "scaled_recommended_deployable_isk": scaled_recommended_deployable_isk,
                "position_cap_isk": position_cap_isk if position_cap_isk > 0 else None,
                "position_cap_source": position_cap_name,
                "remaining_budget_after_isk": remaining_budget_isk,
                "binding_constraints": ["watch_only"],
            },
            remaining_budget_isk,
        )

    budget_share_cap_pct = _deploy_budget_share(confidence_score, risk_profile)
    budget_share_cap_isk = total_budget_isk * budget_share_cap_pct
    liquidity_cap_isk = (
        avg_daily_isk_volume_30d / risk_profile.liquidity_divisor
        if avg_daily_isk_volume_30d > 0 and risk_profile.liquidity_divisor > 0
        else 0.0
    )
    hard_caps = [
        ("remaining_budget", remaining_budget_isk),
        ("budget_share_cap", budget_share_cap_isk),
    ]
    if position_cap_isk > 0:
        hard_caps.append((position_cap_name, position_cap_isk))
    if theoretical_market_capacity_isk > 0:
        hard_caps.append(("theoretical_market_capacity_isk", theoretical_market_capacity_isk))
    if liquidity_cap_isk > 0:
        hard_caps.append(("liquidity_cap", liquidity_cap_isk))

    binding_value = min(value for _, value in hard_caps)
    binding_constraints = [name for name, value in hard_caps if abs(value - binding_value) <= 1e-6]
    suggested_capital_isk = max(binding_value, 0.0)
    if suggested_capital_isk <= 0:
        return (
            {
                "status": "budget_exhausted",
                "priority_rank": priority_rank,
                "risk_profile": risk_profile.name,
                "suggested_capital_isk": 0.0,
                "suggested_units": 0,
                "budget_share_of_total": 0.0,
                "budget_share_cap_isk": budget_share_cap_isk,
                "budget_share_cap_pct": budget_share_cap_pct,
                "liquidity_cap_isk": liquidity_cap_isk if liquidity_cap_isk > 0 else None,
                "liquidity_cap_daily_volume_fraction": 1.0 / risk_profile.liquidity_divisor,
                "deployable_cap_multiplier": risk_profile.deployable_cap_multiplier,
                "base_recommended_deployable_isk": recommended_deployable_isk if recommended_deployable_isk > 0 else None,
                "scaled_recommended_deployable_isk": scaled_recommended_deployable_isk,
                "position_cap_isk": position_cap_isk if position_cap_isk > 0 else None,
                "position_cap_source": position_cap_name,
                "remaining_budget_after_isk": remaining_budget_isk,
                "binding_constraints": binding_constraints or ["remaining_budget"],
            },
            remaining_budget_isk,
        )

    if capital_per_unit is not None and capital_per_unit > 0 and suggested_capital_isk + 1e-6 < capital_per_unit:
        return (
            {
                "status": "watch_only",
                "priority_rank": priority_rank,
                "risk_profile": risk_profile.name,
                "suggested_capital_isk": 0.0,
                "suggested_units": 0,
                "budget_share_of_total": 0.0,
                "budget_share_cap_isk": budget_share_cap_isk,
                "budget_share_cap_pct": budget_share_cap_pct,
                "liquidity_cap_isk": liquidity_cap_isk if liquidity_cap_isk > 0 else None,
                "liquidity_cap_daily_volume_fraction": 1.0 / risk_profile.liquidity_divisor,
                "deployable_cap_multiplier": risk_profile.deployable_cap_multiplier,
                "base_recommended_deployable_isk": recommended_deployable_isk if recommended_deployable_isk > 0 else None,
                "scaled_recommended_deployable_isk": scaled_recommended_deployable_isk,
                "position_cap_isk": position_cap_isk if position_cap_isk > 0 else None,
                "position_cap_source": position_cap_name,
                "remaining_budget_after_isk": remaining_budget_isk,
                "binding_constraints": [*(binding_constraints or []), "cannot_afford_one_unit"],
            },
            remaining_budget_isk,
        )

    suggested_units = 0
    if capital_per_unit is not None and capital_per_unit > 0:
        suggested_units = int(suggested_capital_isk // capital_per_unit)
    budget_share_of_total = suggested_capital_isk / total_budget_isk
    next_remaining_budget_isk = max(remaining_budget_isk - suggested_capital_isk, 0.0)
    return (
        {
            "status": "allocate",
            "priority_rank": priority_rank,
            "risk_profile": risk_profile.name,
            "suggested_capital_isk": suggested_capital_isk,
            "suggested_units": suggested_units,
            "budget_share_of_total": budget_share_of_total,
            "budget_share_cap_isk": budget_share_cap_isk,
            "budget_share_cap_pct": budget_share_cap_pct,
            "liquidity_cap_isk": liquidity_cap_isk if liquidity_cap_isk > 0 else None,
            "liquidity_cap_daily_volume_fraction": 1.0 / risk_profile.liquidity_divisor,
            "deployable_cap_multiplier": risk_profile.deployable_cap_multiplier,
            "base_recommended_deployable_isk": recommended_deployable_isk if recommended_deployable_isk > 0 else None,
            "scaled_recommended_deployable_isk": scaled_recommended_deployable_isk,
            "position_cap_isk": position_cap_isk if position_cap_isk > 0 else None,
            "position_cap_source": position_cap_name,
            "remaining_budget_after_isk": next_remaining_budget_isk,
            "binding_constraints": binding_constraints,
        },
        next_remaining_budget_isk,
    )


def _apply_position_plans(
    selected_advice: list[dict[str, Any]],
    *,
    total_budget_isk: float,
    risk_profile: str,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    risk_profile_config = _resolve_risk_profile_config(risk_profile)
    if total_budget_isk <= 0:
        total_budget_isk = 0.0

    indexed_rows = list(enumerate(selected_advice))
    indexed_rows.sort(
        key=lambda item: (
            _ACTION_PRIORITY.get(str(item[1].get("action") or ""), 99),
            -int(item[1].get("confidence_score") or 0),
            -float(item[1].get("risk_adjusted_return") or 0.0),
            -float(
                item[1].get("recommended_deployable_isk")
                or item[1].get("max_position_isk")
                or item[1].get("capital_required")
                or 0.0
            ),
            float(item[1].get("flip_units_cv_30d") or 999999.0),
            -float(item[1].get("profit_margin") or 0.0),
            str(item[1].get("opportunity_id") or ""),
        )
    )

    updated_rows = [dict(row) for row in selected_advice]
    remaining_budget_isk = total_budget_isk
    allocated_capital_isk = 0.0
    budget_limited_count = 0
    liquidity_limited_count = 0
    capital_required_limited_count = 0
    allocated_candidate_count = 0

    for priority_rank, (original_index, row) in enumerate(indexed_rows, start=1):
        plan, remaining_budget_isk = _position_plan_row(
            advice_row=row,
            total_budget_isk=total_budget_isk,
            remaining_budget_isk=remaining_budget_isk,
            priority_rank=priority_rank,
            risk_profile=risk_profile_config,
        )
        updated_rows[original_index]["position_plan"] = plan
        allocated_capital_isk += float(plan.get("suggested_capital_isk") or 0.0)
        if str(plan.get("status")) == "allocate":
            allocated_candidate_count += 1
        bindings = set(str(item) for item in (plan.get("binding_constraints") or []))
        if "budget_share_cap" in bindings or "remaining_budget" in bindings:
            budget_limited_count += 1
        if "liquidity_cap" in bindings:
            liquidity_limited_count += 1
        if "capital_required" in bindings:
            capital_required_limited_count += 1

    return updated_rows, {
        "risk_profile": risk_profile_config.name,
        "risk_profile_label": risk_profile_config.label,
        "risk_profile_summary": risk_profile_config.summary,
        "deployable_cap_multiplier": risk_profile_config.deployable_cap_multiplier,
        "budget_share_policy": {
            "confidence_ge_90": risk_profile_config.budget_share_high_confidence,
            "confidence_ge_80": risk_profile_config.budget_share_mid_confidence,
            "default": risk_profile_config.budget_share_low_confidence,
        },
        "liquidity_cap_daily_volume_fraction": 1.0 / risk_profile_config.liquidity_divisor,
        "total_budget_isk": total_budget_isk,
        "allocated_capital_isk": allocated_capital_isk,
        "remaining_budget_isk": max(total_budget_isk - allocated_capital_isk, 0.0),
        "allocated_candidate_count": allocated_candidate_count,
        "budget_limited_count": budget_limited_count,
        "liquidity_limited_count": liquidity_limited_count,
        "capital_required_limited_count": capital_required_limited_count,
    }


def _build_execution_plan(selected_advice: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, int]]:
    rows = sorted(
        selected_advice,
        key=lambda row: (
            int((row.get("position_plan") or {}).get("priority_rank") or 999999),
            str(row.get("opportunity_id") or ""),
        ),
    )
    execution_plan: list[dict[str, Any]] = []
    cumulative_allocated_isk = 0.0
    execute_now_count = 0
    deferred_count = 0

    for row in rows:
        position_plan = dict(row.get("position_plan") or {})
        status = str(position_plan.get("status") or "unknown")
        suggested_capital_isk = float(position_plan.get("suggested_capital_isk") or 0.0)
        if status == "allocate":
            execution_status = "execute_now"
            execute_now_count += 1
            cumulative_allocated_isk += suggested_capital_isk
        elif status in {"watch_only", "budget_exhausted"}:
            execution_status = "defer"
            deferred_count += 1
        else:
            execution_status = "skip"
            deferred_count += 1

        execution_plan.append(
            {
                "priority_rank": position_plan.get("priority_rank"),
                "execution_status": execution_status,
                "position_status": status,
                "opportunity_id": row.get("opportunity_id"),
                "type_id": row.get("type_id"),
                "type_name": row.get("type_name"),
                "strategy_type": row.get("strategy_type"),
                "action": row.get("action"),
                "confidence_score": row.get("confidence_score"),
                "suggested_capital_isk": suggested_capital_isk,
                "suggested_units": position_plan.get("suggested_units"),
                "binding_constraints": position_plan.get("binding_constraints"),
                "remaining_budget_after_isk": position_plan.get("remaining_budget_after_isk"),
                "cumulative_allocated_isk": cumulative_allocated_isk,
                "action_rationale": row.get("action_rationale"),
            }
        )

    return execution_plan, {
        "execute_now_count": execute_now_count,
        "deferred_count": deferred_count,
    }


def _advise_candidate(candidate: dict[str, Any]) -> dict[str, Any]:
    strategy_type = str(candidate.get("strategy_type") or candidate.get("strategy_pool") or "station-flip")
    profile = STRATEGY_PROFILES.get(strategy_type, STRATEGY_PROFILES["station-flip"])
    metrics = _extract_metrics(candidate)
    scan_filter_rejection_reasons = [
        str(item) for item in (candidate.get("scan_filter_rejection_reasons") or []) if str(item)
    ]
    passes, failures = _evaluate_thresholds(metrics, profile.thresholds)
    hard_blockers, soft_blockers = _build_blockers(
        strategy_type=strategy_type,
        metrics=metrics,
        failures=failures,
        profile=profile,
    )
    if scan_filter_rejection_reasons:
        hard_blockers = [*scan_filter_rejection_reasons, *hard_blockers]
    confidence_score = _build_confidence_score(
        strategy_type=strategy_type,
        metrics=metrics,
        profile=profile,
        hard_blockers=hard_blockers,
        soft_blockers=soft_blockers,
    )
    action, rationale = _recommend_action(
        strategy_type=strategy_type,
        profile=profile,
        hard_blockers=hard_blockers,
        soft_blockers=soft_blockers,
        confidence_score=confidence_score,
    )

    return {
        "opportunity_id": candidate.get("opportunity_id"),
        "type_id": candidate.get("type_id"),
        "type_name": candidate.get("type_name"),
        "category_id": candidate.get("category_id"),
        "market_group": candidate.get("market_group"),
        "strategy_type": strategy_type,
        "buy_location_id": candidate.get("buy_location_id"),
        "sell_location_id": candidate.get("sell_location_id"),
        "buy_location_name": candidate.get("buy_location_name"),
        "sell_location_name": candidate.get("sell_location_name"),
        "entry_mode": candidate.get("entry_mode"),
        "exit_mode": candidate.get("exit_mode"),
        "profit_margin": candidate.get("profit_margin") or candidate.get("screened_profit_margin"),
        "risk_adjusted_return": candidate.get("risk_adjusted_return"),
        "capital_required": candidate.get("capital_required"),
        "max_position_isk": candidate.get("max_position_isk"),
        "recommended_deployable_isk": candidate.get("recommended_deployable_isk"),
        "competition_adjusted_deployable_isk": candidate.get("competition_adjusted_deployable_isk"),
        "theoretical_market_capacity_isk": candidate.get("theoretical_market_capacity_isk"),
        "safe_exit_capacity_isk": candidate.get("safe_exit_capacity_isk"),
        "safe_inventory_cap_isk": candidate.get("safe_inventory_cap_isk"),
        "captured_sell_capacity_isk": candidate.get("captured_sell_capacity_isk"),
        "tradable_units": candidate.get("tradable_units"),
        "expected_turnover_days": candidate.get("expected_turnover_days") or candidate.get("estimated_exit_days_small_size"),
        "buy_reference_price": candidate.get("buy_reference_price"),
        "sell_reference_price": candidate.get("sell_reference_price"),
        "unit_profit": candidate.get("unit_profit"),
        "daily_capacity_isk_30d": candidate.get("daily_capacity_isk_30d"),
        "flip_units_mean_30d": candidate.get("flip_units_mean_30d"),
        "flip_units_mean_7d": candidate.get("flip_units_mean_7d"),
        "flip_units_cv_30d": candidate.get("flip_units_cv_30d"),
        "flip_units_cv_7d": candidate.get("flip_units_cv_7d"),
        "estimated_daily_buy_units_30d": candidate.get("estimated_daily_buy_units_30d"),
        "estimated_daily_sell_units_30d": candidate.get("estimated_daily_sell_units_30d"),
        "estimated_daily_buy_units_7d": candidate.get("estimated_daily_buy_units_7d"),
        "estimated_daily_sell_units_7d": candidate.get("estimated_daily_sell_units_7d"),
        "estimated_daily_buy_isk_30d": candidate.get("estimated_daily_buy_isk_30d"),
        "estimated_daily_sell_isk_30d": candidate.get("estimated_daily_sell_isk_30d"),
        "estimated_daily_buy_isk_7d": candidate.get("estimated_daily_buy_isk_7d"),
        "estimated_daily_sell_isk_7d": candidate.get("estimated_daily_sell_isk_7d"),
        "estimated_capture_share": candidate.get("estimated_capture_share"),
        "competition_intensity": candidate.get("competition_intensity"),
        "buy_capture_share": candidate.get("buy_capture_share"),
        "sell_capture_share": candidate.get("sell_capture_share"),
        "buy_competition_intensity": candidate.get("buy_competition_intensity"),
        "sell_competition_intensity": candidate.get("sell_competition_intensity"),
        "competition_safety_discount": candidate.get("competition_safety_discount"),
        "sell_liquidity_confidence_score": candidate.get("sell_liquidity_confidence_score"),
        "sell_liquidity_confidence_label": candidate.get("sell_liquidity_confidence_label"),
        "sell_liquidity_confidence_discount": candidate.get("sell_liquidity_confidence_discount"),
        "dominant_attribution_mode_30d": candidate.get("dominant_attribution_mode_30d"),
        "dominant_attribution_mode_7d": candidate.get("dominant_attribution_mode_7d"),
        "single_side_extreme_days_30d": candidate.get("single_side_extreme_days_30d"),
        "single_side_extreme_days_7d": candidate.get("single_side_extreme_days_7d"),
        "recent_high_trade_price_7d": candidate.get("recent_high_trade_price_7d"),
        "sell_price_overhang_ratio": candidate.get("sell_price_overhang_ratio"),
        "sell_price_overhang_steps": candidate.get("sell_price_overhang_steps"),
        "sell_fill_penalty_multiplier": candidate.get("sell_fill_penalty_multiplier"),
        "action": action,
        "action_rationale": rationale,
        "confidence_score": confidence_score,
        "thesis_strength": _thesis_strength(confidence_score),
        "thresholds": asdict(profile.thresholds),
        "deploy_thresholds": {
            "min_profit_margin": profile.deploy_min_profit_margin,
            "max_turnover_days": profile.deploy_max_turnover_days,
            "min_daily_isk_volume": profile.deploy_min_daily_isk_volume,
            "max_volatility": profile.deploy_max_volatility,
            "min_active_days_ratio": profile.deploy_min_active_days_ratio,
            "max_price_zscore": profile.deploy_max_price_zscore,
            "min_price_zscore": profile.deploy_min_price_zscore,
            "deploy_confidence_score": profile.deploy_confidence_score,
            "watch_only": profile.watch_only,
        },
        "passes": passes,
        "failures": failures,
        "scan_filter_rejection_reasons": scan_filter_rejection_reasons,
        "blockers": {
            "hard": hard_blockers,
            "soft": soft_blockers,
        },
        "tailwinds": _build_tailwinds(
            strategy_type=strategy_type,
            metrics=metrics,
            profile=profile,
        ),
        "model_limitations": list(profile.model_limitations),
        "metrics": metrics,
        "trace_refs": {
            "source_order_snapshot_id": candidate.get("source_order_snapshot_id"),
            "source_order_captured_at": candidate.get("source_order_captured_at"),
            "history_latest_market_date": candidate.get("history_latest_market_date"),
            "history_latest_fetched_at": candidate.get("history_latest_fetched_at"),
            "window_end_30d": candidate.get("window_end_30d"),
        },
    }


def _build_advice_basis(primary_scan: dict[str, Any]) -> dict[str, Any]:
    scan_summary = primary_scan["scan_summary"]
    artifact_health = primary_scan.get("artifact_health", {})
    advisory_ready = bool(artifact_health.get("advisory_ready"))
    warnings = list(artifact_health.get("warnings") or [])

    if advisory_ready:
        credibility_tier = "trace-complete"
        basis = "analysis_run + calc_trace + candidate_table"
    else:
        credibility_tier = "degraded"
        basis = "partial artifacts and SQLite snapshot only"
        missing_required = artifact_health.get("missing_required") or []
        if missing_required:
            warnings.append(
                "missing required artifacts: " + ", ".join(str(item) for item in missing_required)
            )

    return {
        "scan_run_id": scan_summary.get("scan_run_id"),
        "credibility_tier": credibility_tier,
        "basis": basis,
        "strategy_model_version": "v1-strategy-advice-2026-04-06",
        "selected_candidate_count": scan_summary.get("selected_candidate_count"),
        "warnings": warnings,
    }


def build_market_advice(
    connection,
    *,
    db_path,
    runs_root,
    log_dir,
    scan_run_id: str | None,
    prefer_latest_selected: bool,
    top_opportunities: int,
    top_near_misses: int,
    risk_profile: str = "standard",
    total_budget_isk_override: float | None = None,
    use_latest_account_budget: bool = False,
    account_snapshot_run_id: str | None = None,
) -> dict[str, Any]:
    report = build_market_report(
        connection,
        db_path=db_path,
        runs_root=runs_root,
        log_dir=log_dir,
        scan_run_id=scan_run_id,
        prefer_latest_selected=prefer_latest_selected,
        top_opportunities=top_opportunities,
        top_near_misses=top_near_misses,
    )
    primary_scan = report["primary_scan"]
    selected_advice = [_advise_candidate(row) for row in primary_scan.get("selected_opportunities", [])]
    near_miss_advice = [_advise_candidate(row) for row in primary_scan.get("near_misses", [])]
    advice_basis = _build_advice_basis(primary_scan)
    account_budget_context = None
    if use_latest_account_budget:
        account_snapshot = load_account_snapshot(
            db_path=db_path,
            runs_root=runs_root,
            account_snapshot_run_id=account_snapshot_run_id,
        )
        account_budget_context = derive_account_budget_context(account_snapshot)
        capital_budget_isk = float(account_budget_context.get("available_budget_isk") or 0.0)
    else:
        capital_budget_isk = (
            float(total_budget_isk_override)
            if total_budget_isk_override is not None
            else float(primary_scan["scan_summary"].get("capital_budget_isk") or 0.0)
        )
    budget_source = "account_wallet_balance" if use_latest_account_budget else "scan_or_manual_budget"
    selected_advice, budget_summary = _apply_position_plans(
        selected_advice,
        total_budget_isk=capital_budget_isk,
        risk_profile=risk_profile,
    )
    execution_plan, execution_summary = _build_execution_plan(selected_advice)
    analysis_context = build_analysis_context(
        budget_source=budget_source,
        total_budget_isk=capital_budget_isk,
        account_budget_context=account_budget_context,
        manual_budget_isk=float(total_budget_isk_override) if total_budget_isk_override is not None else None,
        scan_budget_isk=float(primary_scan["scan_summary"].get("capital_budget_isk") or 0.0),
    )
    budget_summary = {
        **budget_summary,
        **execution_summary,
        "budget_source": budget_source,
        "analysis_context": analysis_context,
        "account_budget_context": account_budget_context,
    }

    return {
        "advice_generated_at_utc": report["report_generated_at_utc"],
        "risk_profile": risk_profile,
        "scan_summary": primary_scan["scan_summary"],
        "traceability": primary_scan["traceability"],
        "artifact_health": primary_scan.get("artifact_health"),
        "data_freshness": primary_scan.get("data_freshness"),
        "advice_basis": advice_basis,
        "budget_summary": budget_summary,
        "analysis_context": analysis_context,
        "account_budget_context": account_budget_context,
        "execution_plan": execution_plan,
        "latest_sync": report.get("latest_sync"),
        "selected_advice": selected_advice,
        "near_miss_advice": near_miss_advice,
        "latest_scan_with_selected_candidates": report.get("latest_scan_with_selected_candidates"),
    }
