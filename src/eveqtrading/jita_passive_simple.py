from __future__ import annotations

import sqlite3
import statistics
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .analysis import persist_scan_run
from .coverage import collect_market_coverage_snapshot
from .db_scan import (
    _is_jita_location_name,
    _load_history_series,
    _load_location_names,
    _load_order_books,
    _load_successful_order_snapshot_refs,
    _safe_div,
    resolve_db_scan_targets,
)
from .models import FeeConfig, ScanConfig
from .order_refresh import build_order_cache_hit_targets
from .run_artifacts import create_run_id, ensure_run_dir, write_json, write_summary


@dataclass(frozen=True)
class JitaPassiveSimpleFilterConfig:
    max_flip_units_cv_30d: float = 0.30
    max_flip_units_cv_7d: float = 0.50
    min_daily_capacity_isk_30d: float = 100_000_000.0
    max_unit_buy_cost_isk: float | None = None
    exclude_blueprint_category: bool = True
    exclude_skin_category: bool = True
    min_buy_price_vs_avg_30d: float = 0.50
    max_sell_price_vs_avg_30d: float = 1.50
    sell_price_overhang_step_pct: float = 0.20
    sell_price_overhang_penalty_factor: float = 0.50


@dataclass(frozen=True)
class JitaPassiveCompetitionConfig:
    operator_refresh_interval_hours: float = 6.0
    top_orders_per_side: int = 8
    price_band_pct: float = 0.003
    min_order_age_hours: float = 0.25
    max_order_age_hours: float = 72.0
    min_capture_share: float = 0.02
    max_capture_share: float = 0.20


def _clamp(value: float, minimum: float, maximum: float) -> float:
    return max(minimum, min(value, maximum))


def _coefficient_of_variation(values: list[float]) -> float:
    if not values:
        return 0.0
    mean_value = statistics.fmean(values)
    if mean_value <= 0.0 or len(values) == 1:
        return 0.0
    squared_diffs = [
        (float(value) - mean_value) * (float(value) - mean_value)
        for value in values
    ]
    variance = sum(squared_diffs) / len(squared_diffs)
    return (variance ** 0.5) / mean_value


def _weighted_average_price(days: list[dict[str, Any]]) -> float:
    weighted_total = 0.0
    total_volume = 0.0
    for day in days:
        volume = float(day["volume"])
        weighted_total += float(day["average_price"]) * volume
        total_volume += volume
    if total_volume > 0.0:
        return weighted_total / total_volume
    if not days:
        return 0.0
    return statistics.fmean(float(day["average_price"]) for day in days)


def _parse_utc_iso(raw_value: Any) -> datetime | None:
    if not isinstance(raw_value, str) or not raw_value:
        return None
    normalized = raw_value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _count_filter_reasons(candidate_traces: list[dict[str, Any]]) -> list[dict[str, Any]]:
    reason_counts: dict[str, int] = {}
    for trace in candidate_traces:
        filter_evaluation = trace.get("filter_evaluation") or {}
        rejection_reasons = filter_evaluation.get("rejection_reasons") or []
        for reason in rejection_reasons:
            reason_key = str(reason)
            reason_counts[reason_key] = reason_counts.get(reason_key, 0) + 1
    rows = [{"reason": reason, "count": count} for reason, count in reason_counts.items()]
    rows.sort(key=lambda row: (-int(row["count"]), str(row["reason"])))
    return rows


def _load_type_metadata(
    connection: sqlite3.Connection,
    *,
    type_ids: set[int],
) -> dict[int, dict[str, Any]]:
    if not type_ids:
        return {}
    placeholders = ",".join("?" for _ in type_ids)
    rows = connection.execute(
        f"SELECT type_id, type_name, category_id, market_group FROM item_catalog WHERE type_id IN ({placeholders})",
        sorted(type_ids),
    ).fetchall()
    return {
        int(row["type_id"]): {
            "type_name": str(row["type_name"]),
            "category_id": int(row["category_id"]) if row["category_id"] is not None else None,
            "market_group": row["market_group"],
        }
        for row in rows
    }


def _best_jita_pair(
    *,
    order_book: dict[str, Any],
    location_names: dict[int, str],
    fee_config: FeeConfig,
) -> dict[str, Any] | None:
    best_pair: dict[str, Any] | None = None
    for pair in order_book.get("same_location_pairs", []):
        location_id = int(pair["location_id"])
        location_name = location_names.get(location_id)
        if not _is_jita_location_name(location_name):
            continue
        best_bid = float(pair["best_bid"])
        best_ask = float(pair["best_ask"])
        buy_cost = best_bid * (1.0 + fee_config.buy_broker_fee_rate)
        sell_net = best_ask * (
            1.0 - fee_config.sell_broker_fee_rate - fee_config.sales_tax_rate - fee_config.relist_fee_rate
        )
        net_margin = _safe_div(sell_net - buy_cost, buy_cost)
        candidate = {
            "location_id": location_id,
            "location_name": location_name,
            "best_bid": best_bid,
            "best_ask": best_ask,
            "buy_cost_per_unit": buy_cost,
            "sell_net_per_unit": sell_net,
            "net_profit_margin": net_margin,
        }
        if best_pair is None or candidate["net_profit_margin"] > best_pair["net_profit_margin"]:
            best_pair = candidate
    return best_pair


def _competitive_orders_for_side(
    *,
    location_book: dict[str, Any],
    is_buy_side: bool,
    best_price: float,
    competition_config: JitaPassiveCompetitionConfig,
) -> list[dict[str, Any]]:
    side_key = "buy_orders" if is_buy_side else "sell_orders"
    orders = [dict(item) for item in (location_book.get(side_key) or [])]
    if not orders:
        return []

    price_band_pct = max(float(competition_config.price_band_pct), 0.0)
    if is_buy_side:
        price_floor = best_price * (1.0 - price_band_pct)
        selected = [item for item in orders if float(item["price"]) >= price_floor]
        selected.sort(key=lambda item: (-float(item["price"]), int(item["order_id"])))
    else:
        price_ceiling = best_price * (1.0 + price_band_pct)
        selected = [item for item in orders if float(item["price"]) <= price_ceiling]
        selected.sort(key=lambda item: (float(item["price"]), int(item["order_id"])))
    return selected[: max(int(competition_config.top_orders_per_side), 1)]


def _estimate_side_capture(
    *,
    side_name: str,
    competitive_orders: list[dict[str, Any]],
    best_price: float,
    snapshot_captured_at: str,
    daily_flip_units: float,
    competition_config: JitaPassiveCompetitionConfig,
) -> dict[str, Any]:
    captured_at = _parse_utc_iso(snapshot_captured_at)
    min_order_age_hours = max(float(competition_config.min_order_age_hours), 0.01)
    max_order_age_hours = max(float(competition_config.max_order_age_hours), min_order_age_hours)
    operator_refresh_interval_hours = max(float(competition_config.operator_refresh_interval_hours), min_order_age_hours)

    age_hours: list[float] = []
    for order in competitive_orders:
        issued_at = _parse_utc_iso(order.get("issued_at"))
        if captured_at is None or issued_at is None:
            age_hours.append(max_order_age_hours)
            continue
        delta_hours = max((captured_at - issued_at).total_seconds() / 3600.0, 0.0)
        age_hours.append(_clamp(delta_hours, min_order_age_hours, max_order_age_hours))

    median_age_hours = statistics.median(age_hours) if age_hours else max_order_age_hours
    competitive_order_count = len(competitive_orders)
    best_price_order_count = sum(1 for order in competitive_orders if float(order["price"]) == best_price)
    queue_depth_units = sum(float(order.get("volume_remain") or 0.0) for order in competitive_orders)
    queue_depth_ratio = _safe_div(queue_depth_units, max(daily_flip_units, 1.0))

    recency_score = _clamp(
        _safe_div(operator_refresh_interval_hours, max(median_age_hours, min_order_age_hours)) / 4.0,
        0.0,
        1.0,
    )
    queue_score = _clamp(_safe_div(max(competitive_order_count - 1, 0), 8.0), 0.0, 1.0)
    best_price_queue_score = _clamp(_safe_div(max(best_price_order_count - 1, 0), 6.0), 0.0, 1.0)
    depth_score = _clamp(_safe_div(queue_depth_ratio, 2.0), 0.0, 1.0)
    competition_intensity = _clamp(
        (0.55 * recency_score) + (0.20 * queue_score) + (0.10 * best_price_queue_score) + (0.15 * depth_score),
        0.0,
        1.0,
    )
    capture_share = _clamp(
        float(competition_config.min_capture_share)
        + ((1.0 - competition_intensity) * (float(competition_config.max_capture_share) - float(competition_config.min_capture_share))),
        float(competition_config.min_capture_share),
        float(competition_config.max_capture_share),
    )

    return {
        "side_name": side_name,
        "competitive_order_count": competitive_order_count,
        "best_price_order_count": best_price_order_count,
        "queue_depth_units": queue_depth_units,
        "queue_depth_ratio_vs_daily_flip": queue_depth_ratio,
        "median_order_age_hours": median_age_hours,
        "oldest_order_age_hours": max(age_hours) if age_hours else None,
        "youngest_order_age_hours": min(age_hours) if age_hours else None,
        "operator_refresh_interval_hours": operator_refresh_interval_hours,
        "recency_score": recency_score,
        "queue_score": queue_score,
        "best_price_queue_score": best_price_queue_score,
        "depth_score": depth_score,
        "competition_intensity": competition_intensity,
        "estimated_capture_share": capture_share,
        "sample_orders": [
            {
                "order_id": int(order["order_id"]),
                "price": float(order["price"]),
                "volume_remain": float(order.get("volume_remain") or 0.0),
                "issued_at": order.get("issued_at"),
            }
            for order in competitive_orders[:5]
        ],
    }


def _competition_safety_discount(*, flip_units_cv_30d: float, flip_units_cv_7d: float) -> float:
    return _clamp(
        0.95 - (0.12 * max(flip_units_cv_30d, 0.0)) - (0.08 * max(flip_units_cv_7d, 0.0)),
        0.70,
        0.95,
    )


def _sell_price_overhang_penalty(
    *,
    best_ask: float,
    latest_7: list[dict[str, Any]],
    filter_config: JitaPassiveSimpleFilterConfig,
) -> dict[str, Any]:
    recent_high_trade_price = max((float(day["highest_price"]) for day in latest_7), default=0.0)
    if recent_high_trade_price <= 0.0:
        return {
            "recent_high_trade_price_7d": recent_high_trade_price,
            "sell_price_overhang_ratio": None,
            "sell_price_overhang_steps": 0,
            "sell_fill_penalty_multiplier": 1.0,
        }

    overhang_ratio = _safe_div(best_ask, recent_high_trade_price) - 1.0
    step_pct = max(float(filter_config.sell_price_overhang_step_pct), 0.0001)
    penalty_factor = _clamp(float(filter_config.sell_price_overhang_penalty_factor), 0.01, 1.0)
    overhang_steps = 0
    if overhang_ratio >= step_pct:
        overhang_steps = int(overhang_ratio // step_pct)
    sell_fill_penalty_multiplier = penalty_factor ** overhang_steps

    return {
        "recent_high_trade_price_7d": recent_high_trade_price,
        "sell_price_overhang_ratio": overhang_ratio,
        "sell_price_overhang_steps": overhang_steps,
        "sell_fill_penalty_multiplier": sell_fill_penalty_multiplier,
    }


def _attribution_confidence_label(score: float) -> str:
    if score >= 0.75:
        return "high"
    if score >= 0.55:
        return "medium"
    return "low"


def _mean_or_zero(values: list[float]) -> float:
    return statistics.fmean(values) if values else 0.0


def _minimum_positive(values: list[float]) -> float:
    positives = [float(value) for value in values if float(value) > 0.0]
    if not positives:
        return 0.0
    return min(positives)


def _attribution_confidence_discount(score: float) -> float:
    return _clamp(0.55 + (0.45 * max(score, 0.0)), 0.55, 1.0)


def _aggregate_flow_window(days: list[dict[str, Any]]) -> dict[str, Any]:
    if not days:
        return {
            "estimated_daily_buy_units": 0.0,
            "estimated_daily_sell_units": 0.0,
            "estimated_daily_flip_units": 0.0,
            "estimated_daily_buy_isk": 0.0,
            "estimated_daily_sell_isk": 0.0,
            "estimated_daily_flip_isk": 0.0,
            "attribution_confidence_score": 0.0,
            "attribution_confidence_label": "low",
            "dominant_attribution_mode": "no_data",
            "single_side_extreme_days": 0,
            "mode_counts": {},
        }

    buy_units = [float(day.get("estimated_buy_fill_units") or 0.0) for day in days]
    sell_units = [float(day.get("estimated_sell_fill_units") or 0.0) for day in days]
    flip_units = [float(day.get("estimated_flip_units") or 0.0) for day in days]
    buy_isk = [
        float(day.get("estimated_buy_fill_units") or 0.0) * float(day.get("average_price") or 0.0)
        for day in days
    ]
    sell_isk = [
        float(day.get("estimated_sell_fill_units") or 0.0) * float(day.get("average_price") or 0.0)
        for day in days
    ]
    flip_isk = [
        float(day.get("estimated_flip_units") or 0.0) * float(day.get("average_price") or 0.0)
        for day in days
    ]
    confidence_scores = [float(day.get("attribution_confidence_score") or 0.0) for day in days]
    mode_counts: dict[str, int] = {}
    for day in days:
        mode = str(day.get("attribution_mode") or "unknown")
        mode_counts[mode] = mode_counts.get(mode, 0) + 1
    dominant_attribution_mode = max(
        mode_counts.items(),
        key=lambda item: (int(item[1]), str(item[0])),
    )[0]
    single_side_extreme_days = sum(
        1
        for day in days
        if str(day.get("attribution_mode") or "") in {"buy_side_dominant", "sell_side_dominant", "ambiguous_single_side"}
    )
    confidence_score = _mean_or_zero(confidence_scores)
    return {
        "estimated_daily_buy_units": _mean_or_zero(buy_units),
        "estimated_daily_sell_units": _mean_or_zero(sell_units),
        "estimated_daily_flip_units": _mean_or_zero(flip_units),
        "estimated_daily_buy_isk": _mean_or_zero(buy_isk),
        "estimated_daily_sell_isk": _mean_or_zero(sell_isk),
        "estimated_daily_flip_isk": _mean_or_zero(flip_isk),
        "attribution_confidence_score": confidence_score,
        "attribution_confidence_label": _attribution_confidence_label(confidence_score),
        "dominant_attribution_mode": dominant_attribution_mode,
        "single_side_extreme_days": single_side_extreme_days,
        "mode_counts": mode_counts,
    }


def _estimate_daily_two_sided_flow(
    *,
    day: dict[str, Any],
    best_bid: float,
    best_ask: float,
) -> dict[str, Any]:
    volume = float(day["volume"])
    average_price = float(day["average_price"])
    highest_price = float(day["highest_price"])
    lowest_price = float(day["lowest_price"])
    spread = best_ask - best_bid

    if volume <= 0.0 or spread <= 0.0:
        return {
            "market_date": day["market_date"],
            "volume": volume,
            "average_price": average_price,
            "highest_price": highest_price,
            "lowest_price": lowest_price,
            "avg_position_in_spread": 0.0,
            "high_position_in_spread": 0.0,
            "low_position_in_spread": 0.0,
            "buy_reach": 0.0,
            "sell_reach": 0.0,
            "buy_fill_share": 0.0,
            "sell_fill_share": 0.0,
            "estimated_buy_fill_units": 0.0,
            "estimated_sell_fill_units": 0.0,
            "estimated_flip_units": 0.0,
            "attribution_mode": "no_volume_or_spread",
            "attribution_confidence_score": 0.0,
            "attribution_confidence_label": "low",
        }

    avg_position_raw = (average_price - best_bid) / spread
    high_position_raw = (highest_price - best_bid) / spread
    low_position_raw = (lowest_price - best_bid) / spread
    avg_position = _clamp(avg_position_raw, 0.0, 1.0)
    high_position = _clamp(high_position_raw, 0.0, 1.0)
    low_position = _clamp(low_position_raw, 0.0, 1.0)
    buy_reach = _clamp((best_ask - lowest_price) / spread, 0.0, 1.0)
    sell_reach = _clamp((highest_price - best_bid) / spread, 0.0, 1.0)

    buy_fill_share = _clamp(((1.0 - avg_position) + buy_reach + (1.0 - low_position)) / 3.0, 0.0, 1.0)
    sell_fill_share = _clamp((avg_position + sell_reach + high_position) / 3.0, 0.0, 1.0)

    no_sell_signal = high_position <= 0.12 and avg_position <= 0.40
    weak_sell_signal = (not no_sell_signal) and high_position <= 0.25 and avg_position <= 0.48
    no_buy_signal = low_position >= 0.88 and avg_position >= 0.60
    weak_buy_signal = (not no_buy_signal) and low_position >= 0.75 and avg_position >= 0.52

    attribution_mode = "two_sided_balanced"
    if no_sell_signal and no_buy_signal:
        attribution_mode = "ambiguous_single_side"
        buy_fill_share = min(max(buy_fill_share, 0.35), 0.65)
        sell_fill_share = min(max(sell_fill_share, 0.35), 0.65)
    elif no_sell_signal:
        attribution_mode = "buy_side_dominant"
        buy_fill_share = max(buy_fill_share, 0.85)
        sell_fill_share = min(sell_fill_share, 0.08)
    elif no_buy_signal:
        attribution_mode = "sell_side_dominant"
        sell_fill_share = max(sell_fill_share, 0.85)
        buy_fill_share = min(buy_fill_share, 0.08)
    else:
        if weak_sell_signal:
            attribution_mode = "buy_side_biased"
            sell_fill_share *= 0.45
            buy_fill_share = max(buy_fill_share, 0.60)
        if weak_buy_signal:
            attribution_mode = "sell_side_biased" if attribution_mode == "two_sided_balanced" else "mixed_side_biased"
            buy_fill_share *= 0.45
            sell_fill_share = max(sell_fill_share, 0.60)

    estimated_buy_fill_units = volume * _clamp(buy_fill_share, 0.0, 1.0)
    estimated_sell_fill_units = volume * _clamp(sell_fill_share, 0.0, 1.0)
    estimated_flip_units = min(estimated_buy_fill_units, estimated_sell_fill_units)

    book_alignment_penalty = (
        abs(avg_position_raw - avg_position) + abs(high_position_raw - high_position) + abs(low_position_raw - low_position)
    )
    book_alignment_score = _clamp(1.0 - (book_alignment_penalty / 3.0), 0.0, 1.0)
    spread_coverage_score = _clamp(high_position - low_position, 0.0, 1.0)
    center_balance_score = 1.0 - min(abs(avg_position - 0.5) * 2.0, 1.0)
    extreme_penalty = 0.0
    if attribution_mode in {"buy_side_dominant", "sell_side_dominant"}:
        extreme_penalty = 0.18
    elif attribution_mode in {"buy_side_biased", "sell_side_biased", "mixed_side_biased"}:
        extreme_penalty = 0.10
    elif attribution_mode == "ambiguous_single_side":
        extreme_penalty = 0.25
    attribution_confidence_score = _clamp(
        0.35
        + (0.35 * book_alignment_score)
        + (0.20 * spread_coverage_score)
        + (0.10 * center_balance_score)
        - extreme_penalty,
        0.15,
        0.98,
    )

    return {
        "market_date": day["market_date"],
        "volume": volume,
        "average_price": average_price,
        "highest_price": highest_price,
        "lowest_price": lowest_price,
        "avg_position_in_spread": avg_position,
        "high_position_in_spread": high_position,
        "low_position_in_spread": low_position,
        "buy_reach": buy_reach,
        "sell_reach": sell_reach,
        "buy_fill_share": buy_fill_share,
        "sell_fill_share": sell_fill_share,
        "estimated_buy_fill_units": estimated_buy_fill_units,
        "estimated_sell_fill_units": estimated_sell_fill_units,
        "estimated_flip_units": estimated_flip_units,
        "attribution_mode": attribution_mode,
        "attribution_confidence_score": attribution_confidence_score,
        "attribution_confidence_label": _attribution_confidence_label(attribution_confidence_score),
    }


def _build_candidate(
    *,
    type_id: int,
    type_name: str,
    category_id: int | None,
    market_group: str | None,
    region_id: int,
    selection_mode: str,
    order_book: dict[str, Any],
    jita_pair: dict[str, Any],
    latest_30: list[dict[str, Any]],
    latest_7: list[dict[str, Any]],
    capital_budget_isk: float,
    competition_config: JitaPassiveCompetitionConfig,
    filter_config: JitaPassiveSimpleFilterConfig,
) -> tuple[dict[str, Any], dict[str, Any]]:
    best_bid = float(jita_pair["best_bid"])
    best_ask = float(jita_pair["best_ask"])
    buy_cost_per_unit = float(jita_pair["buy_cost_per_unit"])
    sell_net_per_unit = float(jita_pair["sell_net_per_unit"])
    net_profit_margin = float(jita_pair["net_profit_margin"])

    sell_price_overhang = _sell_price_overhang_penalty(
        best_ask=best_ask,
        latest_7=latest_7,
        filter_config=filter_config,
    )
    sell_fill_penalty_multiplier = float(sell_price_overhang["sell_fill_penalty_multiplier"])
    flow_30d: list[dict[str, Any]] = []
    for day in latest_30:
        flow_day = _estimate_daily_two_sided_flow(day=day, best_bid=best_bid, best_ask=best_ask)
        raw_estimated_sell_fill_units = float(flow_day["estimated_sell_fill_units"])
        penalized_sell_fill_units = raw_estimated_sell_fill_units * sell_fill_penalty_multiplier
        flow_day["raw_estimated_sell_fill_units"] = raw_estimated_sell_fill_units
        flow_day["sell_fill_penalty_multiplier"] = sell_fill_penalty_multiplier
        flow_day["estimated_sell_fill_units"] = penalized_sell_fill_units
        flow_day["estimated_flip_units"] = min(
            float(flow_day["estimated_buy_fill_units"]),
            penalized_sell_fill_units,
        )
        flow_30d.append(flow_day)
    flow_7d = flow_30d[-7:]
    aggregate_30d = _aggregate_flow_window(flow_30d)
    aggregate_7d = _aggregate_flow_window(flow_7d)
    flip_units_30d = [float(day["estimated_flip_units"]) for day in flow_30d]
    flip_units_7d = [float(day["estimated_flip_units"]) for day in flow_7d]

    flip_units_mean_30d = statistics.fmean(flip_units_30d) if flip_units_30d else 0.0
    flip_units_mean_7d = statistics.fmean(flip_units_7d) if flip_units_7d else 0.0
    flip_units_cv_30d = _coefficient_of_variation(flip_units_30d)
    flip_units_cv_7d = _coefficient_of_variation(flip_units_7d)
    avg_price_weighted_30d = _weighted_average_price(latest_30)
    avg_daily_isk_volume_30d = (
        sum(float(day["average_price"]) * float(day["volume"]) for day in latest_30) / len(latest_30)
    )
    theoretical_market_capacity_isk = flip_units_mean_30d * avg_price_weighted_30d
    estimated_daily_buy_units_30d = float(aggregate_30d["estimated_daily_buy_units"])
    estimated_daily_sell_units_30d = float(aggregate_30d["estimated_daily_sell_units"])
    estimated_daily_buy_units_7d = float(aggregate_7d["estimated_daily_buy_units"])
    estimated_daily_sell_units_7d = float(aggregate_7d["estimated_daily_sell_units"])
    estimated_daily_buy_isk_30d = float(aggregate_30d["estimated_daily_buy_isk"])
    estimated_daily_sell_isk_30d = float(aggregate_30d["estimated_daily_sell_isk"])
    estimated_daily_buy_isk_7d = float(aggregate_7d["estimated_daily_buy_isk"])
    estimated_daily_sell_isk_7d = float(aggregate_7d["estimated_daily_sell_isk"])
    sell_liquidity_confidence_score_30d = float(aggregate_30d["attribution_confidence_score"])
    sell_liquidity_confidence_score_7d = float(aggregate_7d["attribution_confidence_score"])
    sell_liquidity_confidence_score = min(
        score
        for score in (
            sell_liquidity_confidence_score_30d,
            sell_liquidity_confidence_score_7d if sell_liquidity_confidence_score_7d > 0.0 else sell_liquidity_confidence_score_30d,
        )
    )
    sell_liquidity_confidence_discount = _attribution_confidence_discount(sell_liquidity_confidence_score)
    raw_safe_exit_capacity_isk = _minimum_positive(
        [
            estimated_daily_sell_isk_30d,
            estimated_daily_sell_isk_7d,
        ]
    )
    safe_exit_capacity_isk = raw_safe_exit_capacity_isk * sell_liquidity_confidence_discount
    safe_inventory_cap_isk = safe_exit_capacity_isk / 6.0 if safe_exit_capacity_isk > 0.0 else 0.0
    active_days_ratio_30d = _safe_div(
        sum(1 for day in latest_30 if float(day["volume"]) > 0.0),
        len(latest_30),
    )
    price_mean_30d = statistics.fmean(float(day["average_price"]) for day in latest_30)
    price_volatility_30d = _coefficient_of_variation([float(day["average_price"]) for day in latest_30])
    location_id = int(jita_pair["location_id"])
    location_book = dict((order_book.get("location_books") or {}).get(location_id) or {})
    buy_side_competition = _estimate_side_capture(
        side_name="buy",
        competitive_orders=_competitive_orders_for_side(
            location_book=location_book,
            is_buy_side=True,
            best_price=best_bid,
            competition_config=competition_config,
        ),
        best_price=best_bid,
        snapshot_captured_at=str(order_book["captured_at"]),
        daily_flip_units=flip_units_mean_30d,
        competition_config=competition_config,
    )
    sell_side_competition = _estimate_side_capture(
        side_name="sell",
        competitive_orders=_competitive_orders_for_side(
            location_book=location_book,
            is_buy_side=False,
            best_price=best_ask,
            competition_config=competition_config,
        ),
        best_price=best_ask,
        snapshot_captured_at=str(order_book["captured_at"]),
        daily_flip_units=flip_units_mean_30d,
        competition_config=competition_config,
    )
    estimated_capture_share = min(
        float(buy_side_competition["estimated_capture_share"]),
        float(sell_side_competition["estimated_capture_share"]),
    )
    competition_intensity = max(
        float(buy_side_competition["competition_intensity"]),
        float(sell_side_competition["competition_intensity"]),
    )
    competition_safety_discount = _competition_safety_discount(
        flip_units_cv_30d=flip_units_cv_30d,
        flip_units_cv_7d=flip_units_cv_7d,
    )
    competition_adjusted_deployable_isk = (
        theoretical_market_capacity_isk * estimated_capture_share * competition_safety_discount
    )
    recommended_deployable_isk = _minimum_positive(
        [
            safe_inventory_cap_isk,
            competition_adjusted_deployable_isk,
        ]
    )
    captured_sell_capacity_isk = safe_exit_capacity_isk * estimated_capture_share * competition_safety_discount
    capital_required_isk = min(capital_budget_isk, recommended_deployable_isk) if recommended_deployable_isk > 0.0 else 0.0
    tradable_units = _safe_div(capital_required_isk, buy_cost_per_unit)
    unit_profit = sell_net_per_unit - buy_cost_per_unit
    total_profit = unit_profit * tradable_units
    expected_turnover_days = _safe_div(capital_required_isk, max(captured_sell_capacity_isk, 1.0))
    risk_adjusted_return = (
        max(net_profit_margin, 0.0)
        * (1.0 - min(flip_units_cv_30d, 0.95))
        * sell_liquidity_confidence_discount
    )
    buy_price_vs_avg_30d = _safe_div(best_bid, avg_price_weighted_30d)
    sell_price_vs_avg_30d = _safe_div(best_ask, avg_price_weighted_30d)

    source_record = {
        "opportunity_id": f"{type_id}-{region_id}-jita-passive-simple",
        "strategy_hint": "jita-passive-simple",
        "type_id": type_id,
        "type_name": type_name,
        "category_id": category_id,
        "market_group": market_group,
        "region_id": region_id,
        "buy_location_id": int(jita_pair["location_id"]),
        "sell_location_id": int(jita_pair["location_id"]),
        "buy_location_name": jita_pair["location_name"],
        "sell_location_name": jita_pair["location_name"],
        "best_bid": best_bid,
        "best_ask": best_ask,
        "buy_reference_price": best_bid,
        "sell_reference_price": best_ask,
        "gross_spread_pct": _safe_div(best_ask - best_bid, best_bid),
        "screened_profit_margin_override": net_profit_margin,
        "entry_mode_override": "place_buy_order_best_bid",
        "exit_mode_override": "place_sell_order_best_ask",
        "capital_required_override": capital_required_isk,
        "max_position_isk": recommended_deployable_isk,
        "recommended_deployable_isk": recommended_deployable_isk,
        "competition_adjusted_deployable_isk": competition_adjusted_deployable_isk,
        "theoretical_market_capacity_isk": theoretical_market_capacity_isk,
        "safe_exit_capacity_isk": safe_exit_capacity_isk,
        "safe_inventory_cap_isk": safe_inventory_cap_isk,
        "captured_sell_capacity_isk": captured_sell_capacity_isk,
        "estimated_capture_share": estimated_capture_share,
        "competition_intensity": competition_intensity,
        "buy_capture_share": buy_side_competition["estimated_capture_share"],
        "sell_capture_share": sell_side_competition["estimated_capture_share"],
        "buy_competition_intensity": buy_side_competition["competition_intensity"],
        "sell_competition_intensity": sell_side_competition["competition_intensity"],
        "competition_safety_discount": competition_safety_discount,
        "inventory_limit_basis": "min(safe_inventory_cap_isk,competition_adjusted_deployable_isk)_v2",
        "avg_daily_isk_volume_30d": avg_daily_isk_volume_30d,
        "avg_daily_units_7d": flip_units_mean_7d,
        "active_days_ratio_30d": active_days_ratio_30d,
        "cross_location_net_spread_pct": 0.0,
        "volatility_30d": price_volatility_30d,
        "source_order_snapshot_id": order_book["snapshot_id"],
        "source_order_captured_at": order_book["captured_at"],
        "source_selection_mode": selection_mode,
        "flip_units_mean_30d": flip_units_mean_30d,
        "flip_units_mean_7d": flip_units_mean_7d,
        "flip_units_cv_30d": flip_units_cv_30d,
        "flip_units_cv_7d": flip_units_cv_7d,
        "avg_price_weighted_30d": avg_price_weighted_30d,
        "estimated_daily_buy_units_30d": estimated_daily_buy_units_30d,
        "estimated_daily_sell_units_30d": estimated_daily_sell_units_30d,
        "estimated_daily_buy_units_7d": estimated_daily_buy_units_7d,
        "estimated_daily_sell_units_7d": estimated_daily_sell_units_7d,
        "estimated_daily_buy_isk_30d": estimated_daily_buy_isk_30d,
        "estimated_daily_sell_isk_30d": estimated_daily_sell_isk_30d,
        "estimated_daily_buy_isk_7d": estimated_daily_buy_isk_7d,
        "estimated_daily_sell_isk_7d": estimated_daily_sell_isk_7d,
        "sell_liquidity_confidence_score": sell_liquidity_confidence_score,
        "sell_liquidity_confidence_label": _attribution_confidence_label(sell_liquidity_confidence_score),
        "sell_liquidity_confidence_discount": sell_liquidity_confidence_discount,
        "dominant_attribution_mode_30d": aggregate_30d["dominant_attribution_mode"],
        "dominant_attribution_mode_7d": aggregate_7d["dominant_attribution_mode"],
        "single_side_extreme_days_30d": aggregate_30d["single_side_extreme_days"],
        "single_side_extreme_days_7d": aggregate_7d["single_side_extreme_days"],
        "buy_price_vs_avg_30d": buy_price_vs_avg_30d,
        "sell_price_vs_avg_30d": sell_price_vs_avg_30d,
        "recent_high_trade_price_7d": sell_price_overhang["recent_high_trade_price_7d"],
        "sell_price_overhang_ratio": sell_price_overhang["sell_price_overhang_ratio"],
        "sell_price_overhang_steps": sell_price_overhang["sell_price_overhang_steps"],
        "sell_fill_penalty_multiplier": sell_fill_penalty_multiplier,
        "daily_capacity_isk_30d": theoretical_market_capacity_isk,
        "history_days_30d": len(latest_30),
        "history_days_7d": len(latest_7),
        "history_latest_market_date": latest_30[-1]["market_date"],
        "history_latest_fetched_at": max(str(day.get("fetched_at") or "") for day in latest_30),
        "window_start_30d": latest_30[0]["market_date"],
        "window_end_30d": latest_30[-1]["market_date"],
        "window_start_7d": latest_7[0]["market_date"],
        "window_end_7d": latest_7[-1]["market_date"],
        "history_mean_30d": price_mean_30d,
    }

    feature_row = {
        "type_id": type_id,
        "type_name": type_name,
        "region_id": region_id,
        "location_id": int(jita_pair["location_id"]),
        "location_name": jita_pair["location_name"],
        "as_of_time": order_book["captured_at"],
        "best_bid": best_bid,
        "best_ask": best_ask,
        "buy_cost_per_unit": buy_cost_per_unit,
        "sell_net_per_unit": sell_net_per_unit,
        "gross_spread_pct": _safe_div(best_ask - best_bid, best_bid),
        "net_profit_margin": net_profit_margin,
        "avg_daily_isk_volume_30d": avg_daily_isk_volume_30d,
        "avg_daily_units_7d": flip_units_mean_7d,
        "active_days_ratio_30d": active_days_ratio_30d,
        "flip_units_mean_30d": flip_units_mean_30d,
        "flip_units_mean_7d": flip_units_mean_7d,
        "flip_units_cv_30d": flip_units_cv_30d,
        "flip_units_cv_7d": flip_units_cv_7d,
        "avg_price_weighted_30d": avg_price_weighted_30d,
        "buy_price_vs_avg_30d": buy_price_vs_avg_30d,
        "sell_price_vs_avg_30d": sell_price_vs_avg_30d,
        "recent_high_trade_price_7d": sell_price_overhang["recent_high_trade_price_7d"],
        "sell_price_overhang_ratio": sell_price_overhang["sell_price_overhang_ratio"],
        "sell_price_overhang_steps": sell_price_overhang["sell_price_overhang_steps"],
        "sell_fill_penalty_multiplier": sell_fill_penalty_multiplier,
        "daily_capacity_isk_30d": theoretical_market_capacity_isk,
        "theoretical_market_capacity_isk": theoretical_market_capacity_isk,
        "recommended_deployable_isk": recommended_deployable_isk,
        "estimated_capture_share": estimated_capture_share,
        "competition_intensity": competition_intensity,
        "buy_capture_share": buy_side_competition["estimated_capture_share"],
        "sell_capture_share": sell_side_competition["estimated_capture_share"],
        "buy_competition_intensity": buy_side_competition["competition_intensity"],
        "sell_competition_intensity": sell_side_competition["competition_intensity"],
        "competition_adjusted_deployable_isk": competition_adjusted_deployable_isk,
        "safe_exit_capacity_isk": safe_exit_capacity_isk,
        "safe_inventory_cap_isk": safe_inventory_cap_isk,
        "captured_sell_capacity_isk": captured_sell_capacity_isk,
        "expected_turnover_days": expected_turnover_days,
        "estimated_daily_buy_units_30d": estimated_daily_buy_units_30d,
        "estimated_daily_sell_units_30d": estimated_daily_sell_units_30d,
        "estimated_daily_buy_units_7d": estimated_daily_buy_units_7d,
        "estimated_daily_sell_units_7d": estimated_daily_sell_units_7d,
        "estimated_daily_buy_isk_30d": estimated_daily_buy_isk_30d,
        "estimated_daily_sell_isk_30d": estimated_daily_sell_isk_30d,
        "estimated_daily_buy_isk_7d": estimated_daily_buy_isk_7d,
        "estimated_daily_sell_isk_7d": estimated_daily_sell_isk_7d,
        "sell_liquidity_confidence_score": sell_liquidity_confidence_score,
        "sell_liquidity_confidence_label": _attribution_confidence_label(sell_liquidity_confidence_score),
        "sell_liquidity_confidence_discount": sell_liquidity_confidence_discount,
        "dominant_attribution_mode_30d": aggregate_30d["dominant_attribution_mode"],
        "dominant_attribution_mode_7d": aggregate_7d["dominant_attribution_mode"],
        "single_side_extreme_days_30d": aggregate_30d["single_side_extreme_days"],
        "single_side_extreme_days_7d": aggregate_7d["single_side_extreme_days"],
        "source_order_snapshot_id": order_book["snapshot_id"],
        "source_selection_mode": selection_mode,
    }

    candidate_trace = {
        "type_id": type_id,
        "type_name": type_name,
        "region_id": region_id,
        "source_record": source_record,
        "derived": {
            "buy_cost_per_unit": buy_cost_per_unit,
            "sell_net_per_unit": sell_net_per_unit,
            "unit_profit": unit_profit,
            "net_spread_pct": net_profit_margin,
            "screened_profit_margin": net_profit_margin,
            "estimated_exit_days_small_size": expected_turnover_days,
            "capital_required_isk": capital_required_isk,
            "tradable_units": tradable_units,
            "total_profit": total_profit,
            "risk_adjusted_return": risk_adjusted_return,
            "flip_units_mean_30d": flip_units_mean_30d,
            "flip_units_mean_7d": flip_units_mean_7d,
            "flip_units_cv_30d": flip_units_cv_30d,
            "flip_units_cv_7d": flip_units_cv_7d,
            "avg_price_weighted_30d": avg_price_weighted_30d,
            "buy_price_vs_avg_30d": buy_price_vs_avg_30d,
            "sell_price_vs_avg_30d": sell_price_vs_avg_30d,
            "recent_high_trade_price_7d": sell_price_overhang["recent_high_trade_price_7d"],
            "sell_price_overhang_ratio": sell_price_overhang["sell_price_overhang_ratio"],
            "sell_price_overhang_steps": sell_price_overhang["sell_price_overhang_steps"],
            "sell_fill_penalty_multiplier": sell_fill_penalty_multiplier,
            "daily_capacity_isk_30d": theoretical_market_capacity_isk,
            "theoretical_market_capacity_isk": theoretical_market_capacity_isk,
            "recommended_deployable_isk": recommended_deployable_isk,
            "competition_adjusted_deployable_isk": competition_adjusted_deployable_isk,
            "estimated_capture_share": estimated_capture_share,
            "competition_intensity": competition_intensity,
            "competition_safety_discount": competition_safety_discount,
            "safe_exit_capacity_isk": safe_exit_capacity_isk,
            "safe_inventory_cap_isk": safe_inventory_cap_isk,
            "captured_sell_capacity_isk": captured_sell_capacity_isk,
            "estimated_daily_buy_units_30d": estimated_daily_buy_units_30d,
            "estimated_daily_sell_units_30d": estimated_daily_sell_units_30d,
            "estimated_daily_buy_units_7d": estimated_daily_buy_units_7d,
            "estimated_daily_sell_units_7d": estimated_daily_sell_units_7d,
            "estimated_daily_buy_isk_30d": estimated_daily_buy_isk_30d,
            "estimated_daily_sell_isk_30d": estimated_daily_sell_isk_30d,
            "estimated_daily_buy_isk_7d": estimated_daily_buy_isk_7d,
            "estimated_daily_sell_isk_7d": estimated_daily_sell_isk_7d,
            "sell_liquidity_confidence_score": sell_liquidity_confidence_score,
            "sell_liquidity_confidence_label": _attribution_confidence_label(sell_liquidity_confidence_score),
            "sell_liquidity_confidence_discount": sell_liquidity_confidence_discount,
            "dominant_attribution_mode_30d": aggregate_30d["dominant_attribution_mode"],
            "dominant_attribution_mode_7d": aggregate_7d["dominant_attribution_mode"],
            "single_side_extreme_days_30d": aggregate_30d["single_side_extreme_days"],
            "single_side_extreme_days_7d": aggregate_7d["single_side_extreme_days"],
            "buy_side_competition": buy_side_competition,
            "sell_side_competition": sell_side_competition,
            "price_mean_30d": price_mean_30d,
            "price_volatility_30d": price_volatility_30d,
            "aggregate_flow_30d": aggregate_30d,
            "aggregate_flow_7d": aggregate_7d,
            "daily_breakdown_7d": flow_7d,
        },
        "filter_evaluation": {
            "rejection_reasons": [],
            "passed": False,
        },
        "strategy_pool": "jita-passive-simple",
    }
    return feature_row, candidate_trace


def _build_selected_candidate(candidate_trace: dict[str, Any]) -> dict[str, Any]:
    source_record = dict(candidate_trace["source_record"])
    derived = dict(candidate_trace["derived"])
    net_profit_margin = float(derived["screened_profit_margin"])
    unit_profit = float(derived["unit_profit"])
    capital_required_isk = float(derived["capital_required_isk"])
    tradable_units = float(derived["tradable_units"])
    total_profit = float(derived["total_profit"])
    expected_turnover_days = float(derived["estimated_exit_days_small_size"])
    risk_adjusted_return = float(derived["risk_adjusted_return"])
    recommended_deployable_isk = float(source_record.get("recommended_deployable_isk") or source_record.get("max_position_isk") or 0.0)
    theoretical_market_capacity_isk = float(source_record.get("theoretical_market_capacity_isk") or derived.get("daily_capacity_isk_30d") or 0.0)
    return {
        "opportunity_id": source_record["opportunity_id"],
        "strategy_type": "jita-passive-simple",
        "type_id": source_record["type_id"],
        "type_name": source_record["type_name"],
        "category_id": source_record.get("category_id"),
        "market_group": source_record.get("market_group"),
        "region_id": source_record["region_id"],
        "buy_location_id": source_record["buy_location_id"],
        "sell_location_id": source_record["sell_location_id"],
        "buy_location_name": source_record["buy_location_name"],
        "sell_location_name": source_record["sell_location_name"],
        "entry_mode": "place_buy_order_best_bid",
        "exit_mode": "place_sell_order_best_ask",
        "unit_profit": unit_profit,
        "total_profit": total_profit,
        "profit_margin": net_profit_margin,
        "capital_required": capital_required_isk,
        "tradable_units": tradable_units,
        "expected_turnover_days": expected_turnover_days,
        "risk_adjusted_return": risk_adjusted_return,
        "decision_reason": (
            f"30d theoretical capacity {theoretical_market_capacity_isk:.2f} ISK; "
            f"safe inventory cap {float(source_record.get('safe_inventory_cap_isk') or 0.0):.2f} ISK; "
            f"recommended deployable {recommended_deployable_isk:.2f} ISK; "
            f"capture share {derived['estimated_capture_share']:.2%}; "
            f"30d CV {derived['flip_units_cv_30d']:.2%}; "
            f"7d CV {derived['flip_units_cv_7d']:.2%}"
        ),
        "gross_spread_pct": source_record["gross_spread_pct"],
        "net_spread_pct": net_profit_margin,
        "avg_daily_isk_volume_30d": source_record["avg_daily_isk_volume_30d"],
        "avg_daily_units_7d": source_record["avg_daily_units_7d"],
        "active_days_ratio_30d": source_record["active_days_ratio_30d"],
        "volatility_30d": source_record["volatility_30d"],
        "buy_reference_price": source_record["buy_reference_price"],
        "sell_reference_price": source_record["sell_reference_price"],
        "max_position_isk": source_record["max_position_isk"],
        "recommended_deployable_isk": source_record.get("recommended_deployable_isk"),
        "competition_adjusted_deployable_isk": source_record.get("competition_adjusted_deployable_isk"),
        "theoretical_market_capacity_isk": source_record.get("theoretical_market_capacity_isk"),
        "safe_exit_capacity_isk": source_record.get("safe_exit_capacity_isk"),
        "safe_inventory_cap_isk": source_record.get("safe_inventory_cap_isk"),
        "captured_sell_capacity_isk": source_record.get("captured_sell_capacity_isk"),
        "estimated_capture_share": source_record.get("estimated_capture_share"),
        "competition_intensity": source_record.get("competition_intensity"),
        "buy_capture_share": source_record.get("buy_capture_share"),
        "sell_capture_share": source_record.get("sell_capture_share"),
        "buy_competition_intensity": source_record.get("buy_competition_intensity"),
        "sell_competition_intensity": source_record.get("sell_competition_intensity"),
        "competition_safety_discount": source_record.get("competition_safety_discount"),
        "inventory_limit_basis": source_record["inventory_limit_basis"],
        "source_order_snapshot_id": source_record["source_order_snapshot_id"],
        "source_order_captured_at": source_record["source_order_captured_at"],
        "history_latest_market_date": source_record.get("history_latest_market_date"),
        "history_latest_fetched_at": source_record.get("history_latest_fetched_at"),
        "window_start_30d": source_record.get("window_start_30d"),
        "window_end_30d": source_record.get("window_end_30d"),
        "window_start_7d": source_record.get("window_start_7d"),
        "window_end_7d": source_record.get("window_end_7d"),
        "flip_units_mean_30d": derived["flip_units_mean_30d"],
        "flip_units_mean_7d": derived["flip_units_mean_7d"],
        "flip_units_cv_30d": derived["flip_units_cv_30d"],
        "flip_units_cv_7d": derived["flip_units_cv_7d"],
        "daily_capacity_isk_30d": derived["daily_capacity_isk_30d"],
        "estimated_daily_buy_units_30d": derived.get("estimated_daily_buy_units_30d"),
        "estimated_daily_sell_units_30d": derived.get("estimated_daily_sell_units_30d"),
        "estimated_daily_buy_units_7d": derived.get("estimated_daily_buy_units_7d"),
        "estimated_daily_sell_units_7d": derived.get("estimated_daily_sell_units_7d"),
        "estimated_daily_buy_isk_30d": derived.get("estimated_daily_buy_isk_30d"),
        "estimated_daily_sell_isk_30d": derived.get("estimated_daily_sell_isk_30d"),
        "estimated_daily_buy_isk_7d": derived.get("estimated_daily_buy_isk_7d"),
        "estimated_daily_sell_isk_7d": derived.get("estimated_daily_sell_isk_7d"),
        "sell_liquidity_confidence_score": derived.get("sell_liquidity_confidence_score"),
        "sell_liquidity_confidence_label": derived.get("sell_liquidity_confidence_label"),
        "sell_liquidity_confidence_discount": derived.get("sell_liquidity_confidence_discount"),
        "dominant_attribution_mode_30d": derived.get("dominant_attribution_mode_30d"),
        "dominant_attribution_mode_7d": derived.get("dominant_attribution_mode_7d"),
        "single_side_extreme_days_30d": derived.get("single_side_extreme_days_30d"),
        "single_side_extreme_days_7d": derived.get("single_side_extreme_days_7d"),
        "avg_price_weighted_30d": derived["avg_price_weighted_30d"],
        "buy_price_vs_avg_30d": derived["buy_price_vs_avg_30d"],
        "sell_price_vs_avg_30d": derived["sell_price_vs_avg_30d"],
        "recent_high_trade_price_7d": derived.get("recent_high_trade_price_7d"),
        "sell_price_overhang_ratio": derived.get("sell_price_overhang_ratio"),
        "sell_price_overhang_steps": derived.get("sell_price_overhang_steps"),
        "sell_fill_penalty_multiplier": derived.get("sell_fill_penalty_multiplier"),
    }


def _has_explicit_selector(
    *,
    explicit_type_ids: list[int],
    type_id_file: Path | None,
    ready_market_scan_types: bool,
    all_published_market_types: bool,
) -> bool:
    return bool(explicit_type_ids) or type_id_file is not None or ready_market_scan_types or all_published_market_types


def _resolve_scan_targets(
    connection: sqlite3.Connection,
    *,
    region_id: int,
    explicit_type_ids: list[int],
    type_id_file: Path | None,
    ready_market_scan_types: bool,
    all_published_market_types: bool,
    limit: int | None,
    orders_cache_profile: str | None,
    order_type: str,
    cache_only: bool,
) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    if not cache_only:
        selected_targets = resolve_db_scan_targets(
            connection,
            region_id=region_id,
            explicit_type_ids=explicit_type_ids,
            type_id_file=type_id_file,
            ready_market_scan_types=ready_market_scan_types,
            all_published_market_types=all_published_market_types,
            limit=limit,
        )
        return selected_targets or [], None

    if not orders_cache_profile:
        raise ValueError("orders_cache_profile is required when cache_only is enabled")

    base_targets = None
    if _has_explicit_selector(
        explicit_type_ids=explicit_type_ids,
        type_id_file=type_id_file,
        ready_market_scan_types=ready_market_scan_types,
        all_published_market_types=all_published_market_types,
    ):
        base_targets = resolve_db_scan_targets(
            connection,
            region_id=region_id,
            explicit_type_ids=explicit_type_ids,
            type_id_file=type_id_file,
            ready_market_scan_types=ready_market_scan_types,
            all_published_market_types=all_published_market_types,
            limit=None,
        ) or []

    selected_type_ids = {int(row["type_id"]) for row in base_targets} if base_targets is not None else None
    snapshot, selected_targets = build_order_cache_hit_targets(
        connection,
        region_id=region_id,
        profile_name=orders_cache_profile,
        order_type=order_type,
        limit=limit,
        selected_type_ids=selected_type_ids,
    )
    cache_selection = {
        "enabled": True,
        "profile_name": orders_cache_profile,
        "order_type": order_type,
        "selected_target_count": len(selected_targets),
        "cache_hit_count": snapshot["target_counts"]["cache_hit_count"],
        "history_ready_count": snapshot["target_counts"]["history_ready_count"],
        "missing_orders_count": snapshot["target_counts"]["missing_orders_count"],
        "refresh_due_remote_count": snapshot["target_counts"]["refresh_due_remote_count"],
        "refresh_due_wait_count": snapshot["target_counts"]["refresh_due_wait_count"],
        "requested_scope_target_count": (
            len(base_targets) if base_targets is not None else snapshot["target_counts"]["market_type_count"]
        ),
    }
    return selected_targets, cache_selection


def run_jita_passive_simple_scan(
    *,
    connection: sqlite3.Connection,
    db_path: Path,
    output_root: Path,
    region_id: int,
    explicit_type_ids: list[int],
    type_id_file: Path | None,
    ready_market_scan_types: bool,
    all_published_market_types: bool,
    limit: int | None,
    capital_budget_isk: float,
    top_n: int,
    strategy_set: str,
    parameter_version: str,
    fee_config: FeeConfig | None = None,
    filter_config: JitaPassiveSimpleFilterConfig | None = None,
    competition_config: JitaPassiveCompetitionConfig | None = None,
    orders_cache_profile: str | None = None,
    order_type: str = "all",
    cache_only: bool = False,
) -> dict[str, Any]:
    started_at = datetime.now(timezone.utc)
    effective_fee_config = fee_config or FeeConfig()
    effective_filter_config = filter_config or JitaPassiveSimpleFilterConfig()
    effective_competition_config = competition_config or JitaPassiveCompetitionConfig()
    scan_config = ScanConfig(
        strategy_set=strategy_set,
        parameter_version=parameter_version,
        test_position_isk=capital_budget_isk,
        capital_budget_isk=capital_budget_isk,
        top_n=top_n,
    )

    selected_targets, cache_selection = _resolve_scan_targets(
        connection,
        region_id=region_id,
        explicit_type_ids=explicit_type_ids,
        type_id_file=type_id_file,
        ready_market_scan_types=ready_market_scan_types,
        all_published_market_types=all_published_market_types,
        limit=limit,
        orders_cache_profile=orders_cache_profile,
        order_type=order_type,
        cache_only=cache_only,
    )
    selected_scope_type_ids = {int(row["type_id"]) for row in selected_targets} if selected_targets else None
    coverage_snapshot = collect_market_coverage_snapshot(
        connection,
        region_id=region_id,
        selected_type_ids=selected_scope_type_ids,
    )
    selected_target_map = {
        int(row["type_id"]): str(row.get("selection_mode") or "explicit")
        for row in selected_targets
    }
    if selected_target_map:
        selected_type_ids = set(selected_target_map.keys())
    elif cache_only:
        selected_type_ids = set()
    else:
        selected_type_ids = None

    snapshot_refs = _load_successful_order_snapshot_refs(
        connection,
        region_id=region_id,
        type_ids=selected_type_ids,
    )
    order_books = _load_order_books(connection, snapshot_refs=snapshot_refs)
    history_series = _load_history_series(
        connection,
        region_id=region_id,
        type_ids=selected_type_ids,
    )
    candidate_type_ids = set(order_books.keys()) & set(history_series.keys())
    type_metadata = _load_type_metadata(connection, type_ids=candidate_type_ids)

    location_ids: set[int] = set()
    for order_book in order_books.values():
        for pair in order_book.get("same_location_pairs", []):
            location_ids.add(int(pair["location_id"]))
    location_names = _load_location_names(connection, location_ids=location_ids)

    feature_rows: list[dict[str, Any]] = []
    skipped_rows: list[dict[str, Any]] = []
    candidate_traces: list[dict[str, Any]] = []
    selected_candidates: list[dict[str, Any]] = []

    for type_id in sorted(candidate_type_ids):
        order_book = order_books[type_id]
        history_rows = history_series[type_id]
        metadata = type_metadata.get(type_id) or {}
        type_name = str(metadata.get("type_name") or f"type_id={type_id}")
        selection_mode = selected_target_map.get(type_id, "history-orders-intersection")

        if len(history_rows) < 30:
            skipped_rows.append(
                {
                    "type_id": type_id,
                    "type_name": type_name,
                    "reason": "history_days_lt_30",
                    "history_days": len(history_rows),
                }
            )
            continue

        latest_30 = history_rows[-30:]
        latest_7 = latest_30[-7:]
        if len(latest_7) < 7:
            skipped_rows.append(
                {
                    "type_id": type_id,
                    "type_name": type_name,
                    "reason": "history_days_lt_7",
                    "history_days": len(history_rows),
                }
            )
            continue

        jita_pair = _best_jita_pair(
            order_book=order_book,
            location_names=location_names,
            fee_config=effective_fee_config,
        )
        if jita_pair is None:
            skipped_rows.append(
                {
                    "type_id": type_id,
                    "type_name": type_name,
                    "reason": "no_jita_same_station_pair",
                    "snapshot_id": order_book["snapshot_id"],
                }
            )
            continue

        feature_row, candidate_trace = _build_candidate(
            type_id=type_id,
            type_name=type_name,
            category_id=metadata.get("category_id"),
            market_group=metadata.get("market_group"),
            region_id=region_id,
            selection_mode=selection_mode,
            order_book=order_book,
            jita_pair=jita_pair,
            latest_30=latest_30,
            latest_7=latest_7,
            capital_budget_isk=capital_budget_isk,
            competition_config=effective_competition_config,
            filter_config=effective_filter_config,
        )
        feature_rows.append(feature_row)

        derived = candidate_trace["derived"]
        rejection_reasons: list[str] = []
        if float(derived["screened_profit_margin"]) <= 0.0:
            rejection_reasons.append("net_profit_margin not positive after fees")
        if float(derived["flip_units_cv_30d"]) > effective_filter_config.max_flip_units_cv_30d:
            rejection_reasons.append("flip_units_cv_30d above threshold")
        if float(derived["flip_units_cv_7d"]) > effective_filter_config.max_flip_units_cv_7d:
            rejection_reasons.append("flip_units_cv_7d above threshold")
        if float(derived["daily_capacity_isk_30d"]) < effective_filter_config.min_daily_capacity_isk_30d:
            rejection_reasons.append("daily_capacity_isk_30d below threshold")
        if (
            effective_filter_config.max_unit_buy_cost_isk is not None
            and float(feature_row["buy_cost_per_unit"]) > float(effective_filter_config.max_unit_buy_cost_isk)
        ):
            rejection_reasons.append("buy_cost_per_unit above unit price ceiling")
        category_id = metadata.get("category_id")
        if effective_filter_config.exclude_blueprint_category and category_id == 9:
            rejection_reasons.append("blueprint category excluded")
        if effective_filter_config.exclude_skin_category and category_id == 91:
            rejection_reasons.append("skin category excluded")
        if float(derived["buy_price_vs_avg_30d"]) < effective_filter_config.min_buy_price_vs_avg_30d:
            rejection_reasons.append("buy_price_vs_avg_30d below sanity threshold")
        if float(derived["sell_price_vs_avg_30d"]) > effective_filter_config.max_sell_price_vs_avg_30d:
            rejection_reasons.append("sell_price_vs_avg_30d above sanity threshold")

        candidate_trace["filter_evaluation"] = {
            "rejection_reasons": rejection_reasons,
            "passed": len(rejection_reasons) == 0,
        }
        candidate_traces.append(candidate_trace)

        if not rejection_reasons:
            selected_candidates.append(_build_selected_candidate(candidate_trace))

    selected_candidates.sort(
        key=lambda row: (
            float(row["profit_margin"]),
            float(row["daily_capacity_isk_30d"]),
            float(row["risk_adjusted_return"]),
        ),
        reverse=True,
    )
    filter_reason_counts = _count_filter_reasons(candidate_traces)
    universe_summary = {
        "selection_scope_target_count": (
            len(selected_targets)
            if selected_targets
            else int(coverage_snapshot["target_counts"]["market_type_count"] or 0)
        ),
        "market_type_count": int(coverage_snapshot["target_counts"]["market_type_count"] or 0),
        "history_ready_count": int(coverage_snapshot["target_counts"]["history_ready_count"] or 0),
        "orders_ready_count": int(coverage_snapshot["target_counts"]["orders_ready_count"] or 0),
        "ready_count": int(coverage_snapshot["target_counts"]["ready_count"] or 0),
        "candidate_type_count": len(candidate_type_ids),
        "feature_row_count": len(feature_rows),
        "skipped_row_count": len(skipped_rows),
        "selected_candidate_count": len(selected_candidates),
    }

    finished_at = datetime.now(timezone.utc)
    run_id = create_run_id(prefix="jita-passive-simple")
    run_dir = ensure_run_dir(output_root, run_id)
    as_of_time_raw = None
    if feature_rows:
        as_of_time_raw = max(str(row["as_of_time"]) for row in feature_rows)

    analysis_run = {
        "run_id": run_id,
        "started_at": started_at,
        "finished_at": finished_at,
        "input_path": db_path.resolve(),
        "output_dir": str(run_dir.resolve()),
        "input_mode": "sqlite-jita-passive-simple-scan",
        "market_scope": f"region:{region_id}",
        "as_of_time": (
            datetime.fromisoformat(str(as_of_time_raw).replace("Z", "+00:00"))
            if as_of_time_raw
            else finished_at
        ),
        "scan_config": scan_config,
        "fee_config": effective_fee_config,
        "filter_config": effective_filter_config,
        "competition_config": effective_competition_config,
        "cache_selection": cache_selection,
        "universe_summary": universe_summary,
        "candidate_count_total": len(candidate_traces),
        "candidate_count_selected": len(selected_candidates),
        "top_n": top_n,
    }
    calc_trace = {
        "run_id": run_id,
        "source_mode": "sqlite-jita-passive-simple-scan",
        "formula_versions": {
            "net_profit_margin": "v1-passive-buy-best-bid-sell-best-ask-after-fees",
            "flip_units_daily": "v1-min-of-estimated-buy-fill-and-sell-fill",
            "flip_units_daily_side_attribution": "v1-average-price-position-and-high-low-reach",
            "daily_capacity_isk_30d": "v1-flip-units-mean-30d-times-volume-weighted-average-price-30d",
            "competition_capture": "v1-order-age-plus-top-queue-competition-proxy",
            "turnover_days": "v2-capital-required-over-recommended-deployable-isk",
        },
        "fee_config": effective_fee_config,
        "filter_config": effective_filter_config,
        "competition_config": effective_competition_config,
        "scan_config": scan_config,
        "cache_selection": cache_selection,
        "universe_summary": universe_summary,
        "filter_reason_counts": filter_reason_counts,
        "candidate_traces": candidate_traces,
        "skipped_rows": skipped_rows,
    }

    preview_rows = selected_candidates[:top_n]
    summary_lines = [
        "# Jita Passive Simple Scan",
        "",
        f"- Run ID: `{run_id}`",
        f"- Region ID: `{region_id}`",
        f"- Selection Scope Target Count: `{universe_summary['selection_scope_target_count']}`",
        f"- History Ready Count: `{universe_summary['history_ready_count']}`",
        f"- Orders Ready Count: `{universe_summary['orders_ready_count']}`",
        f"- Ready Count: `{universe_summary['ready_count']}`",
        f"- Candidates Evaluated: `{len(candidate_traces)}`",
        f"- Candidates Selected: `{len(selected_candidates)}`",
        f"- Profit Margin Sort: `descending`",
        f"- 30d CV Ceiling: `{effective_filter_config.max_flip_units_cv_30d:.2%}`",
        f"- 7d CV Ceiling: `{effective_filter_config.max_flip_units_cv_7d:.2%}`",
        f"- Minimum 30d Daily Capacity: `{effective_filter_config.min_daily_capacity_isk_30d:.2f} ISK`",
        (
            f"- Maximum Unit Buy Cost: `{float(effective_filter_config.max_unit_buy_cost_isk):.2f} ISK`"
            if effective_filter_config.max_unit_buy_cost_isk is not None
            else "- Maximum Unit Buy Cost: `disabled`"
        ),
        f"- Minimum Buy Price vs 30d Avg: `{effective_filter_config.min_buy_price_vs_avg_30d:.2f}`",
        f"- Maximum Sell Price vs 30d Avg: `{effective_filter_config.max_sell_price_vs_avg_30d:.2f}`",
        f"- Sell Overhang Step: `{effective_filter_config.sell_price_overhang_step_pct:.2%}`",
        f"- Sell Overhang Penalty Factor: `{effective_filter_config.sell_price_overhang_penalty_factor:.2f}`",
        f"- Operator Refresh Interval: `{effective_competition_config.operator_refresh_interval_hours:.2f} hours`",
    ]
    if cache_selection is not None:
        summary_lines.extend(
            [
                f"- Cache Profile: `{cache_selection['profile_name']}`",
                f"- Requested Scope Targets: `{cache_selection['requested_scope_target_count']}`",
                f"- Cache Hit Targets: `{cache_selection['cache_hit_count']}`",
                f"- Selected Cache Targets: `{cache_selection['selected_target_count']}`",
            ]
        )
    if filter_reason_counts:
        summary_lines.extend(["", "## Rejection Counts"])
        for row in filter_reason_counts[:10]:
            summary_lines.append(f"- `{row['reason']}`: `{row['count']}`")
    summary_lines.extend(["", "## Top Candidates"])
    if not preview_rows:
        summary_lines.extend(["", "No candidates passed the current hard filters."])
    else:
        for index, row in enumerate(preview_rows, start=1):
            summary_lines.extend(
                [
                    "",
                    f"{index}. `{row['type_name']}` (`type_id={row['type_id']}`)",
                    f"   profit_margin: `{row['profit_margin']:.4%}`",
                    f"   theoretical_market_capacity_isk: `{row['theoretical_market_capacity_isk']:.2f}`",
                    f"   recommended_deployable_isk: `{row['recommended_deployable_isk']:.2f}`",
                    f"   estimated_capture_share: `{row['estimated_capture_share']:.2%}`",
                    f"   sell_fill_penalty_multiplier: `{row['sell_fill_penalty_multiplier']:.4f}`",
                    f"   flip_units_cv_30d: `{row['flip_units_cv_30d']:.2%}`",
                    f"   flip_units_cv_7d: `{row['flip_units_cv_7d']:.2%}`",
                    f"   expected_turnover_days: `{row['expected_turnover_days']:.2f}`",
                ]
            )

    write_json(run_dir / "analysis_run.json", analysis_run)
    write_json(run_dir / "calc_trace.json", calc_trace)
    write_json(run_dir / "candidate_table.json", selected_candidates)
    write_json(run_dir / "feature_table.json", feature_rows)
    write_json(run_dir / "skipped_table.json", skipped_rows)
    write_summary(run_dir / "summary.md", summary_lines)
    persist_scan_run(connection, run_id, analysis_run, selected_candidates, run_dir.resolve())

    return {
        "run_id": run_id,
        "run_dir": run_dir,
        "analysis_run": analysis_run,
        "calc_trace": calc_trace,
        "feature_rows": feature_rows,
        "skipped_rows": skipped_rows,
        "selected_candidates": selected_candidates,
        "selected_targets": selected_targets,
        "cache_selection": cache_selection,
        "coverage_snapshot": coverage_snapshot,
        "universe_summary": universe_summary,
        "filter_reason_counts": filter_reason_counts,
    }
