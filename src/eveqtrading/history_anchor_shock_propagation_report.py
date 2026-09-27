from __future__ import annotations

import csv
import sqlite3
import statistics
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .history_anchor_relationship_report import _format_history_source, _load_item_meta_map, _load_target_rows
from .history_pair_relationship_report import (
    _align_series,
    _format_number,
    _format_percent,
    _load_region_name,
    _load_type_name,
    _pearson,
    _render_table,
    _safe_div,
)
from .history_pair_shock_report import _rolling_return_zscore, _window_return
from .market_scope import describe_history_source, load_canonical_history_series, load_canonical_history_series_map
from .run_artifacts import create_run_id, ensure_run_dir, write_json, write_summary

PRACTICAL_EXCLUDE_NAME_KEYWORDS = [
    "涂装",
    "限量",
    "蓝图",
    "卫衣",
    "长裤",
    "外套",
    "帽子",
    "面部改装",
    "图案",
    "铭牌",
    "DNA",
]
PRACTICAL_EXCLUDE_GROUP_KEYWORDS = [
    "装饰品和杂物",
    "图案",
    "外套",
    "下装",
    "头饰",
    "身体改装",
]


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _format_isk_human(amount: Any) -> str:
    try:
        numeric = float(amount or 0.0)
    except (TypeError, ValueError):
        return "-"
    abs_numeric = abs(numeric)
    if abs_numeric >= 1_000_000_000_000:
        return f"{numeric / 1_000_000_000_000:.2f}万亿"
    if abs_numeric >= 100_000_000:
        return f"{numeric / 100_000_000:.2f}亿"
    if abs_numeric >= 10_000:
        return f"{numeric / 10_000:.2f}万"
    return f"{numeric:.2f}"


def _dedupe_preserve_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        normalized = str(value).strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        ordered.append(normalized)
    return ordered


def _resolve_filter_settings(
    *,
    filter_profile: str,
    min_median_daily_turnover_isk: float | None,
    min_latest_daily_turnover_isk: float | None,
    min_median_daily_volume: float | None,
    min_event_sample_count: int | None,
    exclude_name_keywords: list[str] | None,
    exclude_group_keywords: list[str] | None,
) -> dict[str, Any]:
    if filter_profile == "practical":
        resolved = {
            "filter_profile": "practical",
            "min_median_daily_turnover_isk": 100_000_000.0,
            "min_latest_daily_turnover_isk": 50_000_000.0,
            "min_median_daily_volume": 1.0,
            "min_event_sample_count": 2,
            "exclude_name_keywords": list(PRACTICAL_EXCLUDE_NAME_KEYWORDS),
            "exclude_group_keywords": list(PRACTICAL_EXCLUDE_GROUP_KEYWORDS),
        }
    else:
        resolved = {
            "filter_profile": "off",
            "min_median_daily_turnover_isk": None,
            "min_latest_daily_turnover_isk": None,
            "min_median_daily_volume": None,
            "min_event_sample_count": 1,
            "exclude_name_keywords": [],
            "exclude_group_keywords": [],
        }
    if min_median_daily_turnover_isk is not None:
        resolved["min_median_daily_turnover_isk"] = float(min_median_daily_turnover_isk)
    if min_latest_daily_turnover_isk is not None:
        resolved["min_latest_daily_turnover_isk"] = float(min_latest_daily_turnover_isk)
    if min_median_daily_volume is not None:
        resolved["min_median_daily_volume"] = float(min_median_daily_volume)
    if min_event_sample_count is not None:
        resolved["min_event_sample_count"] = int(min_event_sample_count)
    resolved["exclude_name_keywords"] = _dedupe_preserve_order(
        list(resolved["exclude_name_keywords"]) + list(exclude_name_keywords or [])
    )
    resolved["exclude_group_keywords"] = _dedupe_preserve_order(
        list(resolved["exclude_group_keywords"]) + list(exclude_group_keywords or [])
    )
    return resolved


def _series_liquidity_stats(series: list[dict[str, Any]]) -> dict[str, Any]:
    turnovers = [float(row["average_price"]) * float(row["volume"]) for row in series]
    volumes = [float(row["volume"]) for row in series]
    return {
        "history_day_count": len(series),
        "median_daily_turnover_isk": statistics.median(turnovers) if turnovers else 0.0,
        "mean_daily_turnover_isk": statistics.fmean(turnovers) if turnovers else 0.0,
        "latest_daily_turnover_isk": turnovers[-1] if turnovers else 0.0,
        "median_daily_volume": statistics.median(volumes) if volumes else 0.0,
        "latest_daily_volume": volumes[-1] if volumes else 0.0,
    }


def _contains_any_keyword(text: str, keywords: list[str]) -> bool:
    normalized_text = str(text or "")
    return any(keyword in normalized_text for keyword in keywords)


def _build_anchor_rows(anchor_series: list[dict[str, Any]]) -> list[dict[str, Any]]:
    prices = [float(row["average_price"]) for row in anchor_series]
    rows: list[dict[str, Any]] = []
    previous_price: float | None = None
    for index, row in enumerate(anchor_series):
        payload = {
            "market_date": str(row["market_date"]),
            "price": float(row["average_price"]),
            "volume": float(row["volume"]),
            "fetched_at": str(row["fetched_at"]),
            "return": _safe_div(float(row["average_price"]), previous_price) - 1.0 if previous_price else None,
        }
        payload["return_3d"] = _window_return(prices, index, 3)
        payload["return_zscore_90d"] = _rolling_return_zscore(
            [
                {"return": rows_item.get("return")}
                for rows_item in rows
            ] + [{"return": payload["return"]}],
            index,
            field_name="return",
            window=90,
        ) if payload["return"] is not None else None
        rows.append(payload)
        previous_price = float(row["average_price"])
    return rows


def _select_anchor_events(
    anchor_rows: list[dict[str, Any]],
    *,
    direction: str,
    daily_shock_threshold: float,
    cumulative_3d_threshold: float,
    return_zscore_threshold: float,
    cooldown_days: int,
) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    last_selected_index: int | None = None
    for index, row in enumerate(anchor_rows):
        daily_return = row.get("return")
        if daily_return is None:
            continue
        return_3d = row.get("return_3d")
        zscore_90d = row.get("return_zscore_90d")
        signed_return = float(daily_return)
        if direction == "up":
            direction_ok = signed_return >= daily_shock_threshold
            cumulative_ok = return_3d is not None and float(return_3d) >= cumulative_3d_threshold
            zscore_ok = zscore_90d is not None and float(zscore_90d) >= return_zscore_threshold
        else:
            direction_ok = signed_return <= -daily_shock_threshold
            cumulative_ok = return_3d is not None and float(return_3d) <= -cumulative_3d_threshold
            zscore_ok = zscore_90d is not None and float(zscore_90d) <= -return_zscore_threshold
        if not direction_ok:
            continue
        if last_selected_index is not None and index - last_selected_index <= cooldown_days:
            continue
        selected.append(
            {
                "direction": direction,
                "market_date": row["market_date"],
                "anchor_return": daily_return,
                "anchor_return_3d": return_3d,
                "anchor_return_zscore_90d": zscore_90d,
                "meets_cumulative_filter": cumulative_ok,
                "meets_zscore_filter": zscore_ok,
            }
        )
        last_selected_index = index
    return selected


def _compute_cumulative_return(prices: list[float], start_index: int, horizon: int) -> float | None:
    end_index = start_index + horizon
    if start_index < 0 or end_index >= len(prices):
        return None
    entry_price = float(prices[start_index])
    exit_price = float(prices[end_index])
    if entry_price <= 0:
        return None
    return (exit_price / entry_price) - 1.0


def _event_horizon_metrics(
    *,
    aligned_rows: list[dict[str, Any]],
    events: list[dict[str, Any]],
    direction: str,
    horizons: list[int],
) -> list[dict[str, Any]]:
    prices = [float(row["right_price"]) for row in aligned_rows]
    event_index_map = {
        int(index): event
        for index, row in enumerate(aligned_rows)
        for event in events
        if str(row["market_date"]) == str(event["market_date"])
    }
    event_indexes = set(event_index_map)
    metrics: list[dict[str, Any]] = []
    for horizon in horizons:
        event_returns = [
            _compute_cumulative_return(prices, index, horizon)
            for index in sorted(event_indexes)
        ]
        event_returns = [float(value) for value in event_returns if value is not None]
        baseline_returns = [
            float(value)
            for index in range(len(prices))
            if index not in event_indexes
            for value in [_compute_cumulative_return(prices, index, horizon)]
            if value is not None
        ]
        if not event_returns:
            metrics.append(
                {
                    "horizon_days": horizon,
                    "event_sample_count": 0,
                    "baseline_sample_count": len(baseline_returns),
                    "event_mean_return": None,
                    "baseline_mean_return": statistics.fmean(baseline_returns) if baseline_returns else None,
                    "baseline_stddev_return": statistics.pstdev(baseline_returns) if len(baseline_returns) > 1 else 0.0,
                    "abnormal_mean_return": None,
                    "abnormal_zscore": None,
                    "event_hit_rate": None,
                    "baseline_hit_rate": None,
                }
            )
            continue
        event_mean = statistics.fmean(event_returns)
        baseline_mean = statistics.fmean(baseline_returns) if baseline_returns else 0.0
        baseline_stddev = statistics.pstdev(baseline_returns) if len(baseline_returns) > 1 else 0.0
        if direction == "up":
            event_hits = sum(1 for value in event_returns if value > 0.0)
            baseline_hits = sum(1 for value in baseline_returns if value > 0.0)
        else:
            event_hits = sum(1 for value in event_returns if value < 0.0)
            baseline_hits = sum(1 for value in baseline_returns if value < 0.0)
        metrics.append(
            {
                "horizon_days": horizon,
                "event_sample_count": len(event_returns),
                "baseline_sample_count": len(baseline_returns),
                "event_mean_return": event_mean,
                "baseline_mean_return": baseline_mean if baseline_returns else None,
                "baseline_stddev_return": baseline_stddev,
                "abnormal_mean_return": event_mean - baseline_mean if baseline_returns else None,
                "abnormal_zscore": _safe_div(event_mean - baseline_mean, baseline_stddev) if baseline_returns and baseline_stddev > 0 else None,
                "event_hit_rate": _safe_div(event_hits, len(event_returns)),
                "baseline_hit_rate": _safe_div(baseline_hits, len(baseline_returns)) if baseline_returns else None,
            }
        )
    return metrics


def _choose_best_horizon(
    metrics: list[dict[str, Any]],
    *,
    direction: str,
    min_event_sample_count: int,
) -> dict[str, Any] | None:
    candidates = [
        row
        for row in metrics
        if row.get("abnormal_mean_return") is not None
        and row.get("abnormal_zscore") is not None
        and int(row.get("event_sample_count") or 0) >= int(min_event_sample_count)
    ]
    if not candidates:
        return None
    if direction == "up":
        return max(
            candidates,
            key=lambda row: (
                float(row["abnormal_zscore"]),
                float(row["abnormal_mean_return"]),
                float(row["event_hit_rate"] or 0.0),
                int(row["event_sample_count"]),
                -int(row["horizon_days"]),
            ),
        )
    return min(
        candidates,
        key=lambda row: (
            float(row["abnormal_zscore"]),
            float(row["abnormal_mean_return"]),
            -float(row["event_hit_rate"] or 0.0),
            -int(row["event_sample_count"]),
            int(row["horizon_days"]),
        ),
    )


def _summarize_target_propagation(
    *,
    region_id: int,
    anchor_type_id: int,
    anchor_label: str,
    anchor_source: dict[str, Any],
    target_meta: dict[str, Any],
    anchor_series: list[dict[str, Any]],
    target_series: list[dict[str, Any]],
    up_events: list[dict[str, Any]],
    down_events: list[dict[str, Any]],
    horizons: list[int],
    min_overlap_days: int,
    min_event_sample_count: int,
    liquidity_stats: dict[str, Any],
) -> dict[str, Any] | None:
    aligned_rows = _align_series(anchor_series, target_series)
    if len(aligned_rows) < min_overlap_days:
        return None

    up_metrics = _event_horizon_metrics(aligned_rows=aligned_rows, events=up_events, direction="up", horizons=horizons)
    down_metrics = _event_horizon_metrics(aligned_rows=aligned_rows, events=down_events, direction="down", horizons=horizons)
    up_best = _choose_best_horizon(up_metrics, direction="up", min_event_sample_count=min_event_sample_count)
    down_best = _choose_best_horizon(down_metrics, direction="down", min_event_sample_count=min_event_sample_count)
    target_returns = [float(row["right_return"]) for row in aligned_rows if row.get("right_return") is not None]
    anchor_returns = [float(row["left_return"]) for row in aligned_rows if row.get("left_return") is not None]

    return {
        "anchor_type_id": int(anchor_type_id),
        "anchor_label": str(anchor_label),
        "anchor_history_source": anchor_source,
        "target_type_id": int(target_meta["type_id"]),
        "target_label": str(target_meta["type_name"]),
        "target_market_group": str(target_meta["market_group"]),
        "target_market_group_id": target_meta.get("market_group_id"),
        "target_history_source": describe_history_source(region_id=region_id, type_id=int(target_meta["type_id"])),
        "overlap_day_count": len(aligned_rows),
        "overlap_market_date_min": aligned_rows[0]["market_date"],
        "overlap_market_date_max": aligned_rows[-1]["market_date"],
        "anchor_latest_fetched_at": max(str(row["fetched_at"]) for row in anchor_series),
        "target_latest_fetched_at": max(str(row["fetched_at"]) for row in target_series),
        "latest_anchor_price": float(aligned_rows[-1]["left_price"]),
        "latest_target_price": float(aligned_rows[-1]["right_price"]),
        "latest_target_volume": float(aligned_rows[-1]["right_volume"]),
        "latest_target_turnover_isk": float(aligned_rows[-1]["right_turnover_isk"]),
        "median_daily_turnover_isk": float(liquidity_stats["median_daily_turnover_isk"]),
        "latest_daily_turnover_isk": float(liquidity_stats["latest_daily_turnover_isk"]),
        "median_daily_volume": float(liquidity_stats["median_daily_volume"]),
        "same_day_return_corr": _pearson(anchor_returns, target_returns),
        "up_shock_metrics": up_metrics,
        "down_shock_metrics": down_metrics,
        "up_best_horizon": up_best,
        "down_best_horizon": down_best,
    }


def _rank_response_rows(
    rows: list[dict[str, Any]],
    *,
    direction: str,
    top_n: int,
) -> list[dict[str, Any]]:
    key_name = "up_best_horizon" if direction == "up" else "down_best_horizon"
    candidates = [row for row in rows if row.get(key_name) is not None]
    if direction == "up":
        ranked = sorted(
            candidates,
            key=lambda row: (
                float(row[key_name]["abnormal_zscore"] or 0.0),
                float(row[key_name]["abnormal_mean_return"]),
                float(row[key_name]["event_hit_rate"] or 0.0),
                int(row[key_name]["event_sample_count"]),
                str(row["target_label"]),
            ),
            reverse=True,
        )
    else:
        ranked = sorted(
            candidates,
            key=lambda row: (
                float(row[key_name]["abnormal_zscore"] or 0.0),
                float(row[key_name]["abnormal_mean_return"]),
                -float(row[key_name]["event_hit_rate"] or 0.0),
                -int(row[key_name]["event_sample_count"]),
                str(row["target_label"]),
            ),
        )
    return ranked[:top_n]


def _screen_table_rows(rows: list[dict[str, Any]], *, direction: str) -> list[list[str]]:
    rendered: list[list[str]] = []
    best_key = "up_best_horizon" if direction == "up" else "down_best_horizon"
    for index, row in enumerate(rows, start=1):
        best = row[best_key]
        rendered.append(
            [
                str(index),
                str(row["target_label"]),
                str(row["target_market_group"]),
                _format_isk_human(row["median_daily_turnover_isk"]),
                _format_isk_human(row["latest_daily_turnover_isk"]),
                f"T+{int(best['horizon_days'])}",
                str(int(best["event_sample_count"])),
                _format_percent(best["event_mean_return"]),
                _format_percent(best["baseline_mean_return"]),
                _format_percent(best["abnormal_mean_return"]),
                _format_number(best["abnormal_zscore"], 2),
                _format_percent(best["event_hit_rate"]),
                _format_percent(best["baseline_hit_rate"]),
                _format_number(row["same_day_return_corr"], 4),
                str(row["overlap_market_date_max"]),
            ]
        )
    return rendered


def _write_screen_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fieldnames = [
        "target_type_id",
        "target_label",
        "target_market_group",
        "overlap_day_count",
        "median_daily_turnover_isk",
        "latest_daily_turnover_isk",
        "median_daily_volume",
        "same_day_return_corr",
        "up_best_horizon_days",
        "up_best_event_sample_count",
        "up_best_event_mean_return",
        "up_best_baseline_mean_return",
        "up_best_abnormal_mean_return",
        "up_best_abnormal_zscore",
        "up_best_event_hit_rate",
        "up_best_baseline_hit_rate",
        "down_best_horizon_days",
        "down_best_event_sample_count",
        "down_best_event_mean_return",
        "down_best_baseline_mean_return",
        "down_best_abnormal_mean_return",
        "down_best_abnormal_zscore",
        "down_best_event_hit_rate",
        "down_best_baseline_hit_rate",
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            up_best = row.get("up_best_horizon") or {}
            down_best = row.get("down_best_horizon") or {}
            writer.writerow(
                {
                    "target_type_id": row.get("target_type_id"),
                    "target_label": row.get("target_label"),
                    "target_market_group": row.get("target_market_group"),
                    "overlap_day_count": row.get("overlap_day_count"),
                    "median_daily_turnover_isk": row.get("median_daily_turnover_isk"),
                    "latest_daily_turnover_isk": row.get("latest_daily_turnover_isk"),
                    "median_daily_volume": row.get("median_daily_volume"),
                    "same_day_return_corr": row.get("same_day_return_corr"),
                    "up_best_horizon_days": up_best.get("horizon_days"),
                    "up_best_event_sample_count": up_best.get("event_sample_count"),
                    "up_best_event_mean_return": up_best.get("event_mean_return"),
                    "up_best_baseline_mean_return": up_best.get("baseline_mean_return"),
                    "up_best_abnormal_mean_return": up_best.get("abnormal_mean_return"),
                    "up_best_abnormal_zscore": up_best.get("abnormal_zscore"),
                    "up_best_event_hit_rate": up_best.get("event_hit_rate"),
                    "up_best_baseline_hit_rate": up_best.get("baseline_hit_rate"),
                    "down_best_horizon_days": down_best.get("horizon_days"),
                    "down_best_event_sample_count": down_best.get("event_sample_count"),
                    "down_best_event_mean_return": down_best.get("event_mean_return"),
                    "down_best_baseline_mean_return": down_best.get("baseline_mean_return"),
                    "down_best_abnormal_mean_return": down_best.get("abnormal_mean_return"),
                    "down_best_abnormal_zscore": down_best.get("abnormal_zscore"),
                    "down_best_event_hit_rate": down_best.get("event_hit_rate"),
                    "down_best_baseline_hit_rate": down_best.get("baseline_hit_rate"),
                }
            )


def build_history_anchor_shock_propagation_report(
    connection: sqlite3.Connection,
    *,
    output_root: Path,
    region_id: int,
    anchor_type_id: int,
    anchor_label: str | None = None,
    selected_targets: list[dict[str, Any]] | None = None,
    min_overlap_days: int = 180,
    top_n: int = 20,
    daily_shock_threshold: float = 0.05,
    cumulative_3d_threshold: float = 0.08,
    return_zscore_threshold: float = 2.0,
    cooldown_days: int = 7,
    horizons: list[int] | None = None,
    filter_profile: str = "off",
    min_median_daily_turnover_isk: float | None = None,
    min_latest_daily_turnover_isk: float | None = None,
    min_median_daily_volume: float | None = None,
    min_event_sample_count: int | None = None,
    exclude_name_keywords: list[str] | None = None,
    exclude_group_keywords: list[str] | None = None,
) -> dict[str, Any]:
    resolved_horizons = list(horizons or [1, 3, 7, 14])
    filter_settings = _resolve_filter_settings(
        filter_profile=filter_profile,
        min_median_daily_turnover_isk=min_median_daily_turnover_isk,
        min_latest_daily_turnover_isk=min_latest_daily_turnover_isk,
        min_median_daily_volume=min_median_daily_volume,
        min_event_sample_count=min_event_sample_count,
        exclude_name_keywords=exclude_name_keywords,
        exclude_group_keywords=exclude_group_keywords,
    )
    anchor_name = anchor_label or _load_type_name(connection, anchor_type_id)
    anchor_series = load_canonical_history_series(connection, region_id=region_id, type_id=anchor_type_id)
    if len(anchor_series) < 60:
        raise RuntimeError(f"锚点物品 {anchor_name} ({anchor_type_id}) history 不足 60 天。")
    anchor_source = describe_history_source(region_id=region_id, type_id=anchor_type_id)
    anchor_rows = _build_anchor_rows(anchor_series)
    up_events = _select_anchor_events(
        anchor_rows,
        direction="up",
        daily_shock_threshold=daily_shock_threshold,
        cumulative_3d_threshold=cumulative_3d_threshold,
        return_zscore_threshold=return_zscore_threshold,
        cooldown_days=cooldown_days,
    )
    down_events = _select_anchor_events(
        anchor_rows,
        direction="down",
        daily_shock_threshold=daily_shock_threshold,
        cumulative_3d_threshold=cumulative_3d_threshold,
        return_zscore_threshold=return_zscore_threshold,
        cooldown_days=cooldown_days,
    )

    target_rows = _load_target_rows(connection, anchor_type_id=anchor_type_id, selected_targets=selected_targets)
    target_ids = {int(row["type_id"]) for row in target_rows}
    target_meta_map = _load_item_meta_map(connection, type_ids=target_ids)
    target_series_map = load_canonical_history_series_map(connection, region_id=region_id, type_ids=target_ids)

    response_rows: list[dict[str, Any]] = []
    skipped_rows: list[dict[str, Any]] = []
    for target_row in target_rows:
        target_type_id = int(target_row["type_id"])
        target_meta = target_meta_map.get(
            target_type_id,
            {
                "type_id": target_type_id,
                "type_name": target_row["type_name"],
                "market_group": target_row.get("market_group") or "未分类",
                "market_group_id": target_row.get("market_group_id"),
            },
        )
        if _contains_any_keyword(str(target_meta["type_name"]), list(filter_settings["exclude_name_keywords"])):
            skipped_rows.append({"type_id": target_type_id, "type_name": target_meta["type_name"], "reason": "excluded_by_name_keyword"})
            continue
        if _contains_any_keyword(str(target_meta["market_group"]), list(filter_settings["exclude_group_keywords"])):
            skipped_rows.append({"type_id": target_type_id, "type_name": target_meta["type_name"], "reason": "excluded_by_group_keyword"})
            continue
        target_series = target_series_map.get(target_type_id, [])
        if not target_series:
            skipped_rows.append({"type_id": target_type_id, "type_name": target_row["type_name"], "reason": "missing_history"})
            continue
        liquidity_stats = _series_liquidity_stats(target_series)
        if (
            filter_settings["min_median_daily_turnover_isk"] is not None
            and float(liquidity_stats["median_daily_turnover_isk"]) < float(filter_settings["min_median_daily_turnover_isk"])
        ):
            skipped_rows.append({"type_id": target_type_id, "type_name": target_meta["type_name"], "reason": "below_min_median_daily_turnover"})
            continue
        if (
            filter_settings["min_latest_daily_turnover_isk"] is not None
            and float(liquidity_stats["latest_daily_turnover_isk"]) < float(filter_settings["min_latest_daily_turnover_isk"])
        ):
            skipped_rows.append({"type_id": target_type_id, "type_name": target_meta["type_name"], "reason": "below_min_latest_daily_turnover"})
            continue
        if (
            filter_settings["min_median_daily_volume"] is not None
            and float(liquidity_stats["median_daily_volume"]) < float(filter_settings["min_median_daily_volume"])
        ):
            skipped_rows.append({"type_id": target_type_id, "type_name": target_meta["type_name"], "reason": "below_min_median_daily_volume"})
            continue
        summary = _summarize_target_propagation(
            region_id=region_id,
            anchor_type_id=anchor_type_id,
            anchor_label=anchor_name,
            anchor_source=anchor_source,
            target_meta=target_meta,
            anchor_series=anchor_series,
            target_series=target_series,
            up_events=up_events,
            down_events=down_events,
            horizons=resolved_horizons,
            min_overlap_days=min_overlap_days,
            min_event_sample_count=int(filter_settings["min_event_sample_count"]),
            liquidity_stats=liquidity_stats,
        )
        if summary is None:
            skipped_rows.append({"type_id": target_type_id, "type_name": target_row["type_name"], "reason": "overlap_below_threshold"})
            continue
        if summary.get("up_best_horizon") is None and summary.get("down_best_horizon") is None:
            skipped_rows.append({"type_id": target_type_id, "type_name": target_meta["type_name"], "reason": "no_usable_event_windows"})
            continue
        response_rows.append(summary)

    if not response_rows:
        raise RuntimeError("没有满足最小重叠天数要求的目标物品，无法生成锚点冲击传导报告。")

    up_rankings = _rank_response_rows(response_rows, direction="up", top_n=top_n)
    down_rankings = _rank_response_rows(response_rows, direction="down", top_n=top_n)

    run_id = create_run_id("history-anchor-shock")
    run_dir = ensure_run_dir(output_root, run_id)
    markdown_path = run_dir / "history_anchor_shock_propagation_report.md"
    json_path = run_dir / "history_anchor_shock_propagation_report.json"
    csv_path = run_dir / "history_anchor_shock_propagation_screen.csv"

    report = {
        "report_generated_at_utc": _utc_now_iso(),
        "report_run_id": run_id,
        "report_run_dir": str(run_dir),
        "region_id": int(region_id),
        "region_name": _load_region_name(connection, region_id),
        "anchor_type_id": int(anchor_type_id),
        "anchor_label": str(anchor_name),
        "anchor_history_source": anchor_source,
        "event_definition": {
            "daily_shock_threshold": float(daily_shock_threshold),
            "cumulative_3d_threshold": float(cumulative_3d_threshold),
            "return_zscore_threshold": float(return_zscore_threshold),
            "cooldown_days": int(cooldown_days),
            "horizons": resolved_horizons,
        },
        "filters": filter_settings,
        "universe_summary": {
            "selected_target_count": len(target_rows),
            "qualified_target_count": len(response_rows),
            "skipped_target_count": len(skipped_rows),
            "min_overlap_days": int(min_overlap_days),
            "top_n": int(top_n),
            "anchor_history_day_count": len(anchor_series),
            "anchor_market_date_min": anchor_series[0]["market_date"],
            "anchor_market_date_max": anchor_series[-1]["market_date"],
            "anchor_latest_fetched_at": max(str(row["fetched_at"]) for row in anchor_series),
            "up_event_count": len(up_events),
            "down_event_count": len(down_events),
        },
        "up_events": up_events,
        "down_events": down_events,
        "up_rankings": up_rankings,
        "down_rankings": down_rankings,
        "response_rows": response_rows,
        "skipped_rows_sample": skipped_rows[:50],
        "files": {
            "markdown_path": str(markdown_path),
            "json_path": str(json_path),
            "csv_path": str(csv_path),
        },
    }

    lines: list[str] = [
        f"# {anchor_name} 冲击传导筛查报告",
        "",
        f"- 星域: {report['region_name']} ({region_id})",
        f"- 锚点物品: {anchor_name} ({anchor_type_id})",
        f"- 锚点 history 口径: {_format_history_source(anchor_source)}",
        f"- 锚点样本区间: {report['universe_summary']['anchor_market_date_min']} 到 {report['universe_summary']['anchor_market_date_max']}",
        f"- 锚点 history 天数: {report['universe_summary']['anchor_history_day_count']}",
        f"- 锚点最新抓取: {report['universe_summary']['anchor_latest_fetched_at']}",
        f"- 上涨冲击事件数: {report['universe_summary']['up_event_count']}",
        f"- 下跌冲击事件数: {report['universe_summary']['down_event_count']}",
        f"- 扫描目标数: {report['universe_summary']['selected_target_count']}",
        f"- 合格目标数: {report['universe_summary']['qualified_target_count']}",
        f"- 跳过目标数: {report['universe_summary']['skipped_target_count']}",
        "",
        "## 过滤设置",
        "",
        f"- 过滤预设: {report['filters']['filter_profile']}",
        f"- 最低中位日成交额: {_format_isk_human(report['filters']['min_median_daily_turnover_isk']) if report['filters']['min_median_daily_turnover_isk'] is not None else '-'}",
        f"- 最低最新日成交额: {_format_isk_human(report['filters']['min_latest_daily_turnover_isk']) if report['filters']['min_latest_daily_turnover_isk'] is not None else '-'}",
        f"- 最低中位日成交量: {report['filters']['min_median_daily_volume'] if report['filters']['min_median_daily_volume'] is not None else '-'}",
        f"- 最低事件样本数: {report['filters']['min_event_sample_count']}",
        f"- 排除商品关键词: {', '.join(report['filters']['exclude_name_keywords']) if report['filters']['exclude_name_keywords'] else '-'}",
        f"- 排除市场组关键词: {', '.join(report['filters']['exclude_group_keywords']) if report['filters']['exclude_group_keywords'] else '-'}",
        "",
        "## 说明",
        "",
        "- 这份报告不是严格的因果证明，而是“冲击后响应筛查”。",
        "- 先用锚点物品的单日大涨/大跌事件定义冲击日，再看目标商品在 `T+1/T+3/T+7/T+14` 的累计收益。",
        "- `事件均值`：目标商品在冲击日后的平均累计收益。",
        "- `基线均值`：目标商品在非冲击日、同一窗口下的平均累计收益。",
        "- `超额反应` = `事件均值 - 基线均值`。上涨冲击里越大越像“被带动”；下跌冲击里越负越像“被带崩”。",
        "- `异常分数` = `超额反应 / 平时窗口收益标准差`。排序时优先看它，而不是只看超额收益绝对值。",
        "- `命中率`：上涨冲击里看窗口收益为正的比例；下跌冲击里看窗口收益为负的比例。",
        "",
        "## 锚点事件清单",
        "",
    ]
    anchor_event_rows = [
        [
            str(index),
            str(event["market_date"]),
            "上涨" if event["direction"] == "up" else "下跌",
            _format_percent(event["anchor_return"]),
            _format_percent(event["anchor_return_3d"]) if event.get("anchor_return_3d") is not None else "-",
            _format_number(event["anchor_return_zscore_90d"], 4) if event.get("anchor_return_zscore_90d") is not None else "-",
            "Y" if event["meets_cumulative_filter"] else "N",
            "Y" if event["meets_zscore_filter"] else "N",
        ]
        for index, event in enumerate([*up_events, *down_events], start=1)
    ]
    lines.extend(
        _render_table(
            ["序号", "日期", "方向", "锚点当日收益", "锚点3日累计", "锚点90日z", "过3日阈值", "过z阈值"],
            anchor_event_rows,
        )
    )
    lines.extend(["", "## PLEX 上涨后最容易被带动的商品 Top", ""])
    lines.extend(
        _render_table(
            ["排名", "商品", "市场组", "中位日成交额", "最新日成交额", "最佳窗口", "事件数", "事件均值", "基线均值", "超额反应", "异常分数", "命中率", "基线命中率", "同期收益率相关", "样本截止"],
            _screen_table_rows(up_rankings, direction="up"),
        )
    )
    lines.extend(["", "## PLEX 下跌后最容易被带崩的商品 Top", ""])
    lines.extend(
        _render_table(
            ["排名", "商品", "市场组", "中位日成交额", "最新日成交额", "最佳窗口", "事件数", "事件均值", "基线均值", "超额反应", "异常分数", "命中率", "基线命中率", "同期收益率相关", "样本截止"],
            _screen_table_rows(down_rankings, direction="down"),
        )
    )

    write_summary(markdown_path, lines)
    write_json(json_path, report)
    _write_screen_csv(csv_path, response_rows)
    return report
