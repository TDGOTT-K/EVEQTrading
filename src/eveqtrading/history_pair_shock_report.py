from __future__ import annotations

import csv
import statistics
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .history_pair_relationship_report import (
    _align_series,
    _describe,
    _format_isk_human,
    _format_number,
    _format_percent,
    _load_history_series,
    _load_region_name,
    _load_type_name,
    _pearson,
    _render_table,
    _safe_div,
)
from .run_artifacts import create_run_id, ensure_run_dir, write_json, write_summary


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _compute_cumulative_return(prices: list[float], start_index: int, horizon: int) -> float | None:
    end_index = start_index + horizon
    if start_index < 0 or end_index >= len(prices):
        return None
    entry = float(prices[start_index])
    exit_price = float(prices[end_index])
    return _safe_div(exit_price, entry) - 1.0 if entry > 0 else None


def _window_return(prices: list[float], end_index: int, lookback_days: int) -> float | None:
    start_index = end_index - lookback_days
    if start_index < 0 or end_index >= len(prices):
        return None
    entry = float(prices[start_index])
    exit_price = float(prices[end_index])
    return _safe_div(exit_price, entry) - 1.0 if entry > 0 else None


def _rolling_return_zscore(
    rows: list[dict[str, Any]],
    index: int,
    *,
    field_name: str,
    window: int,
) -> float | None:
    start_index = max(0, index - window)
    values = [
        float(row[field_name])
        for row in rows[start_index:index]
        if row.get(field_name) is not None
    ]
    if len(values) < 20:
        return None
    mean_value = statistics.fmean(values)
    stddev = statistics.pstdev(values) if len(values) > 1 else 0.0
    if stddev <= 0:
        return 0.0
    return (float(rows[index][field_name]) - mean_value) / stddev


def _enrich_rows(aligned_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    left_prices = [float(row["left_price"]) for row in aligned_rows]
    right_prices = [float(row["right_price"]) for row in aligned_rows]
    enriched: list[dict[str, Any]] = []
    for index, row in enumerate(aligned_rows):
        payload = dict(row)
        payload["left_return_3d"] = _window_return(left_prices, index, 3)
        payload["right_return_3d"] = _window_return(right_prices, index, 3)
        payload["left_return_zscore_90d"] = _rolling_return_zscore(aligned_rows, index, field_name="left_return", window=90)
        payload["right_return_zscore_90d"] = _rolling_return_zscore(aligned_rows, index, field_name="right_return", window=90)
        enriched.append(payload)
    return enriched


def _event_response(row_index: int, rows: list[dict[str, Any]], horizons: list[int]) -> dict[str, Any]:
    left_prices = [float(row["left_price"]) for row in rows]
    right_prices = [float(row["right_price"]) for row in rows]
    response: dict[str, Any] = {}
    for horizon in horizons:
        response[f"left_cum_return_t{horizon}"] = _compute_cumulative_return(left_prices, row_index, horizon)
        response[f"right_cum_return_t{horizon}"] = _compute_cumulative_return(right_prices, row_index, horizon)
    return response


def _select_events(
    rows: list[dict[str, Any]],
    *,
    direction: str,
    daily_shock_threshold: float,
    cumulative_3d_threshold: float,
    return_zscore_threshold: float,
    cooldown_days: int,
    horizons: list[int],
) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    last_selected_index: int | None = None
    for index, row in enumerate(rows):
        left_return = row.get("left_return")
        left_return_3d = row.get("left_return_3d")
        left_return_zscore_90d = row.get("left_return_zscore_90d")
        if left_return is None:
            continue
        signed_return = float(left_return)
        if direction == "up":
            direction_ok = signed_return >= daily_shock_threshold
            cumulative_ok = left_return_3d is not None and float(left_return_3d) >= cumulative_3d_threshold
            zscore_ok = left_return_zscore_90d is not None and float(left_return_zscore_90d) >= return_zscore_threshold
        else:
            direction_ok = signed_return <= -daily_shock_threshold
            cumulative_ok = left_return_3d is not None and float(left_return_3d) <= -cumulative_3d_threshold
            zscore_ok = left_return_zscore_90d is not None and float(left_return_zscore_90d) <= -return_zscore_threshold
        if not direction_ok:
            continue
        if last_selected_index is not None and index - last_selected_index <= cooldown_days:
            continue
        event = {
            "direction": direction,
            "market_date": row["market_date"],
            "row_index": index,
            "left_return": left_return,
            "right_return": row.get("right_return"),
            "left_return_3d": left_return_3d,
            "right_return_3d": row.get("right_return_3d"),
            "left_return_zscore_90d": left_return_zscore_90d,
            "right_return_zscore_90d": row.get("right_return_zscore_90d"),
            "meets_cumulative_filter": cumulative_ok,
            "meets_zscore_filter": zscore_ok,
        }
        event.update(_event_response(index, rows, horizons))
        selected.append(event)
        last_selected_index = index
    return selected


def _aggregate_event_window(events: list[dict[str, Any]], horizons: list[int]) -> list[dict[str, Any]]:
    summary_rows: list[dict[str, Any]] = []
    for horizon in horizons:
        left_values = [float(event[f"left_cum_return_t{horizon}"]) for event in events if event.get(f"left_cum_return_t{horizon}") is not None]
        right_values = [float(event[f"right_cum_return_t{horizon}"]) for event in events if event.get(f"right_cum_return_t{horizon}") is not None]
        if not left_values or not right_values:
            summary_rows.append(
                {
                    "horizon_days": horizon,
                    "sample_count": 0,
                    "left_mean": None,
                    "right_mean": None,
                    "spread_mean": None,
                    "corr": None,
                }
            )
            continue
        spread_values = [right - left for left, right in zip(left_values, right_values)]
        summary_rows.append(
            {
                "horizon_days": horizon,
                "sample_count": min(len(left_values), len(right_values)),
                "left_mean": statistics.fmean(left_values),
                "right_mean": statistics.fmean(right_values),
                "spread_mean": statistics.fmean(spread_values),
                "corr": _pearson(left_values, right_values),
            }
        )
    return summary_rows


def _conditional_corr(rows: list[dict[str, Any]], event_indexes: set[int]) -> dict[str, float]:
    shock_rows = [row for index, row in enumerate(rows) if index in event_indexes and row.get("left_return") is not None and row.get("right_return") is not None]
    calm_rows = [row for index, row in enumerate(rows) if index not in event_indexes and row.get("left_return") is not None and row.get("right_return") is not None]
    shock_left = [float(row["left_return"]) for row in shock_rows]
    shock_right = [float(row["right_return"]) for row in shock_rows]
    calm_left = [float(row["left_return"]) for row in calm_rows]
    calm_right = [float(row["right_return"]) for row in calm_rows]
    return {
        "shock_sample_count": float(len(shock_rows)),
        "shock_return_corr": _pearson(shock_left, shock_right),
        "calm_sample_count": float(len(calm_rows)),
        "calm_return_corr": _pearson(calm_left, calm_right),
    }


def _write_events_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        fieldnames = [
            "direction",
            "market_date",
            "left_return",
            "right_return",
            "left_return_3d",
            "right_return_3d",
            "left_return_zscore_90d",
            "right_return_zscore_90d",
        ]
    else:
        fieldnames = list(rows[0].keys())
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _event_detail_rows(
    events: list[dict[str, Any]],
    *,
    left_name: str,
    right_name: str,
) -> list[list[str]]:
    rows: list[list[str]] = []
    for event in events:
        rows.append(
            [
                str(event["market_date"]),
                "上涨" if event["direction"] == "up" else "下跌",
                _format_percent(event["left_return"]),
                _format_percent(event["right_return"]) if event["right_return"] is not None else "-",
                _format_percent(event["left_cum_return_t1"]) if event.get("left_cum_return_t1") is not None else "-",
                _format_percent(event["right_cum_return_t1"]) if event.get("right_cum_return_t1") is not None else "-",
                _format_percent(event["left_cum_return_t3"]) if event.get("left_cum_return_t3") is not None else "-",
                _format_percent(event["right_cum_return_t3"]) if event.get("right_cum_return_t3") is not None else "-",
                _format_percent(event["left_cum_return_t7"]) if event.get("left_cum_return_t7") is not None else "-",
                _format_percent(event["right_cum_return_t7"]) if event.get("right_cum_return_t7") is not None else "-",
                _format_percent(event["left_cum_return_t14"]) if event.get("left_cum_return_t14") is not None else "-",
                _format_percent(event["right_cum_return_t14"]) if event.get("right_cum_return_t14") is not None else "-",
                _format_number(event["left_return_zscore_90d"], 4) if event["left_return_zscore_90d"] is not None else "-",
                _format_number(event["right_return_zscore_90d"], 4) if event["right_return_zscore_90d"] is not None else "-",
            ]
        )
    return rows


def build_history_pair_shock_report(
    connection: sqlite3.Connection,
    *,
    output_root: Path,
    region_id: int,
    left_type_id: int,
    right_type_id: int,
    left_label: str | None = None,
    right_label: str | None = None,
    daily_shock_threshold: float = 0.05,
    cumulative_3d_threshold: float = 0.08,
    return_zscore_threshold: float = 2.0,
    cooldown_days: int = 7,
) -> dict[str, Any]:
    left_name = left_label or _load_type_name(connection, left_type_id)
    right_name = right_label or _load_type_name(connection, right_type_id)
    left_series = _load_history_series(connection, region_id=region_id, type_id=left_type_id)
    right_series = _load_history_series(connection, region_id=region_id, type_id=right_type_id)
    if not left_series or not right_series:
        raise RuntimeError("缺少至少一侧 history 数据，无法生成冲击传导报告。")

    aligned = _align_series(left_series, right_series)
    if len(aligned) < 60:
        raise RuntimeError(f"重叠样本只有 {len(aligned)} 天，低于最小分析要求 60 天。")
    rows = _enrich_rows(aligned)
    horizons = [0, 1, 3, 7, 14]
    up_events = _select_events(
        rows,
        direction="up",
        daily_shock_threshold=daily_shock_threshold,
        cumulative_3d_threshold=cumulative_3d_threshold,
        return_zscore_threshold=return_zscore_threshold,
        cooldown_days=cooldown_days,
        horizons=horizons,
    )
    down_events = _select_events(
        rows,
        direction="down",
        daily_shock_threshold=daily_shock_threshold,
        cumulative_3d_threshold=cumulative_3d_threshold,
        return_zscore_threshold=return_zscore_threshold,
        cooldown_days=cooldown_days,
        horizons=horizons,
    )
    all_events = sorted([*up_events, *down_events], key=lambda row: str(row["market_date"]))
    event_indexes = {int(row["row_index"]) for row in all_events}

    up_summary = _aggregate_event_window(up_events, horizons)
    down_summary = _aggregate_event_window(down_events, horizons)
    all_summary = _aggregate_event_window(all_events, horizons)
    conditional_corr = _conditional_corr(rows, event_indexes)

    run_id = create_run_id("history-pair-shock")
    run_dir = ensure_run_dir(output_root, run_id)
    markdown_path = run_dir / "history_pair_shock_report.md"
    json_path = run_dir / "history_pair_shock_report.json"
    events_csv_path = run_dir / "history_pair_shock_events.csv"
    series_csv_path = run_dir / "history_pair_shock_series.csv"

    report = {
        "report_generated_at_utc": _utc_now_iso(),
        "report_run_id": run_id,
        "report_run_dir": str(run_dir),
        "region_id": region_id,
        "region_name": _load_region_name(connection, region_id),
        "left_type_id": left_type_id,
        "right_type_id": right_type_id,
        "left_label": left_name,
        "right_label": right_name,
        "data_window": {
            "overlap_day_count": len(rows),
            "market_date_min": rows[0]["market_date"],
            "market_date_max": rows[-1]["market_date"],
            "left_fetched_at_max": max(str(row["fetched_at"]) for row in left_series),
            "right_fetched_at_max": max(str(row["fetched_at"]) for row in right_series),
        },
        "event_definition": {
            "daily_shock_threshold": daily_shock_threshold,
            "cumulative_3d_threshold": cumulative_3d_threshold,
            "return_zscore_threshold": return_zscore_threshold,
            "cooldown_days": cooldown_days,
            "horizons": horizons,
        },
        "event_counts": {
            "up_event_count": len(up_events),
            "down_event_count": len(down_events),
            "all_event_count": len(all_events),
            "up_daily_threshold_count": sum(1 for row in rows if row.get("left_return") is not None and float(row["left_return"]) >= daily_shock_threshold),
            "down_daily_threshold_count": sum(1 for row in rows if row.get("left_return") is not None and float(row["left_return"]) <= -daily_shock_threshold),
        },
        "statistics": {
            "left_return": _describe([float(row["left_return"]) for row in rows if row.get("left_return") is not None]),
            "right_return": _describe([float(row["right_return"]) for row in rows if row.get("right_return") is not None]),
            "conditional_corr": conditional_corr,
            "up_event_summary": up_summary,
            "down_event_summary": down_summary,
            "all_event_summary": all_summary,
        },
        "up_events": up_events,
        "down_events": down_events,
        "all_events": all_events,
        "files": {
            "markdown_path": str(markdown_path),
            "json_path": str(json_path),
            "events_csv_path": str(events_csv_path),
            "series_csv_path": str(series_csv_path),
        },
    }

    lines: list[str] = [
        f"# {left_name} -> {right_name} 冲击传导报告",
        "",
        f"- 星域: {report['region_name']} ({region_id})",
        f"- 样本区间: {report['data_window']['market_date_min']} 到 {report['data_window']['market_date_max']}",
        f"- 样本天数: {report['data_window']['overlap_day_count']}",
        f"- 左侧物品: {left_name} ({left_type_id})",
        f"- 右侧物品: {right_name} ({right_type_id})",
        "",
        "## 事件定义",
        "",
        f"- 单日冲击阈值: {daily_shock_threshold:.2%}",
        f"- 3日累计冲击参考阈值: {cumulative_3d_threshold:.2%}",
        f"- 90日收益率 z-score 参考阈值: {return_zscore_threshold:.2f}",
        f"- 冷却期: {cooldown_days} 天",
        "",
        "## 事件数量",
        "",
        f"- 左侧原始单日大涨样本数: {report['event_counts']['up_daily_threshold_count']}",
        f"- 左侧原始单日大跌样本数: {report['event_counts']['down_daily_threshold_count']}",
        f"- 去重后上涨事件数: {report['event_counts']['up_event_count']}",
        f"- 去重后下跌事件数: {report['event_counts']['down_event_count']}",
        f"- 去重后总事件数: {report['event_counts']['all_event_count']}",
        "",
        "## 条件相关",
        "",
    ]
    conditional = report["statistics"]["conditional_corr"]
    conditional_rows = [
        ["冲击日", str(int(conditional["shock_sample_count"])), _format_number(conditional["shock_return_corr"], 6)],
        ["平静日", str(int(conditional["calm_sample_count"])), _format_number(conditional["calm_return_corr"], 6)],
    ]
    lines.extend(_render_table(["样本类型", "样本数", "同期收益率相关"], conditional_rows))
    lines.extend(["", "## 上涨事件后的平均反应", ""])
    lines.extend(
        _render_table(
            ["窗口", f"{left_name}平均累计收益", f"{right_name}平均累计收益", "右减左", "左右相关", "样本数"],
            [
                [
                    f"T+{int(row['horizon_days'])}",
                    _format_percent(row["left_mean"]) if row["left_mean"] is not None else "-",
                    _format_percent(row["right_mean"]) if row["right_mean"] is not None else "-",
                    _format_percent(row["spread_mean"]) if row["spread_mean"] is not None else "-",
                    _format_number(row["corr"], 6) if row["corr"] is not None else "-",
                    str(int(row["sample_count"])),
                ]
                for row in up_summary
            ],
        )
    )
    lines.extend(["", "## 下跌事件后的平均反应", ""])
    lines.extend(
        _render_table(
            ["窗口", f"{left_name}平均累计收益", f"{right_name}平均累计收益", "右减左", "左右相关", "样本数"],
            [
                [
                    f"T+{int(row['horizon_days'])}",
                    _format_percent(row["left_mean"]) if row["left_mean"] is not None else "-",
                    _format_percent(row["right_mean"]) if row["right_mean"] is not None else "-",
                    _format_percent(row["spread_mean"]) if row["spread_mean"] is not None else "-",
                    _format_number(row["corr"], 6) if row["corr"] is not None else "-",
                    str(int(row["sample_count"])),
                ]
                for row in down_summary
            ],
        )
    )
    lines.extend(["", "## 全部事件后的平均反应", ""])
    lines.extend(
        _render_table(
            ["窗口", f"{left_name}平均累计收益", f"{right_name}平均累计收益", "右减左", "左右相关", "样本数"],
            [
                [
                    f"T+{int(row['horizon_days'])}",
                    _format_percent(row["left_mean"]) if row["left_mean"] is not None else "-",
                    _format_percent(row["right_mean"]) if row["right_mean"] is not None else "-",
                    _format_percent(row["spread_mean"]) if row["spread_mean"] is not None else "-",
                    _format_number(row["corr"], 6) if row["corr"] is not None else "-",
                    str(int(row["sample_count"])),
                ]
                for row in all_summary
            ],
        )
    )
    lines.extend(["", "## 事件清单", ""])
    event_rows = [
        [
            str(event["market_date"]),
            "上涨" if event["direction"] == "up" else "下跌",
            _format_percent(event["left_return"]),
            _format_percent(event["right_return"]) if event["right_return"] is not None else "-",
            _format_percent(event["left_return_3d"]) if event["left_return_3d"] is not None else "-",
            _format_number(event["left_return_zscore_90d"], 4) if event["left_return_zscore_90d"] is not None else "-",
            "Y" if event["meets_cumulative_filter"] else "N",
            "Y" if event["meets_zscore_filter"] else "N",
        ]
        for event in all_events
    ]
    lines.extend(
        _render_table(
            ["日期", "方向", f"{left_name}当日收益", f"{right_name}当日收益", f"{left_name}3日累计", "左侧90日z", "过3日阈值", "过z阈值"],
            event_rows,
        )
    )
    lines.extend(["", "## 全事件逐条结果", ""])
    lines.extend(
        _render_table(
            [
                "日期",
                "方向",
                f"{left_name}当日",
                f"{right_name}当日",
                f"{left_name}T+1",
                f"{right_name}T+1",
                f"{left_name}T+3",
                f"{right_name}T+3",
                f"{left_name}T+7",
                f"{right_name}T+7",
                f"{left_name}T+14",
                f"{right_name}T+14",
                f"{left_name}90日z",
                f"{right_name}90日z",
            ],
            _event_detail_rows(all_events, left_name=left_name, right_name=right_name),
        )
    )
    lines.extend(["", "## 上涨事件逐条结果", ""])
    lines.extend(
        _render_table(
            [
                "日期",
                f"{left_name}当日",
                f"{right_name}当日",
                f"{left_name}T+1",
                f"{right_name}T+1",
                f"{left_name}T+3",
                f"{right_name}T+3",
                f"{left_name}T+7",
                f"{right_name}T+7",
                f"{left_name}T+14",
                f"{right_name}T+14",
                f"{left_name}90日z",
                f"{right_name}90日z",
            ],
            [
                row[0:1] + row[2:]
                for row in _event_detail_rows(up_events, left_name=left_name, right_name=right_name)
            ],
        )
    )
    lines.extend(["", "## 下跌事件逐条结果", ""])
    lines.extend(
        _render_table(
            [
                "日期",
                f"{left_name}当日",
                f"{right_name}当日",
                f"{left_name}T+1",
                f"{right_name}T+1",
                f"{left_name}T+3",
                f"{right_name}T+3",
                f"{left_name}T+7",
                f"{right_name}T+7",
                f"{left_name}T+14",
                f"{right_name}T+14",
                f"{left_name}90日z",
                f"{right_name}90日z",
            ],
            [
                row[0:1] + row[2:]
                for row in _event_detail_rows(down_events, left_name=left_name, right_name=right_name)
            ],
        )
    )
    lines.extend(["", "## 文件", ""])
    lines.extend(
        [
            f"- Markdown: {markdown_path}",
            f"- JSON: {json_path}",
            f"- 事件 CSV: {events_csv_path}",
            f"- 对齐序列 CSV: {series_csv_path}",
        ]
    )

    write_summary(markdown_path, lines)
    write_json(json_path, report)
    _write_events_csv(events_csv_path, all_events)
    _write_events_csv(series_csv_path, rows)
    return report
