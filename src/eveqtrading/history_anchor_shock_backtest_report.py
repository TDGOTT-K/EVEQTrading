from __future__ import annotations

import csv
import sqlite3
import statistics
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .history_anchor_relationship_report import _format_history_source, _load_item_meta_map, _load_target_rows
from .history_anchor_shock_propagation_report import (
    _build_anchor_rows,
    _contains_any_keyword,
    _format_isk_human,
    _resolve_filter_settings,
    _select_anchor_events,
    _series_liquidity_stats,
)
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
from .market_scope import describe_history_source, load_canonical_history_series, load_canonical_history_series_map
from .models import FeeConfig
from .run_artifacts import create_run_id, ensure_run_dir, write_json, write_summary


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _compute_trade_returns(
    *,
    entry_price: float,
    exit_price: float,
    fee_config: FeeConfig,
    entry_slippage_rate: float,
    exit_slippage_rate: float,
) -> dict[str, float] | None:
    if entry_price <= 0 or exit_price <= 0:
        return None
    effective_buy_cost = float(entry_price) * (
        1.0 + float(fee_config.buy_broker_fee_rate) + float(entry_slippage_rate)
    )
    effective_sell_proceeds = float(exit_price) * (
        1.0
        - float(fee_config.sell_broker_fee_rate)
        - float(fee_config.sales_tax_rate)
        - float(fee_config.relist_fee_rate)
        - float(exit_slippage_rate)
    )
    if effective_buy_cost <= 0 or effective_sell_proceeds <= 0:
        return None
    gross_return = (float(exit_price) / float(entry_price)) - 1.0
    net_return = (effective_sell_proceeds / effective_buy_cost) - 1.0
    return {
        "gross_return": gross_return,
        "net_return": net_return,
        "effective_buy_cost": effective_buy_cost,
        "effective_sell_proceeds": effective_sell_proceeds,
    }


def _profit_factor(values: list[float]) -> float | None:
    positive_sum = sum(value for value in values if value > 0.0)
    negative_sum = -sum(value for value in values if value < 0.0)
    if negative_sum <= 0.0:
        return None
    return positive_sum / negative_sum


def _event_index_map(
    aligned_rows: list[dict[str, Any]],
    events: list[dict[str, Any]],
) -> dict[int, dict[str, Any]]:
    event_by_date = {str(event["market_date"]): event for event in events}
    return {
        index: event_by_date[str(row["market_date"])]
        for index, row in enumerate(aligned_rows)
        if str(row["market_date"]) in event_by_date
    }


def _simulate_holding_plan(
    *,
    aligned_rows: list[dict[str, Any]],
    event_index_map: dict[int, dict[str, Any]],
    entry_delay_days: int,
    holding_days: int,
    fee_config: FeeConfig,
    entry_slippage_rate: float,
    exit_slippage_rate: float,
) -> dict[str, Any]:
    event_trades: list[dict[str, Any]] = []
    baseline_trades: list[dict[str, Any]] = []
    for trigger_index, trigger_row in enumerate(aligned_rows):
        entry_index = trigger_index + int(entry_delay_days)
        exit_index = entry_index + int(holding_days)
        if entry_index >= len(aligned_rows) or exit_index >= len(aligned_rows):
            continue
        trade_returns = _compute_trade_returns(
            entry_price=float(aligned_rows[entry_index]["right_price"]),
            exit_price=float(aligned_rows[exit_index]["right_price"]),
            fee_config=fee_config,
            entry_slippage_rate=entry_slippage_rate,
            exit_slippage_rate=exit_slippage_rate,
        )
        if trade_returns is None:
            continue
        trade_row = {
            "trigger_market_date": str(trigger_row["market_date"]),
            "entry_market_date": str(aligned_rows[entry_index]["market_date"]),
            "exit_market_date": str(aligned_rows[exit_index]["market_date"]),
            "entry_price": float(aligned_rows[entry_index]["right_price"]),
            "exit_price": float(aligned_rows[exit_index]["right_price"]),
            "gross_return": float(trade_returns["gross_return"]),
            "net_return": float(trade_returns["net_return"]),
            "entry_turnover_isk": float(aligned_rows[entry_index]["right_turnover_isk"]),
            "exit_turnover_isk": float(aligned_rows[exit_index]["right_turnover_isk"]),
        }
        if trigger_index in event_index_map:
            anchor_event = event_index_map[trigger_index]
            event_trades.append(
                {
                    **trade_row,
                    "anchor_market_date": str(anchor_event["market_date"]),
                    "anchor_return": float(anchor_event["anchor_return"]),
                    "anchor_return_3d": float(anchor_event["anchor_return_3d"])
                    if anchor_event.get("anchor_return_3d") is not None
                    else None,
                    "anchor_return_zscore_90d": float(anchor_event["anchor_return_zscore_90d"])
                    if anchor_event.get("anchor_return_zscore_90d") is not None
                    else None,
                }
            )
        else:
            baseline_trades.append(trade_row)

    event_net_returns = [float(row["net_return"]) for row in event_trades]
    baseline_net_returns = [float(row["net_return"]) for row in baseline_trades]
    event_gross_returns = [float(row["gross_return"]) for row in event_trades]
    baseline_gross_returns = [float(row["gross_return"]) for row in baseline_trades]

    event_mean_net = statistics.fmean(event_net_returns) if event_net_returns else None
    baseline_mean_net = statistics.fmean(baseline_net_returns) if baseline_net_returns else None
    event_mean_gross = statistics.fmean(event_gross_returns) if event_gross_returns else None
    baseline_mean_gross = statistics.fmean(baseline_gross_returns) if baseline_gross_returns else None

    return {
        "entry_delay_days": int(entry_delay_days),
        "holding_days": int(holding_days),
        "event_trade_count": len(event_trades),
        "baseline_trade_count": len(baseline_trades),
        "event_mean_gross_return": event_mean_gross,
        "event_mean_net_return": event_mean_net,
        "event_median_net_return": statistics.median(event_net_returns) if event_net_returns else None,
        "baseline_mean_gross_return": baseline_mean_gross,
        "baseline_mean_net_return": baseline_mean_net,
        "baseline_median_net_return": statistics.median(baseline_net_returns) if baseline_net_returns else None,
        "abnormal_mean_net_return": (
            float(event_mean_net) - float(baseline_mean_net)
            if event_mean_net is not None and baseline_mean_net is not None
            else None
        ),
        "event_win_rate": _safe_div(sum(1 for value in event_net_returns if value > 0.0), len(event_net_returns))
        if event_net_returns
        else None,
        "baseline_win_rate": _safe_div(sum(1 for value in baseline_net_returns if value > 0.0), len(baseline_net_returns))
        if baseline_net_returns
        else None,
        "event_best_net_return": max(event_net_returns) if event_net_returns else None,
        "event_worst_net_return": min(event_net_returns) if event_net_returns else None,
        "event_profit_factor": _profit_factor(event_net_returns),
        "sample_event_trades": event_trades[:5],
        "sample_baseline_trades": baseline_trades[:3],
    }


def _choose_best_plan(
    plan_rows: list[dict[str, Any]],
    *,
    min_event_sample_count: int,
    require_positive: bool,
) -> dict[str, Any] | None:
    candidates = [
        row
        for row in plan_rows
        if int(row.get("event_trade_count") or 0) >= int(min_event_sample_count)
        and row.get("event_mean_net_return") is not None
        and row.get("baseline_mean_net_return") is not None
        and row.get("abnormal_mean_net_return") is not None
    ]
    if require_positive:
        candidates = [
            row
            for row in candidates
            if float(row["event_mean_net_return"]) > 0.0
            and float(row["abnormal_mean_net_return"]) > 0.0
        ]
    if not candidates:
        return None
    return max(
        candidates,
        key=lambda row: (
            float(row["abnormal_mean_net_return"]),
            float(row["event_mean_net_return"]),
            float(row["event_win_rate"] or 0.0),
            int(row["event_trade_count"]),
            -int(row["holding_days"]),
        ),
    )


def _summarize_target_backtest(
    *,
    region_id: int,
    anchor_type_id: int,
    anchor_label: str,
    anchor_source: dict[str, Any],
    target_meta: dict[str, Any],
    anchor_series: list[dict[str, Any]],
    target_series: list[dict[str, Any]],
    up_events: list[dict[str, Any]],
    min_overlap_days: int,
    min_event_sample_count: int,
    liquidity_stats: dict[str, Any],
    fee_config: FeeConfig,
    entry_delay_days: int,
    holding_days_list: list[int],
    entry_slippage_rate: float,
    exit_slippage_rate: float,
) -> dict[str, Any] | None:
    aligned_rows = _align_series(anchor_series, target_series)
    if len(aligned_rows) < min_overlap_days:
        return None
    event_index_map = _event_index_map(aligned_rows, up_events)
    if not event_index_map:
        return None
    anchor_returns = [float(row["left_return"]) for row in aligned_rows if row.get("left_return") is not None]
    target_returns = [float(row["right_return"]) for row in aligned_rows if row.get("right_return") is not None]
    plan_rows = [
        _simulate_holding_plan(
            aligned_rows=aligned_rows,
            event_index_map=event_index_map,
            entry_delay_days=entry_delay_days,
            holding_days=int(holding_days),
            fee_config=fee_config,
            entry_slippage_rate=entry_slippage_rate,
            exit_slippage_rate=exit_slippage_rate,
        )
        for holding_days in holding_days_list
    ]
    best_plan = _choose_best_plan(plan_rows, min_event_sample_count=min_event_sample_count, require_positive=False)
    best_profitable_plan = _choose_best_plan(plan_rows, min_event_sample_count=min_event_sample_count, require_positive=True)
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
        "holding_plan_rows": plan_rows,
        "best_plan": best_plan,
        "best_profitable_plan": best_profitable_plan,
    }


def _rank_profitable_rows(rows: list[dict[str, Any]], *, top_n: int) -> list[dict[str, Any]]:
    candidates = [row for row in rows if row.get("best_profitable_plan") is not None]
    ranked = sorted(
        candidates,
        key=lambda row: (
            float(row["best_profitable_plan"]["abnormal_mean_net_return"] or 0.0),
            float(row["best_profitable_plan"]["event_mean_net_return"] or 0.0),
            float(row["best_profitable_plan"]["event_win_rate"] or 0.0),
            int(row["best_profitable_plan"]["event_trade_count"] or 0),
            str(row["target_label"]),
        ),
        reverse=True,
    )
    return ranked[:top_n]


def _screen_table_rows(rows: list[dict[str, Any]]) -> list[list[str]]:
    rendered: list[list[str]] = []
    for index, row in enumerate(rows, start=1):
        best = row["best_profitable_plan"]
        rendered.append(
            [
                str(index),
                str(row["target_label"]),
                str(row["target_market_group"]),
                _format_isk_human(row["median_daily_turnover_isk"]),
                _format_isk_human(row["latest_daily_turnover_isk"]),
                f"D+{int(best['entry_delay_days'])}",
                f"{int(best['holding_days'])}天",
                str(int(best["event_trade_count"])),
                _format_percent(best["event_mean_gross_return"]),
                _format_percent(best["event_mean_net_return"]),
                _format_percent(best["baseline_mean_net_return"]),
                _format_percent(best["abnormal_mean_net_return"]),
                _format_percent(best["event_win_rate"]),
                _format_percent(best["event_worst_net_return"]),
                _format_isk_human((best["event_mean_net_return"] or 0.0) * 100_000_000.0),
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
        "best_entry_delay_days",
        "best_holding_days",
        "best_event_trade_count",
        "best_event_mean_gross_return",
        "best_event_mean_net_return",
        "best_baseline_mean_net_return",
        "best_abnormal_mean_net_return",
        "best_event_win_rate",
        "best_event_worst_net_return",
        "best_event_profit_factor",
        "is_profitable_after_costs",
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            best = row.get("best_profitable_plan") or row.get("best_plan") or {}
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
                    "best_entry_delay_days": best.get("entry_delay_days"),
                    "best_holding_days": best.get("holding_days"),
                    "best_event_trade_count": best.get("event_trade_count"),
                    "best_event_mean_gross_return": best.get("event_mean_gross_return"),
                    "best_event_mean_net_return": best.get("event_mean_net_return"),
                    "best_baseline_mean_net_return": best.get("baseline_mean_net_return"),
                    "best_abnormal_mean_net_return": best.get("abnormal_mean_net_return"),
                    "best_event_win_rate": best.get("event_win_rate"),
                    "best_event_worst_net_return": best.get("event_worst_net_return"),
                    "best_event_profit_factor": best.get("event_profit_factor"),
                    "is_profitable_after_costs": row.get("best_profitable_plan") is not None,
                }
            )


def build_history_anchor_shock_backtest_report(
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
    filter_profile: str = "off",
    min_median_daily_turnover_isk: float | None = None,
    min_latest_daily_turnover_isk: float | None = None,
    min_median_daily_volume: float | None = None,
    min_event_sample_count: int | None = None,
    exclude_name_keywords: list[str] | None = None,
    exclude_group_keywords: list[str] | None = None,
    entry_delay_days: int = 1,
    holding_days_list: list[int] | None = None,
    fee_config: FeeConfig | None = None,
    entry_slippage_rate: float = 0.0,
    exit_slippage_rate: float = 0.0,
) -> dict[str, Any]:
    resolved_holding_days = list(holding_days_list or [1, 3, 7, 14])
    effective_fee_config = fee_config or FeeConfig()
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
    if not up_events:
        raise RuntimeError("锚点上涨冲击事件数为 0，无法做上涨后买入回测。")

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
            skipped_rows.append({"type_id": target_type_id, "type_name": target_meta["type_name"], "reason": "missing_history"})
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
        summary = _summarize_target_backtest(
            region_id=region_id,
            anchor_type_id=anchor_type_id,
            anchor_label=anchor_name,
            anchor_source=anchor_source,
            target_meta=target_meta,
            anchor_series=anchor_series,
            target_series=target_series,
            up_events=up_events,
            min_overlap_days=min_overlap_days,
            min_event_sample_count=int(filter_settings["min_event_sample_count"]),
            liquidity_stats=liquidity_stats,
            fee_config=effective_fee_config,
            entry_delay_days=entry_delay_days,
            holding_days_list=resolved_holding_days,
            entry_slippage_rate=entry_slippage_rate,
            exit_slippage_rate=exit_slippage_rate,
        )
        if summary is None:
            skipped_rows.append({"type_id": target_type_id, "type_name": target_meta["type_name"], "reason": "overlap_below_threshold_or_no_events"})
            continue
        response_rows.append(summary)

    if not response_rows:
        raise RuntimeError("没有满足最小重叠天数要求的目标物品，无法生成冲击后屯货回测报告。")

    profitable_rankings = _rank_profitable_rows(response_rows, top_n=top_n)
    profitable_target_count = sum(1 for row in response_rows if row.get("best_profitable_plan") is not None)

    run_id = create_run_id("history-anchor-shock-backtest")
    run_dir = ensure_run_dir(output_root, run_id)
    markdown_path = run_dir / "history_anchor_shock_backtest_report.md"
    json_path = run_dir / "history_anchor_shock_backtest_report.json"
    csv_path = run_dir / "history_anchor_shock_backtest_screen.csv"

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
        },
        "backtest_config": {
            "entry_delay_days": int(entry_delay_days),
            "holding_days_list": resolved_holding_days,
            "entry_slippage_rate": float(entry_slippage_rate),
            "exit_slippage_rate": float(exit_slippage_rate),
            "fee_config": effective_fee_config.to_dict(),
        },
        "filters": filter_settings,
        "universe_summary": {
            "selected_target_count": len(target_rows),
            "qualified_target_count": len(response_rows),
            "profitable_target_count": profitable_target_count,
            "skipped_target_count": len(skipped_rows),
            "min_overlap_days": int(min_overlap_days),
            "top_n": int(top_n),
            "anchor_history_day_count": len(anchor_series),
            "anchor_market_date_min": anchor_series[0]["market_date"],
            "anchor_market_date_max": anchor_series[-1]["market_date"],
            "anchor_latest_fetched_at": max(str(row["fetched_at"]) for row in anchor_series),
            "up_event_count": len(up_events),
        },
        "up_events": up_events,
        "profitable_rankings": profitable_rankings,
        "response_rows": response_rows,
        "skipped_rows_sample": skipped_rows[:50],
        "files": {
            "markdown_path": str(markdown_path),
            "json_path": str(json_path),
            "csv_path": str(csv_path),
        },
    }

    lines: list[str] = [
        f"# {anchor_name} 冲击后屯货回测报告",
        "",
        f"- 星域: {report['region_name']} ({region_id})",
        f"- 锚点物品: {anchor_name} ({anchor_type_id})",
        f"- 锚点 history 口径: {_format_history_source(anchor_source)}",
        f"- 锚点样本区间: {report['universe_summary']['anchor_market_date_min']} 到 {report['universe_summary']['anchor_market_date_max']}",
        f"- 锚点 history 天数: {report['universe_summary']['anchor_history_day_count']}",
        f"- 锚点最新抓取: {report['universe_summary']['anchor_latest_fetched_at']}",
        f"- 上涨冲击事件数: {report['universe_summary']['up_event_count']}",
        f"- 扫描目标数: {report['universe_summary']['selected_target_count']}",
        f"- 合格目标数: {report['universe_summary']['qualified_target_count']}",
        f"- 扣成本后仍为正的目标数: {report['universe_summary']['profitable_target_count']}",
        "",
        "## 回测假设",
        "",
        "- 这不是“看到矿涨当天就神预判抢跑”，而是保守地假设你在锚点冲击发生后，最早要到下一交易日才能行动。",
        f"- 入场时点: `D+{int(entry_delay_days)}`。",
        f"- 持有窗口: {', '.join(f'{int(day)}天' for day in resolved_holding_days)}。",
        "- 入场价 / 出场价都用日级 history 的 `average_price` 近似，所以这份回测比真实挂单成交更乐观。",
        "- 结果里已经扣掉交易成本，但还没有引入真实订单簿排队、点差挤压、改单失败、仓位容量约束。",
        f"- 买入经纪人费: {_format_percent(effective_fee_config.buy_broker_fee_rate)}",
        f"- 卖出经纪人费: {_format_percent(effective_fee_config.sell_broker_fee_rate)}",
        f"- 交易税: {_format_percent(effective_fee_config.sales_tax_rate)}",
        f"- 改单费: {_format_percent(effective_fee_config.relist_fee_rate)}",
        f"- 入场滑点假设: {_format_percent(entry_slippage_rate)}",
        f"- 出场滑点假设: {_format_percent(exit_slippage_rate)}",
        "",
        "## 怎么读",
        "",
        "- `事件均值毛收益`: 只看价格变动，不扣成本。",
        "- `事件均值净收益`: 扣掉税费和滑点后的平均单笔收益率。",
        "- `基线净收益`: 在非冲击日，用同样入场和持有规则得到的平均净收益率。",
        "- `超额净收益`: `事件均值净收益 - 基线净收益`。它更接近“因为锚点冲击才多出来的利润”。",
        "- `最差单笔`: 历史事件样本里最亏的一次，方便看风险下限。",
        "",
        "## 锚点上涨事件清单",
        "",
    ]
    lines.extend(
        _render_table(
            ["序号", "日期", "锚点当日收益", "锚点3日累计", "锚点90日z"],
            [
                [
                    str(index),
                    str(event["market_date"]),
                    _format_percent(event["anchor_return"]),
                    _format_percent(event["anchor_return_3d"]) if event.get("anchor_return_3d") is not None else "-",
                    _format_number(event["anchor_return_zscore_90d"], 4) if event.get("anchor_return_zscore_90d") is not None else "-",
                ]
                for index, event in enumerate(up_events, start=1)
            ],
        )
    )
    lines.extend(["", "## 扣掉成本后仍有正收益的商品 Top", ""])
    if profitable_rankings:
        lines.extend(
            _render_table(
                [
                    "排名",
                    "商品",
                    "市场组",
                    "中位日成交额",
                    "最新日成交额",
                    "入场",
                    "最佳持有",
                    "事件数",
                    "事件均值毛收益",
                    "事件均值净收益",
                    "基线净收益",
                    "超额净收益",
                    "胜率",
                    "最差单笔",
                    "100m 期望净利润",
                    "样本截止",
                ],
                _screen_table_rows(profitable_rankings),
            )
        )
    else:
        lines.append("- 在当前税费、滑点和 `D+1` 入场假设下，没有找到扣成本后仍然为正且优于基线的目标商品。")

    write_summary(markdown_path, lines)
    write_json(json_path, report)
    _write_screen_csv(csv_path, response_rows)
    return report
