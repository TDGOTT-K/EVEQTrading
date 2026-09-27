from __future__ import annotations

import csv
import sqlite3
import statistics
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .history_anchor_relationship_report import _format_history_source, _load_item_meta_map, _load_target_rows
from .history_pair_relationship_report import _format_percent, _render_table, _safe_div
from .market_scope import describe_history_source, load_canonical_history_series_map
from .models import FeeConfig
from .run_artifacts import create_run_id, ensure_run_dir, write_json, write_summary


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _format_number(value: Any, digits: int = 2) -> str:
    try:
        return f"{float(value):,.{digits}f}"
    except (TypeError, ValueError):
        return "-"


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


def _net_return_from_prices(
    *,
    entry_price: float,
    exit_price: float,
    fee_config: FeeConfig,
) -> float | None:
    if entry_price <= 0.0 or exit_price <= 0.0:
        return None
    effective_buy_cost = float(entry_price) * (1.0 + float(fee_config.buy_broker_fee_rate))
    effective_sell_proceeds = float(exit_price) * (
        1.0
        - float(fee_config.sell_broker_fee_rate)
        - float(fee_config.sales_tax_rate)
        - float(fee_config.relist_fee_rate)
    )
    if effective_buy_cost <= 0.0 or effective_sell_proceeds <= 0.0:
        return None
    return (effective_sell_proceeds / effective_buy_cost) - 1.0


def _break_even_exit_price(*, entry_price: float, fee_config: FeeConfig, target_net_return: float) -> float:
    effective_buy_cost = float(entry_price) * (1.0 + float(fee_config.buy_broker_fee_rate))
    sell_keep_ratio = (
        1.0
        - float(fee_config.sell_broker_fee_rate)
        - float(fee_config.sales_tax_rate)
        - float(fee_config.relist_fee_rate)
    )
    if effective_buy_cost <= 0.0 or sell_keep_ratio <= 0.0:
        return 0.0
    return effective_buy_cost * (1.0 + float(target_net_return)) / sell_keep_ratio


def _percentile(sorted_values: list[float], ratio: float) -> float | None:
    if not sorted_values:
        return None
    if len(sorted_values) == 1:
        return float(sorted_values[0])
    clamped_ratio = min(max(float(ratio), 0.0), 1.0)
    index = int(round((len(sorted_values) - 1) * clamped_ratio))
    return float(sorted_values[index])


def _winsorized_mean(values: list[float], *, lower_ratio: float = 0.1, upper_ratio: float = 0.9) -> float | None:
    if not values:
        return None
    ordered = sorted(float(value) for value in values)
    lower_cap = _percentile(ordered, lower_ratio)
    upper_cap = _percentile(ordered, upper_ratio)
    if lower_cap is None or upper_cap is None:
        return statistics.fmean(ordered)
    adjusted = [min(max(value, lower_cap), upper_cap) for value in ordered]
    return statistics.fmean(adjusted)


def _rolling_price_signal(
    series: list[dict[str, Any]],
    *,
    index: int,
    lookback_days: int,
) -> dict[str, Any] | None:
    if index < lookback_days:
        return None
    window = series[index - lookback_days : index]
    prices = [float(row["average_price"]) for row in window if float(row["average_price"]) > 0.0]
    if len(prices) < lookback_days:
        return None
    mean_price = statistics.fmean(prices)
    std_price = statistics.pstdev(prices) if len(prices) > 1 else 0.0
    entry_price = float(series[index]["average_price"])
    if entry_price <= 0.0:
        return None
    zscore = _safe_div(entry_price - mean_price, std_price) if std_price > 0.0 else 0.0
    return {
        "entry_mean_price": mean_price,
        "entry_std_price": std_price,
        "entry_price": entry_price,
        "entry_zscore": zscore,
    }


def _simulate_entry_signal(
    series: list[dict[str, Any]],
    *,
    entry_index: int,
    lookback_days: int,
    max_hold_days: int,
    entry_zscore_threshold: float,
    min_entry_price_vs_mean_ratio: float,
    target_net_return: float,
    fee_config: FeeConfig,
) -> dict[str, Any] | None:
    signal = _rolling_price_signal(series, index=entry_index, lookback_days=lookback_days)
    if signal is None:
        return None
    if float(signal["entry_zscore"]) > float(entry_zscore_threshold):
        return None
    if entry_index + 1 >= len(series):
        return None

    entry_row = series[entry_index]
    entry_price = float(signal["entry_price"])
    entry_mean_price = float(signal["entry_mean_price"])
    entry_price_vs_mean_ratio = _safe_div(entry_price, entry_mean_price) if entry_mean_price > 0.0 else None
    if (
        entry_price_vs_mean_ratio is None
        or float(entry_price_vs_mean_ratio) < float(min_entry_price_vs_mean_ratio)
    ):
        return None
    required_exit_price = _break_even_exit_price(
        entry_price=entry_price,
        fee_config=fee_config,
        target_net_return=target_net_return,
    )

    best_net_return: float | None = None
    best_exit_day: int | None = None
    earliest_profitable_day: int | None = None
    earliest_profitable_net_return: float | None = None
    max_adverse_net_return: float | None = None
    exit_market_date: str | None = None
    best_exit_market_date: str | None = None

    for day_offset in range(1, max_hold_days + 1):
        future_index = entry_index + day_offset
        if future_index >= len(series):
            break
        future_row = series[future_index]
        high_price = float(future_row["highest_price"])
        low_price = float(future_row["lowest_price"])
        average_exit_price = float(future_row["average_price"])

        high_net_return = _net_return_from_prices(entry_price=entry_price, exit_price=high_price, fee_config=fee_config)
        low_net_return = _net_return_from_prices(entry_price=entry_price, exit_price=low_price, fee_config=fee_config)
        average_net_return = _net_return_from_prices(
            entry_price=entry_price,
            exit_price=average_exit_price,
            fee_config=fee_config,
        )

        if low_net_return is not None and (max_adverse_net_return is None or low_net_return < max_adverse_net_return):
            max_adverse_net_return = low_net_return

        realized_net_return = average_net_return
        if high_price >= required_exit_price:
            realized_net_return = max(float(target_net_return), float(average_net_return or target_net_return))
        if realized_net_return is not None and (best_net_return is None or realized_net_return > best_net_return):
            best_net_return = realized_net_return
            best_exit_day = day_offset
            best_exit_market_date = str(future_row["market_date"])

        if earliest_profitable_day is None and high_price >= required_exit_price and high_net_return is not None:
            earliest_profitable_day = day_offset
            earliest_profitable_net_return = max(float(target_net_return), float(average_net_return or target_net_return))
            exit_market_date = str(future_row["market_date"])

    if best_net_return is None:
        return None

    return {
        "entry_market_date": str(entry_row["market_date"]),
        "entry_price": entry_price,
        "entry_zscore": float(signal["entry_zscore"]),
        "entry_mean_price": entry_mean_price,
        "entry_price_vs_mean_ratio": entry_price_vs_mean_ratio,
        "required_exit_price": required_exit_price,
        "best_net_return": best_net_return,
        "best_exit_day": best_exit_day,
        "best_exit_market_date": best_exit_market_date,
        "earliest_profitable_day": earliest_profitable_day,
        "earliest_profitable_net_return": earliest_profitable_net_return,
        "profitable_within_horizon": earliest_profitable_day is not None,
        "max_adverse_net_return": max_adverse_net_return,
        "future_horizon_days": min(max_hold_days, len(series) - entry_index - 1),
        "exit_market_date": exit_market_date,
    }


def _summarize_series_backtest(
    *,
    series: list[dict[str, Any]],
    type_id: int,
    type_name: str,
    market_group: str,
    market_group_id: int | None,
    region_id: int,
    lookback_days: int,
    entry_zscore_threshold: float,
    min_entry_price_vs_mean_ratio: float,
    max_hold_days: int,
    target_net_return: float,
    fee_config: FeeConfig,
) -> dict[str, Any] | None:
    if len(series) < lookback_days + 2:
        return None

    turnover_30d = [
        float(row["average_price"]) * float(row["volume"])
        for row in series[-30:]
        if float(row["average_price"]) > 0.0 and float(row["volume"]) > 0.0
    ]
    volume_30d = [float(row["volume"]) for row in series[-30:] if float(row["volume"]) > 0.0]
    active_days_ratio_30d = _safe_div(sum(1 for row in series[-30:] if float(row["volume"]) > 0.0), min(len(series), 30))

    signal_rows: list[dict[str, Any]] = []
    for entry_index in range(lookback_days, len(series) - 1):
        event = _simulate_entry_signal(
            series,
            entry_index=entry_index,
            lookback_days=lookback_days,
            max_hold_days=max_hold_days,
            entry_zscore_threshold=entry_zscore_threshold,
            min_entry_price_vs_mean_ratio=min_entry_price_vs_mean_ratio,
            target_net_return=target_net_return,
            fee_config=fee_config,
        )
        if event is not None:
            signal_rows.append(event)

    latest_signal = _rolling_price_signal(series, index=len(series) - 1, lookback_days=lookback_days)
    latest_price = float(series[-1]["average_price"])
    latest_entry_price_vs_mean_ratio = (
        _safe_div(float(latest_signal["entry_price"]), float(latest_signal["entry_mean_price"]))
        if latest_signal is not None and float(latest_signal["entry_mean_price"]) > 0.0
        else None
    )
    latest_break_even_exit_price = (
        _break_even_exit_price(entry_price=latest_price, fee_config=fee_config, target_net_return=target_net_return)
        if latest_price > 0.0
        else None
    )

    profitable_signal_rows = [row for row in signal_rows if bool(row["profitable_within_horizon"])]
    profitable_exit_days = sorted(int(row["earliest_profitable_day"]) for row in profitable_signal_rows if row["earliest_profitable_day"] is not None)
    best_net_returns = [float(row["best_net_return"]) for row in signal_rows]
    profitable_best_net_returns = [float(row["best_net_return"]) for row in profitable_signal_rows]
    first_exit_net_returns = [
        float(row["earliest_profitable_net_return"])
        for row in profitable_signal_rows
        if row.get("earliest_profitable_net_return") is not None
    ]
    adverse_returns = [float(row["max_adverse_net_return"]) for row in signal_rows if row["max_adverse_net_return"] is not None]

    mean_best_net_return = statistics.fmean(best_net_returns) if best_net_returns else None
    robust_mean_best_net_return = _winsorized_mean(best_net_returns)
    mean_first_exit_net_return = statistics.fmean(first_exit_net_returns) if first_exit_net_returns else None
    robust_mean_first_exit_net_return = _winsorized_mean(first_exit_net_returns)
    win_rate = _safe_div(len(profitable_signal_rows), len(signal_rows)) if signal_rows else 0.0
    median_exit_days = statistics.median(profitable_exit_days) if profitable_exit_days else None
    edge_per_day = (
        _safe_div(float(robust_mean_first_exit_net_return), float(median_exit_days))
        if robust_mean_first_exit_net_return is not None and median_exit_days not in (None, 0)
        else 0.0
    )
    historical_score = float(win_rate) * edge_per_day

    return {
        "type_id": int(type_id),
        "type_name": str(type_name),
        "market_group": str(market_group),
        "market_group_id": market_group_id,
        "region_id": int(region_id),
        "history_source": describe_history_source(region_id=region_id, type_id=int(type_id)),
        "history_day_count": len(series),
        "history_market_date_min": str(series[0]["market_date"]),
        "history_market_date_max": str(series[-1]["market_date"]),
        "latest_fetched_at": max(str(row["fetched_at"]) for row in series),
        "latest_price": latest_price,
        "latest_volume": float(series[-1]["volume"]),
        "latest_turnover_isk": latest_price * float(series[-1]["volume"]),
        "median_daily_turnover_isk_30d": statistics.median(turnover_30d) if turnover_30d else 0.0,
        "latest_daily_turnover_isk": latest_price * float(series[-1]["volume"]),
        "median_daily_volume_30d": statistics.median(volume_30d) if volume_30d else 0.0,
        "active_days_ratio_30d": active_days_ratio_30d,
        "lookback_days": int(lookback_days),
        "max_hold_days": int(max_hold_days),
        "entry_zscore_threshold": float(entry_zscore_threshold),
        "min_entry_price_vs_mean_ratio": float(min_entry_price_vs_mean_ratio),
        "target_net_return": float(target_net_return),
        "signal_count": len(signal_rows),
        "profitable_signal_count": len(profitable_signal_rows),
        "win_rate": win_rate,
        "mean_entry_zscore": statistics.fmean(float(row["entry_zscore"]) for row in signal_rows) if signal_rows else None,
        "latest_entry_zscore": float(latest_signal["entry_zscore"]) if latest_signal is not None else None,
        "latest_entry_mean_price": float(latest_signal["entry_mean_price"]) if latest_signal is not None else None,
        "latest_entry_price_vs_mean_ratio": latest_entry_price_vs_mean_ratio,
        "latest_signal_active": bool(
            latest_signal is not None
            and float(latest_signal["entry_zscore"]) <= float(entry_zscore_threshold)
            and latest_entry_price_vs_mean_ratio is not None
            and float(latest_entry_price_vs_mean_ratio) >= float(min_entry_price_vs_mean_ratio)
        ),
        "latest_break_even_exit_price": latest_break_even_exit_price,
        "mean_best_net_return": mean_best_net_return,
        "robust_mean_best_net_return": robust_mean_best_net_return,
        "median_best_net_return": statistics.median(best_net_returns) if best_net_returns else None,
        "mean_profitable_best_net_return": statistics.fmean(profitable_best_net_returns) if profitable_best_net_returns else None,
        "mean_first_exit_net_return": mean_first_exit_net_return,
        "robust_mean_first_exit_net_return": robust_mean_first_exit_net_return,
        "median_first_exit_net_return": statistics.median(first_exit_net_returns) if first_exit_net_returns else None,
        "median_exit_days": median_exit_days,
        "p90_exit_days": _percentile(profitable_exit_days, 0.9),
        "median_max_adverse_net_return": statistics.median(adverse_returns) if adverse_returns else None,
        "worst_max_adverse_net_return": min(adverse_returns) if adverse_returns else None,
        "edge_per_day": edge_per_day,
        "historical_score": historical_score,
        "sample_signal_rows": signal_rows[:5],
    }


def _passes_floor_filters(
    row: dict[str, Any],
    *,
    min_history_days: int,
    min_signal_count: int,
    min_win_rate: float,
    min_mean_first_exit_net_return: float,
    min_median_daily_turnover_isk: float,
    min_latest_daily_turnover_isk: float,
    min_median_daily_volume: float,
) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    if int(row.get("history_day_count") or 0) < int(min_history_days):
        reasons.append("history_day_count below threshold")
    if int(row.get("signal_count") or 0) < int(min_signal_count):
        reasons.append("signal_count below threshold")
    if float(row.get("win_rate") or 0.0) < float(min_win_rate):
        reasons.append("win_rate below threshold")
    if float(row.get("robust_mean_first_exit_net_return") or 0.0) < float(min_mean_first_exit_net_return):
        reasons.append("robust_mean_first_exit_net_return below threshold")
    if float(row.get("median_daily_turnover_isk_30d") or 0.0) < float(min_median_daily_turnover_isk):
        reasons.append("median_daily_turnover_isk_30d below threshold")
    if float(row.get("latest_daily_turnover_isk") or 0.0) < float(min_latest_daily_turnover_isk):
        reasons.append("latest_daily_turnover_isk below threshold")
    if float(row.get("median_daily_volume_30d") or 0.0) < float(min_median_daily_volume):
        reasons.append("median_daily_volume_30d below threshold")
    return len(reasons) == 0, reasons


def _render_summary_table(rows: list[dict[str, Any]]) -> list[list[str]]:
    rendered: list[list[str]] = []
    for index, row in enumerate(rows, start=1):
        rendered.append(
            [
                str(index),
                str(row["type_name"]),
                str(row["market_group"]),
                _format_isk_human(row.get("latest_price")),
                _format_number(row.get("latest_entry_zscore")),
                _format_percent(row.get("win_rate")),
                _format_percent(row.get("robust_mean_first_exit_net_return")),
                _format_number(row.get("median_exit_days")),
                _format_number(row.get("p90_exit_days")),
                _format_isk_human(row.get("median_daily_turnover_isk_30d")),
                _format_number(row.get("historical_score"), digits=4),
            ]
        )
    return rendered


def _rank_history_rows(rows: list[dict[str, Any]], *, top_n: int) -> list[dict[str, Any]]:
    ranked = sorted(
        rows,
        key=lambda row: (
            float(row.get("historical_score") or 0.0),
            float(row.get("win_rate") or 0.0),
            float(row.get("robust_mean_first_exit_net_return") or 0.0),
            -float(row.get("median_exit_days") or 999999.0),
            float(row.get("median_daily_turnover_isk_30d") or 0.0),
            str(row.get("type_name") or ""),
        ),
        reverse=True,
    )
    return ranked[:top_n]


def build_history_reservoir_backtest_report(
    connection: sqlite3.Connection,
    *,
    output_root: Path,
    region_id: int,
    selected_targets: list[dict[str, Any]] | None,
    lookback_days: int,
    max_hold_days: int,
    entry_zscore_threshold: float,
    min_entry_price_vs_mean_ratio: float,
    target_net_return: float,
    min_history_days: int,
    min_signal_count: int,
    min_win_rate: float,
    min_mean_first_exit_net_return: float,
    min_median_daily_turnover_isk: float,
    min_latest_daily_turnover_isk: float,
    min_median_daily_volume: float,
    top_n: int,
    top_current_n: int,
    fee_config: FeeConfig,
) -> dict[str, Any]:
    target_rows = _load_target_rows(connection, anchor_type_id=-1, selected_targets=selected_targets)
    target_type_ids = {int(row["type_id"]) for row in target_rows}
    target_meta_map = _load_item_meta_map(connection, type_ids=target_type_ids)
    history_series_map = load_canonical_history_series_map(connection, region_id=region_id, type_ids=target_type_ids)

    summary_rows: list[dict[str, Any]] = []
    rejection_reason_counts: dict[str, int] = {}
    for type_id in sorted(target_type_ids):
        series = history_series_map.get(type_id)
        target_meta = target_meta_map.get(type_id)
        if not series or target_meta is None:
            continue
        summary = _summarize_series_backtest(
            series=series,
            type_id=type_id,
            type_name=str(target_meta["type_name"]),
            market_group=str(target_meta["market_group"]),
            market_group_id=target_meta.get("market_group_id"),
            region_id=region_id,
            lookback_days=lookback_days,
            entry_zscore_threshold=entry_zscore_threshold,
            min_entry_price_vs_mean_ratio=min_entry_price_vs_mean_ratio,
            max_hold_days=max_hold_days,
            target_net_return=target_net_return,
            fee_config=fee_config,
        )
        if summary is None:
            continue
        passed, reasons = _passes_floor_filters(
            summary,
            min_history_days=min_history_days,
            min_signal_count=min_signal_count,
            min_win_rate=min_win_rate,
            min_mean_first_exit_net_return=min_mean_first_exit_net_return,
            min_median_daily_turnover_isk=min_median_daily_turnover_isk,
            min_latest_daily_turnover_isk=min_latest_daily_turnover_isk,
            min_median_daily_volume=min_median_daily_volume,
        )
        summary["filter_passed"] = passed
        summary["filter_reasons"] = reasons
        for reason in reasons:
            rejection_reason_counts[reason] = rejection_reason_counts.get(reason, 0) + 1
        summary_rows.append(summary)

    eligible_rows = [row for row in summary_rows if bool(row.get("filter_passed"))]
    historical_top_rows = _rank_history_rows(eligible_rows, top_n=top_n)
    current_signal_rows = [
        row for row in eligible_rows if bool(row.get("latest_signal_active"))
    ]
    current_top_rows = _rank_history_rows(current_signal_rows, top_n=top_current_n)

    report_run_id = create_run_id(prefix="history-reservoir-backtest")
    report_run_dir = ensure_run_dir(output_root, report_run_id)
    markdown_path = report_run_dir / "history_reservoir_backtest_report.md"
    json_path = report_run_dir / "history_reservoir_backtest_report.json"
    csv_path = report_run_dir / "history_reservoir_backtest_report.csv"

    with csv_path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "type_id",
                "type_name",
                "market_group",
                "history_day_count",
                "signal_count",
                "profitable_signal_count",
                "win_rate",
                "mean_best_net_return",
                "robust_mean_best_net_return",
                "mean_first_exit_net_return",
                "robust_mean_first_exit_net_return",
                "median_exit_days",
                "p90_exit_days",
                "median_daily_turnover_isk_30d",
                "latest_daily_turnover_isk",
                "median_daily_volume_30d",
                "latest_price",
                "latest_entry_zscore",
                "latest_signal_active",
                "latest_break_even_exit_price",
                "historical_score",
                "filter_passed",
                "filter_reasons",
            ],
        )
        writer.writeheader()
        for row in _rank_history_rows(summary_rows, top_n=len(summary_rows)):
            writer.writerow(
                {
                    "type_id": row["type_id"],
                    "type_name": row["type_name"],
                    "market_group": row["market_group"],
                    "history_day_count": row["history_day_count"],
                    "signal_count": row["signal_count"],
                    "profitable_signal_count": row["profitable_signal_count"],
                    "win_rate": row["win_rate"],
                    "mean_best_net_return": row["mean_best_net_return"],
                    "robust_mean_best_net_return": row["robust_mean_best_net_return"],
                    "mean_first_exit_net_return": row["mean_first_exit_net_return"],
                    "robust_mean_first_exit_net_return": row["robust_mean_first_exit_net_return"],
                    "median_exit_days": row["median_exit_days"],
                    "p90_exit_days": row["p90_exit_days"],
                    "median_daily_turnover_isk_30d": row["median_daily_turnover_isk_30d"],
                    "latest_daily_turnover_isk": row["latest_daily_turnover_isk"],
                    "median_daily_volume_30d": row["median_daily_volume_30d"],
                    "latest_price": row["latest_price"],
                    "latest_entry_zscore": row["latest_entry_zscore"],
                    "latest_signal_active": row["latest_signal_active"],
                    "latest_break_even_exit_price": row["latest_break_even_exit_price"],
                    "historical_score": row["historical_score"],
                    "filter_passed": row["filter_passed"],
                    "filter_reasons": "；".join(str(item) for item in row.get("filter_reasons") or []),
                }
            )

    report_payload = {
        "report_name": "history_reservoir_backtest_report",
        "report_generated_at_utc": _utc_now_iso(),
        "report_run_id": report_run_id,
        "report_run_dir": str(report_run_dir),
        "region_id": int(region_id),
        "region_label": f"region:{region_id}",
        "settings": {
            "lookback_days": int(lookback_days),
            "max_hold_days": int(max_hold_days),
            "entry_zscore_threshold": float(entry_zscore_threshold),
            "min_entry_price_vs_mean_ratio": float(min_entry_price_vs_mean_ratio),
            "target_net_return": float(target_net_return),
            "min_history_days": int(min_history_days),
            "min_signal_count": int(min_signal_count),
            "min_win_rate": float(min_win_rate),
            "min_mean_first_exit_net_return": float(min_mean_first_exit_net_return),
            "min_median_daily_turnover_isk": float(min_median_daily_turnover_isk),
            "min_latest_daily_turnover_isk": float(min_latest_daily_turnover_isk),
            "min_median_daily_volume": float(min_median_daily_volume),
        },
        "fee_config": fee_config.to_dict(),
        "coverage_summary": {
            "target_type_count": len(target_rows),
            "history_type_count": len(summary_rows),
            "eligible_type_count": len(eligible_rows),
            "current_signal_type_count": len(current_signal_rows),
        },
        "history_source": _format_history_source(describe_history_source(region_id=region_id, type_id=int(historical_top_rows[0]["type_id"]))) if historical_top_rows else "unknown",
        "historical_top_rows": historical_top_rows,
        "current_top_rows": current_top_rows,
        "rejection_reason_counts": rejection_reason_counts,
        "files": {
            "markdown_path": str(markdown_path),
            "json_path": str(json_path),
            "csv_path": str(csv_path),
        },
    }
    write_json(json_path, report_payload)

    markdown_lines = [
        "# History-Only 蓄水池回测报告",
        "",
        "这份报告只使用 `history`，不读取订单簿。",
        "",
        "核心问题不是“预测它会不会涨”，而是：",
        "- 历史上，当商品跌到相对低位时，未来若干天的自然波动，是否经常足以让蓄水池盈利退出。",
        "- 这个商品是否属于天然适合做蓄水池的品种。",
        "",
        "## 回测口径",
        "",
        f"- 入场信号: 相对前 `{lookback_days}` 天均价的 `zscore <= {entry_zscore_threshold:.2f}`",
        f"- 价格带异常保护: 入场价至少要达到前 `{lookback_days}` 天均价的 `{min_entry_price_vs_mean_ratio:.0%}`，极端错价日直接忽略",
        "- 入场价代理: 信号日 `average_price`",
        "- 退出可行性代理: 未来持有窗口内的 `highest_price`",
        "- 收益排序口径: 使用对异常值做过压缩的 `稳健平均首次可放水净收益`，不再拿窗口内最好一天给自己贴金",
        f"- 最大持有天数: `{max_hold_days}`",
        f"- 目标净收益门槛: `{target_net_return:.2%}`",
        "- 费用已计入: 买侧经纪费、卖侧经纪费、销售税、重挂缓冲",
        "",
        "## 覆盖情况",
        "",
        f"- 目标商品数: `{len(target_rows)}`",
        f"- 有有效 history 的商品数: `{len(summary_rows)}`",
        f"- 通过历史回测和流动性底线的商品数: `{len(eligible_rows)}`",
        f"- 最新一日仍处于蓄水信号的商品数: `{len(current_signal_rows)}`",
        "",
        "## 当前仍处于蓄水信号的 Top",
        "",
        *_render_table(
            [
                "序号",
                "商品",
                "市场组",
                "最新价",
                "最新zscore",
                "历史胜率",
                "稳健平均首次可放水净收益",
                "中位退出天数",
                "P90退出天数",
                "近30天中位日成交额",
                "历史分数",
            ],
            _render_summary_table(current_top_rows),
        ),
        "",
        "## 历史上最适合做蓄水池的 Top",
        "",
        *_render_table(
            [
                "序号",
                "商品",
                "市场组",
                "最新价",
                "最新zscore",
                "历史胜率",
                "稳健平均首次可放水净收益",
                "中位退出天数",
                "P90退出天数",
                "近30天中位日成交额",
                "历史分数",
            ],
            _render_summary_table(historical_top_rows),
        ),
        "",
        "## 主要淘汰原因",
        "",
    ]
    if rejection_reason_counts:
        for reason, count in sorted(rejection_reason_counts.items(), key=lambda item: (-item[1], item[0])):
            markdown_lines.append(f"- {reason}: `{count}`")
    else:
        markdown_lines.append("- 无")

    write_summary(markdown_path, markdown_lines)
    return report_payload
