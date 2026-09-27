from __future__ import annotations

import csv
import math
import sqlite3
import statistics
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .market_scope import describe_history_source, load_canonical_history_series
from .run_artifacts import create_run_id, ensure_run_dir, write_json, write_summary


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _safe_div(numerator: float, denominator: float) -> float:
    if denominator == 0:
        return 0.0
    return numerator / denominator


def _format_number(value: Any, digits: int = 4) -> str:
    try:
        return f"{float(value):,.{digits}f}"
    except (TypeError, ValueError):
        return "-"


def _format_percent(value: Any) -> str:
    try:
        return f"{float(value) * 100.0:.2f}%"
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


def _format_history_source(payload: dict[str, Any]) -> str:
    segments = list(payload.get("segments") or [])
    if not segments:
        return "unknown"
    formatted = []
    for segment in segments:
        source_region_id = int(segment["source_region_id"])
        market_scope = str(segment["market_scope"])
        start_date = segment.get("start_date_inclusive")
        end_date = segment.get("end_date_exclusive")
        window = []
        if start_date is not None:
            window.append(f">={start_date}")
        if end_date is not None:
            window.append(f"<{end_date}")
        window_label = " ".join(window) if window else "all"
        formatted.append(f"{source_region_id}:{market_scope}:{window_label}")
    return " + ".join(formatted)


def _render_table(headers: list[str], rows: list[list[str]]) -> list[str]:
    if not rows:
        return ["- 无"]
    divider = ["---" for _ in headers]
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(divider) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")
    return lines


def _load_region_name(connection: sqlite3.Connection, region_id: int) -> str:
    row = connection.execute(
        "SELECT location_name FROM market_location_catalog WHERE location_id = ? AND location_kind = 'region'",
        (region_id,),
    ).fetchone()
    if row is not None and row["location_name"]:
        return str(row["location_name"])
    return f"region:{region_id}"


def _load_type_name(connection: sqlite3.Connection, type_id: int) -> str:
    row = connection.execute(
        """
        SELECT type_name, type_name_zh, type_name_en
        FROM item_catalog
        WHERE type_id = ?
        """,
        (type_id,),
    ).fetchone()
    if row is None:
        return f"type:{type_id}"
    return str(row["type_name"] or row["type_name_zh"] or row["type_name_en"] or f"type:{type_id}")


def _load_history_series(
    connection: sqlite3.Connection,
    *,
    region_id: int,
    type_id: int,
) -> list[dict[str, Any]]:
    return load_canonical_history_series(
        connection,
        region_id=region_id,
        type_id=type_id,
    )


def _align_series(
    left_series: list[dict[str, Any]],
    right_series: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    left_map = {str(row["market_date"]): row for row in left_series}
    right_map = {str(row["market_date"]): row for row in right_series}
    overlap_dates = sorted(set(left_map) & set(right_map))
    aligned: list[dict[str, Any]] = []
    previous_left_price: float | None = None
    previous_right_price: float | None = None
    for market_date in overlap_dates:
        left_row = left_map[market_date]
        right_row = right_map[market_date]
        left_price = float(left_row["average_price"])
        right_price = float(right_row["average_price"])
        aligned.append(
            {
                "market_date": market_date,
                "left_price": left_price,
                "right_price": right_price,
                "left_volume": float(left_row["volume"]),
                "right_volume": float(right_row["volume"]),
                "left_turnover_isk": left_price * float(left_row["volume"]),
                "right_turnover_isk": right_price * float(right_row["volume"]),
                "left_order_count": int(left_row["order_count"]),
                "right_order_count": int(right_row["order_count"]),
                "left_fetched_at": str(left_row["fetched_at"]),
                "right_fetched_at": str(right_row["fetched_at"]),
                "price_ratio": _safe_div(right_price, left_price),
                "price_spread": right_price - left_price,
                "left_return": _safe_div(left_price, previous_left_price) - 1.0 if previous_left_price else None,
                "right_return": _safe_div(right_price, previous_right_price) - 1.0 if previous_right_price else None,
            }
        )
        previous_left_price = left_price
        previous_right_price = right_price
    return aligned


def _rank(values: list[float]) -> list[float]:
    indexed = sorted(enumerate(values), key=lambda item: item[1])
    ranks = [0.0] * len(values)
    start = 0
    while start < len(indexed):
        end = start + 1
        while end < len(indexed) and indexed[end][1] == indexed[start][1]:
            end += 1
        avg_rank = (start + 1 + end) / 2.0
        for offset in range(start, end):
            ranks[indexed[offset][0]] = avg_rank
        start = end
    return ranks


def _pearson(x_values: list[float], y_values: list[float]) -> float:
    if len(x_values) != len(y_values) or len(x_values) < 2:
        return 0.0
    x_mean = statistics.fmean(x_values)
    y_mean = statistics.fmean(y_values)
    x_var = sum((value - x_mean) ** 2 for value in x_values)
    y_var = sum((value - y_mean) ** 2 for value in y_values)
    if x_var <= 0.0 or y_var <= 0.0:
        return 0.0
    covariance = sum((x - x_mean) * (y - y_mean) for x, y in zip(x_values, y_values))
    return covariance / math.sqrt(x_var * y_var)


def _spearman(x_values: list[float], y_values: list[float]) -> float:
    if len(x_values) != len(y_values) or len(x_values) < 2:
        return 0.0
    return _pearson(_rank(x_values), _rank(y_values))


def _ols_regression(x_values: list[float], y_values: list[float]) -> dict[str, float]:
    if len(x_values) != len(y_values) or len(x_values) < 2:
        return {"alpha": 0.0, "beta": 0.0, "r_squared": 0.0}
    x_mean = statistics.fmean(x_values)
    y_mean = statistics.fmean(y_values)
    x_var = sum((value - x_mean) ** 2 for value in x_values)
    if x_var <= 0.0:
        return {"alpha": y_mean, "beta": 0.0, "r_squared": 0.0}
    covariance = sum((x - x_mean) * (y - y_mean) for x, y in zip(x_values, y_values))
    beta = covariance / x_var
    alpha = y_mean - beta * x_mean
    fitted = [alpha + beta * x for x in x_values]
    ss_total = sum((y - y_mean) ** 2 for y in y_values)
    ss_residual = sum((y - fit) ** 2 for y, fit in zip(y_values, fitted))
    r_squared = 1.0 - _safe_div(ss_residual, ss_total) if ss_total > 0 else 0.0
    return {"alpha": alpha, "beta": beta, "r_squared": r_squared}


def _describe(values: list[float]) -> dict[str, float]:
    if not values:
        return {
            "count": 0.0,
            "mean": 0.0,
            "median": 0.0,
            "min": 0.0,
            "max": 0.0,
            "stddev": 0.0,
            "cv": 0.0,
        }
    mean_value = statistics.fmean(values)
    stddev = statistics.pstdev(values) if len(values) > 1 else 0.0
    return {
        "count": float(len(values)),
        "mean": mean_value,
        "median": statistics.median(values),
        "min": min(values),
        "max": max(values),
        "stddev": stddev,
        "cv": _safe_div(stddev, mean_value),
    }


def _window_zscore(values: list[float], window: int) -> float | None:
    if len(values) < window:
        return None
    sample = values[-window:]
    mean_value = statistics.fmean(sample)
    stddev = statistics.pstdev(sample) if len(sample) > 1 else 0.0
    if stddev <= 0:
        return 0.0
    return (sample[-1] - mean_value) / stddev


def _rolling_corr(x_values: list[float], y_values: list[float], window: int) -> float | None:
    if len(x_values) != len(y_values) or len(x_values) < window:
        return None
    return _pearson(x_values[-window:], y_values[-window:])


def _lagged_corr(x_values: list[float], y_values: list[float], lag: int) -> dict[str, Any]:
    if lag > 0:
        left = x_values[:-lag]
        right = y_values[lag:]
    elif lag < 0:
        left = x_values[-lag:]
        right = y_values[:lag]
    else:
        left = x_values
        right = y_values
    if len(left) < 2 or len(right) < 2:
        return {"lag": lag, "corr": 0.0, "sample_count": 0}
    return {"lag": lag, "corr": _pearson(left, right), "sample_count": len(left)}


def _residual_half_life(residuals: list[float]) -> float | None:
    if len(residuals) < 3:
        return None
    lagged = residuals[:-1]
    current = residuals[1:]
    regression = _ols_regression(lagged, current)
    phi = float(regression["beta"])
    if phi <= 0.0 or phi >= 1.0:
        return None
    return -math.log(2.0) / math.log(phi)


def _build_narrative(report: dict[str, Any]) -> list[str]:
    summary = dict(report["statistics"]["summary"])
    lag_best = dict(report["statistics"]["lag_correlation_best"])
    regression = dict(report["statistics"]["price_regression"])
    lines = [
        (
            f"{report['right_label']} 相对 {report['left_label']} 的价格水平相关系数为 "
            f"{summary['price_pearson_corr']:.4f}，收益率相关系数为 {summary['return_pearson_corr']:.4f}。"
        ),
        (
            f"线性回归（{report['right_label']} = alpha + beta * {report['left_label']}）得到 "
            f"alpha={regression['alpha']:.2f}，beta={regression['beta']:.6f}，R²={regression['r_squared']:.4f}。"
        ),
        (
            f"最新价格比值为 {summary['latest_price_ratio']:.6f}，"
            f"相对 30 日窗口 z-score 为 {summary['latest_ratio_zscore_30d'] if summary['latest_ratio_zscore_30d'] is not None else 0.0:.4f}。"
        ),
    ]
    lag = int(lag_best.get("lag") or 0)
    corr = float(lag_best.get("corr") or 0.0)
    if lag > 0:
        lines.append(f"收益率滞后相关的最强点出现在 +{lag} 天，相关系数 {corr:.4f}，更像是 {report['left_label']} 领先 {report['right_label']}。")
    elif lag < 0:
        lines.append(f"收益率滞后相关的最强点出现在 {lag} 天，相关系数 {corr:.4f}，更像是 {report['right_label']} 领先 {report['left_label']}。")
    else:
        lines.append(f"收益率滞后相关的最强点就在同期，相关系数 {corr:.4f}。")
    half_life = report["statistics"]["residual_mean_reversion"].get("half_life_days")
    if half_life is not None:
        lines.append(f"回归残差的 AR(1) 均值回复半衰期估算约为 {half_life:.2f} 天。")
    else:
        lines.append("回归残差没有给出稳定的正半衰期，说明这段样本下残差均值回复不够稳定。")
    return lines


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fieldnames = [
        "market_date",
        "left_price",
        "right_price",
        "left_return",
        "right_return",
        "left_volume",
        "right_volume",
        "left_turnover_isk",
        "right_turnover_isk",
        "price_ratio",
        "price_spread",
        "residual",
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field) for field in fieldnames})


def build_history_pair_relationship_report(
    connection: sqlite3.Connection,
    *,
    output_root: Path,
    region_id: int,
    left_type_id: int,
    right_type_id: int,
    left_label: str | None = None,
    right_label: str | None = None,
    max_lag_days: int = 14,
) -> dict[str, Any]:
    left_name = left_label or _load_type_name(connection, left_type_id)
    right_name = right_label or _load_type_name(connection, right_type_id)
    left_series = _load_history_series(connection, region_id=region_id, type_id=left_type_id)
    right_series = _load_history_series(connection, region_id=region_id, type_id=right_type_id)
    if not left_series or not right_series:
        raise RuntimeError("缺少至少一侧的 history 数据，无法生成双物品关系报告。")

    aligned = _align_series(left_series, right_series)
    if len(aligned) < 30:
        raise RuntimeError(f"重叠样本只有 {len(aligned)} 天，低于最小分析要求 30 天。")

    left_prices = [float(row["left_price"]) for row in aligned]
    right_prices = [float(row["right_price"]) for row in aligned]
    left_returns = [float(row["left_return"]) for row in aligned if row["left_return"] is not None]
    right_returns = [float(row["right_return"]) for row in aligned if row["right_return"] is not None]
    ratios = [float(row["price_ratio"]) for row in aligned]
    spreads = [float(row["price_spread"]) for row in aligned]
    regression = _ols_regression(left_prices, right_prices)
    residuals = [
        float(row["right_price"]) - (float(regression["alpha"]) + float(regression["beta"]) * float(row["left_price"]))
        for row in aligned
    ]
    for row, residual in zip(aligned, residuals):
        row["residual"] = residual

    return_pairs = [
        (float(row["left_return"]), float(row["right_return"]))
        for row in aligned
        if row["left_return"] is not None and row["right_return"] is not None
    ]
    lag_table = [
        _lagged_corr(left_returns, right_returns, lag)
        for lag in range(-max_lag_days, max_lag_days + 1)
    ]
    lag_best = max(lag_table, key=lambda row: abs(float(row["corr"])))

    statistics_payload = {
        "left_price": _describe(left_prices),
        "right_price": _describe(right_prices),
        "left_return": _describe(left_returns),
        "right_return": _describe(right_returns),
        "price_ratio": _describe(ratios),
        "price_spread": _describe(spreads),
        "residual": _describe(residuals),
        "summary": {
            "price_pearson_corr": _pearson(left_prices, right_prices),
            "price_spearman_corr": _spearman(left_prices, right_prices),
            "return_pearson_corr": _pearson(
                [pair[0] for pair in return_pairs],
                [pair[1] for pair in return_pairs],
            ),
            "return_spearman_corr": _spearman(
                [pair[0] for pair in return_pairs],
                [pair[1] for pair in return_pairs],
            ),
            "latest_left_price": left_prices[-1],
            "latest_right_price": right_prices[-1],
            "latest_price_ratio": ratios[-1],
            "latest_price_spread": spreads[-1],
            "latest_ratio_zscore_30d": _window_zscore(ratios, 30),
            "latest_ratio_zscore_90d": _window_zscore(ratios, 90),
            "latest_spread_zscore_30d": _window_zscore(spreads, 30),
            "latest_spread_zscore_90d": _window_zscore(spreads, 90),
        },
        "price_regression": regression,
        "residual_mean_reversion": {
            "half_life_days": _residual_half_life(residuals),
            "lag1_autocorr": _pearson(residuals[:-1], residuals[1:]) if len(residuals) > 2 else 0.0,
        },
        "rolling": {
            "price_corr_7d": _rolling_corr(left_prices, right_prices, 7),
            "price_corr_30d": _rolling_corr(left_prices, right_prices, 30),
            "price_corr_90d": _rolling_corr(left_prices, right_prices, 90),
            "return_corr_7d": _rolling_corr(left_returns, right_returns, 7),
            "return_corr_30d": _rolling_corr(left_returns, right_returns, 30),
            "return_corr_90d": _rolling_corr(left_returns, right_returns, 90),
        },
        "lag_correlation": lag_table,
        "lag_correlation_best": lag_best,
    }

    run_id = create_run_id("history-pair")
    run_dir = ensure_run_dir(output_root, run_id)
    markdown_path = run_dir / "history_pair_relationship_report.md"
    json_path = run_dir / "history_pair_relationship_report.json"
    csv_path = run_dir / "history_pair_relationship_series.csv"

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
        "left_history_source": describe_history_source(region_id=region_id, type_id=left_type_id),
        "right_history_source": describe_history_source(region_id=region_id, type_id=right_type_id),
        "data_window": {
            "overlap_day_count": len(aligned),
            "market_date_min": aligned[0]["market_date"],
            "market_date_max": aligned[-1]["market_date"],
            "left_history_count": len(left_series),
            "right_history_count": len(right_series),
            "left_fetched_at_max": max(str(row["fetched_at"]) for row in left_series),
            "right_fetched_at_max": max(str(row["fetched_at"]) for row in right_series),
        },
        "statistics": statistics_payload,
        "narrative": _build_narrative(
            {
                "statistics": statistics_payload,
                "left_label": left_name,
                "right_label": right_name,
            }
        ),
        "recent_aligned_rows": aligned[-30:],
        "files": {
            "markdown_path": str(markdown_path),
            "json_path": str(json_path),
            "csv_path": str(csv_path),
        },
    }

    lines: list[str] = [
        f"# {left_name} / {right_name} 历史关系报告",
        "",
        f"- 星域: {report['region_name']} ({region_id})",
        f"- 左侧物品: {left_name} ({left_type_id})",
        f"- 右侧物品: {right_name} ({right_type_id})",
        f"- 重叠样本区间: {report['data_window']['market_date_min']} 到 {report['data_window']['market_date_max']}",
        f"- 重叠样本天数: {report['data_window']['overlap_day_count']}",
        f"- 左侧 history 最新抓取: {report['data_window']['left_fetched_at_max']}",
        f"- 右侧 history 最新抓取: {report['data_window']['right_fetched_at_max']}",
        f"- 左侧 history 口径: {_format_history_source(report['left_history_source'])}",
        f"- 右侧 history 口径: {_format_history_source(report['right_history_source'])}",
        "",
        "## 核心结论",
        "",
    ]
    lines.extend([f"- {line}" for line in report["narrative"]])
    lines.extend(["", "## 水平统计", ""])
    level_rows = [
        [
            left_name,
            _format_isk_human(report["statistics"]["left_price"]["mean"]),
            _format_isk_human(report["statistics"]["left_price"]["median"]),
            _format_isk_human(report["statistics"]["left_price"]["min"]),
            _format_isk_human(report["statistics"]["left_price"]["max"]),
            _format_percent(report["statistics"]["left_price"]["cv"]),
        ],
        [
            right_name,
            _format_isk_human(report["statistics"]["right_price"]["mean"]),
            _format_isk_human(report["statistics"]["right_price"]["median"]),
            _format_isk_human(report["statistics"]["right_price"]["min"]),
            _format_isk_human(report["statistics"]["right_price"]["max"]),
            _format_percent(report["statistics"]["right_price"]["cv"]),
        ],
    ]
    lines.extend(_render_table(["物品", "均价", "中位价", "最低价", "最高价", "价格 CV"], level_rows))
    lines.extend(["", "## 相关性与回归", ""])
    corr_rows = [
        ["价格 Pearson", _format_number(report["statistics"]["summary"]["price_pearson_corr"], 6)],
        ["价格 Spearman", _format_number(report["statistics"]["summary"]["price_spearman_corr"], 6)],
        ["收益率 Pearson", _format_number(report["statistics"]["summary"]["return_pearson_corr"], 6)],
        ["收益率 Spearman", _format_number(report["statistics"]["summary"]["return_spearman_corr"], 6)],
        ["回归 alpha", _format_number(report["statistics"]["price_regression"]["alpha"], 6)],
        ["回归 beta", _format_number(report["statistics"]["price_regression"]["beta"], 6)],
        ["回归 R²", _format_number(report["statistics"]["price_regression"]["r_squared"], 6)],
    ]
    lines.extend(_render_table(["指标", "结果"], corr_rows))
    lines.extend(["", "## 比值 / 价差 / 残差", ""])
    spread_rows = [
        ["最新价格比值", _format_number(report["statistics"]["summary"]["latest_price_ratio"], 6)],
        ["最新价格价差", _format_isk_human(report["statistics"]["summary"]["latest_price_spread"])],
        ["比值 30d z-score", _format_number(report["statistics"]["summary"]["latest_ratio_zscore_30d"], 6)],
        ["比值 90d z-score", _format_number(report["statistics"]["summary"]["latest_ratio_zscore_90d"], 6)],
        ["价差 30d z-score", _format_number(report["statistics"]["summary"]["latest_spread_zscore_30d"], 6)],
        ["价差 90d z-score", _format_number(report["statistics"]["summary"]["latest_spread_zscore_90d"], 6)],
        ["残差半衰期", _format_number(report["statistics"]["residual_mean_reversion"]["half_life_days"], 4)],
        ["残差 lag1 自相关", _format_number(report["statistics"]["residual_mean_reversion"]["lag1_autocorr"], 6)],
    ]
    lines.extend(_render_table(["指标", "结果"], spread_rows))
    lines.extend(["", "## 滚动窗口相关", ""])
    rolling_rows = [
        ["价格相关 7d", _format_number(report["statistics"]["rolling"]["price_corr_7d"], 6)],
        ["价格相关 30d", _format_number(report["statistics"]["rolling"]["price_corr_30d"], 6)],
        ["价格相关 90d", _format_number(report["statistics"]["rolling"]["price_corr_90d"], 6)],
        ["收益率相关 7d", _format_number(report["statistics"]["rolling"]["return_corr_7d"], 6)],
        ["收益率相关 30d", _format_number(report["statistics"]["rolling"]["return_corr_30d"], 6)],
        ["收益率相关 90d", _format_number(report["statistics"]["rolling"]["return_corr_90d"], 6)],
    ]
    lines.extend(_render_table(["指标", "结果"], rolling_rows))
    lines.extend(["", "## 滞后相关（收益率）", ""])
    lag_rows = [
        [
            f"{int(row['lag']):+d}",
            _format_number(row["corr"], 6),
            str(int(row["sample_count"])),
        ]
        for row in sorted(
            report["statistics"]["lag_correlation"],
            key=lambda row: (-abs(float(row["corr"])), abs(int(row["lag"]))),
        )[:10]
    ]
    lines.extend(_render_table(["滞后天数", "相关系数", "样本数"], lag_rows))
    lines.extend(["", "## 最近 30 天对齐样本", ""])
    recent_rows = [
        [
            row["market_date"],
            _format_isk_human(row["left_price"]),
            _format_isk_human(row["right_price"]),
            _format_number(row["price_ratio"], 6),
            _format_isk_human(row["price_spread"]),
            _format_percent(row["left_return"]) if row["left_return"] is not None else "-",
            _format_percent(row["right_return"]) if row["right_return"] is not None else "-",
            _format_isk_human(row["residual"]),
        ]
        for row in report["recent_aligned_rows"]
    ]
    lines.extend(
        _render_table(
            ["日期", left_name, right_name, "价格比值", "价格价差", f"{left_name}收益率", f"{right_name}收益率", "回归残差"],
            recent_rows,
        )
    )
    lines.extend(["", "## 文件", ""])
    lines.extend(
        [
            f"- Markdown: {markdown_path}",
            f"- JSON: {json_path}",
            f"- 对齐序列 CSV: {csv_path}",
        ]
    )

    write_summary(markdown_path, lines)
    write_json(json_path, report)
    _write_csv(csv_path, aligned)
    return report
