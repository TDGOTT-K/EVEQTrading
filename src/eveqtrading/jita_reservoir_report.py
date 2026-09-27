from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .report import resolve_runtime_roots
from .run_artifacts import create_run_id, ensure_run_dir, write_json, write_summary


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _safe_float(value: Any, *, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _format_pct(value: Any, *, digits: int = 2) -> str:
    return f"{_safe_float(value):.{digits}%}"


def _format_float(value: Any, *, digits: int = 2) -> str:
    return f"{_safe_float(value):.{digits}f}"


def _format_isk_human(value: Any) -> str:
    numeric = _safe_float(value)
    abs_numeric = abs(numeric)
    if abs_numeric >= 100_000_000.0:
        return f"{numeric / 100_000_000.0:.2f}亿"
    if abs_numeric >= 10_000.0:
        return f"{numeric / 10_000.0:.2f}万"
    return f"{numeric:.2f}"


def _read_json(path: Path | None) -> dict[str, Any] | list[Any] | None:
    if path is None or not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _coerce_path(raw_value: Any) -> Path | None:
    if not isinstance(raw_value, str) or not raw_value:
        return None
    return Path(raw_value)


def _fetch_recent_scan_rows(connection: sqlite3.Connection, *, limit: int = 30) -> list[sqlite3.Row]:
    return list(
        connection.execute(
            """
            SELECT scan_run_id, started_at, finished_at, market_scope, strategy_set,
                   capital_budget_isk, parameter_version, input_path, output_dir,
                   selected_candidate_count, status
            FROM scan_run
            WHERE status = 'completed'
            ORDER BY started_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    )


def _fetch_scan_row(connection: sqlite3.Connection, *, scan_run_id: str) -> sqlite3.Row | None:
    return connection.execute(
        """
        SELECT scan_run_id, started_at, finished_at, market_scope, strategy_set,
               capital_budget_isk, parameter_version, input_path, output_dir,
               selected_candidate_count, status
        FROM scan_run
        WHERE scan_run_id = ? AND status = 'completed'
        """,
        (scan_run_id,),
    ).fetchone()


def _resolve_scan_output_dir(scan_row: sqlite3.Row, runs_root: Path) -> Path:
    raw_output_dir = _coerce_path(scan_row["output_dir"])
    if raw_output_dir is not None and raw_output_dir.exists():
        return raw_output_dir
    fallback = runs_root / str(scan_row["scan_run_id"])
    if fallback.exists():
        return fallback
    return raw_output_dir or fallback


def _artifact_paths(output_dir: Path) -> dict[str, str]:
    return {
        "analysis_run_path": str(output_dir / "analysis_run.json"),
        "calc_trace_path": str(output_dir / "calc_trace.json"),
        "candidate_table_path": str(output_dir / "candidate_table.json"),
        "feature_table_path": str(output_dir / "feature_table.json"),
        "skipped_table_path": str(output_dir / "skipped_table.json"),
        "summary_path": str(output_dir / "summary.md"),
    }


def _artifact_health(paths: dict[str, str]) -> dict[str, Any]:
    missing_required: list[str] = []
    health: dict[str, Any] = {}
    for key, raw_path in paths.items():
        exists = bool(raw_path) and Path(raw_path).exists()
        health[f"{key}_exists"] = exists
        if key in {"analysis_run_path", "calc_trace_path"} and not exists:
            missing_required.append(key)
    health["missing_required"] = missing_required
    health["report_ready"] = not missing_required
    return health


def _count_selected_reservoir(connection: sqlite3.Connection, *, scan_run_id: str) -> int:
    row = connection.execute(
        """
        SELECT COUNT(*) AS count
        FROM opportunity_snapshot
        WHERE scan_run_id = ? AND strategy_type = 'reservoir'
        """,
        (scan_run_id,),
    ).fetchone()
    return int(row["count"]) if row is not None else 0


def _load_reservoir_traces(calc_trace_payload: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not isinstance(calc_trace_payload, dict):
        return []
    traces: list[dict[str, Any]] = []
    for trace in list(calc_trace_payload.get("candidate_traces") or []):
        if not isinstance(trace, dict):
            continue
        if str(trace.get("strategy_pool") or "") != "reservoir":
            continue
        traces.append(trace)
    return traces


def _resolve_scan_for_reservoir(
    connection: sqlite3.Connection,
    *,
    db_path: Path,
    runs_root: Path | None,
    scan_run_id: str | None,
    prefer_latest_selected: bool,
) -> tuple[sqlite3.Row, Path, dict[str, str], dict[str, Any] | None, list[dict[str, Any]]]:
    effective_runs_root, _ = resolve_runtime_roots(db_path=db_path, runs_root=runs_root, log_dir=None)

    if scan_run_id is not None:
        scan_row = _fetch_scan_row(connection, scan_run_id=scan_run_id)
        if scan_row is None:
            raise SystemExit(f"Completed scan run not found: {scan_run_id}")
        output_dir = _resolve_scan_output_dir(scan_row, effective_runs_root)
        paths = _artifact_paths(output_dir)
        calc_trace_payload = _read_json(Path(paths["calc_trace_path"]))
        reservoir_traces = _load_reservoir_traces(calc_trace_payload if isinstance(calc_trace_payload, dict) else None)
        return scan_row, output_dir, paths, calc_trace_payload if isinstance(calc_trace_payload, dict) else None, reservoir_traces

    fallback: tuple[sqlite3.Row, Path, dict[str, str], dict[str, Any] | None, list[dict[str, Any]]] | None = None
    fallback_with_trace: tuple[sqlite3.Row, Path, dict[str, str], dict[str, Any] | None, list[dict[str, Any]]] | None = None

    for row in _fetch_recent_scan_rows(connection):
        output_dir = _resolve_scan_output_dir(row, effective_runs_root)
        paths = _artifact_paths(output_dir)
        calc_trace_payload = _read_json(Path(paths["calc_trace_path"]))
        typed_payload = calc_trace_payload if isinstance(calc_trace_payload, dict) else None
        reservoir_traces = _load_reservoir_traces(typed_payload)
        bundle = (row, output_dir, paths, typed_payload, reservoir_traces)

        if fallback is None:
            fallback = bundle
        if reservoir_traces and fallback_with_trace is None:
            fallback_with_trace = bundle

        if prefer_latest_selected and reservoir_traces and _count_selected_reservoir(connection, scan_run_id=str(row["scan_run_id"])) > 0:
            return bundle
        if not prefer_latest_selected and reservoir_traces:
            return bundle

    if fallback_with_trace is not None:
        return fallback_with_trace
    if fallback is not None:
        return fallback
    raise SystemExit("No completed scan run was found.")


def _fetch_reservoir_selected_rows(connection: sqlite3.Connection, *, scan_run_id: str) -> list[sqlite3.Row]:
    return list(
        connection.execute(
            """
            SELECT scan_run_id, opportunity_id, strategy_type, type_id, type_name, region_id,
                   buy_location_id, sell_location_id, entry_mode, exit_mode, unit_profit,
                   total_profit, profit_margin, capital_required, tradable_units,
                   expected_turnover_days, risk_adjusted_return, decision_reason
            FROM opportunity_snapshot
            WHERE scan_run_id = ? AND strategy_type = 'reservoir'
            ORDER BY risk_adjusted_return DESC, profit_margin DESC, total_profit DESC, opportunity_id
            """,
            (scan_run_id,),
        ).fetchall()
    )


def _find_trace_by_opportunity(
    reservoir_traces: list[dict[str, Any]],
    *,
    opportunity_id: str | None,
    type_id: int | None,
) -> dict[str, Any] | None:
    for trace in reservoir_traces:
        source_record = dict(trace.get("source_record") or {})
        if opportunity_id is not None and str(source_record.get("opportunity_id") or "") == opportunity_id:
            return trace
    if type_id is not None:
        for trace in reservoir_traces:
            if int(trace.get("type_id") or -1) == type_id:
                return trace
    return None


def _parse_iso(raw_value: Any) -> datetime | None:
    if not isinstance(raw_value, str) or not raw_value:
        return None
    try:
        parsed = datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _build_freshness(traces: list[dict[str, Any]]) -> dict[str, Any]:
    order_times = sorted(
        dt.isoformat().replace("+00:00", "Z")
        for dt in (_parse_iso((trace.get("source_record") or {}).get("source_order_captured_at")) for trace in traces)
        if dt is not None
    )
    history_dates = sorted(
        str((trace.get("source_record") or {}).get("history_latest_market_date") or "")
        for trace in traces
        if (trace.get("source_record") or {}).get("history_latest_market_date")
    )
    history_fetch_times = sorted(
        dt.isoformat().replace("+00:00", "Z")
        for dt in (_parse_iso((trace.get("source_record") or {}).get("history_latest_fetched_at")) for trace in traces)
        if dt is not None
    )
    return {
        "order_snapshot_time_min": order_times[0] if order_times else None,
        "order_snapshot_time_max": order_times[-1] if order_times else None,
        "history_market_date_min": history_dates[0] if history_dates else None,
        "history_market_date_max": history_dates[-1] if history_dates else None,
        "history_fetched_at_min": history_fetch_times[0] if history_fetch_times else None,
        "history_fetched_at_max": history_fetch_times[-1] if history_fetch_times else None,
    }


def _build_selected_entry(row: sqlite3.Row, trace: dict[str, Any] | None) -> dict[str, Any]:
    source_record = dict((trace or {}).get("source_record") or {})
    derived = dict((trace or {}).get("derived") or {})
    target_price = source_record.get("reservoir_target_price") or source_record.get("sell_reference_price")
    buy_price = source_record.get("buy_reference_price")
    return {
        "opportunity_id": row["opportunity_id"],
        "type_id": row["type_id"],
        "type_name": row["type_name"],
        "buy_location_name": source_record.get("buy_location_name"),
        "sell_location_name": source_record.get("sell_location_name"),
        "entry_mode": row["entry_mode"],
        "exit_mode": row["exit_mode"],
        "profit_margin": row["profit_margin"],
        "unit_profit": row["unit_profit"],
        "total_profit": row["total_profit"],
        "capital_required": row["capital_required"],
        "tradable_units": row["tradable_units"],
        "expected_turnover_days": row["expected_turnover_days"],
        "risk_adjusted_return": row["risk_adjusted_return"],
        "decision_reason": row["decision_reason"],
        "buy_reference_price": buy_price,
        "target_sell_price": target_price,
        "history_mean_30d": source_record.get("history_mean_30d"),
        "price_zscore_30d": derived.get("price_zscore_30d"),
        "volatility_30d": source_record.get("volatility_30d"),
        "avg_daily_isk_volume_30d": source_record.get("avg_daily_isk_volume_30d"),
        "active_days_ratio_30d": source_record.get("active_days_ratio_30d"),
        "avg_daily_units_7d": source_record.get("avg_daily_units_7d"),
        "estimated_exit_days_small_size_ask": source_record.get("estimated_exit_days_small_size_ask"),
        "max_position_isk": source_record.get("max_position_isk"),
        "recommended_test_position_isk": source_record.get("recommended_test_position_isk"),
        "inventory_limit_basis": source_record.get("inventory_limit_basis"),
        "reservoir_target_discount_rate": source_record.get("reservoir_target_discount_rate"),
        "source_order_snapshot_id": source_record.get("source_order_snapshot_id"),
        "source_order_captured_at": source_record.get("source_order_captured_at"),
        "history_latest_market_date": source_record.get("history_latest_market_date"),
        "history_latest_fetched_at": source_record.get("history_latest_fetched_at"),
    }


def _build_near_miss_entry(trace: dict[str, Any]) -> dict[str, Any]:
    source_record = dict(trace.get("source_record") or {})
    derived = dict(trace.get("derived") or {})
    filter_evaluation = dict(trace.get("filter_evaluation") or {})
    return {
        "opportunity_id": source_record.get("opportunity_id"),
        "type_id": trace.get("type_id"),
        "type_name": trace.get("type_name"),
        "buy_location_name": source_record.get("buy_location_name"),
        "sell_location_name": source_record.get("sell_location_name"),
        "profit_margin": derived.get("screened_profit_margin"),
        "expected_turnover_days": derived.get("estimated_exit_days_small_size"),
        "buy_reference_price": source_record.get("buy_reference_price"),
        "target_sell_price": source_record.get("reservoir_target_price") or source_record.get("sell_reference_price"),
        "history_mean_30d": source_record.get("history_mean_30d"),
        "price_zscore_30d": derived.get("price_zscore_30d"),
        "max_position_isk": source_record.get("max_position_isk"),
        "recommended_test_position_isk": source_record.get("recommended_test_position_isk"),
        "estimated_exit_days_small_size_ask": source_record.get("estimated_exit_days_small_size_ask"),
        "rejection_reasons": list(filter_evaluation.get("rejection_reasons") or []),
        "source_order_captured_at": source_record.get("source_order_captured_at"),
        "history_latest_market_date": source_record.get("history_latest_market_date"),
    }


def _render_table(rows: list[dict[str, Any]], *, selected: bool) -> str:
    headers = (
        [
            "商品",
            "买价",
            "目标卖价",
            "30天均价",
            "低估程度(zscore)",
            "利润率",
            "建议测试仓位",
            "持仓上限",
            "预计退出天数",
            "订单快照时间",
            "理由",
        ]
        if selected
        else [
            "商品",
            "买价",
            "目标卖价",
            "30天均价",
            "低估程度(zscore)",
            "利润率",
            "建议测试仓位",
            "持仓上限",
            "预计退出天数",
            "订单快照时间",
            "未通过原因",
        ]
    )

    def escape(value: Any) -> str:
        return str(value or "-").replace("|", "\\|").replace("\n", " ")

    rendered_rows: list[dict[str, Any]] = []
    for row in rows:
        rendered_rows.append(
            {
                "商品": row.get("type_name"),
                "买价": _format_isk_human(row.get("buy_reference_price")),
                "目标卖价": _format_isk_human(row.get("target_sell_price")),
                "30天均价": _format_isk_human(row.get("history_mean_30d")),
                "低估程度(zscore)": _format_float(row.get("price_zscore_30d")),
                "利润率": _format_pct(row.get("profit_margin"), digits=4),
                "建议测试仓位": _format_isk_human(row.get("recommended_test_position_isk") or row.get("capital_required")),
                "持仓上限": _format_isk_human(row.get("max_position_isk")),
                "预计退出天数": _format_float(
                    row.get("estimated_exit_days_small_size_ask") or row.get("expected_turnover_days")
                ),
                "订单快照时间": row.get("source_order_captured_at") or "-",
                "理由": row.get("decision_reason") or "-",
                "未通过原因": "；".join(str(item) for item in (row.get("rejection_reasons") or [])) or "-",
            }
        )

    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for row in rendered_rows:
        lines.append("| " + " | ".join(escape(row.get(header)) for header in headers) + " |")
    return "\n".join(lines)


def _render_lines(report: dict[str, Any]) -> list[str]:
    scan_summary = dict(report.get("scan_summary") or {})
    source_summary = dict(report.get("source_summary") or {})
    freshness = dict(report.get("data_freshness") or {})
    strategy_summary = dict(report.get("strategy_summary") or {})
    formula_versions = dict(report.get("formula_versions") or {})
    lines = [
        "# 吉他蓄水池策略报告",
        "",
        "这份报告严格按文档里的蓄水池定义来读：",
        "- 只看明显低于常态价格的货。",
        "- 买入不是赌长期上涨，而是把自己当成短期库存缓冲器。",
        "- 目标卖价不是当前卖一，而是回归到 30 天常态价附近。",
        "",
        "## 扫描基准",
        "",
        f"- 生成时间: {report.get('report_generated_at_utc') or '-'}",
        f"- 扫描批次: {scan_summary.get('scan_run_id') or '-'}",
        f"- 扫描范围: {scan_summary.get('market_scope') or '-'}",
        f"- 扫描策略集: {scan_summary.get('strategy_set') or '-'}",
        f"- 本次 reservoir 候选数: {strategy_summary.get('reservoir_candidate_count') or 0}",
        f"- 通过硬过滤的 reservoir 数: {strategy_summary.get('reservoir_passed_count') or 0}",
        f"- 实际入选 reservoir 数: {strategy_summary.get('reservoir_selected_count') or 0}",
        f"- Reservoir 候选来源文件: {source_summary.get('calc_trace_path') or '-'}",
        "",
        "## 数据时间基准",
        "",
        f"- 订单快照时间范围: {(freshness.get('order_snapshot_time_min') or '-')} -> {(freshness.get('order_snapshot_time_max') or '-')}",
        f"- History 截止日期范围: {(freshness.get('history_market_date_min') or '-')} -> {(freshness.get('history_market_date_max') or '-')}",
        f"- History 最近抓取时间范围: {(freshness.get('history_fetched_at_min') or '-')} -> {(freshness.get('history_fetched_at_max') or '-')}",
        "",
        "## 当前实现口径",
        "",
        f"- 蓄水池触发条件: `price_zscore_30d <= -1.0`",
        f"- 目标卖价公式: `{formula_versions.get('reservoir_target_price') or 'v2-min(history_mean_30d*0.99, history_high_max_7d*1.15)'}`",
        f"- 当前目标卖价代理: `min(history_mean_30d * 0.99, history_high_max_7d * 1.15)`",
        f"- 近期价格中枢护栏: `{formula_versions.get('reservoir_regime_guard') or 'v2-history_mean_7d-over-history_mean_30d-floor-0.75-and-buy-over-history_mean_7d-floor-0.70'}`",
        f"- 退出速度惩罚: `{formula_versions.get('reservoir_exit_penalty') or 'v2-estimated_exit_days_small_size_ask-times-max(1,target/buy)^2'}`",
        f"- 持仓上限口径: `max_position_isk`，当前记录值会随扫描结果一起回读",
        "",
        "## 已入选 Reservoir",
        "",
        str(report.get("selected_table_md") or "无"),
        "",
        "## Reservoir 近失之交臂",
        "",
        str(report.get("near_miss_table_md") or "无"),
        "",
    ]
    return lines


def build_jita_reservoir_report(
    connection: sqlite3.Connection,
    *,
    db_path: Path,
    runs_root: Path | None,
    log_dir: Path | None,
    output_root: Path | None,
    scan_run_id: str | None,
    prefer_latest_selected: bool,
    top_selected: int,
    top_near_misses: int,
) -> dict[str, Any]:
    effective_runs_root, _ = resolve_runtime_roots(db_path=db_path, runs_root=runs_root, log_dir=log_dir)
    scan_row, scan_output_dir, traceability_paths, calc_trace_payload, reservoir_traces = _resolve_scan_for_reservoir(
        connection,
        db_path=db_path,
        runs_root=runs_root,
        scan_run_id=scan_run_id,
        prefer_latest_selected=prefer_latest_selected,
    )

    selected_rows = _fetch_reservoir_selected_rows(connection, scan_run_id=str(scan_row["scan_run_id"]))
    selected_entries = [
        _build_selected_entry(
            row,
            _find_trace_by_opportunity(
                reservoir_traces,
                opportunity_id=str(row["opportunity_id"]),
                type_id=int(row["type_id"]),
            ),
        )
        for row in selected_rows[:top_selected]
    ]

    near_miss_entries = [
        _build_near_miss_entry(trace)
        for trace in reservoir_traces
        if not bool((trace.get("filter_evaluation") or {}).get("passed"))
    ]
    near_miss_entries.sort(
        key=lambda row: (
            int(len(row.get("rejection_reasons") or [])),
            -_safe_float(row.get("profit_margin")),
            _safe_float(row.get("expected_turnover_days"), default=999999.0),
            str(row.get("type_name") or ""),
        )
    )
    near_miss_entries = near_miss_entries[:top_near_misses]

    report_output_root = output_root or effective_runs_root
    run_id = create_run_id(prefix="jita-reservoir-report")
    run_dir = ensure_run_dir(report_output_root, run_id)
    markdown_path = run_dir / "jita_reservoir_report.md"
    json_path = run_dir / "jita_reservoir_report.json"

    formula_versions = dict((calc_trace_payload or {}).get("formula_versions") or {})
    data_freshness = _build_freshness(reservoir_traces)

    report = {
        "report_run_id": run_id,
        "report_run_dir": str(run_dir),
        "report_generated_at_utc": _utc_now_iso(),
        "scan_summary": {
            "scan_run_id": str(scan_row["scan_run_id"]),
            "started_at": scan_row["started_at"],
            "finished_at": scan_row["finished_at"],
            "market_scope": scan_row["market_scope"],
            "strategy_set": scan_row["strategy_set"],
            "selected_candidate_count": scan_row["selected_candidate_count"],
            "status": scan_row["status"],
            "scan_output_dir": str(scan_output_dir),
        },
        "source_summary": {
            "calc_trace_path": traceability_paths["calc_trace_path"],
            "analysis_run_path": traceability_paths["analysis_run_path"],
            "candidate_table_path": traceability_paths["candidate_table_path"],
        },
        "artifact_health": _artifact_health(traceability_paths),
        "data_freshness": data_freshness,
        "formula_versions": formula_versions,
        "strategy_summary": {
            "reservoir_candidate_count": len(reservoir_traces),
            "reservoir_passed_count": sum(
                1 for trace in reservoir_traces if bool((trace.get("filter_evaluation") or {}).get("passed"))
            ),
            "reservoir_selected_count": len(selected_rows),
        },
        "selected_entries": selected_entries,
        "near_miss_entries": near_miss_entries,
        "selected_table_md": _render_table(selected_entries, selected=True) if selected_entries else "无",
        "near_miss_table_md": _render_table(near_miss_entries, selected=False) if near_miss_entries else "无",
        "files": {
            "markdown_path": str(markdown_path),
            "json_path": str(json_path),
        },
    }

    write_summary(markdown_path, _render_lines(report))
    write_json(json_path, report)
    return report
