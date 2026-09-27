from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .advisor import build_market_advice


_RISK_PROFILE_ORDER = ("conservative", "standard", "aggressive")
_JITA_PASSIVE_ALLOWED_STRATEGY_SETS = frozenset({"jita-passive-simple", "jita-passive-full"})
_JITA_PASSIVE_ALLOWED_STRATEGY_TYPES = frozenset({"jita-passive-simple", "jita-passive-flip"})
_JITA_PASSIVE_ALLOWED_ENTRY_MODES = frozenset({"place_buy_order_best_bid"})
_JITA_PASSIVE_ALLOWED_EXIT_MODES = frozenset({"place_sell_order_best_ask"})


def _risk_profile_label_zh(name: str) -> str:
    return {
        "conservative": "保守",
        "standard": "标准",
        "aggressive": "激进",
    }.get(name, name)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _format_isk_human(amount: Any) -> str:
    try:
        numeric = float(amount or 0.0)
    except (TypeError, ValueError):
        return "-"
    abs_numeric = abs(numeric)
    if abs_numeric >= 100_000_000.0:
        return f"{numeric / 100_000_000.0:.2f}亿"
    if abs_numeric >= 10_000.0:
        return f"{numeric / 10_000.0:.2f}万"
    return f"{numeric:.2f}"


def _format_pct(value: Any, *, digits: int = 4) -> str:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return "-"
    return f"{numeric:.{digits}%}"


def _format_float(value: Any, *, digits: int = 2) -> str:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return "-"
    return f"{numeric:.{digits}f}"


def _position_status_text(status: Any) -> str:
    return {
        "allocate": "立即执行",
        "watch_only": "先观察",
        "budget_exhausted": "预算不足",
        "skip": "跳过",
    }.get(str(status or ""), "未知")


def _strategy_set_label_zh(strategy_set: Any) -> str:
    return {
        "jita-passive-simple": "吉他站内被动挂单简版扫描",
        "jita-passive-full": "吉他站内被动挂单完整版扫描",
    }.get(str(strategy_set or ""), str(strategy_set or "-"))


def _report_profile_label_zh(report_profile: str) -> str:
    return {
        "generic": "通用市场短名单",
        "jita-passive": "吉他站内挂买挂卖差价报告",
    }.get(report_profile, report_profile)


def _constraint_text_zh(binding_constraints: Any) -> str:
    values = [str(item) for item in (binding_constraints or []) if str(item)]
    if not values:
        return "-"
    mapped = {
        "remaining_budget": "总预算剩余不足",
        "budget_share_cap": "单品预算占比上限",
        "liquidity_cap": "流动性上限",
        "capital_required": "样本仓位上限",
        "max_position_isk": "旧版仓位上限",
        "recommended_deployable_isk": "单品建议上限",
        "recommended_deployable_isk_scaled": "扩容后单品上限",
        "theoretical_market_capacity_isk": "理论容量上限",
        "safe_inventory_cap_isk": "安全库存上限",
    }
    return "、".join(mapped.get(item, item) for item in values)


def _sell_overhang_summary_zh(row: dict[str, Any]) -> str:
    penalty_multiplier = row.get("sell_fill_penalty_multiplier")
    overhang_ratio = row.get("sell_price_overhang_ratio")
    steps = row.get("sell_price_overhang_steps")
    try:
        penalty_numeric = float(penalty_multiplier)
        overhang_numeric = float(overhang_ratio)
        step_count = int(steps or 0)
    except (TypeError, ValueError):
        return "-"
    if penalty_numeric >= 0.9999 or overhang_numeric < 0.20 or step_count <= 0:
        return "无"
    return f"高于7日高点{overhang_numeric:.2%}，卖出流量保留{penalty_numeric:.2%}"


def _sort_key(row: dict[str, Any]) -> tuple[int, int, float, str]:
    position_plan = dict(row.get("position_plan") or {})
    status = str(position_plan.get("status") or "")
    status_order = {
        "allocate": 0,
        "watch_only": 1,
        "budget_exhausted": 2,
        "skip": 3,
    }.get(status, 9)
    priority_rank = int(position_plan.get("priority_rank") or 999999)
    planned_profit_isk = float(position_plan.get("suggested_capital_isk") or 0.0) * float(row.get("profit_margin") or 0.0)
    return (status_order, priority_rank, -planned_profit_isk, str(row.get("opportunity_id") or ""))


def _semantic_mode_summary_zh(entry_mode: Any, exit_mode: Any) -> str:
    entry_label = {
        "place_buy_order_best_bid": "挂买单收货",
        "buy_best_ask": "直接买现货",
        "buy_remote_best_ask": "异地下单买现货",
    }.get(str(entry_mode or ""), str(entry_mode or "-"))
    exit_label = {
        "place_sell_order_best_ask": "挂卖单出货",
        "sell_best_bid": "直接卖给买单",
        "sell_remote_best_bid": "异地直接卖给买单",
        "sell_mean_reversion_target": "等回归目标价再卖",
    }.get(str(exit_mode or ""), str(exit_mode or "-"))
    return f"{entry_label} -> {exit_label}"


def _build_row(row: dict[str, Any]) -> dict[str, Any]:
    position_plan = dict(row.get("position_plan") or {})
    trace_refs = dict(row.get("trace_refs") or {})
    suggested_capital_isk = float(position_plan.get("suggested_capital_isk") or 0.0)
    profit_margin = float(row.get("profit_margin") or 0.0)
    planned_profit_isk = suggested_capital_isk * profit_margin

    machine_row = {
        "priority_rank": position_plan.get("priority_rank"),
        "type_id": row.get("type_id"),
        "type_name": row.get("type_name"),
        "position_status": position_plan.get("status"),
        "position_status_text": _position_status_text(position_plan.get("status")),
        "strategy_type": row.get("strategy_type"),
        "buy_location_id": row.get("buy_location_id"),
        "sell_location_id": row.get("sell_location_id"),
        "buy_location_name": row.get("buy_location_name"),
        "sell_location_name": row.get("sell_location_name"),
        "entry_mode": row.get("entry_mode"),
        "exit_mode": row.get("exit_mode"),
        "semantic_mode_summary_zh": _semantic_mode_summary_zh(row.get("entry_mode"), row.get("exit_mode")),
        "buy_price": row.get("buy_reference_price"),
        "sell_price": row.get("sell_reference_price"),
        "profit_margin": row.get("profit_margin"),
        "unit_profit": row.get("unit_profit"),
        "suggested_capital_isk": suggested_capital_isk,
        "base_recommended_deployable_isk": position_plan.get("base_recommended_deployable_isk")
        or row.get("recommended_deployable_isk"),
        "scaled_recommended_deployable_isk": position_plan.get("scaled_recommended_deployable_isk"),
        "position_cap_isk": position_plan.get("position_cap_isk") or row.get("recommended_deployable_isk"),
        "position_cap_source": position_plan.get("position_cap_source"),
        "deployable_cap_multiplier": position_plan.get("deployable_cap_multiplier"),
        "theoretical_market_capacity_isk": row.get("theoretical_market_capacity_isk")
        or row.get("daily_capacity_isk_30d"),
        "safe_exit_capacity_isk": row.get("safe_exit_capacity_isk"),
        "safe_inventory_cap_isk": row.get("safe_inventory_cap_isk"),
        "captured_sell_capacity_isk": row.get("captured_sell_capacity_isk"),
        "competition_adjusted_deployable_isk": row.get("competition_adjusted_deployable_isk"),
        "planned_profit_isk": planned_profit_isk,
        "estimated_capture_share": row.get("estimated_capture_share"),
        "competition_intensity": row.get("competition_intensity"),
        "expected_turnover_days": row.get("expected_turnover_days"),
        "flip_units_cv_30d": row.get("flip_units_cv_30d"),
        "flip_units_cv_7d": row.get("flip_units_cv_7d"),
        "estimated_daily_sell_isk_30d": row.get("estimated_daily_sell_isk_30d"),
        "sell_liquidity_confidence_score": row.get("sell_liquidity_confidence_score"),
        "sell_liquidity_confidence_label": row.get("sell_liquidity_confidence_label"),
        "dominant_attribution_mode_30d": row.get("dominant_attribution_mode_30d"),
        "sell_price_overhang_ratio": row.get("sell_price_overhang_ratio"),
        "sell_fill_penalty_multiplier": row.get("sell_fill_penalty_multiplier"),
        "binding_constraints": position_plan.get("binding_constraints") or [],
        "binding_constraints_text_zh": _constraint_text_zh(position_plan.get("binding_constraints") or []),
        "source_order_captured_at": trace_refs.get("source_order_captured_at"),
        "source_order_snapshot_id": trace_refs.get("source_order_snapshot_id"),
    }

    display_row = {
        "优先级": machine_row["priority_rank"],
        "商品": machine_row["type_name"],
        "状态": machine_row["position_status_text"],
        "买价": _format_isk_human(machine_row["buy_price"]),
        "卖价": _format_isk_human(machine_row["sell_price"]),
        "利润率": _format_pct(machine_row["profit_margin"]),
        "单件利润": _format_isk_human(machine_row["unit_profit"]),
        "建议建仓资金": _format_isk_human(machine_row["suggested_capital_isk"]),
        "基础建议上限": _format_isk_human(machine_row["base_recommended_deployable_isk"]),
        "当前仓位上限": _format_isk_human(machine_row["position_cap_isk"]),
        "理论容量": _format_isk_human(machine_row["theoretical_market_capacity_isk"]),
        "计划利润": _format_isk_human(machine_row["planned_profit_isk"]),
        "预计可吃份额": _format_pct(machine_row["estimated_capture_share"], digits=2),
        "竞争烈度": _format_float(machine_row["competition_intensity"]),
        "周转天数": _format_float(machine_row["expected_turnover_days"]),
        "30天波动": _format_pct(machine_row["flip_units_cv_30d"], digits=2),
        "7天波动": _format_pct(machine_row["flip_units_cv_7d"], digits=2),
        "卖价高悬": _sell_overhang_summary_zh(row),
        "主要约束": machine_row["binding_constraints_text_zh"],
        "订单快照时间": machine_row["source_order_captured_at"] or "-",
    }
    return {
        "machine": machine_row,
        "display_zh": display_row,
    }


def _validate_report_profile(
    *,
    report_profile: str,
    scan_summary: dict[str, Any],
    selected_rows_all: list[dict[str, Any]],
) -> dict[str, Any]:
    strategy_set = str(scan_summary.get("strategy_set") or "")
    if report_profile != "jita-passive":
        return {
            "report_profile": report_profile,
            "report_profile_label_zh": _report_profile_label_zh(report_profile),
            "strategy_set_label_zh": _strategy_set_label_zh(strategy_set),
            "semantic_guard_status": "not_enforced",
            "guard_notes_zh": ["本报告未启用吉他站内专用语义护栏。"],
        }

    if strategy_set not in _JITA_PASSIVE_ALLOWED_STRATEGY_SETS:
        allowed = ", ".join(sorted(_JITA_PASSIVE_ALLOWED_STRATEGY_SETS))
        raise ValueError(
            f"jita-passive shortlist requires scan strategy_set in {{{allowed}}}; got '{strategy_set or '-'}'."
        )

    unexpected_strategy_types = sorted(
        {
            str(row.get("strategy_type") or "-")
            for row in selected_rows_all
            if str(row.get("strategy_type") or "") not in _JITA_PASSIVE_ALLOWED_STRATEGY_TYPES
        }
    )
    if unexpected_strategy_types:
        raise ValueError(
            "jita-passive shortlist found unexpected strategy_type rows: "
            + ", ".join(unexpected_strategy_types)
        )

    cross_location_rows = [
        row
        for row in selected_rows_all
        if row.get("buy_location_id") is not None
        and row.get("sell_location_id") is not None
        and row.get("buy_location_id") != row.get("sell_location_id")
    ]
    if cross_location_rows:
        sample = cross_location_rows[0]
        raise ValueError(
            "jita-passive shortlist found cross-location row: "
            f"{sample.get('type_name') or sample.get('type_id')} "
            f"{sample.get('buy_location_name') or sample.get('buy_location_id')} -> "
            f"{sample.get('sell_location_name') or sample.get('sell_location_id')}"
        )

    bad_mode_rows = [
        row
        for row in selected_rows_all
        if str(row.get("entry_mode") or "") not in _JITA_PASSIVE_ALLOWED_ENTRY_MODES
        or str(row.get("exit_mode") or "") not in _JITA_PASSIVE_ALLOWED_EXIT_MODES
    ]
    if bad_mode_rows:
        sample = bad_mode_rows[0]
        raise ValueError(
            "jita-passive shortlist found non-passive execution row: "
            f"{sample.get('type_name') or sample.get('type_id')} "
            f"{sample.get('entry_mode') or '-'} -> {sample.get('exit_mode') or '-'}"
        )

    location_names = sorted(
        {
            str(row.get("buy_location_name") or row.get("sell_location_name") or row.get("buy_location_id") or "-")
            for row in selected_rows_all
        }
    )
    guard_notes = [
        "这份报告只接受吉他站内被动挂单扫描结果。",
        "每一行都要求买卖地点同站，且入场方式必须是挂买单收货、出场方式必须是挂卖单出货。",
    ]
    if location_names:
        preview = "、".join(location_names[:3])
        if len(location_names) > 3:
            preview += f" 等 {len(location_names)} 个地点"
        guard_notes.append(f"本批次候选地点摘要: {preview}")

    return {
        "report_profile": report_profile,
        "report_profile_label_zh": _report_profile_label_zh(report_profile),
        "strategy_set_label_zh": _strategy_set_label_zh(strategy_set),
        "semantic_guard_status": "enforced",
        "guard_notes_zh": guard_notes,
        "allowed_strategy_sets": sorted(_JITA_PASSIVE_ALLOWED_STRATEGY_SETS),
        "allowed_strategy_types": sorted(_JITA_PASSIVE_ALLOWED_STRATEGY_TYPES),
        "allowed_entry_modes": sorted(_JITA_PASSIVE_ALLOWED_ENTRY_MODES),
        "allowed_exit_modes": sorted(_JITA_PASSIVE_ALLOWED_EXIT_MODES),
    }


def _render_table_tsv(rows_zh: list[dict[str, Any]]) -> str:
    headers = [
        "优先级",
        "商品",
        "状态",
        "买价",
        "卖价",
        "利润率",
        "单件利润",
        "建议建仓资金",
        "基础建议上限",
        "当前仓位上限",
        "理论容量",
        "计划利润",
        "预计可吃份额",
        "竞争烈度",
        "周转天数",
        "30天波动",
        "7天波动",
        "卖价高悬",
        "主要约束",
        "订单快照时间",
    ]
    lines = ["\t".join(headers)]
    for row in rows_zh:
        lines.append("\t".join(str(row.get(header) or "-") for header in headers))
    return "\n".join(lines)


def _render_table_md(rows_zh: list[dict[str, Any]]) -> str:
    headers = [
        "优先级",
        "商品",
        "状态",
        "买价",
        "卖价",
        "利润率",
        "单件利润",
        "建议建仓资金",
        "基础建议上限",
        "当前仓位上限",
        "理论容量",
        "计划利润",
        "预计可吃份额",
        "竞争烈度",
        "周转天数",
        "30天波动",
        "7天波动",
        "卖价高悬",
        "主要约束",
        "订单快照时间",
    ]

    def escape_cell(value: Any) -> str:
        text = str(value or "-")
        return text.replace("|", "\\|").replace("\n", " ")

    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for row in rows_zh:
        lines.append("| " + " | ".join(escape_cell(row.get(header)) for header in headers) + " |")
    return "\n".join(lines)


def render_market_shortlist_report(shortlist: dict[str, Any]) -> list[str]:
    scan_summary = dict(shortlist.get("scan_summary") or {})
    semantic_guard = dict(shortlist.get("semantic_guard") or {})
    analysis_context = dict(shortlist.get("analysis_context") or {})
    data_freshness = dict(shortlist.get("data_freshness") or {})
    budget_summary = dict(shortlist.get("budget_summary") or {})
    advice_basis = dict(shortlist.get("advice_basis") or {})
    sizing_comparison = list(shortlist.get("position_sizing_comparison") or [])
    lines = [
        "# 市场短名单",
        "",
        shortlist.get("headline") or "",
        "",
        "## 扫描信息",
        "",
        f"- 生成时间: {shortlist.get('shortlist_generated_at_utc') or '-'}",
        f"- 报告类型: {semantic_guard.get('report_profile_label_zh') or '-'}",
        f"- 扫描批次: {scan_summary.get('scan_run_id') or '-'}",
        f"- 策略: {scan_summary.get('strategy_set') or '-'}",
        f"- 策略说明: {semantic_guard.get('strategy_set_label_zh') or '-'}",
        f"- 本轮入选数: {scan_summary.get('selected_candidate_count') or 0}",
        f"- 本次展示数: {shortlist.get('displayed_row_count') or 0}",
        f"- 本次评估候选数: {shortlist.get('selected_row_count_considered') or 0}",
        "",
        "## 报告语义",
        "",
        f"- 语义护栏状态: {semantic_guard.get('semantic_guard_status') or '-'}",
        *[
            f"- {note}"
            for note in list(semantic_guard.get("guard_notes_zh") or [])
        ],
        "",
        "## 数据时间基准",
        "",
        f"- 时间基准摘要: {data_freshness.get('summary_zh') or '-'}",
        f"- 批次 as_of_time: {data_freshness.get('analysis_as_of_time') or '-'}",
        f"- 订单快照最早时间: {((data_freshness.get('order_data_time_basis') or {}).get('snapshot_time_min')) or '-'}",
        f"- 订单快照最晚时间: {((data_freshness.get('order_data_time_basis') or {}).get('snapshot_time_max')) or '-'}",
        f"- History 窗口截止日期: {((data_freshness.get('history_data_time_basis') or {}).get('window_end_date_max')) or '-'}",
        f"- History 最近抓取时间: {((data_freshness.get('history_data_time_basis') or {}).get('latest_fetched_at_max')) or '-'}",
        "",
        "## 分析上下文",
        "",
        f"- 分析模式: {analysis_context.get('analysis_mode_label_zh') or '-'}",
        f"- 预算来源: {analysis_context.get('budget_source_label_zh') or '-'}",
        f"- 预算说明: {analysis_context.get('budget_source_summary_zh') or '-'}",
        f"- 关联角色: {analysis_context.get('character_name') or '-'}",
        f"- 角色选择方式: {analysis_context.get('selection_mode_label_zh') or '-'}",
        "",
        "## 资金信息",
        "",
        f"- 风险档位: {budget_summary.get('risk_profile') or '-'}",
        f"- 单品扩容倍数: {budget_summary.get('deployable_cap_multiplier') or '-'}x",
        f"- 总预算: {_format_isk_human(budget_summary.get('total_budget_isk'))}",
        f"- 已分配资金: {_format_isk_human(budget_summary.get('allocated_capital_isk'))}",
        f"- 剩余预算: {_format_isk_human(budget_summary.get('remaining_budget_isk'))}",
        f"- 可立即执行数: {budget_summary.get('execute_now_count') or 0}",
        f"- 观察或延后数: {budget_summary.get('deferred_count') or 0}",
        "",
        "## 仓位扩容对比",
        "",
        str(shortlist.get("position_sizing_comparison_md") or ""),
        "",
        "## 可信度依据",
        "",
        f"- 依据层级: {advice_basis.get('credibility_tier') or '-'}",
        f"- 依据来源: {advice_basis.get('basis') or '-'}",
        "",
        "## 候选表",
        "",
        str(shortlist.get("rendered_table_md") or ""),
        "",
    ]
    return lines


def _build_position_sizing_comparison(
    *,
    advice_by_profile: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for risk_profile in _RISK_PROFILE_ORDER:
        advice = advice_by_profile.get(risk_profile)
        if not advice:
            continue
        budget_summary = dict(advice.get("budget_summary") or {})
        rows.append(
            {
                "risk_profile": risk_profile,
                "risk_profile_label_zh": _risk_profile_label_zh(risk_profile),
                "deployable_cap_multiplier": budget_summary.get("deployable_cap_multiplier"),
                "allocated_capital_isk": budget_summary.get("allocated_capital_isk"),
                "remaining_budget_isk": budget_summary.get("remaining_budget_isk"),
                "execute_now_count": budget_summary.get("execute_now_count"),
                "deferred_count": budget_summary.get("deferred_count"),
            }
        )
    return rows


def _render_position_sizing_comparison_md(rows: list[dict[str, Any]]) -> str:
    headers = ["档位", "单品扩容倍数", "已分配资金", "剩余预算", "可立即执行数", "观察或延后数"]
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for row in rows:
        lines.append(
            "| "
            + " | ".join(
                [
                    str(row.get("risk_profile_label_zh") or "-"),
                    f"{float(row.get('deployable_cap_multiplier') or 0.0):.1f}x",
                    _format_isk_human(row.get("allocated_capital_isk")),
                    _format_isk_human(row.get("remaining_budget_isk")),
                    str(row.get("execute_now_count") or 0),
                    str(row.get("deferred_count") or 0),
                ]
            )
            + " |"
        )
    return "\n".join(lines)


def build_market_shortlist(
    connection,
    *,
    db_path: Path,
    runs_root: Path | None,
    log_dir: Path | None,
    scan_run_id: str | None,
    prefer_latest_selected: bool,
    total_budget_isk: float | None,
    risk_profile: str = "standard",
    top_opportunities: int = 50,
    top_near_misses: int = 10,
    max_rows: int = 20,
    use_latest_account_budget: bool = False,
    account_snapshot_run_id: str | None = None,
    report_profile: str = "generic",
) -> dict[str, Any]:
    advice_by_profile: dict[str, dict[str, Any]] = {}
    for profile_name in _RISK_PROFILE_ORDER:
        advice_by_profile[profile_name] = build_market_advice(
            connection,
            db_path=db_path,
            runs_root=runs_root,
            log_dir=log_dir,
            scan_run_id=scan_run_id,
            prefer_latest_selected=prefer_latest_selected,
            top_opportunities=top_opportunities,
            top_near_misses=top_near_misses,
            risk_profile=profile_name,
            total_budget_isk_override=total_budget_isk,
            use_latest_account_budget=use_latest_account_budget,
            account_snapshot_run_id=account_snapshot_run_id,
        )
    advice = advice_by_profile[risk_profile]
    selected_rows_all = sorted(list(advice.get("selected_advice") or []), key=_sort_key)
    semantic_guard = _validate_report_profile(
        report_profile=report_profile,
        scan_summary=dict(advice.get("scan_summary") or {}),
        selected_rows_all=selected_rows_all,
    )
    selected_rows = list(selected_rows_all)
    if max_rows > 0:
        selected_rows = selected_rows[:max_rows]
    built_rows = [_build_row(row) for row in selected_rows]
    machine_rows = [row["machine"] for row in built_rows]
    display_rows = [row["display_zh"] for row in built_rows]

    budget_summary = dict(advice.get("budget_summary") or {})
    execute_now_count = int(budget_summary.get("execute_now_count") or 0)
    watch_count = int(budget_summary.get("deferred_count") or 0)
    headline = (
        f"本次短名单展示 {len(display_rows)} 个商品，"
        f"来自本轮评估的 {len(selected_rows_all)} 个候选；"
        f"其中 {execute_now_count} 个可立即执行，{watch_count} 个先观察或延后。"
    )
    position_sizing_comparison = _build_position_sizing_comparison(advice_by_profile=advice_by_profile)

    return {
        "shortlist_generated_at_utc": _utc_now_iso(),
        "headline": headline,
        "scan_summary": advice.get("scan_summary"),
        "semantic_guard": semantic_guard,
        "budget_summary": budget_summary,
        "analysis_context": advice.get("analysis_context"),
        "data_freshness": advice.get("data_freshness"),
        "account_budget_context": advice.get("account_budget_context"),
        "traceability": advice.get("traceability"),
        "artifact_health": advice.get("artifact_health"),
        "advice_basis": advice.get("advice_basis"),
        "sort_basis": "先按仓位状态，再按执行优先级，再按计划利润排序",
        "displayed_row_count": len(display_rows),
        "selected_row_count_considered": len(selected_rows_all),
        "position_sizing_comparison": position_sizing_comparison,
        "position_sizing_comparison_md": _render_position_sizing_comparison_md(position_sizing_comparison),
        "shortlist_rows": machine_rows,
        "shortlist_rows_zh": display_rows,
        "rendered_table_md": _render_table_md(display_rows),
        "rendered_table_tsv": _render_table_tsv(display_rows),
    }
