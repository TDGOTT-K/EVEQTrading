from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .advisor import build_market_advice
from .jita_passive_cache_report import resolve_jita_passive_cache_cycle_context
from .report import build_scan_diagnosis

_RISK_PROFILE_ORDER = ("conservative", "standard", "aggressive")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _format_isk_compact(amount: Any) -> str:
    try:
        numeric = float(amount or 0.0)
    except (TypeError, ValueError):
        numeric = 0.0
    abs_numeric = abs(numeric)
    if abs_numeric >= 1_000_000_000.0:
        return f"{numeric / 1_000_000_000.0:.2f}B ISK"
    if abs_numeric >= 1_000_000.0:
        return f"{numeric / 1_000_000.0:.2f}M ISK"
    if abs_numeric >= 1_000.0:
        return f"{numeric / 1_000.0:.2f}K ISK"
    return f"{numeric:.2f} ISK"


def _selected_action_counts(selected_advice: list[dict[str, Any]]) -> dict[str, int]:
    counts = {"deploy_small_count": 0, "watch_count": 0, "reject_count": 0}
    for row in selected_advice:
        action = str(row.get("action") or "")
        if action == "deploy-small":
            counts["deploy_small_count"] += 1
        elif action == "watch":
            counts["watch_count"] += 1
        elif action == "reject":
            counts["reject_count"] += 1
    return counts


def _recommended_focus(
    *,
    selected_count: int,
    deploy_small_count: int,
    watch_count: int,
    near_miss_count: int,
    rejection_reason_count: int,
) -> str:
    if deploy_small_count > 0:
        return "review_deploy_small_candidates"
    if selected_count > 0 or watch_count > 0:
        return "review_watchlist_candidates"
    if near_miss_count > 0:
        return "review_near_passes"
    if rejection_reason_count > 0:
        return "review_binding_rejections"
    return "no_actionable_candidates"


def _headline(
    *,
    selected_count: int,
    candidate_count_built: int,
    deploy_small_count: int,
    watch_count: int,
    near_miss_count: int,
) -> str:
    if selected_count > 0:
        return (
            f"{selected_count} selected from {candidate_count_built} built; "
            f"{deploy_small_count} deploy-small and {watch_count} watch."
        )
    if near_miss_count > 0:
        return f"0 selected from {candidate_count_built} built; review {near_miss_count} near-passes."
    return f"0 selected from {candidate_count_built} built; no near-passes were retained."


def _selected_actions_preview(selected_advice: list[dict[str, Any]], *, limit: int) -> list[dict[str, Any]]:
    preview: list[dict[str, Any]] = []
    for row in selected_advice[:limit]:
        metrics = dict(row.get("metrics") or {})
        trace_refs = dict(row.get("trace_refs") or {})
        position_plan = dict(row.get("position_plan") or {})
        preview.append(
            {
                "opportunity_id": row.get("opportunity_id"),
                "type_id": row.get("type_id"),
                "type_name": row.get("type_name"),
                "strategy_type": row.get("strategy_type"),
                "action": row.get("action"),
                "confidence_score": row.get("confidence_score"),
                "thesis_strength": row.get("thesis_strength"),
                "profit_margin": metrics.get("profit_margin"),
                "turnover_days": metrics.get("turnover_days"),
                "avg_daily_isk_volume_30d": metrics.get("avg_daily_isk_volume_30d"),
                "source_order_captured_at": trace_refs.get("source_order_captured_at"),
                "action_rationale": row.get("action_rationale"),
                "capital_required": row.get("capital_required"),
                "position_plan": position_plan or None,
            }
        )
    return preview


def _near_miss_preview(near_miss_advice: list[dict[str, Any]], *, limit: int) -> list[dict[str, Any]]:
    preview: list[dict[str, Any]] = []
    for row in near_miss_advice[:limit]:
        metrics = dict(row.get("metrics") or {})
        blockers = dict(row.get("blockers") or {})
        preview.append(
            {
                "opportunity_id": row.get("opportunity_id"),
                "type_id": row.get("type_id"),
                "type_name": row.get("type_name"),
                "strategy_type": row.get("strategy_type"),
                "action": row.get("action"),
                "confidence_score": row.get("confidence_score"),
                "profit_margin": metrics.get("profit_margin"),
                "turnover_days": metrics.get("turnover_days"),
                "avg_daily_isk_volume_30d": metrics.get("avg_daily_isk_volume_30d"),
                "hard_blockers": blockers.get("hard"),
                "soft_blockers": blockers.get("soft"),
            }
        )
    return preview


def _execution_plan_preview(execution_plan: list[dict[str, Any]], *, limit: int) -> list[dict[str, Any]]:
    preview: list[dict[str, Any]] = []
    for row in execution_plan[:limit]:
        preview.append(
            {
                "priority_rank": row.get("priority_rank"),
                "execution_status": row.get("execution_status"),
                "position_status": row.get("position_status"),
                "opportunity_id": row.get("opportunity_id"),
                "type_id": row.get("type_id"),
                "type_name": row.get("type_name"),
                "action": row.get("action"),
                "confidence_score": row.get("confidence_score"),
                "suggested_capital_isk": row.get("suggested_capital_isk"),
                "suggested_units": row.get("suggested_units"),
                "binding_constraints": row.get("binding_constraints"),
                "cumulative_allocated_isk": row.get("cumulative_allocated_isk"),
            }
        )
    return preview


def _binding_constraints_text(binding_constraints: Any) -> str | None:
    values = [str(item) for item in (binding_constraints or []) if str(item)]
    if not values:
        return None
    return ", ".join(values)


def _risk_profile_comparison_summary(advice: dict[str, Any]) -> dict[str, Any]:
    budget_summary = dict(advice.get("budget_summary") or {})
    execution_plan = list(advice.get("execution_plan") or [])
    first_step = execution_plan[0] if execution_plan else {}
    return {
        "risk_profile": budget_summary.get("risk_profile") or advice.get("risk_profile"),
        "risk_profile_label": budget_summary.get("risk_profile_label"),
        "risk_profile_summary": budget_summary.get("risk_profile_summary"),
        "allocated_capital_isk": budget_summary.get("allocated_capital_isk"),
        "remaining_budget_isk": budget_summary.get("remaining_budget_isk"),
        "allocated_candidate_count": budget_summary.get("allocated_candidate_count"),
        "execute_now_count": budget_summary.get("execute_now_count"),
        "deferred_count": budget_summary.get("deferred_count"),
        "budget_limited_count": budget_summary.get("budget_limited_count"),
        "liquidity_limited_count": budget_summary.get("liquidity_limited_count"),
        "next_type_name": first_step.get("type_name"),
        "next_execution_status": first_step.get("execution_status"),
        "next_suggested_capital_isk": first_step.get("suggested_capital_isk"),
    }


def _allocation_comparison_preview(
    advice_by_profile: dict[str, dict[str, Any]],
    *,
    base_risk_profile: str,
    limit: int,
) -> list[dict[str, Any]]:
    base_advice = advice_by_profile.get(base_risk_profile) or next(iter(advice_by_profile.values()), {})
    base_rows = sorted(
        list(base_advice.get("selected_advice") or []),
        key=lambda row: (
            int((row.get("position_plan") or {}).get("priority_rank") or 999999),
            str(row.get("opportunity_id") or ""),
        ),
    )

    preview: list[dict[str, Any]] = []
    for row in base_rows[:limit]:
        comparison_row = {
            "opportunity_id": row.get("opportunity_id"),
            "type_id": row.get("type_id"),
            "type_name": row.get("type_name"),
            "strategy_type": row.get("strategy_type"),
            "action": row.get("action"),
            "confidence_score": row.get("confidence_score"),
            "per_profile": {},
        }
        opportunity_id = row.get("opportunity_id")
        for risk_profile in _RISK_PROFILE_ORDER:
            profile_advice = advice_by_profile.get(risk_profile)
            if not profile_advice:
                continue
            matched = next(
                (
                    candidate
                    for candidate in list(profile_advice.get("selected_advice") or [])
                    if candidate.get("opportunity_id") == opportunity_id
                ),
                None,
            )
            position_plan = dict((matched or {}).get("position_plan") or {})
            comparison_row["per_profile"][risk_profile] = {
                "action": (matched or {}).get("action"),
                "position_status": position_plan.get("status"),
                "priority_rank": position_plan.get("priority_rank"),
                "suggested_capital_isk": position_plan.get("suggested_capital_isk"),
                "suggested_units": position_plan.get("suggested_units"),
                "binding_constraints": position_plan.get("binding_constraints"),
                "budget_share_cap_pct": position_plan.get("budget_share_cap_pct"),
                "liquidity_cap_daily_volume_fraction": position_plan.get("liquidity_cap_daily_volume_fraction"),
            }
        preview.append(comparison_row)
    return preview


def _comparison_driver_summary(allocation_comparison_preview: list[dict[str, Any]]) -> str | None:
    if not allocation_comparison_preview:
        return None
    top_row = allocation_comparison_preview[0]
    constraints: list[set[str]] = []
    for risk_profile in _RISK_PROFILE_ORDER:
        profile_row = dict(top_row.get("per_profile", {}).get(risk_profile) or {})
        constraints.append(set(str(item) for item in (profile_row.get("binding_constraints") or [])))
    if constraints and all("liquidity_cap" in row for row in constraints):
        return "Sizing differences are mainly driven by the liquidity cap."
    if constraints and all("budget_share_cap" in row for row in constraints):
        return "Sizing differences are mainly driven by the per-item budget share cap."
    return "Sizing differences come from a mix of per-item budget caps and liquidity caps."


def _build_operator_narrative(
    *,
    operator_summary: dict[str, Any],
    budget_summary: dict[str, Any],
    selected_actions_preview: list[dict[str, Any]],
    execution_plan_preview: list[dict[str, Any]],
    risk_profile_comparison: list[dict[str, Any]],
    allocation_comparison_preview: list[dict[str, Any]],
) -> dict[str, str]:
    risk_profile_label = str(
        operator_summary.get("risk_profile_label")
        or budget_summary.get("risk_profile_label")
        or operator_summary.get("risk_profile")
        or "standard"
    )
    execute_now_count = int(operator_summary.get("execute_now_count") or 0)
    selected_count = int(operator_summary.get("selected_count") or 0)
    remaining_budget = budget_summary.get("remaining_budget_isk")
    allocated_budget = budget_summary.get("allocated_capital_isk")
    first_execution = execution_plan_preview[0] if execution_plan_preview else {}
    first_type_name = first_execution.get("type_name")
    first_capital = first_execution.get("suggested_capital_isk")

    if execute_now_count > 0 and first_type_name:
        executive_summary = (
            f"Base profile {risk_profile_label} would execute {execute_now_count} item(s) now. "
            f"First priority is {first_type_name} at {_format_isk_compact(first_capital)}."
        )
    elif selected_count > 0:
        executive_summary = (
            f"Base profile {risk_profile_label} found {selected_count} selected item(s), "
            "but none are marked execute-now after position sizing."
        )
    else:
        executive_summary = (
            f"Base profile {risk_profile_label} has no immediate deploy candidates in this cache cycle."
        )

    if selected_actions_preview:
        deploy_names = [
            str(row.get("type_name") or "")
            for row in selected_actions_preview
            if str((row.get("position_plan") or {}).get("status") or "") == "allocate"
        ]
        if deploy_names:
            deployment_summary = (
                "Deploy-now shortlist: " + ", ".join(deploy_names[:3]) + "."
            )
        else:
            deployment_summary = "No candidates are currently sized for immediate deployment."
    else:
        deployment_summary = "No selected candidates were retained for deployment review."

    top_constraints: list[str] = []
    for row in selected_actions_preview:
        position_plan = dict(row.get("position_plan") or {})
        for item in position_plan.get("binding_constraints") or []:
            item_str = str(item)
            if item_str not in top_constraints:
                top_constraints.append(item_str)
    if allocated_budget is not None and remaining_budget is not None:
        constraint_summary = (
            f"Planned allocation uses {_format_isk_compact(allocated_budget)} and leaves "
            f"{_format_isk_compact(remaining_budget)} unallocated."
        )
        if top_constraints:
            constraint_summary += " Main sizing limits: " + ", ".join(top_constraints[:2]) + "."
    else:
        constraint_summary = "No budget-level sizing summary is available."

    if risk_profile_comparison:
        comparison_map = {str(row.get("risk_profile")): row for row in risk_profile_comparison}
        conservative = comparison_map.get("conservative")
        standard = comparison_map.get("standard")
        aggressive = comparison_map.get("aggressive")
        if conservative and standard and aggressive:
            risk_comparison_summary = (
                "Across profiles, planned allocation moves from "
                f"{_format_isk_compact(conservative.get('allocated_capital_isk'))} conservative "
                f"to {_format_isk_compact(standard.get('allocated_capital_isk'))} standard "
                f"to {_format_isk_compact(aggressive.get('allocated_capital_isk'))} aggressive."
            )
            first_comparison = allocation_comparison_preview[0] if allocation_comparison_preview else {}
            first_name = first_comparison.get("type_name")
            if first_name:
                risk_comparison_summary += f" The top compared item remains {first_name}."
            comparison_driver = _comparison_driver_summary(allocation_comparison_preview)
            if comparison_driver:
                risk_comparison_summary += " " + comparison_driver
        else:
            risk_comparison_summary = "Risk-profile comparison is available but incomplete."
    else:
        risk_comparison_summary = "Risk-profile comparison was not requested for this brief."

    return {
        "executive_summary": executive_summary,
        "deployment_summary": deployment_summary,
        "constraint_summary": constraint_summary,
        "risk_comparison_summary": risk_comparison_summary,
    }


def _build_operator_action_plan(
    *,
    execution_plan: list[dict[str, Any]],
    selected_actions_preview: list[dict[str, Any]],
    near_miss_watchlist: list[dict[str, Any]],
    execute_limit: int,
    deferred_limit: int,
    watch_limit: int,
    near_pass_limit: int,
) -> dict[str, Any]:
    execute_now: list[dict[str, Any]] = []
    deferred: list[dict[str, Any]] = []
    watchlist: list[dict[str, Any]] = []
    near_pass_actions: list[dict[str, Any]] = []

    for row in execution_plan:
        execution_status = str(row.get("execution_status") or "")
        binding_text = _binding_constraints_text(row.get("binding_constraints"))
        if execution_status == "execute_now" and len(execute_now) < execute_limit:
            execute_now.append(
                {
                    "priority_rank": row.get("priority_rank"),
                    "type_id": row.get("type_id"),
                    "type_name": row.get("type_name"),
                    "strategy_type": row.get("strategy_type"),
                    "suggested_capital_isk": row.get("suggested_capital_isk"),
                    "suggested_units": row.get("suggested_units"),
                    "binding_constraints": row.get("binding_constraints"),
                    "recommended_step": "prepare_entry_order",
                    "reason": (
                        f"execute now with {_format_isk_compact(row.get('suggested_capital_isk'))}"
                        + (f"; capped by {binding_text}" if binding_text else "")
                    ),
                }
            )
        elif execution_status == "defer" and len(deferred) < deferred_limit:
            position_status = str(row.get("position_status") or "")
            if position_status == "budget_exhausted":
                reason = "candidate is still valid, but this cycle's budget is already exhausted"
            elif position_status == "watch_only":
                reason = "candidate stays on watch because sizing did not open an execute-now slot"
            else:
                reason = "candidate is deferred for this cycle"
            if binding_text:
                reason += f"; current limit: {binding_text}"
            deferred.append(
                {
                    "priority_rank": row.get("priority_rank"),
                    "type_id": row.get("type_id"),
                    "type_name": row.get("type_name"),
                    "strategy_type": row.get("strategy_type"),
                    "position_status": position_status,
                    "suggested_capital_isk": row.get("suggested_capital_isk"),
                    "binding_constraints": row.get("binding_constraints"),
                    "recommended_step": "hold_for_next_cycle",
                    "reason": reason,
                }
            )

    for row in selected_actions_preview:
        position_plan = dict(row.get("position_plan") or {})
        if str(position_plan.get("status") or "") != "watch_only":
            continue
        if len(watchlist) >= watch_limit:
            break
        watchlist.append(
            {
                "type_id": row.get("type_id"),
                "type_name": row.get("type_name"),
                "strategy_type": row.get("strategy_type"),
                "action": row.get("action"),
                "recommended_step": "monitor_only",
                "reason": row.get("action_rationale"),
            }
        )

    for row in near_miss_watchlist[:near_pass_limit]:
        hard_blockers = [str(item) for item in (row.get("hard_blockers") or []) if str(item)]
        soft_blockers = [str(item) for item in (row.get("soft_blockers") or []) if str(item)]
        reason_parts: list[str] = []
        if hard_blockers:
            reason_parts.append("hard blockers: " + ", ".join(hard_blockers[:2]))
        if soft_blockers:
            reason_parts.append("soft blockers: " + ", ".join(soft_blockers[:2]))
        near_pass_actions.append(
            {
                "type_id": row.get("type_id"),
                "type_name": row.get("type_name"),
                "strategy_type": row.get("strategy_type"),
                "recommended_step": "recheck_next_refresh",
                "reason": "; ".join(reason_parts) if reason_parts else "near-pass candidate needs one more refresh",
            }
        )

    summary = (
        f"{len(execute_now)} execute now, {len(deferred)} deferred, "
        f"{len(watchlist)} watch-only, {len(near_pass_actions)} near-pass rechecks."
    )
    return {
        "summary": summary,
        "execute_now": execute_now,
        "deferred": deferred,
        "watchlist": watchlist,
        "near_pass_watchlist": near_pass_actions,
    }


def build_jita_passive_cache_brief(
    connection: sqlite3.Connection,
    *,
    db_path: Path,
    runs_root: Path | None,
    log_dir: Path | None,
    cycle_run_id: str | None,
    prefer_latest_selected: bool,
    top_candidates: int,
    top_opportunities: int,
    top_near_misses: int,
    top_rejection_reasons: int,
    top_near_passes: int,
    top_skipped_reasons: int,
    risk_profile: str = "standard",
    compare_risk_profiles: bool = False,
    use_latest_account_budget: bool = False,
    account_snapshot_run_id: str | None = None,
) -> dict[str, Any]:
    cycle_context = resolve_jita_passive_cache_cycle_context(
        connection,
        db_path=db_path,
        runs_root=runs_root,
        cycle_run_id=cycle_run_id,
        prefer_latest_selected=prefer_latest_selected,
        top_candidates=top_candidates,
    )
    scan_run_id = cycle_context["cycle_summary"].get("scan_run_id")
    if not isinstance(scan_run_id, str) or not scan_run_id:
        raise SystemExit("The selected jita passive cache cycle does not contain a scan_run_id.")

    advice = build_market_advice(
        connection,
        db_path=db_path,
        runs_root=runs_root,
        log_dir=log_dir,
        scan_run_id=scan_run_id,
        prefer_latest_selected=False,
        top_opportunities=top_opportunities,
        top_near_misses=top_near_misses,
        risk_profile=risk_profile,
        use_latest_account_budget=use_latest_account_budget,
        account_snapshot_run_id=account_snapshot_run_id,
    )
    advice_by_profile: dict[str, dict[str, Any]] = {risk_profile: advice}
    if compare_risk_profiles:
        for comparison_profile in _RISK_PROFILE_ORDER:
            if comparison_profile == risk_profile:
                continue
            advice_by_profile[comparison_profile] = build_market_advice(
                connection,
                db_path=db_path,
                runs_root=runs_root,
                log_dir=log_dir,
                scan_run_id=scan_run_id,
                prefer_latest_selected=False,
                top_opportunities=top_opportunities,
                top_near_misses=top_near_misses,
                risk_profile=comparison_profile,
                use_latest_account_budget=use_latest_account_budget,
                account_snapshot_run_id=account_snapshot_run_id,
            )
    diagnosis = build_scan_diagnosis(
        connection,
        db_path=db_path,
        runs_root=runs_root,
        log_dir=log_dir,
        scan_run_id=scan_run_id,
        prefer_latest_selected=False,
        top_rejection_reasons=top_rejection_reasons,
        top_near_passes=top_near_passes,
        top_skipped_reasons=top_skipped_reasons,
    )

    selected_advice = list(advice.get("selected_advice") or [])
    near_miss_advice = list(advice.get("near_miss_advice") or [])
    selected_count = len(selected_advice)
    action_counts = _selected_action_counts(selected_advice)
    rejection_reasons = list(diagnosis.get("top_rejection_reasons") or [])
    near_passes = list(diagnosis.get("closest_near_passes") or [])
    scan_summary = dict(advice.get("scan_summary") or {})
    orders_fetch_mode_summary = dict(cycle_context.get("orders_fetch_mode_summary") or {})
    operator_summary = {
        "headline": _headline(
            selected_count=selected_count,
            candidate_count_built=int(cycle_context["cycle_summary"].get("candidate_count_built") or 0),
            deploy_small_count=action_counts["deploy_small_count"],
            watch_count=action_counts["watch_count"],
            near_miss_count=len(near_miss_advice),
        ),
        "recommended_focus": _recommended_focus(
            selected_count=selected_count,
            deploy_small_count=action_counts["deploy_small_count"],
            watch_count=action_counts["watch_count"],
            near_miss_count=len(near_miss_advice),
            rejection_reason_count=len(rejection_reasons),
        ),
        "selected_count": selected_count,
        "candidate_count_built": int(cycle_context["cycle_summary"].get("candidate_count_built") or 0),
        "cache_hit_count_after_refresh": int(cycle_context["cycle_summary"].get("cache_hit_count_after_refresh") or 0),
        "deploy_small_count": action_counts["deploy_small_count"],
        "watch_count": action_counts["watch_count"],
        "reject_count": action_counts["reject_count"],
        "near_miss_count": len(near_miss_advice),
        "risk_profile": risk_profile,
        "orders_requested_fetch_mode": orders_fetch_mode_summary.get("requested_fetch_mode"),
        "orders_effective_fetch_mode": orders_fetch_mode_summary.get("dominant_effective_fetch_mode"),
        "orders_fetch_mode_narrative": orders_fetch_mode_summary.get("narrative"),
    }
    analysis_context = dict(advice.get("analysis_context") or {})
    data_freshness = dict(advice.get("data_freshness") or {})
    budget_summary = dict(advice.get("budget_summary") or {})
    execution_plan = list(advice.get("execution_plan") or [])
    if budget_summary:
        operator_summary["risk_profile"] = budget_summary.get("risk_profile")
        operator_summary["risk_profile_label"] = budget_summary.get("risk_profile_label")
        operator_summary["allocated_capital_isk"] = budget_summary.get("allocated_capital_isk")
        operator_summary["remaining_budget_isk"] = budget_summary.get("remaining_budget_isk")
        operator_summary["budget_limited_count"] = budget_summary.get("budget_limited_count")
        operator_summary["liquidity_limited_count"] = budget_summary.get("liquidity_limited_count")
        operator_summary["execute_now_count"] = budget_summary.get("execute_now_count")
        operator_summary["deferred_count"] = budget_summary.get("deferred_count")
    if analysis_context:
        operator_summary["analysis_mode"] = analysis_context.get("analysis_mode")
        operator_summary["analysis_mode_label_zh"] = analysis_context.get("analysis_mode_label_zh")
        operator_summary["budget_source_label_zh"] = analysis_context.get("budget_source_label_zh")
        operator_summary["budget_source_summary_zh"] = analysis_context.get("budget_source_summary_zh")
    if data_freshness:
        operator_summary["data_time_basis_summary_zh"] = data_freshness.get("summary_zh")
        operator_summary["order_snapshot_time_max"] = (
            (data_freshness.get("order_data_time_basis") or {}).get("snapshot_time_max")
        )
        operator_summary["history_window_end_date_max"] = (
            (data_freshness.get("history_data_time_basis") or {}).get("window_end_date_max")
        )
    if execution_plan:
        first_step = execution_plan[0]
        operator_summary["next_action_summary"] = (
            f"priority {first_step.get('priority_rank')}: {first_step.get('execution_status')} "
            f"{first_step.get('type_name')} for {float(first_step.get('suggested_capital_isk') or 0.0):.2f} ISK"
        )
    if compare_risk_profiles:
        comparison_rows = [_risk_profile_comparison_summary(advice_by_profile[name]) for name in _RISK_PROFILE_ORDER if name in advice_by_profile]
        operator_summary["risk_profile_comparison_available"] = True
        operator_summary["risk_profile_comparison_count"] = len(comparison_rows)
    else:
        comparison_rows = []
    allocation_comparison_preview = (
        _allocation_comparison_preview(
            advice_by_profile,
            base_risk_profile=risk_profile,
            limit=top_opportunities,
        )
        if compare_risk_profiles
        else []
    )
    selected_actions_preview = _selected_actions_preview(selected_advice, limit=top_opportunities)
    execution_plan_preview = _execution_plan_preview(execution_plan, limit=top_opportunities)
    operator_narrative = _build_operator_narrative(
        operator_summary=operator_summary,
        budget_summary=budget_summary,
        selected_actions_preview=selected_actions_preview,
        execution_plan_preview=execution_plan_preview,
        risk_profile_comparison=comparison_rows,
        allocation_comparison_preview=allocation_comparison_preview,
    )
    near_miss_watchlist = _near_miss_preview(near_miss_advice, limit=top_near_misses)
    operator_action_plan = _build_operator_action_plan(
        execution_plan=execution_plan,
        selected_actions_preview=selected_actions_preview,
        near_miss_watchlist=near_miss_watchlist,
        execute_limit=top_opportunities,
        deferred_limit=top_opportunities,
        watch_limit=top_opportunities,
        near_pass_limit=top_near_misses,
    )

    return {
        "brief_generated_at_utc": _utc_now_iso(),
        "cache_cycle_summary": cycle_context["cycle_summary"],
        "scan_summary": scan_summary,
        "orders_fetch_mode_summary": orders_fetch_mode_summary,
        "operator_summary": operator_summary,
        "operator_narrative": operator_narrative,
        "operator_action_plan": operator_action_plan,
        "advice_basis": advice.get("advice_basis"),
        "budget_summary": budget_summary,
        "analysis_context": analysis_context,
        "data_freshness": data_freshness,
        "execution_plan_preview": execution_plan_preview,
        "risk_profile_comparison": comparison_rows,
        "allocation_comparison_preview": allocation_comparison_preview,
        "top_candidates_preview": list(cycle_context.get("top_candidates_preview") or [])[:top_candidates],
        "selected_actions_preview": selected_actions_preview,
        "near_miss_watchlist": near_miss_watchlist,
        "top_rejection_reasons": rejection_reasons[:top_rejection_reasons],
        "closest_near_passes": near_passes[:top_near_passes],
        "skipped_summary": diagnosis.get("skipped_summary"),
        "diagnosis_summary": diagnosis.get("diagnosis_summary"),
        "cache_cycle_traceability": cycle_context["traceability"],
        "cache_cycle_artifact_health": cycle_context["artifact_health"],
    }
