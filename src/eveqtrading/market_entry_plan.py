from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .advisor import build_market_advice


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _flow_quality(*, flip_units_cv_30d: float | None, flip_units_cv_7d: float | None) -> str:
    if flip_units_cv_30d is None or flip_units_cv_7d is None:
        return "unknown"
    if flip_units_cv_30d <= 0.70 and flip_units_cv_7d <= 0.35:
        return "stable"
    if flip_units_cv_30d <= 1.00 and flip_units_cv_7d <= 0.50:
        return "workable"
    return "choppy"


def _position_band(position_cap_isk: float | None) -> str:
    if position_cap_isk is None:
        return "unknown"
    if position_cap_isk >= 2_000_000_000.0:
        return "deep"
    if position_cap_isk >= 1_000_000_000.0:
        return "broad"
    if position_cap_isk >= 500_000_000.0:
        return "medium"
    return "narrow"


def _constraint_summary(binding_constraints: list[str] | None) -> str | None:
    items = [str(item) for item in (binding_constraints or []) if str(item)]
    if not items:
        return None
    mapped = {
        "remaining_budget": "total budget remaining",
        "budget_share_cap": "single-item budget share cap",
        "liquidity_cap": "daily liquidity cap",
        "capital_required": "scan-time test position cap",
        "max_position_isk": "legacy position cap",
        "recommended_deployable_isk": "recommended deployable cap",
        "recommended_deployable_isk_scaled": "scaled recommended deployable cap",
        "theoretical_market_capacity_isk": "theoretical market capacity cap",
        "cannot_afford_one_unit": "insufficient capital for one unit",
    }
    return ", ".join(mapped.get(item, item) for item in items)


def _planned_profit_isk(row: dict[str, Any]) -> float:
    suggested_capital_isk = float((row.get("position_plan") or {}).get("suggested_capital_isk") or 0.0)
    profit_margin = float(row.get("profit_margin") or 0.0)
    return suggested_capital_isk * profit_margin


def _sell_overhang_summary(*, sell_fill_penalty_multiplier: float | None, sell_price_overhang_ratio: float | None) -> str | None:
    if sell_fill_penalty_multiplier is None or sell_price_overhang_ratio is None:
        return None
    if sell_fill_penalty_multiplier >= 0.9999 or sell_price_overhang_ratio < 0.20:
        return "no penalty"
    return (
        f"ask is {sell_price_overhang_ratio:.2%} above 7d high; "
        f"sell-side flow kept at {sell_fill_penalty_multiplier:.2%}"
    )


def _build_entry_row(row: dict[str, Any]) -> dict[str, Any]:
    metrics = dict(row.get("metrics") or {})
    position_plan = dict(row.get("position_plan") or {})
    binding_constraints = [str(item) for item in (position_plan.get("binding_constraints") or []) if str(item)]
    tailwinds = [str(item) for item in (row.get("tailwinds") or []) if str(item)]
    flow_quality = _flow_quality(
        flip_units_cv_30d=metrics.get("flip_units_cv_30d"),
        flip_units_cv_7d=metrics.get("flip_units_cv_7d"),
    )
    recommended_deployable_isk = (
        position_plan.get("position_cap_isk")
        or row.get("recommended_deployable_isk")
        or row.get("max_position_isk")
        or row.get("capital_required")
    )
    sell_fill_penalty_multiplier = row.get("sell_fill_penalty_multiplier")
    sell_price_overhang_ratio = row.get("sell_price_overhang_ratio")
    return {
        "priority_rank": position_plan.get("priority_rank"),
        "type_id": row.get("type_id"),
        "type_name": row.get("type_name"),
        "strategy_type": row.get("strategy_type"),
        "action": row.get("action"),
        "position_status": position_plan.get("status"),
        "confidence_score": row.get("confidence_score"),
        "thesis_strength": row.get("thesis_strength"),
        "buy_reference_price": row.get("buy_reference_price"),
        "sell_reference_price": row.get("sell_reference_price"),
        "profit_margin": row.get("profit_margin"),
        "unit_profit": row.get("unit_profit"),
        "suggested_capital_isk": position_plan.get("suggested_capital_isk"),
        "suggested_units": position_plan.get("suggested_units"),
        "planned_profit_isk": _planned_profit_isk(row),
        "recommended_deployable_isk": recommended_deployable_isk,
        "theoretical_market_capacity_isk": row.get("theoretical_market_capacity_isk") or row.get("daily_capacity_isk_30d"),
        "estimated_capture_share": row.get("estimated_capture_share"),
        "competition_intensity": row.get("competition_intensity"),
        "buy_capture_share": row.get("buy_capture_share"),
        "sell_capture_share": row.get("sell_capture_share"),
        "recent_high_trade_price_7d": row.get("recent_high_trade_price_7d"),
        "sell_price_overhang_ratio": sell_price_overhang_ratio,
        "sell_price_overhang_steps": row.get("sell_price_overhang_steps"),
        "sell_fill_penalty_multiplier": sell_fill_penalty_multiplier,
        "sell_overhang_summary": _sell_overhang_summary(
            sell_fill_penalty_multiplier=(
                float(sell_fill_penalty_multiplier) if sell_fill_penalty_multiplier is not None else None
            ),
            sell_price_overhang_ratio=(
                float(sell_price_overhang_ratio) if sell_price_overhang_ratio is not None else None
            ),
        ),
        "max_position_isk": row.get("max_position_isk") or row.get("capital_required"),
        "position_band": _position_band(
            float(recommended_deployable_isk) if recommended_deployable_isk is not None else None
        ),
        "expected_turnover_days": row.get("expected_turnover_days"),
        "avg_daily_isk_volume_30d": metrics.get("avg_daily_isk_volume_30d"),
        "flip_units_cv_30d": metrics.get("flip_units_cv_30d"),
        "flip_units_cv_7d": metrics.get("flip_units_cv_7d"),
        "flow_quality": flow_quality,
        "binding_constraints": binding_constraints,
        "binding_constraint_summary": _constraint_summary(binding_constraints),
        "action_rationale": row.get("action_rationale"),
        "tailwinds": tailwinds[:3],
        "trace_refs": row.get("trace_refs"),
    }


def _build_watch_row(row: dict[str, Any]) -> dict[str, Any]:
    metrics = dict(row.get("metrics") or {})
    sell_fill_penalty_multiplier = row.get("sell_fill_penalty_multiplier")
    sell_price_overhang_ratio = row.get("sell_price_overhang_ratio")
    return {
        "type_id": row.get("type_id"),
        "type_name": row.get("type_name"),
        "strategy_type": row.get("strategy_type"),
        "action": row.get("action"),
        "confidence_score": row.get("confidence_score"),
        "profit_margin": row.get("profit_margin"),
        "expected_turnover_days": row.get("expected_turnover_days"),
        "flip_units_cv_30d": metrics.get("flip_units_cv_30d"),
        "flip_units_cv_7d": metrics.get("flip_units_cv_7d"),
        "sell_price_overhang_ratio": sell_price_overhang_ratio,
        "sell_fill_penalty_multiplier": sell_fill_penalty_multiplier,
        "sell_overhang_summary": _sell_overhang_summary(
            sell_fill_penalty_multiplier=(
                float(sell_fill_penalty_multiplier) if sell_fill_penalty_multiplier is not None else None
            ),
            sell_price_overhang_ratio=(
                float(sell_price_overhang_ratio) if sell_price_overhang_ratio is not None else None
            ),
        ),
        "action_rationale": row.get("action_rationale"),
        "blockers": row.get("blockers"),
    }


def build_market_entry_plan(
    connection,
    *,
    db_path: Path,
    runs_root: Path | None,
    log_dir: Path | None,
    scan_run_id: str | None,
    prefer_latest_selected: bool,
    total_budget_isk: float | None,
    risk_profile: str = "standard",
    max_candidates: int = 50,
    max_positions: int = 12,
    near_miss_limit: int = 10,
    use_latest_account_budget: bool = False,
    account_snapshot_run_id: str | None = None,
) -> dict[str, Any]:
    effective_max_candidates = max(max_candidates, max_positions, 1)
    advice = build_market_advice(
        connection,
        db_path=db_path,
        runs_root=runs_root,
        log_dir=log_dir,
        scan_run_id=scan_run_id,
        prefer_latest_selected=prefer_latest_selected,
        top_opportunities=effective_max_candidates,
        top_near_misses=near_miss_limit,
        risk_profile=risk_profile,
        total_budget_isk_override=total_budget_isk,
        use_latest_account_budget=use_latest_account_budget,
        account_snapshot_run_id=account_snapshot_run_id,
    )

    selected_advice = list(advice.get("selected_advice") or [])
    allocated_rows = sorted(
        [
            row
            for row in selected_advice
            if str((row.get("position_plan") or {}).get("status") or "") == "allocate"
        ],
        key=lambda row: (
            int((row.get("position_plan") or {}).get("priority_rank") or 999999),
            str(row.get("opportunity_id") or ""),
        ),
    )
    watch_rows = [
        _build_watch_row(row)
        for row in selected_advice
        if str((row.get("position_plan") or {}).get("status") or "") == "watch_only"
    ]
    near_miss_rows = [
        _build_watch_row(row)
        for row in list(advice.get("near_miss_advice") or [])[:near_miss_limit]
    ]

    first_wave = [_build_entry_row(row) for row in allocated_rows[:max_positions]]
    first_wave_capital_isk = sum(float(row.get("suggested_capital_isk") or 0.0) for row in first_wave)
    first_wave_planned_profit_isk = sum(float(row.get("planned_profit_isk") or 0.0) for row in first_wave)
    execute_now_count = len(allocated_rows)
    budget_summary = dict(advice.get("budget_summary") or {})
    scan_summary = dict(advice.get("scan_summary") or {})

    effective_total_budget_isk = float((budget_summary.get("total_budget_isk") or total_budget_isk or 0.0))
    portfolio_summary = {
        "total_budget_isk": effective_total_budget_isk,
        "risk_profile": risk_profile,
        "candidate_count_reviewed": len(selected_advice),
        "execute_now_count": execute_now_count,
        "watch_only_count": len(watch_rows),
        "near_miss_count": len(near_miss_rows),
        "first_wave_count": len(first_wave),
        "first_wave_capital_isk": first_wave_capital_isk,
        "first_wave_remaining_budget_isk": max(effective_total_budget_isk - first_wave_capital_isk, 0.0),
        "first_wave_planned_profit_isk": first_wave_planned_profit_isk,
        "scan_selected_candidate_count": scan_summary.get("selected_candidate_count"),
        "budget_source": budget_summary.get("budget_source"),
    }

    recommended_focus = "build_first_wave_orders" if first_wave else "review_watchlist_and_near_misses"
    if first_wave:
        recommended_focus_summary = (
            f"first wave has {len(first_wave)} candidates; start from priority 1 and place buy orders in sequence"
        )
    elif watch_rows:
        recommended_focus_summary = "no execute-now positions; watch the selected candidates and wait for the next refresh"
    else:
        recommended_focus_summary = "no usable first-wave candidates; review near misses and scan filters"

    return {
        "plan_generated_at_utc": _utc_now_iso(),
        "scan_summary": scan_summary,
        "traceability": advice.get("traceability"),
        "artifact_health": advice.get("artifact_health"),
        "data_freshness": advice.get("data_freshness"),
        "advice_basis": advice.get("advice_basis"),
        "budget_summary": budget_summary,
        "analysis_context": advice.get("analysis_context"),
        "account_budget_context": advice.get("account_budget_context"),
        "portfolio_summary": portfolio_summary,
        "recommended_focus": recommended_focus,
        "recommended_focus_summary": recommended_focus_summary,
        "first_wave_execute_now": first_wave,
        "watchlist": watch_rows[:max_positions],
        "near_miss_watchlist": near_miss_rows,
    }
