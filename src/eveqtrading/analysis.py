from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from .db_scan import RESERVOIR_ENTRY_TO_RECENT_MEAN_FLOOR_RATIO, RESERVOIR_RECENT_MEAN_FLOOR_RATIO
from .models import FeeConfig, FilterConfig, ScanConfig


def _safe_div(numerator: float, denominator: float) -> float:
    if denominator == 0:
        return 0.0
    return numerator / denominator


def _parse_as_of_time(raw_value: Optional[str]) -> datetime:
    if not raw_value:
        return datetime.now(timezone.utc)
    return datetime.fromisoformat(raw_value.replace("Z", "+00:00"))


def _strategy_pool(record: dict[str, Any]) -> str:
    strategy_hint = record.get("strategy_hint")
    if isinstance(strategy_hint, str) and strategy_hint:
        return strategy_hint
    zscore = float(record["price_zscore_30d"])
    if float(record.get("cross_location_net_spread_pct", 0.0)) > 0:
        return "cross-location"
    if zscore <= -1.0:
        return "reservoir"
    return "station-flip"


def analyze_market_scan(
    payload: dict[str, Any],
    input_path: Path,
    fee_config: FeeConfig,
    filter_config: FilterConfig,
    scan_config: ScanConfig,
) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]], list[str]]:
    started_at = datetime.now(timezone.utc)
    as_of_time = _parse_as_of_time(payload.get("as_of_time"))
    market_scope = str(payload.get("market_scope", "unknown"))

    traces: list[dict[str, Any]] = []
    selected: list[dict[str, Any]] = []

    for raw_candidate in payload.get("candidates", []):
        best_bid = float(raw_candidate["best_bid"])
        best_ask = float(raw_candidate["best_ask"])
        buy_reference_price = float(raw_candidate.get("buy_reference_price", best_ask))
        sell_reference_price = float(raw_candidate.get("sell_reference_price", best_bid))
        capital_required_isk = float(raw_candidate.get("capital_required_override") or scan_config.test_position_isk)
        avg_daily_isk_volume_30d = float(raw_candidate["avg_daily_isk_volume_30d"])
        active_days_ratio_30d = float(raw_candidate["active_days_ratio_30d"])
        avg_daily_units_7d = float(raw_candidate["avg_daily_units_7d"])
        history_mean_30d = float(raw_candidate["history_mean_30d"])
        history_std_30d = float(raw_candidate["history_std_30d"])
        volatility_30d = float(raw_candidate["volatility_30d"])
        price_percentile_30d = float(raw_candidate["price_percentile_30d"])
        cross_location_net_spread_pct = float(raw_candidate.get("cross_location_net_spread_pct", 0.0))
        gross_spread_pct_override = raw_candidate.get("gross_spread_pct")
        screened_profit_margin_override = raw_candidate.get("screened_profit_margin_override")

        mid_price = (best_bid + best_ask) / 2.0
        gross_spread_pct = (
            float(gross_spread_pct_override)
            if gross_spread_pct_override is not None
            else _safe_div(best_ask - best_bid, best_bid)
        )
        buy_cost_per_unit = buy_reference_price * (1.0 + fee_config.buy_broker_fee_rate)
        sell_net_per_unit = sell_reference_price * (
            1.0 - fee_config.sell_broker_fee_rate - fee_config.sales_tax_rate - fee_config.relist_fee_rate
        )
        unit_profit = sell_net_per_unit - buy_cost_per_unit
        net_spread_pct = _safe_div(unit_profit, buy_cost_per_unit)
        price_zscore_30d = _safe_div(mid_price - history_mean_30d, history_std_30d)
        estimated_exit_days_override = raw_candidate.get("estimated_exit_days_override")
        estimated_exit_days_small_size = (
            float(estimated_exit_days_override)
            if estimated_exit_days_override is not None
            else _safe_div(capital_required_isk, avg_daily_isk_volume_30d)
        )
        screened_profit_margin = (
            float(screened_profit_margin_override)
            if screened_profit_margin_override is not None
            else max(net_spread_pct, cross_location_net_spread_pct)
        )
        effective_strategy_type = _strategy_pool(
            {
                **raw_candidate,
                "price_zscore_30d": price_zscore_30d,
                "cross_location_net_spread_pct": cross_location_net_spread_pct,
            }
        )

        rejection_reasons: list[str] = []
        if avg_daily_isk_volume_30d < filter_config.min_avg_daily_isk_volume_30d:
            rejection_reasons.append("avg_daily_isk_volume_30d below threshold")
        if active_days_ratio_30d < filter_config.min_active_days_ratio_30d:
            rejection_reasons.append("active_days_ratio_30d below threshold")
        if avg_daily_units_7d < filter_config.min_avg_daily_units_7d:
            rejection_reasons.append("avg_daily_units_7d below threshold")
        if screened_profit_margin < filter_config.min_net_spread_pct:
            rejection_reasons.append("screened_profit_margin below threshold")
        if estimated_exit_days_small_size > filter_config.max_estimated_exit_days_small_size:
            rejection_reasons.append("estimated_exit_days_small_size above threshold")

        if effective_strategy_type == "reservoir":
            reservoir_recent_mean_ratio = float(raw_candidate.get("reservoir_recent_mean_ratio_7d_30d") or 0.0)
            if reservoir_recent_mean_ratio < RESERVOIR_RECENT_MEAN_FLOOR_RATIO:
                rejection_reasons.append("reservoir_recent_mean_ratio_7d_30d below floor")
            reservoir_entry_to_recent_mean_ratio = float(raw_candidate.get("reservoir_entry_to_recent_mean_ratio") or 0.0)
            if reservoir_entry_to_recent_mean_ratio < RESERVOIR_ENTRY_TO_RECENT_MEAN_FLOOR_RATIO:
                rejection_reasons.append("reservoir_entry_to_recent_mean_ratio below floor")

        tradable_units = _safe_div(capital_required_isk, buy_cost_per_unit)
        effective_profit_margin = screened_profit_margin
        effective_unit_profit = buy_cost_per_unit * effective_profit_margin
        total_profit = effective_unit_profit * tradable_units
        risk_adjusted_return = max(effective_profit_margin, 0.0) * (1.0 - min(volatility_30d, 0.95))

        candidate_trace = {
            "type_id": raw_candidate["type_id"],
            "type_name": raw_candidate["type_name"],
            "region_id": raw_candidate["region_id"],
            "source_record": raw_candidate,
            "derived": {
                "mid_price": mid_price,
                "gross_spread_pct": gross_spread_pct,
                "buy_reference_price": buy_reference_price,
                "sell_reference_price": sell_reference_price,
                "buy_cost_per_unit": buy_cost_per_unit,
                "sell_net_per_unit": sell_net_per_unit,
                "unit_profit": unit_profit,
                "net_spread_pct": net_spread_pct,
                "screened_profit_margin": screened_profit_margin,
                "price_zscore_30d": price_zscore_30d,
                "estimated_exit_days_small_size": estimated_exit_days_small_size,
                "estimated_exit_days_source": (
                    "candidate_override" if estimated_exit_days_override is not None else "avg_daily_isk_volume_30d"
                ),
                "reservoir_recent_mean_ratio_7d_30d": raw_candidate.get("reservoir_recent_mean_ratio_7d_30d"),
                "reservoir_entry_to_recent_mean_ratio": raw_candidate.get("reservoir_entry_to_recent_mean_ratio"),
                "reservoir_target_to_recent_high_ratio": raw_candidate.get("reservoir_target_to_recent_high_ratio"),
                "reservoir_exit_penalty_multiplier": raw_candidate.get("reservoir_exit_penalty_multiplier"),
                "capital_required_isk": capital_required_isk,
                "tradable_units": tradable_units,
                "total_profit": total_profit,
                "risk_adjusted_return": risk_adjusted_return,
            },
            "filter_evaluation": {
                "rejection_reasons": rejection_reasons,
                "passed": len(rejection_reasons) == 0,
            },
            "strategy_pool": effective_strategy_type,
        }

        if not rejection_reasons:
            effective_opportunity_id = raw_candidate.get(
                "opportunity_id",
                f"{raw_candidate['type_id']}-{raw_candidate['region_id']}-{effective_strategy_type}",
            )
            selected_candidate = {
                "opportunity_id": effective_opportunity_id,
                "strategy_type": effective_strategy_type,
                "type_id": raw_candidate["type_id"],
                "type_name": raw_candidate["type_name"],
                "region_id": raw_candidate["region_id"],
                "buy_location_id": raw_candidate.get("buy_location_id"),
                "sell_location_id": raw_candidate.get("sell_location_id"),
                "entry_mode": raw_candidate.get("entry_mode_override", "buy_best_ask"),
                "exit_mode": raw_candidate.get(
                    "exit_mode_override",
                    "sell_best_bid" if effective_strategy_type != "cross-location" else "sell_remote_best_bid",
                ),
                "unit_profit": effective_unit_profit,
                "total_profit": total_profit,
                "profit_margin": effective_profit_margin,
                "capital_required": capital_required_isk,
                "tradable_units": tradable_units,
                "expected_turnover_days": estimated_exit_days_small_size,
                "risk_adjusted_return": risk_adjusted_return,
                "decision_reason": "",
                "gross_spread_pct": gross_spread_pct,
                "net_spread_pct": net_spread_pct,
                "price_zscore_30d": price_zscore_30d,
                "price_percentile_30d": price_percentile_30d,
                "volatility_30d": volatility_30d,
                "active_days_ratio_30d": active_days_ratio_30d,
                "avg_daily_isk_volume_30d": avg_daily_isk_volume_30d,
                "avg_daily_units_7d": avg_daily_units_7d,
                "cross_location_net_spread_pct": cross_location_net_spread_pct,
                "buy_reference_price": buy_reference_price,
                "sell_reference_price": sell_reference_price,
                "max_position_isk": raw_candidate.get("max_position_isk"),
                "recommended_test_position_isk": raw_candidate.get("recommended_test_position_isk"),
                "inventory_limit_basis": raw_candidate.get("inventory_limit_basis"),
                "buy_location_name": raw_candidate.get("buy_location_name"),
                "sell_location_name": raw_candidate.get("sell_location_name"),
            }
            selected_candidate["decision_reason"] = (
                f"passed hard filters; screened_profit_margin={effective_profit_margin:.4f}; "
                f"estimated_exit_days_small_size={estimated_exit_days_small_size:.2f}; "
                f"price_zscore_30d={price_zscore_30d:.2f}"
            )
            selected.append(selected_candidate)

        traces.append(candidate_trace)

    selected.sort(
        key=lambda row: (
            row["risk_adjusted_return"],
            row["profit_margin"],
            row["avg_daily_isk_volume_30d"],
        ),
        reverse=True,
    )
    top_selected = selected[: scan_config.top_n]

    finished_at = datetime.now(timezone.utc)
    analysis_run = {
        "started_at": started_at,
        "finished_at": finished_at,
        "input_path": input_path,
        "market_scope": market_scope,
        "as_of_time": as_of_time,
        "scan_config": scan_config,
        "fee_config": fee_config,
        "filter_config": filter_config,
        "candidate_count_total": len(payload.get("candidates", [])),
        "candidate_count_selected": len(top_selected),
        "top_n": scan_config.top_n,
    }
    calc_trace = {
        "formula_versions": {
            "net_spread_pct": "v1-conservative-best-ask-best-bid",
            "price_zscore_30d": "v1-mid-price-30d",
            "estimated_exit_days_small_size": "v1-test-position-over-avg-daily-isk-volume-30d",
            "estimated_exit_days_override": "v1-candidate-specific-exit-proxy",
            "reservoir_target_price": "v2-min(history_mean_30d*0.99, history_high_max_7d*1.15)",
            "reservoir_regime_guard": "v2-history_mean_7d-over-history_mean_30d-floor-0.75-and-buy-over-history_mean_7d-floor-0.70",
            "reservoir_exit_penalty": "v2-estimated_exit_days_small_size_ask-times-max(1,target/buy)^2",
        },
        "fee_config": fee_config,
        "filter_config": filter_config,
        "scan_config": scan_config,
        "candidate_traces": traces,
    }

    summary_lines = [
        "# EVEQTrading Scan Summary",
        "",
        f"- Market Scope: `{market_scope}`",
        f"- As Of Time: `{as_of_time.isoformat()}`",
        f"- Input Path: `{input_path}`",
        f"- Candidates Evaluated: `{len(payload.get('candidates', []))}`",
        f"- Candidates Selected: `{len(top_selected)}`",
        f"- Parameter Version: `{scan_config.parameter_version}`",
        "",
        "## Top Candidates",
    ]
    if not top_selected:
        summary_lines.append("")
        summary_lines.append("No candidates passed the current hard filters.")
    else:
        for index, row in enumerate(top_selected, start=1):
            summary_lines.extend(
                [
                    "",
                    f"{index}. `{row['type_name']}` (`type_id={row['type_id']}`)",
                    f"   strategy: `{row['strategy_type']}`",
                    f"   screened_profit_margin: `{row['profit_margin']:.4%}`",
                    f"   total_profit: `{row['total_profit']:.2f} ISK`",
                    f"   expected_turnover_days: `{row['expected_turnover_days']:.2f}`",
                    f"   reason: {row['decision_reason']}",
                ]
            )

    return analysis_run, calc_trace, top_selected, summary_lines


def persist_scan_run(
    connection: Any,
    run_id: str,
    analysis_run: dict[str, Any],
    selected_candidates: list[dict[str, Any]],
    output_dir: Path,
) -> None:
    connection.execute(
        """
        INSERT INTO scan_run (
            scan_run_id, started_at, finished_at, market_scope, strategy_set,
            capital_budget_isk, parameter_version, input_path, output_dir,
            selected_candidate_count, status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            analysis_run["started_at"].isoformat(),
            analysis_run["finished_at"].isoformat(),
            analysis_run["market_scope"],
            analysis_run["scan_config"].strategy_set,
            analysis_run["scan_config"].capital_budget_isk,
            analysis_run["scan_config"].parameter_version,
            str(analysis_run["input_path"]),
            str(output_dir),
            len(selected_candidates),
            "completed",
        ),
    )

    for row in selected_candidates:
        connection.execute(
            """
            INSERT INTO opportunity_snapshot (
                scan_run_id, opportunity_id, strategy_type, type_id, type_name, region_id,
                buy_location_id, sell_location_id, entry_mode, exit_mode, unit_profit,
                total_profit, profit_margin, capital_required, tradable_units,
                expected_turnover_days, risk_adjusted_return, decision_reason
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                row["opportunity_id"],
                row["strategy_type"],
                row["type_id"],
                row["type_name"],
                row["region_id"],
                row["buy_location_id"],
                row["sell_location_id"],
                row["entry_mode"],
                row["exit_mode"],
                row["unit_profit"],
                row["total_profit"],
                row["profit_margin"],
                row["capital_required"],
                row["tradable_units"],
                row["expected_turnover_days"],
                row["risk_adjusted_return"],
                row["decision_reason"],
            ),
        )

    connection.commit()
