from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from .db import record_data_fetch_run
from .jita_passive_simple import JitaPassiveSimpleFilterConfig, run_jita_passive_simple_scan
from .models import FeeConfig
from .order_fetch import OrderFetchConfig
from .order_refresh import collect_order_refresh_snapshot
from .order_refresh_cycle import run_order_refresh_cycle
from .run_artifacts import create_run_id, ensure_run_dir, write_json, write_summary


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _candidate_preview_rows(
    selected_candidates: list[dict[str, object]],
    *,
    limit: int = 10,
) -> list[dict[str, object]]:
    preview: list[dict[str, object]] = []
    for row in selected_candidates[:limit]:
        preview.append(
            {
                "type_id": row["type_id"],
                "type_name": row["type_name"],
                "profit_margin": row["profit_margin"],
                "daily_capacity_isk_30d": row["daily_capacity_isk_30d"],
                "expected_turnover_days": row["expected_turnover_days"],
                "risk_adjusted_return": row["risk_adjusted_return"],
                "source_order_captured_at": row["source_order_captured_at"],
                "decision_reason": row["decision_reason"],
            }
        )
    return preview


def run_jita_passive_cache_cycle(
    connection,
    *,
    db_path: Path,
    output_root: Path,
    region_id: int,
    orders_chunk_size: int,
    max_orders_chunks: int | None,
    limit: int | None,
    orders_cache_profile: str,
    orders_config: OrderFetchConfig,
    capital_budget_isk: float,
    top_n: int,
    strategy_set: str,
    parameter_version: str,
    fee_config: FeeConfig | None = None,
    filter_config: JitaPassiveSimpleFilterConfig | None = None,
) -> dict[str, object]:
    started_at = _utc_now()
    run_id = create_run_id(prefix="jita-passive-cache-cycle")
    run_dir = ensure_run_dir(output_root, run_id)

    before_snapshot = collect_order_refresh_snapshot(
        connection,
        region_id=region_id,
        profile_name=orders_cache_profile,
        order_type=orders_config.order_type,
    )
    refresh_result = run_order_refresh_cycle(
        connection,
        output_root=output_root,
        region_id=region_id,
        orders_chunk_size=orders_chunk_size,
        max_orders_chunks=max_orders_chunks,
        limit=limit,
        profile_name=orders_cache_profile,
        orders_config=orders_config,
    )
    scan_result = run_jita_passive_simple_scan(
        connection=connection,
        db_path=db_path,
        output_root=output_root,
        region_id=region_id,
        explicit_type_ids=[],
        type_id_file=None,
        ready_market_scan_types=False,
        all_published_market_types=False,
        limit=limit,
        capital_budget_isk=capital_budget_isk,
        top_n=top_n,
        strategy_set=strategy_set,
        parameter_version=parameter_version,
        fee_config=fee_config,
        filter_config=filter_config,
        orders_cache_profile=orders_cache_profile,
        order_type=orders_config.order_type,
        cache_only=True,
    )
    after_snapshot = collect_order_refresh_snapshot(
        connection,
        region_id=region_id,
        profile_name=orders_cache_profile,
        order_type=orders_config.order_type,
    )
    finished_at = _utc_now()

    scan_summary = {
        "candidate_count_built": scan_result["analysis_run"]["candidate_count_total"],
        "candidate_count_selected": len(scan_result["selected_candidates"]),
        "skipped_count": len(scan_result["skipped_rows"]),
        "cache_selection": scan_result["cache_selection"],
    }
    top_candidates_preview = _candidate_preview_rows(scan_result["selected_candidates"])
    files = {
        "order_refresh_cycle_summary_path": str(refresh_result["run_dir"] / "summary.md"),
        "scan_summary_path": str(scan_result["run_dir"] / "summary.md"),
        "scan_analysis_run_path": str(scan_result["run_dir"] / "analysis_run.json"),
        "scan_calc_trace_path": str(scan_result["run_dir"] / "calc_trace.json"),
        "scan_candidate_table_path": str(scan_result["run_dir"] / "candidate_table.json"),
        "scan_skipped_table_path": str(scan_result["run_dir"] / "skipped_table.json"),
    }
    payload = {
        "cycle_run_id": run_id,
        "started_at_utc": started_at.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "finished_at_utc": finished_at.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "duration_seconds": round((finished_at - started_at).total_seconds(), 3),
        "region_id": region_id,
        "config": {
            "orders_chunk_size": orders_chunk_size,
            "max_orders_chunks": max_orders_chunks,
            "limit": limit,
            "orders_cache_profile": orders_cache_profile,
            "capital_budget_isk": capital_budget_isk,
            "top_n": top_n,
            "strategy_set": strategy_set,
            "parameter_version": parameter_version,
            "orders_fetch": orders_config.to_dict(),
        },
        "before_order_cache_counts": before_snapshot["target_counts"],
        "after_order_cache_counts": after_snapshot["target_counts"],
        "order_refresh_cycle_run_id": refresh_result["cycle_run_id"],
        "order_refresh_cycle_run_dir": str(refresh_result["run_dir"]),
        "order_refresh_summary": refresh_result["payload"],
        "scan_run_id": str(scan_result["run_id"]),
        "scan_run_dir": str(scan_result["run_dir"]),
        "scan_summary": scan_summary,
        "top_candidates_preview": top_candidates_preview,
        "files": files,
    }

    summary_lines = [
        "# EVEQTrading Jita Passive Cache Cycle",
        "",
        f"- Cycle Run ID: `{run_id}`",
        f"- Region ID: `{region_id}`",
        f"- Orders Cache Profile: `{orders_cache_profile}`",
        f"- Order Refresh Cycle Run ID: `{payload['order_refresh_cycle_run_id']}`",
        f"- Scan Run ID: `{payload['scan_run_id']}`",
        f"- Warm Cache Hits After Refresh: `{after_snapshot['target_counts']['cache_hit_count']}`",
        f"- Scan Candidates Built: `{scan_summary['candidate_count_built']}`",
        f"- Scan Candidates Selected: `{scan_summary['candidate_count_selected']}`",
    ]
    if top_candidates_preview:
        summary_lines.extend(["", "## Top Candidates"])
        for index, row in enumerate(top_candidates_preview, start=1):
            summary_lines.extend(
                [
                    "",
                    f"{index}. `{row['type_name']}` (`type_id={row['type_id']}`)",
                    f"   profit_margin: `{float(row['profit_margin']):.4%}`",
                    f"   daily_capacity_isk_30d: `{float(row['daily_capacity_isk_30d']):.2f}`",
                    f"   expected_turnover_days: `{float(row['expected_turnover_days']):.2f}`",
                    f"   source_order_captured_at: `{row['source_order_captured_at']}`",
                ]
            )
    else:
        summary_lines.extend(["", "## Top Candidates", "", "No candidates passed the current hard filters."])

    write_json(run_dir / "jita_passive_cache_cycle.json", payload)
    write_json(run_dir / "before_order_cache_snapshot.json", before_snapshot)
    write_json(run_dir / "after_order_cache_snapshot.json", after_snapshot)
    write_summary(run_dir / "summary.md", summary_lines)

    record_data_fetch_run(
        connection,
        fetch_run_id=run_id,
        fetch_kind="jita_passive_cache_cycle",
        source="jita-passive-cache-cycle",
        output_dir=run_dir.resolve(),
        scope={
            "region_id": region_id,
            "orders_cache_profile": orders_cache_profile,
            "orders_chunk_size": orders_chunk_size,
            "max_orders_chunks": max_orders_chunks,
            "limit": limit,
        },
        details={
            "before_order_cache_counts": before_snapshot["target_counts"],
            "after_order_cache_counts": after_snapshot["target_counts"],
            "order_refresh_cycle_run_id": payload["order_refresh_cycle_run_id"],
            "scan_run_id": payload["scan_run_id"],
            "scan_summary": scan_summary,
        },
        started_at=started_at.isoformat(),
        finished_at=finished_at.isoformat(),
        status="completed",
    )
    connection.commit()

    return {
        "cycle_run_id": run_id,
        "run_dir": run_dir.resolve(),
        "payload": payload,
        "refresh_result": refresh_result,
        "scan_result": scan_result,
    }
