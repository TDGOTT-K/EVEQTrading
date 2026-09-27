from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .coverage import collect_market_coverage_snapshot
from .db import record_data_fetch_run
from .jita_passive_simple import JitaPassiveSimpleFilterConfig, run_jita_passive_simple_scan
from .models import FeeConfig
from .order_batch import run_order_batch_fetch
from .order_fetch import OrderFetchConfig
from .order_refresh import build_order_refresh_targets
from .run_artifacts import create_run_id, ensure_run_dir, write_json, write_summary


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _write_type_id_file(path: Path, type_ids: list[int]) -> None:
    path.write_text("".join(f"{type_id}\n" for type_id in type_ids), encoding="utf-8")


def _resolve_executed_targets(
    type_targets: list[dict[str, Any]],
    *,
    chunk_size: int,
    max_chunks: int | None,
) -> list[dict[str, Any]]:
    if chunk_size <= 0:
        raise ValueError("orders chunk size must be positive")
    selected_chunks = [
        type_targets[index : index + chunk_size]
        for index in range(0, len(type_targets), chunk_size)
    ]
    if max_chunks is not None:
        selected_chunks = selected_chunks[:max_chunks]
    return [row for chunk in selected_chunks for row in chunk]


def run_jita_passive_cycle(
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
) -> dict[str, Any]:
    started_at = _utc_now()
    run_id = create_run_id(prefix="jita-passive-cycle")
    run_dir = ensure_run_dir(output_root, run_id)

    before_coverage = collect_market_coverage_snapshot(
        connection,
        region_id=region_id,
        chunk_size=orders_chunk_size,
    )
    refresh_snapshot, priority_targets = build_order_refresh_targets(
        connection,
        region_id=region_id,
        profile_name=orders_cache_profile,
        order_type=orders_config.order_type,
        limit=limit,
    )
    executed_targets = _resolve_executed_targets(
        priority_targets,
        chunk_size=orders_chunk_size,
        max_chunks=max_orders_chunks,
    )

    files = {
        "selected_priority_type_ids_path": str(run_dir / "selected_priority_type_ids.txt"),
        "executed_orders_type_ids_path": str(run_dir / "executed_orders_type_ids.txt"),
    }
    _write_type_id_file(
        run_dir / "selected_priority_type_ids.txt",
        [int(row["type_id"]) for row in priority_targets],
    )
    _write_type_id_file(
        run_dir / "executed_orders_type_ids.txt",
        [int(row["type_id"]) for row in executed_targets],
    )

    orders_batch_result = None
    if executed_targets:
        orders_batch_result = run_order_batch_fetch(
            connection,
            output_root=output_root,
            type_targets=priority_targets,
            config=orders_config,
            chunk_size=orders_chunk_size,
            max_chunks=max_orders_chunks,
        )

    scan_result = None
    if executed_targets:
        scan_result = run_jita_passive_simple_scan(
            connection=connection,
            db_path=db_path,
            output_root=output_root,
            region_id=region_id,
            explicit_type_ids=[int(row["type_id"]) for row in executed_targets],
            type_id_file=None,
            ready_market_scan_types=False,
            all_published_market_types=False,
            limit=None,
            capital_budget_isk=capital_budget_isk,
            top_n=top_n,
            strategy_set=strategy_set,
            parameter_version=parameter_version,
            fee_config=fee_config,
            filter_config=filter_config,
        )

    after_coverage = collect_market_coverage_snapshot(
        connection,
        region_id=region_id,
        chunk_size=orders_chunk_size,
    )
    finished_at = _utc_now()

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
            "filter_config": (
                {
                    "max_flip_units_cv_30d": filter_config.max_flip_units_cv_30d,
                    "max_flip_units_cv_7d": filter_config.max_flip_units_cv_7d,
                    "min_daily_capacity_isk_30d": filter_config.min_daily_capacity_isk_30d,
                }
                if filter_config is not None
                else None
            ),
        },
        "before_coverage_counts": before_coverage["target_counts"],
        "after_coverage_counts": after_coverage["target_counts"],
        "order_refresh_target_counts": refresh_snapshot["target_counts"],
        "selected_priority_target_count": len(priority_targets),
        "executed_orders_target_count": len(executed_targets),
        "orders_batch_run_id": (
            orders_batch_result["batch_run_id"] if orders_batch_result is not None else None
        ),
        "orders_batch_run_dir": (
            str(orders_batch_result["run_dir"]) if orders_batch_result is not None else None
        ),
        "scan_run_id": str(scan_result["run_id"]) if scan_result is not None else None,
        "scan_run_dir": str(scan_result["run_dir"]) if scan_result is not None else None,
        "scan_summary": (
            {
                "candidate_count_built": scan_result["analysis_run"]["candidate_count_total"],
                "candidate_count_selected": len(scan_result["selected_candidates"]),
                "skipped_count": len(scan_result["skipped_rows"]),
            }
            if scan_result is not None
            else {
                "candidate_count_built": 0,
                "candidate_count_selected": 0,
                "skipped_count": 0,
            }
        ),
        "files": files,
    }

    summary_lines = [
        "# EVEQTrading Jita Passive Cycle",
        "",
        f"- Cycle Run ID: `{run_id}`",
        f"- Region ID: `{region_id}`",
        f"- Orders Cache Profile: `{orders_cache_profile}`",
        f"- Selected Priority Targets: `{len(priority_targets)}`",
        f"- Executed Orders Targets: `{len(executed_targets)}`",
        f"- Orders Batch Run ID: `{payload['orders_batch_run_id']}`",
        f"- Scan Run ID: `{payload['scan_run_id']}`",
        f"- Scan Candidates Built: `{payload['scan_summary']['candidate_count_built']}`",
        f"- Scan Candidates Selected: `{payload['scan_summary']['candidate_count_selected']}`",
        "",
        "## Output Files",
        "",
        f"- selected_priority_type_ids: `{files['selected_priority_type_ids_path']}`",
        f"- executed_orders_type_ids: `{files['executed_orders_type_ids_path']}`",
    ]

    write_json(run_dir / "jita_passive_cycle.json", payload)
    write_json(run_dir / "before_coverage_snapshot.json", before_coverage)
    write_json(run_dir / "after_coverage_snapshot.json", after_coverage)
    write_summary(run_dir / "summary.md", summary_lines)

    record_data_fetch_run(
        connection,
        fetch_run_id=run_id,
        fetch_kind="jita_passive_cycle",
        source="jita-passive-cycle",
        output_dir=run_dir.resolve(),
        scope={
            "region_id": region_id,
            "orders_cache_profile": orders_cache_profile,
            "orders_chunk_size": orders_chunk_size,
            "max_orders_chunks": max_orders_chunks,
            "limit": limit,
        },
        details={
            "selected_priority_target_count": len(priority_targets),
            "order_refresh_target_counts": refresh_snapshot["target_counts"],
            "executed_orders_target_count": len(executed_targets),
            "orders_batch_run_id": payload["orders_batch_run_id"],
            "scan_run_id": payload["scan_run_id"],
            "scan_summary": payload["scan_summary"],
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
        "orders_batch_result": orders_batch_result,
        "scan_result": scan_result,
    }
