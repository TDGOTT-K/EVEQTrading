from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .benchmark import run_scan_benchmark
from .coverage import collect_market_coverage_snapshot
from .db import record_data_fetch_run
from .history_batch import run_history_batch_fetch
from .history_fetch import HistoryFetchConfig
from .order_batch import run_order_batch_fetch
from .order_fetch import OrderFetchConfig
from .report import build_scan_diagnosis
from .run_artifacts import create_run_id, ensure_run_dir, write_json, write_summary
from .scan_runtime import execute_db_scan


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _elapsed_seconds(started_at: datetime, finished_at: datetime) -> float:
    return round((finished_at - started_at).total_seconds(), 3)


def _write_type_id_file(path: Path, type_ids: list[int]) -> None:
    path.write_text("".join(f"{type_id}\n" for type_id in type_ids), encoding="utf-8")


def _build_type_targets(
    rows: list[dict[str, Any]],
    *,
    selection_mode: str,
) -> list[dict[str, Any]]:
    return [
        {
            "type_id": int(row["type_id"]),
            "type_name": str(row["type_name"]),
            "selection_mode": selection_mode,
        }
        for row in rows
    ]


def run_coverage_cycle(
    connection,
    *,
    db_path: Path,
    output_root: Path,
    region_id: int,
    history_chunk_size: int,
    max_history_chunks: int | None,
    orders_chunk_size: int,
    max_orders_chunks: int | None,
    history_config: HistoryFetchConfig,
    orders_config: OrderFetchConfig,
    benchmark_chunk_size: int,
    benchmark_max_chunks: int | None,
    capital_budget_isk: float,
    test_position_isk: float,
    top_n: int,
    strategy_set: str,
    parameter_version: str,
) -> dict[str, Any]:
    cycle_run_id = create_run_id(prefix="coverage-cycle")
    cycle_run_dir = ensure_run_dir(output_root, cycle_run_id)
    started_at = _utc_now()

    before_snapshot = collect_market_coverage_snapshot(
        connection,
        region_id=region_id,
        chunk_size=history_chunk_size,
    )
    before_rows = before_snapshot["annotated_rows"]
    history_target_rows = [row for row in before_rows if not row["has_history_30d"]]
    history_targets = _build_type_targets(
        history_target_rows,
        selection_mode="coverage-cycle-missing-history",
    )

    files = {
        "history_target_ids_path": str(cycle_run_dir / "history_target_ids.txt"),
        "orders_target_ids_path": str(cycle_run_dir / "orders_target_ids.txt"),
    }
    _write_type_id_file(cycle_run_dir / "history_target_ids.txt", [int(row["type_id"]) for row in history_targets])

    history_batch_result = None
    history_batch_seconds = 0.0
    if history_targets:
        history_started_at = _utc_now()
        history_batch_result = run_history_batch_fetch(
            connection,
            output_root=output_root,
            type_targets=history_targets,
            config=history_config,
            chunk_size=history_chunk_size,
            max_chunks=max_history_chunks,
        )
        history_batch_seconds = _elapsed_seconds(history_started_at, _utc_now())

    after_history_snapshot = collect_market_coverage_snapshot(
        connection,
        region_id=region_id,
        chunk_size=orders_chunk_size,
    )
    before_history_ready = set(before_snapshot["type_ids"]["history_ready"])
    after_history_ready = set(after_history_snapshot["type_ids"]["history_ready"])
    newly_history_ready = sorted(after_history_ready - before_history_ready)

    after_history_rows = after_history_snapshot["annotated_rows"]
    priority_rows = [
        row
        for row in after_history_rows
        if int(row["type_id"]) in newly_history_ready and row["has_history_30d"] and not row["has_order_snapshot_ok"]
    ]
    remaining_rows = [
        row
        for row in after_history_rows
        if int(row["type_id"]) not in newly_history_ready and row["has_history_30d"] and not row["has_order_snapshot_ok"]
    ]
    orders_target_rows = priority_rows + remaining_rows
    orders_targets = _build_type_targets(
        orders_target_rows,
        selection_mode="coverage-cycle-missing-orders",
    )
    _write_type_id_file(cycle_run_dir / "orders_target_ids.txt", [int(row["type_id"]) for row in orders_targets])

    orders_batch_result = None
    orders_batch_seconds = 0.0
    if orders_targets:
        orders_started_at = _utc_now()
        orders_batch_result = run_order_batch_fetch(
            connection,
            output_root=output_root,
            type_targets=orders_targets,
            config=orders_config,
            chunk_size=orders_chunk_size,
            max_chunks=max_orders_chunks,
        )
        orders_batch_seconds = _elapsed_seconds(orders_started_at, _utc_now())

    after_orders_snapshot = collect_market_coverage_snapshot(
        connection,
        region_id=region_id,
        chunk_size=orders_chunk_size,
    )
    benchmark_started_at = _utc_now()
    benchmark = run_scan_benchmark(
        connection,
        db_path=db_path,
        output_root=output_root,
        region_id=region_id,
        explicit_type_ids=[],
        type_id_file=None,
        ready_market_scan_types=True,
        all_published_market_types=False,
        limit=None,
        chunk_size=benchmark_chunk_size,
        max_chunks=benchmark_max_chunks,
        capital_budget_isk=capital_budget_isk,
        test_position_isk=test_position_isk,
        top_n=top_n,
        strategy_set=strategy_set,
        parameter_version=parameter_version,
    )
    benchmark_seconds = _elapsed_seconds(benchmark_started_at, _utc_now())
    scan_started_at = _utc_now()
    scan_result = execute_db_scan(
        connection=connection,
        db_path=db_path,
        output_root=output_root,
        region_id=region_id,
        explicit_type_ids=[],
        type_id_file=None,
        ready_market_scan_types=True,
        all_published_market_types=False,
        limit=None,
        capital_budget_isk=capital_budget_isk,
        test_position_isk=test_position_isk,
        top_n=top_n,
        strategy_set=strategy_set,
        parameter_version=parameter_version,
    )
    scan_seconds = _elapsed_seconds(scan_started_at, _utc_now())
    diagnosis_started_at = _utc_now()
    diagnosis = build_scan_diagnosis(
        connection,
        db_path=db_path,
        runs_root=output_root,
        log_dir=None,
        scan_run_id=str(scan_result["run_id"]),
        prefer_latest_selected=False,
        top_rejection_reasons=5,
        top_near_passes=5,
        top_skipped_reasons=5,
    )
    diagnosis_seconds = _elapsed_seconds(diagnosis_started_at, _utc_now())

    finished_at = _utc_now()
    duration_seconds = _elapsed_seconds(started_at, finished_at)
    payload = {
        "cycle_run_id": cycle_run_id,
        "started_at_utc": started_at.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "finished_at_utc": finished_at.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "duration_seconds": duration_seconds,
        "region_id": region_id,
        "config": {
            "history_chunk_size": history_chunk_size,
            "max_history_chunks": max_history_chunks,
            "orders_chunk_size": orders_chunk_size,
            "max_orders_chunks": max_orders_chunks,
            "benchmark_chunk_size": benchmark_chunk_size,
            "benchmark_max_chunks": benchmark_max_chunks,
            "capital_budget_isk": capital_budget_isk,
            "test_position_isk": test_position_isk,
            "top_n": top_n,
            "strategy_set": strategy_set,
            "parameter_version": parameter_version,
            "history_fetch": history_config.to_dict(),
            "orders_fetch": orders_config.to_dict(),
        },
        "before_counts": before_snapshot["target_counts"],
        "after_history_counts": after_history_snapshot["target_counts"],
        "after_orders_counts": after_orders_snapshot["target_counts"],
        "newly_history_ready_type_ids": newly_history_ready,
        "history_target_count": len(history_targets),
        "orders_target_count": len(orders_targets),
        "history_batch_run_id": history_batch_result["batch_run_id"] if history_batch_result is not None else None,
        "orders_batch_run_id": orders_batch_result["batch_run_id"] if orders_batch_result is not None else None,
        "history_batch_run_dir": str(history_batch_result["run_dir"]) if history_batch_result is not None else None,
        "orders_batch_run_dir": str(orders_batch_result["run_dir"]) if orders_batch_result is not None else None,
        "benchmark_run_id": benchmark["benchmark_run_id"],
        "benchmark_run_dir": benchmark["run_dir"],
        "scan_run_id": str(scan_result["run_id"]),
        "scan_run_dir": str(scan_result["run_dir"]),
        "phase_durations": {
            "history_batch_seconds": history_batch_seconds,
            "orders_batch_seconds": orders_batch_seconds,
            "benchmark_seconds": benchmark_seconds,
            "scan_seconds": scan_seconds,
            "diagnosis_seconds": diagnosis_seconds,
        },
        "benchmark_summary": {
            "selected_target_count": benchmark["target_selection"]["selected_target_count"],
            "types_with_both_order_and_history_30d": benchmark["coverage_snapshot"][
                "types_with_both_order_and_history_30d"
            ],
            "candidate_count_built": benchmark["aggregate_results"]["candidate_count_built"],
            "candidate_count_selected": benchmark["aggregate_results"]["candidate_count_selected"],
            "skipped_count": benchmark["aggregate_results"]["skipped_count"],
        },
        "scan_summary": {
            "candidate_count_built": len(scan_result["candidate_build"].payload["candidates"]),
            "candidate_count_selected": len(scan_result["selected_candidates"]),
            "skipped_count": len(scan_result["candidate_build"].skipped_rows),
        },
        "diagnosis_summary": diagnosis["diagnosis_summary"],
        "top_rejection_reasons": diagnosis["top_rejection_reasons"],
        "top_skipped_reasons": diagnosis["skipped_summary"]["reason_counts"],
        "files": files,
    }

    summary_lines = [
        "# EVEQTrading Coverage Cycle",
        "",
        f"- Cycle Run ID: `{cycle_run_id}`",
        f"- Region ID: `{region_id}`",
        f"- Duration Seconds: `{duration_seconds}`",
        f"- Before Ready Count: `{before_snapshot['target_counts']['ready_count']}`",
        f"- After History Ready Count: `{after_history_snapshot['target_counts']['ready_count']}`",
        f"- After Orders Ready Count: `{after_orders_snapshot['target_counts']['ready_count']}`",
        f"- History Target Count: `{len(history_targets)}`",
        f"- Orders Target Count: `{len(orders_targets)}`",
        f"- Newly History Ready: `{len(newly_history_ready)}`",
        f"- History Batch Run ID: `{payload['history_batch_run_id']}`",
        f"- Orders Batch Run ID: `{payload['orders_batch_run_id']}`",
        f"- Benchmark Run ID: `{payload['benchmark_run_id']}`",
        f"- Scan Run ID: `{payload['scan_run_id']}`",
        f"- Benchmark Ready Universe: `{payload['benchmark_summary']['selected_target_count']}`",
        f"- Benchmark Candidates Built: `{payload['benchmark_summary']['candidate_count_built']}`",
        f"- Benchmark Candidates Selected: `{payload['benchmark_summary']['candidate_count_selected']}`",
        f"- Scan Candidates Built: `{payload['scan_summary']['candidate_count_built']}`",
        f"- Scan Candidates Selected: `{payload['scan_summary']['candidate_count_selected']}`",
        f"- Diagnosis Selected Count: `{payload['diagnosis_summary']['selected_count']}`",
    ]

    write_json(cycle_run_dir / "coverage_cycle.json", payload)
    write_json(cycle_run_dir / "before_coverage_snapshot.json", before_snapshot)
    write_json(cycle_run_dir / "after_history_coverage_snapshot.json", after_history_snapshot)
    write_json(cycle_run_dir / "after_orders_coverage_snapshot.json", after_orders_snapshot)
    write_json(cycle_run_dir / "scan_diagnosis.json", diagnosis)
    write_summary(cycle_run_dir / "summary.md", summary_lines)

    record_data_fetch_run(
        connection,
        fetch_run_id=cycle_run_id,
        fetch_kind="market_coverage_cycle",
        source="coverage-cycle",
        output_dir=cycle_run_dir.resolve(),
        scope={
            "region_id": region_id,
            "history_chunk_size": history_chunk_size,
            "orders_chunk_size": orders_chunk_size,
        },
        details={
            "before_counts": before_snapshot["target_counts"],
            "after_history_counts": after_history_snapshot["target_counts"],
            "after_orders_counts": after_orders_snapshot["target_counts"],
            "benchmark_summary": payload["benchmark_summary"],
            "scan_summary": payload["scan_summary"],
            "diagnosis_summary": payload["diagnosis_summary"],
        },
        started_at=started_at.isoformat(),
        finished_at=finished_at.isoformat(),
        status="completed",
    )
    connection.commit()

    return {
        "cycle_run_id": cycle_run_id,
        "run_dir": cycle_run_dir.resolve(),
        "payload": payload,
    }
