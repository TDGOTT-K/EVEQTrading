from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .batch_pipeline import chunked
from .coverage import collect_market_coverage_snapshot
from .db import record_data_fetch_run
from .history_fetch import HistoryFetchConfig, fetch_market_history
from .run_artifacts import create_run_id, ensure_run_dir, write_json, write_summary


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _write_type_id_file(path: Path, type_ids: list[int]) -> None:
    path.write_text("".join(f"{type_id}\n" for type_id in type_ids), encoding="utf-8")


def run_history_batch_fetch(
    connection,
    *,
    output_root: Path,
    type_targets: list[dict[str, Any]],
    config: HistoryFetchConfig,
    chunk_size: int,
    max_chunks: int | None,
) -> dict[str, Any]:
    run_id = create_run_id(prefix="history-batch-fetch")
    run_dir = ensure_run_dir(output_root, run_id)
    started_at = _utc_now()

    selected_chunks = chunked(type_targets, chunk_size) if type_targets else []
    selected_chunk_count = len(selected_chunks)
    if max_chunks is not None:
        selected_chunks = selected_chunks[: max_chunks]

    executed_type_ids = [int(row["type_id"]) for chunk in selected_chunks for row in chunk]
    executed_type_id_set = set(executed_type_ids)

    before_scope = collect_market_coverage_snapshot(
        connection,
        region_id=config.region_id,
        selected_type_ids=executed_type_id_set,
    )
    before_global = collect_market_coverage_snapshot(connection, region_id=config.region_id)

    chunk_results: list[dict[str, Any]] = []
    totals = {
        "selected_target_count": len(type_targets),
        "executed_target_count": len(executed_type_ids),
        "selected_chunk_count": selected_chunk_count,
        "executed_chunk_count": len(selected_chunks),
        "success": 0,
        "not_found": 0,
        "failed": 0,
        "history_rows_written": 0,
    }

    for chunk_index, chunk_targets in enumerate(selected_chunks, start=1):
        history_result = fetch_market_history(
            connection,
            output_root=output_root,
            type_targets=chunk_targets,
            config=config,
        )
        fetch_run = history_result["fetch_run"]
        fetch_counts = {
            "requested": int(fetch_run["type_count_requested"]),
            "success": int(fetch_run["type_count_success"]),
            "not_found": int(fetch_run["type_count_not_found"]),
            "failed": int(fetch_run["type_count_failed"]),
            "history_rows_written": int(fetch_run["history_rows_written"]),
        }
        totals["success"] += fetch_counts["success"]
        totals["not_found"] += fetch_counts["not_found"]
        totals["failed"] += fetch_counts["failed"]
        totals["history_rows_written"] += fetch_counts["history_rows_written"]

        chunk_results.append(
            {
                "chunk_index": chunk_index,
                "target_count": len(chunk_targets),
                "type_ids": [int(row["type_id"]) for row in chunk_targets],
                "history_run_id": history_result["run_id"],
                "history_run_dir": str(history_result["run_dir"]),
                "fetch_counts": fetch_counts,
            }
        )

    after_scope = collect_market_coverage_snapshot(
        connection,
        region_id=config.region_id,
        selected_type_ids=executed_type_id_set,
    )
    after_global = collect_market_coverage_snapshot(connection, region_id=config.region_id)

    files = {
        "requested_type_ids_path": str(run_dir / "requested_type_ids.txt"),
        "executed_type_ids_path": str(run_dir / "executed_type_ids.txt"),
        "history_ready_type_ids_path": str(run_dir / "history_ready_type_ids.txt"),
        "remaining_missing_history_type_ids_path": str(run_dir / "remaining_missing_history_type_ids.txt"),
    }
    requested_type_ids = [int(row["type_id"]) for row in type_targets]
    history_ready_type_ids = after_scope["type_ids"]["history_ready"]
    remaining_missing_history_type_ids = after_scope["type_ids"]["missing_history"]
    _write_type_id_file(run_dir / "requested_type_ids.txt", requested_type_ids)
    _write_type_id_file(run_dir / "executed_type_ids.txt", executed_type_ids)
    _write_type_id_file(run_dir / "history_ready_type_ids.txt", history_ready_type_ids)
    _write_type_id_file(run_dir / "remaining_missing_history_type_ids.txt", remaining_missing_history_type_ids)

    payload = {
        "batch_run_id": run_id,
        "region_id": config.region_id,
        "selection_mode": type_targets[0]["selection_mode"] if type_targets else "none",
        "chunk_size": chunk_size,
        "max_chunks": max_chunks,
        "config": config.to_dict(),
        "totals": totals,
        "before_executed_scope_counts": before_scope["target_counts"],
        "after_executed_scope_counts": after_scope["target_counts"],
        "before_global_counts": before_global["target_counts"],
        "after_global_counts": after_global["target_counts"],
        "history_ready_gain_in_executed_scope": (
            after_scope["target_counts"]["history_ready_count"] - before_scope["target_counts"]["history_ready_count"]
        ),
        "remaining_missing_history_count_in_executed_scope": len(remaining_missing_history_type_ids),
        "chunks": chunk_results,
        "files": files,
    }

    summary_lines = [
        "# EVEQTrading History Batch Fetch",
        "",
        f"- Batch Run ID: `{run_id}`",
        f"- Region ID: `{config.region_id}`",
        f"- Chunk Size: `{chunk_size}`",
        f"- Selected Targets: `{totals['selected_target_count']}`",
        f"- Executed Targets: `{totals['executed_target_count']}`",
        f"- Selected Chunks: `{totals['selected_chunk_count']}`",
        f"- Executed Chunks: `{totals['executed_chunk_count']}`",
        f"- Success: `{totals['success']}`",
        f"- Not Found: `{totals['not_found']}`",
        f"- Failed: `{totals['failed']}`",
        f"- History Rows Written: `{totals['history_rows_written']}`",
        f"- Executed Scope History Ready Before: `{before_scope['target_counts']['history_ready_count']}`",
        f"- Executed Scope History Ready After: `{after_scope['target_counts']['history_ready_count']}`",
        f"- Remaining Missing History In Executed Scope: `{len(remaining_missing_history_type_ids)}`",
        "",
        "## Chunk Runs",
    ]
    if not chunk_results:
        summary_lines.extend(["", "No chunks were executed."])
    else:
        for row in chunk_results[:20]:
            summary_lines.extend(
                [
                    "",
                    f"- Chunk `{row['chunk_index']}`",
                    f"  - targets: `{row['target_count']}`",
                    f"  - history_run_id: `{row['history_run_id']}`",
                    f"  - success: `{row['fetch_counts']['success']}`",
                    f"  - not_found: `{row['fetch_counts']['not_found']}`",
                    f"  - failed: `{row['fetch_counts']['failed']}`",
                ]
            )

    write_json(run_dir / "history_batch_fetch.json", payload)
    write_summary(run_dir / "summary.md", summary_lines)

    finished_at = _utc_now()
    record_data_fetch_run(
        connection,
        fetch_run_id=run_id,
        fetch_kind="market_history_batch",
        source="esi-public-history-batch",
        output_dir=run_dir.resolve(),
        scope={
            "region_id": config.region_id,
            "selection_mode": payload["selection_mode"],
            "chunk_size": chunk_size,
            "max_chunks": max_chunks,
        },
        details={
            "totals": totals,
            "before_executed_scope_counts": before_scope["target_counts"],
            "after_executed_scope_counts": after_scope["target_counts"],
            "chunk_results_sample": chunk_results[:20],
        },
        started_at=started_at.isoformat(),
        finished_at=finished_at.isoformat(),
        status="completed" if totals["failed"] == 0 else "completed_with_failures",
    )
    connection.commit()

    return {
        "batch_run_id": run_id,
        "run_dir": run_dir.resolve(),
        "payload": payload,
    }
