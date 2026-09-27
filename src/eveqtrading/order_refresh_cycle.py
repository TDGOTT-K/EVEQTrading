from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .db import record_data_fetch_run
from .order_batch import run_order_batch_fetch
from .order_fetch import OrderFetchConfig
from .order_refresh import build_order_remote_refresh_targets, collect_order_refresh_snapshot
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


def _remote_due_count(snapshot: dict[str, Any]) -> int:
    counts = snapshot.get("target_counts", {})
    return int(counts.get("missing_orders_count") or 0) + int(counts.get("refresh_due_remote_count") or 0)


def run_order_refresh_cycle(
    connection,
    *,
    output_root: Path,
    region_id: int,
    orders_chunk_size: int,
    max_orders_chunks: int | None,
    limit: int | None,
    profile_name: str,
    orders_config: OrderFetchConfig,
) -> dict[str, Any]:
    started_at = _utc_now()
    run_id = create_run_id(prefix="order-refresh-cycle")
    run_dir = ensure_run_dir(output_root, run_id)

    before_snapshot = collect_order_refresh_snapshot(
        connection,
        region_id=region_id,
        profile_name=profile_name,
        order_type=orders_config.order_type,
    )
    plan_snapshot, remote_targets = build_order_remote_refresh_targets(
        connection,
        region_id=region_id,
        profile_name=profile_name,
        order_type=orders_config.order_type,
        limit=limit,
    )
    executed_targets = _resolve_executed_targets(
        remote_targets,
        chunk_size=orders_chunk_size,
        max_chunks=max_orders_chunks,
    )

    files = {
        "selected_remote_type_ids_path": str(run_dir / "selected_remote_type_ids.txt"),
        "executed_remote_type_ids_path": str(run_dir / "executed_remote_type_ids.txt"),
    }
    _write_type_id_file(
        run_dir / "selected_remote_type_ids.txt",
        [int(row["type_id"]) for row in remote_targets],
    )
    _write_type_id_file(
        run_dir / "executed_remote_type_ids.txt",
        [int(row["type_id"]) for row in executed_targets],
    )

    orders_batch_result = None
    if executed_targets:
        orders_batch_result = run_order_batch_fetch(
            connection,
            output_root=output_root,
            type_targets=remote_targets,
            config=orders_config,
            chunk_size=orders_chunk_size,
            max_chunks=max_orders_chunks,
        )

    after_snapshot = collect_order_refresh_snapshot(
        connection,
        region_id=region_id,
        profile_name=profile_name,
        order_type=orders_config.order_type,
    )
    finished_at = _utc_now()

    payload = {
        "cycle_run_id": run_id,
        "started_at_utc": started_at.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "finished_at_utc": finished_at.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "duration_seconds": round((finished_at - started_at).total_seconds(), 3),
        "region_id": region_id,
        "profile_name": profile_name,
        "config": {
            "orders_chunk_size": orders_chunk_size,
            "max_orders_chunks": max_orders_chunks,
            "limit": limit,
            "orders_fetch": orders_config.to_dict(),
        },
        "before_counts": before_snapshot["target_counts"],
        "plan_counts": plan_snapshot["target_counts"],
        "after_counts": after_snapshot["target_counts"],
        "selected_remote_target_count": len(remote_targets),
        "executed_remote_target_count": len(executed_targets),
        "orders_batch_run_id": (
            orders_batch_result["batch_run_id"] if orders_batch_result is not None else None
        ),
        "orders_batch_run_dir": (
            str(orders_batch_result["run_dir"]) if orders_batch_result is not None else None
        ),
        "orders_batch_totals": (
            dict(orders_batch_result["payload"]["totals"]) if orders_batch_result is not None else None
        ),
        "cache_hit_gain": (
            int(after_snapshot["target_counts"]["cache_hit_count"])
            - int(before_snapshot["target_counts"]["cache_hit_count"])
        ),
        "remote_due_before_count": _remote_due_count(before_snapshot),
        "remote_due_after_count": _remote_due_count(after_snapshot),
        "files": files,
    }

    summary_lines = [
        "# EVEQTrading Order Refresh Cycle",
        "",
        f"- Cycle Run ID: `{run_id}`",
        f"- Region ID: `{region_id}`",
        f"- Profile: `{profile_name}`",
        f"- Selected Remote Targets: `{len(remote_targets)}`",
        f"- Executed Remote Targets: `{len(executed_targets)}`",
        f"- Remote Due Before: `{payload['remote_due_before_count']}`",
        f"- Remote Due After: `{payload['remote_due_after_count']}`",
        f"- Cache Hit Gain: `{payload['cache_hit_gain']}`",
        f"- Orders Batch Run ID: `{payload['orders_batch_run_id']}`",
        "",
        "## Output Files",
        "",
        f"- selected_remote_type_ids: `{files['selected_remote_type_ids_path']}`",
        f"- executed_remote_type_ids: `{files['executed_remote_type_ids_path']}`",
    ]

    write_json(run_dir / "order_refresh_cycle.json", payload)
    write_json(run_dir / "before_order_refresh_snapshot.json", before_snapshot)
    write_json(run_dir / "after_order_refresh_snapshot.json", after_snapshot)
    write_summary(run_dir / "summary.md", summary_lines)

    record_data_fetch_run(
        connection,
        fetch_run_id=run_id,
        fetch_kind="order_refresh_cycle",
        source="order-refresh-cycle",
        output_dir=run_dir.resolve(),
        scope={
            "region_id": region_id,
            "profile_name": profile_name,
            "orders_chunk_size": orders_chunk_size,
            "max_orders_chunks": max_orders_chunks,
            "limit": limit,
            "order_type": orders_config.order_type,
        },
        details={
            "before_counts": before_snapshot["target_counts"],
            "after_counts": after_snapshot["target_counts"],
            "selected_remote_target_count": len(remote_targets),
            "executed_remote_target_count": len(executed_targets),
            "orders_batch_run_id": payload["orders_batch_run_id"],
            "orders_batch_totals": payload["orders_batch_totals"],
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
    }
