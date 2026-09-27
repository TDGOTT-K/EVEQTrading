from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .batch_pipeline import chunked
from .run_artifacts import create_run_id, ensure_run_dir, write_json, write_summary


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _write_type_id_file(path: Path, type_ids: list[int]) -> None:
    path.write_text("".join(f"{type_id}\n" for type_id in type_ids), encoding="utf-8")


def _load_market_type_rows(connection) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT type_id, type_name, market_group
        FROM item_catalog
        WHERE is_published = 1
          AND market_group IS NOT NULL
        ORDER BY type_id
        """
    ).fetchall()
    return [
        {
            "type_id": int(row["type_id"]),
            "type_name": str(row["type_name"]),
            "market_group": str(row["market_group"]),
        }
        for row in rows
    ]


def _load_history_counts(connection, *, region_id: int) -> dict[int, int]:
    rows = connection.execute(
        """
        SELECT type_id, COUNT(*) AS row_count
        FROM market_history_daily
        WHERE region_id = ?
        GROUP BY type_id
        """,
        (region_id,),
    ).fetchall()
    return {int(row["type_id"]): int(row["row_count"]) for row in rows}


def _load_order_sync_rows(connection, *, region_id: int) -> dict[int, dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT type_id, last_success_at, failure_count, last_error, coverage_marker
        FROM sync_scope_state
        WHERE scope_type = 'market_orders'
          AND region_id = ?
        """,
        (region_id,),
    ).fetchall()
    result: dict[int, dict[str, Any]] = {}
    for row in rows:
        result[int(row["type_id"])] = {
            "last_success_at": row["last_success_at"],
            "failure_count": int(row["failure_count"] or 0),
            "last_error": row["last_error"],
            "coverage_marker": row["coverage_marker"],
        }
    return result


def _top_market_groups(rows: list[dict[str, Any]], *, limit: int) -> list[dict[str, Any]]:
    counter = Counter(str(row.get("market_group") or "unknown") for row in rows)
    return [{"market_group": group, "count": count} for group, count in counter.most_common(limit)]


def _summarize_target_counts(
    *,
    market_type_count: int,
    ready_rows: list[dict[str, Any]],
    history_ready_type_ids: list[int],
    orders_ready_type_ids: list[int],
    missing_history_rows: list[dict[str, Any]],
    missing_orders_rows: list[dict[str, Any]],
    missing_both_rows: list[dict[str, Any]],
) -> dict[str, int]:
    return {
        "market_type_count": market_type_count,
        "ready_count": len(ready_rows),
        "history_ready_count": len(history_ready_type_ids),
        "orders_ready_count": len(orders_ready_type_ids),
        "missing_history_count": len(missing_history_rows),
        "missing_orders_count": len(missing_orders_rows),
        "missing_both_count": len(missing_both_rows),
    }


def collect_market_coverage_snapshot(
    connection,
    *,
    region_id: int,
    chunk_size: int | None = None,
    limit: int | None = None,
    selected_type_ids: set[int] | None = None,
) -> dict[str, Any]:
    market_rows = _load_market_type_rows(connection)
    if selected_type_ids is not None:
        market_rows = [row for row in market_rows if int(row["type_id"]) in selected_type_ids]
    history_counts = _load_history_counts(connection, region_id=region_id)
    order_sync_rows = _load_order_sync_rows(connection, region_id=region_id)

    effective_rows = market_rows[:limit] if limit is not None else market_rows

    ready_rows: list[dict[str, Any]] = []
    missing_history_rows: list[dict[str, Any]] = []
    missing_orders_rows: list[dict[str, Any]] = []
    missing_both_rows: list[dict[str, Any]] = []

    for row in effective_rows:
        type_id = int(row["type_id"])
        history_row_count = history_counts.get(type_id, 0)
        has_history_30d = history_row_count >= 30
        order_row = order_sync_rows.get(type_id)
        coverage_marker = str(order_row.get("coverage_marker") or "") if order_row is not None else ""
        has_order_snapshot_ok = coverage_marker.startswith("ok:")

        annotated = {
            **row,
            "history_row_count": history_row_count,
            "has_history_30d": has_history_30d,
            "has_order_snapshot_ok": has_order_snapshot_ok,
            "order_last_success_at": order_row.get("last_success_at") if order_row is not None else None,
            "order_failure_count": order_row.get("failure_count") if order_row is not None else 0,
            "order_last_error": order_row.get("last_error") if order_row is not None else None,
            "order_coverage_marker": order_row.get("coverage_marker") if order_row is not None else None,
        }

        if has_history_30d and has_order_snapshot_ok:
            ready_rows.append(annotated)
        elif not has_history_30d and not has_order_snapshot_ok:
            missing_both_rows.append(annotated)
            missing_history_rows.append(annotated)
        elif not has_history_30d:
            missing_history_rows.append(annotated)
        else:
            missing_orders_rows.append(annotated)

    ready_type_ids = [int(row["type_id"]) for row in ready_rows]
    history_ready_type_ids = [int(row["type_id"]) for row in effective_rows if row["type_id"] in history_counts and history_counts[row["type_id"]] >= 30]
    orders_ready_type_ids = [
        int(row["type_id"])
        for row in effective_rows
        if str((order_sync_rows.get(int(row["type_id"])) or {}).get("coverage_marker") or "").startswith("ok:")
    ]
    missing_history_type_ids = [int(row["type_id"]) for row in missing_history_rows]
    missing_orders_type_ids = [int(row["type_id"]) for row in missing_orders_rows]
    missing_both_type_ids = [int(row["type_id"]) for row in missing_both_rows]

    payload = {
        "region_id": region_id,
        "limit": limit,
        "chunk_size": chunk_size,
        "annotated_rows": ready_rows + missing_history_rows + missing_orders_rows,
        "target_counts": _summarize_target_counts(
            market_type_count=len(effective_rows),
            ready_rows=ready_rows,
            history_ready_type_ids=history_ready_type_ids,
            orders_ready_type_ids=orders_ready_type_ids,
            missing_history_rows=missing_history_rows,
            missing_orders_rows=missing_orders_rows,
            missing_both_rows=missing_both_rows,
        ),
        "type_ids": {
            "ready": ready_type_ids,
            "history_ready": history_ready_type_ids,
            "orders_ready": orders_ready_type_ids,
            "missing_history": missing_history_type_ids,
            "missing_orders": missing_orders_type_ids,
            "missing_both": missing_both_type_ids,
        },
        "chunk_counts": {
            "ready": len(chunked(ready_type_ids, chunk_size)) if chunk_size is not None and ready_type_ids else 0,
            "missing_history": (
                len(chunked(missing_history_type_ids, chunk_size))
                if chunk_size is not None and missing_history_type_ids
                else 0
            ),
            "missing_orders": (
                len(chunked(missing_orders_type_ids, chunk_size))
                if chunk_size is not None and missing_orders_type_ids
                else 0
            ),
            "missing_both": (
                len(chunked(missing_both_type_ids, chunk_size))
                if chunk_size is not None and missing_both_type_ids
                else 0
            ),
        },
        "top_market_groups": {
            "ready": _top_market_groups(ready_rows, limit=10),
            "missing_history": _top_market_groups(missing_history_rows, limit=10),
            "missing_orders": _top_market_groups(missing_orders_rows, limit=10),
            "missing_both": _top_market_groups(missing_both_rows, limit=10),
        },
        "sample_rows": {
            "ready": ready_rows[:10],
            "missing_history": missing_history_rows[:10],
            "missing_orders": missing_orders_rows[:10],
            "missing_both": missing_both_rows[:10],
        },
    }
    return payload


def plan_market_coverage(
    connection,
    *,
    output_root: Path,
    region_id: int,
    chunk_size: int,
    limit: int | None,
) -> dict[str, Any]:
    run_id = create_run_id(prefix="coverage-plan")
    run_dir = ensure_run_dir(output_root, run_id)
    snapshot = collect_market_coverage_snapshot(
        connection,
        region_id=region_id,
        chunk_size=chunk_size,
        limit=limit,
    )

    files = {
        "ready_type_ids_path": str(run_dir / "ready_type_ids.txt"),
        "missing_history_type_ids_path": str(run_dir / "missing_history_type_ids.txt"),
        "missing_orders_type_ids_path": str(run_dir / "missing_orders_type_ids.txt"),
        "missing_both_type_ids_path": str(run_dir / "missing_both_type_ids.txt"),
    }
    _write_type_id_file(run_dir / "ready_type_ids.txt", snapshot["type_ids"]["ready"])
    _write_type_id_file(run_dir / "missing_history_type_ids.txt", snapshot["type_ids"]["missing_history"])
    _write_type_id_file(run_dir / "missing_orders_type_ids.txt", snapshot["type_ids"]["missing_orders"])
    _write_type_id_file(run_dir / "missing_both_type_ids.txt", snapshot["type_ids"]["missing_both"])

    payload = {
        "coverage_plan_run_id": run_id,
        "generated_at_utc": _utc_now_iso(),
        "region_id": region_id,
        "limit": limit,
        "chunk_size": chunk_size,
        "target_counts": snapshot["target_counts"],
        "chunk_counts": snapshot["chunk_counts"],
        "top_market_groups": snapshot["top_market_groups"],
        "sample_rows": snapshot["sample_rows"],
        "files": files,
    }

    summary_lines = [
        "# EVEQTrading Coverage Plan",
        "",
        f"- Coverage Plan Run ID: `{run_id}`",
        f"- Region ID: `{region_id}`",
        f"- Market Types Considered: `{snapshot['target_counts']['market_type_count']}`",
        f"- Ready Types: `{snapshot['target_counts']['ready_count']}`",
        f"- Missing History: `{snapshot['target_counts']['missing_history_count']}`",
        f"- Missing Orders: `{snapshot['target_counts']['missing_orders_count']}`",
        f"- Missing Both: `{snapshot['target_counts']['missing_both_count']}`",
        f"- Chunk Size: `{chunk_size}`",
        "",
        "## Output Files",
        "",
        f"- ready: `{files['ready_type_ids_path']}`",
        f"- missing_history: `{files['missing_history_type_ids_path']}`",
        f"- missing_orders: `{files['missing_orders_type_ids_path']}`",
        f"- missing_both: `{files['missing_both_type_ids_path']}`",
    ]

    write_json(run_dir / "coverage_plan.json", payload)
    write_summary(run_dir / "summary.md", summary_lines)
    return payload
