from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .db import record_data_fetch_run
from .history_batch import run_history_batch_fetch
from .history_fetch import HistoryFetchConfig
from .run_artifacts import create_run_id, ensure_run_dir, write_json, write_summary


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_now_iso() -> str:
    return _utc_now().replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_utc_iso(raw_value: Any) -> datetime | None:
    if not isinstance(raw_value, str) or not raw_value:
        return None
    normalized = raw_value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


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


def _load_history_stats(connection, *, region_id: int) -> dict[int, dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT
            type_id,
            COUNT(*) AS row_count,
            MIN(market_date) AS first_market_date,
            MAX(market_date) AS latest_market_date,
            MAX(fetched_at) AS latest_fetched_at
        FROM market_history_daily
        WHERE region_id = ?
        GROUP BY type_id
        """,
        (region_id,),
    ).fetchall()
    return {
        int(row["type_id"]): {
            "row_count": int(row["row_count"]),
            "first_market_date": row["first_market_date"],
            "latest_market_date": row["latest_market_date"],
            "latest_fetched_at": row["latest_fetched_at"],
        }
        for row in rows
    }


def _load_history_sync_rows(connection, *, region_id: int) -> dict[int, dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT type_id, last_success_at, last_attempt_at, next_scheduled_at, failure_count, last_error, coverage_marker
        FROM sync_scope_state
        WHERE scope_type = 'market_history'
          AND region_id = ?
        """,
        (region_id,),
    ).fetchall()
    return {
        int(row["type_id"]): {
            "last_success_at": row["last_success_at"],
            "last_attempt_at": row["last_attempt_at"],
            "next_scheduled_at": row["next_scheduled_at"],
            "failure_count": int(row["failure_count"] or 0),
            "last_error": row["last_error"],
            "coverage_marker": row["coverage_marker"],
        }
        for row in rows
    }


def _hours_since(raw_value: Any, *, now: datetime) -> float | None:
    parsed = _parse_utc_iso(raw_value)
    if parsed is None:
        return None
    return max((now - parsed).total_seconds() / 3600.0, 0.0)


def _history_sync_status(coverage_marker: Any) -> str:
    marker = str(coverage_marker or "")
    if marker.startswith("ok:"):
        return "ok"
    if marker == "not_found":
        return "not_found"
    if marker.startswith("failed:"):
        return "failed"
    if marker:
        return "unknown"
    return "never_attempted"


def _desired_history_action(*, row_count: int, sync_status: str) -> str:
    # A type is not history-ready until it has the full rolling 30-day window.
    # Partial history must stay on the backfill path instead of downgrading to
    # latest-day refresh, otherwise the backlog can stall permanently at 1-29 rows.
    if row_count < 30:
        return "backfill"
    if sync_status in {"ok", "failed"}:
        return "refresh"
    return "refresh" if row_count > 0 else "backfill"


def collect_history_sync_snapshot(
    connection,
    *,
    region_id: int,
    refresh_interval_hours: float,
    not_found_recheck_hours: float,
    limit: int | None = None,
    selected_type_ids: set[int] | None = None,
) -> dict[str, Any]:
    now = _utc_now()
    market_rows = _load_market_type_rows(connection)
    if selected_type_ids is not None:
        market_rows = [row for row in market_rows if int(row["type_id"]) in selected_type_ids]
    if limit is not None:
        market_rows = market_rows[:limit]

    history_stats = _load_history_stats(connection, region_id=region_id)
    history_sync_rows = _load_history_sync_rows(connection, region_id=region_id)

    healthy_rows: list[dict[str, Any]] = []
    backfill_due_rows: list[dict[str, Any]] = []
    backfill_wait_rows: list[dict[str, Any]] = []
    refresh_due_rows: list[dict[str, Any]] = []
    refresh_wait_rows: list[dict[str, Any]] = []

    for row in market_rows:
        type_id = int(row["type_id"])
        stats = history_stats.get(type_id, {})
        sync_row = history_sync_rows.get(type_id, {})
        row_count = int(stats.get("row_count") or 0)
        coverage_marker = sync_row.get("coverage_marker")
        sync_status = _history_sync_status(coverage_marker)
        last_success_age_hours = _hours_since(sync_row.get("last_success_at"), now=now)
        next_scheduled_at = _parse_utc_iso(sync_row.get("next_scheduled_at"))
        schedule_due = next_scheduled_at is None or next_scheduled_at <= now
        desired_action = _desired_history_action(
            row_count=row_count,
            sync_status=sync_status,
        )

        if desired_action == "backfill":
            if next_scheduled_at is not None:
                action_due = schedule_due
            elif sync_status == "not_found":
                action_due = (
                    last_success_age_hours is None
                    or last_success_age_hours >= not_found_recheck_hours
                )
            else:
                action_due = True
        else:
            if next_scheduled_at is not None:
                action_due = schedule_due
            elif sync_status == "ok":
                action_due = (
                    last_success_age_hours is not None
                    and last_success_age_hours >= refresh_interval_hours
                )
            else:
                action_due = True

        not_found_recheck_due = desired_action == "backfill" and sync_status == "not_found" and action_due
        needs_backfill = desired_action == "backfill" and action_due
        needs_latest_refresh = desired_action == "refresh" and action_due
        annotated = {
            **row,
            "row_count": row_count,
            "first_market_date": stats.get("first_market_date"),
            "latest_market_date": stats.get("latest_market_date"),
            "latest_fetched_at": stats.get("latest_fetched_at"),
            "last_success_at": sync_row.get("last_success_at"),
            "last_attempt_at": sync_row.get("last_attempt_at"),
            "next_scheduled_at": sync_row.get("next_scheduled_at"),
            "failure_count": int(sync_row.get("failure_count") or 0),
            "last_error": sync_row.get("last_error"),
            "coverage_marker": coverage_marker,
            "sync_status": sync_status,
            "desired_action": desired_action,
            "has_history_any": row_count > 0,
            "has_history_30d": row_count >= 30,
            "last_success_age_hours": last_success_age_hours,
            "schedule_due": schedule_due,
            "not_found_recheck_due": not_found_recheck_due,
            "needs_backfill": needs_backfill,
            "needs_latest_refresh": needs_latest_refresh,
        }
        if needs_backfill:
            backfill_due_rows.append(annotated)
        elif desired_action == "backfill":
            backfill_wait_rows.append(annotated)
        elif needs_latest_refresh:
            refresh_due_rows.append(annotated)
        elif sync_status != "ok":
            refresh_wait_rows.append(annotated)
        else:
            healthy_rows.append(annotated)

    return {
        "generated_at_utc": _utc_now_iso(),
        "region_id": region_id,
        "refresh_interval_hours": refresh_interval_hours,
        "not_found_recheck_hours": not_found_recheck_hours,
        "target_counts": {
            "market_type_count": len(market_rows),
            "healthy_count": len(healthy_rows),
            "backfill_due_count": len(backfill_due_rows),
            "backfill_wait_count": len(backfill_wait_rows),
            "refresh_due_count": len(refresh_due_rows),
            "refresh_wait_count": len(refresh_wait_rows),
            "history_any_count": sum(1 for row in market_rows if int(history_stats.get(int(row["type_id"]), {}).get("row_count") or 0) > 0),
            "history_ready_30d_count": sum(1 for row in market_rows if int(history_stats.get(int(row["type_id"]), {}).get("row_count") or 0) >= 30),
        },
        "type_ids": {
            "healthy": [int(row["type_id"]) for row in healthy_rows],
            "backfill_due": [int(row["type_id"]) for row in backfill_due_rows],
            "backfill_wait": [int(row["type_id"]) for row in backfill_wait_rows],
            "refresh_due": [int(row["type_id"]) for row in refresh_due_rows],
            "refresh_wait": [int(row["type_id"]) for row in refresh_wait_rows],
        },
        "rows": {
            "healthy": healthy_rows,
            "backfill_due": backfill_due_rows,
            "backfill_wait": backfill_wait_rows,
            "refresh_due": refresh_due_rows,
            "refresh_wait": refresh_wait_rows,
        },
        "sample_rows": {
            "healthy": healthy_rows[:10],
            "backfill_due": backfill_due_rows[:10],
            "backfill_wait": backfill_wait_rows[:10],
            "refresh_due": refresh_due_rows[:10],
            "refresh_wait": refresh_wait_rows[:10],
        },
    }


def _build_type_targets(rows: list[dict[str, Any]], *, selection_mode: str) -> list[dict[str, Any]]:
    return [
        {
            "type_id": int(row["type_id"]),
            "type_name": str(row["type_name"]),
            "selection_mode": selection_mode,
        }
        for row in rows
    ]


def run_history_sync_cycle(
    connection,
    *,
    db_path: Path,
    output_root: Path,
    region_id: int,
    backfill_chunk_size: int,
    max_backfill_chunks: int | None,
    refresh_chunk_size: int,
    max_refresh_chunks: int | None,
    history_config: HistoryFetchConfig,
    refresh_interval_hours: float,
    not_found_recheck_hours: float,
    limit: int | None = None,
) -> dict[str, Any]:
    cycle_run_id = create_run_id(prefix="history-sync")
    cycle_run_dir = ensure_run_dir(output_root, cycle_run_id)
    started_at = _utc_now()

    before_snapshot = collect_history_sync_snapshot(
        connection,
        region_id=region_id,
        refresh_interval_hours=refresh_interval_hours,
        not_found_recheck_hours=not_found_recheck_hours,
        limit=limit,
    )

    backfill_targets = _build_type_targets(
        before_snapshot["rows"]["backfill_due"],
        selection_mode="history-sync-backfill",
    )
    _write_type_id_file(cycle_run_dir / "backfill_target_ids.txt", [int(row["type_id"]) for row in backfill_targets])
    _write_type_id_file(cycle_run_dir / "refresh_target_ids.txt", [])

    backfill_result = None
    if backfill_targets:
        backfill_result = run_history_batch_fetch(
            connection,
            output_root=output_root,
            type_targets=backfill_targets,
            config=HistoryFetchConfig(
                region_id=history_config.region_id,
                datasource=history_config.datasource,
                esi_base_url=history_config.esi_base_url,
                user_agent=history_config.user_agent,
                request_timeout_seconds=history_config.request_timeout_seconds,
                concurrent_requests=history_config.concurrent_requests,
                request_wall_timeout_seconds=history_config.request_wall_timeout_seconds,
                max_retries=history_config.max_retries,
                pause_seconds=history_config.pause_seconds,
                refresh_mode="full",
                refresh_interval_hours=refresh_interval_hours,
                not_found_recheck_hours=not_found_recheck_hours,
                failure_backoff_seconds=history_config.failure_backoff_seconds,
            ),
            chunk_size=backfill_chunk_size,
            max_chunks=max_backfill_chunks,
        )

    after_backfill_snapshot = collect_history_sync_snapshot(
        connection,
        region_id=region_id,
        refresh_interval_hours=refresh_interval_hours,
        not_found_recheck_hours=not_found_recheck_hours,
        limit=limit,
    )
    refresh_targets = _build_type_targets(
        after_backfill_snapshot["rows"]["refresh_due"],
        selection_mode="history-sync-refresh",
    )
    _write_type_id_file(cycle_run_dir / "refresh_target_ids.txt", [int(row["type_id"]) for row in refresh_targets])

    refresh_result = None
    if refresh_targets:
        refresh_result = run_history_batch_fetch(
            connection,
            output_root=output_root,
            type_targets=refresh_targets,
            config=HistoryFetchConfig(
                region_id=history_config.region_id,
                datasource=history_config.datasource,
                esi_base_url=history_config.esi_base_url,
                user_agent=history_config.user_agent,
                request_timeout_seconds=history_config.request_timeout_seconds,
                concurrent_requests=history_config.concurrent_requests,
                request_wall_timeout_seconds=history_config.request_wall_timeout_seconds,
                max_retries=history_config.max_retries,
                pause_seconds=history_config.pause_seconds,
                refresh_mode="latest_only",
                refresh_interval_hours=refresh_interval_hours,
                not_found_recheck_hours=not_found_recheck_hours,
                failure_backoff_seconds=history_config.failure_backoff_seconds,
            ),
            chunk_size=refresh_chunk_size,
            max_chunks=max_refresh_chunks,
        )

    after_refresh_snapshot = collect_history_sync_snapshot(
        connection,
        region_id=region_id,
        refresh_interval_hours=refresh_interval_hours,
        not_found_recheck_hours=not_found_recheck_hours,
        limit=limit,
    )

    finished_at = _utc_now()
    payload = {
        "cycle_run_id": cycle_run_id,
        "started_at_utc": started_at.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "finished_at_utc": finished_at.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "region_id": region_id,
        "limit": limit,
        "config": {
            "backfill_chunk_size": backfill_chunk_size,
            "max_backfill_chunks": max_backfill_chunks,
            "refresh_chunk_size": refresh_chunk_size,
            "max_refresh_chunks": max_refresh_chunks,
            "refresh_interval_hours": refresh_interval_hours,
            "not_found_recheck_hours": not_found_recheck_hours,
            "history_fetch": history_config.to_dict(),
        },
        "before_counts": before_snapshot["target_counts"],
        "after_backfill_counts": after_backfill_snapshot["target_counts"],
        "after_refresh_counts": after_refresh_snapshot["target_counts"],
        "backfill_target_count": len(backfill_targets),
        "refresh_target_count": len(refresh_targets),
        "backfill_executed_target_count": (
            int(backfill_result["payload"]["totals"]["executed_target_count"])
            if backfill_result is not None
            else 0
        ),
        "refresh_executed_target_count": (
            int(refresh_result["payload"]["totals"]["executed_target_count"])
            if refresh_result is not None
            else 0
        ),
        "backfill_batch_run_id": backfill_result["batch_run_id"] if backfill_result is not None else None,
        "refresh_batch_run_id": refresh_result["batch_run_id"] if refresh_result is not None else None,
        "backfill_batch_run_dir": str(backfill_result["run_dir"]) if backfill_result is not None else None,
        "refresh_batch_run_dir": str(refresh_result["run_dir"]) if refresh_result is not None else None,
        "files": {
            "backfill_target_ids_path": str(cycle_run_dir / "backfill_target_ids.txt"),
            "refresh_target_ids_path": str(cycle_run_dir / "refresh_target_ids.txt"),
        },
    }

    summary_lines = [
        "# EVEQTrading History Sync Cycle",
        "",
        f"- Cycle Run ID: `{cycle_run_id}`",
        f"- Region ID: `{region_id}`",
        f"- Market Types Considered: `{before_snapshot['target_counts']['market_type_count']}`",
        f"- Backfill Due Before: `{before_snapshot['target_counts']['backfill_due_count']}`",
        f"- Backfill Wait Before: `{before_snapshot['target_counts']['backfill_wait_count']}`",
        f"- Refresh Due Before: `{before_snapshot['target_counts']['refresh_due_count']}`",
        f"- Refresh Wait Before: `{before_snapshot['target_counts']['refresh_wait_count']}`",
        f"- Backfill Selected Target Count: `{len(backfill_targets)}`",
        f"- Backfill Executed Target Count: `{payload['backfill_executed_target_count']}`",
        f"- Refresh Selected Target Count: `{len(refresh_targets)}`",
        f"- Refresh Executed Target Count: `{payload['refresh_executed_target_count']}`",
        f"- Backfill Batch Run ID: `{payload['backfill_batch_run_id']}`",
        f"- Refresh Batch Run ID: `{payload['refresh_batch_run_id']}`",
        f"- Healthy Count After Refresh: `{after_refresh_snapshot['target_counts']['healthy_count']}`",
        f"- Backfill Due After Refresh: `{after_refresh_snapshot['target_counts']['backfill_due_count']}`",
        f"- Backfill Wait After Refresh: `{after_refresh_snapshot['target_counts']['backfill_wait_count']}`",
        f"- Refresh Due After Refresh: `{after_refresh_snapshot['target_counts']['refresh_due_count']}`",
        f"- Refresh Wait After Refresh: `{after_refresh_snapshot['target_counts']['refresh_wait_count']}`",
    ]

    write_json(cycle_run_dir / "history_sync_cycle.json", payload)
    write_json(cycle_run_dir / "before_history_sync_snapshot.json", before_snapshot)
    write_json(cycle_run_dir / "after_backfill_history_sync_snapshot.json", after_backfill_snapshot)
    write_json(cycle_run_dir / "after_refresh_history_sync_snapshot.json", after_refresh_snapshot)
    write_summary(cycle_run_dir / "summary.md", summary_lines)

    record_data_fetch_run(
        connection,
        fetch_run_id=cycle_run_id,
        fetch_kind="market_history_sync_cycle",
        source="history-sync-cycle",
        output_dir=cycle_run_dir.resolve(),
        scope={
            "region_id": region_id,
            "backfill_chunk_size": backfill_chunk_size,
            "refresh_chunk_size": refresh_chunk_size,
            "limit": limit,
        },
        details={
            "before_counts": before_snapshot["target_counts"],
            "after_backfill_counts": after_backfill_snapshot["target_counts"],
            "after_refresh_counts": after_refresh_snapshot["target_counts"],
            "backfill_target_count": len(backfill_targets),
            "refresh_target_count": len(refresh_targets),
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
        "before_snapshot": before_snapshot,
        "after_backfill_snapshot": after_backfill_snapshot,
        "after_refresh_snapshot": after_refresh_snapshot,
    }
