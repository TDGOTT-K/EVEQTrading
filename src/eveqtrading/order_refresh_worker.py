from __future__ import annotations

import json
import os
import time
import traceback
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator

from .db import connect
from .order_fetch import OrderFetchConfig
from .order_refresh import collect_order_refresh_snapshot
from .order_refresh_cycle import run_order_refresh_cycle
from .run_artifacts import create_run_id, ensure_run_dir, write_json, write_summary

BROKEN_LOCK_STALE_SECONDS = 120


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


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


def _read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        raw_text = path.read_text(encoding="utf-8")
    except OSError:
        return None
    if not raw_text.strip():
        return None
    try:
        payload = json.loads(raw_text)
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) else None


def _lock_age_seconds(lock_path: Path) -> float | None:
    try:
        stat = lock_path.stat()
    except OSError:
        return None
    return max(datetime.now(timezone.utc).timestamp() - stat.st_mtime, 0.0)


def _pid_exists(raw_pid: Any) -> bool | None:
    try:
        pid = int(raw_pid)
    except (TypeError, ValueError):
        return None
    if pid <= 0:
        return None
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def _write_state(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.tmp")
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    tmp_path.replace(path)


def _remote_due_count(snapshot: dict[str, Any]) -> int:
    counts = snapshot.get("target_counts", {})
    return int(counts.get("missing_orders_count") or 0) + int(counts.get("refresh_due_remote_count") or 0)


def _build_cycle_summary(cycle_result: dict[str, Any]) -> dict[str, Any]:
    payload = cycle_result["payload"]
    return {
        "cycle_run_id": cycle_result["cycle_run_id"],
        "cycle_run_dir": str(cycle_result["run_dir"]),
        "selected_remote_target_count": int(payload.get("selected_remote_target_count") or 0),
        "executed_remote_target_count": int(payload.get("executed_remote_target_count") or 0),
        "remote_due_before_count": int(payload.get("remote_due_before_count") or 0),
        "remote_due_after_count": int(payload.get("remote_due_after_count") or 0),
        "cache_hit_gain": int(payload.get("cache_hit_gain") or 0),
        "orders_batch_run_id": payload.get("orders_batch_run_id"),
        "orders_batch_totals": payload.get("orders_batch_totals") or {},
    }


def _build_summary_lines(payload: dict[str, Any]) -> list[str]:
    aggregate = payload.get("aggregate", {})
    final_counts = payload.get("final_counts", {})
    cycle_runs = payload.get("cycle_runs", [])
    summary_lines = [
        "# EVEQTrading Order Refresh Worker",
        "",
        f"- Worker Run ID: `{payload['worker_run_id']}`",
        f"- Status: `{payload['status']}`",
        f"- Region ID: `{payload['region_id']}`",
        f"- Profile: `{payload['profile_name']}`",
        f"- Stop Reason: `{payload['stop_reason']}`",
        f"- Cycle Count: `{payload['cycle_count']}`",
        f"- Executed Remote Target Count: `{aggregate.get('executed_remote_target_count', 0)}`",
        f"- Success Count: `{aggregate.get('success_count', 0)}`",
        f"- Skipped Fresh Count: `{aggregate.get('skipped_fresh_count', 0)}`",
        f"- Failed Count: `{aggregate.get('failed_count', 0)}`",
        f"- Remote Due Remaining: `{int(final_counts.get('missing_orders_count', 0)) + int(final_counts.get('refresh_due_remote_count', 0))}`",
        f"- Cache Hit Count: `{final_counts.get('cache_hit_count', 0)}`",
    ]
    if cycle_runs:
        summary_lines.extend(["", "## Cycle Runs"])
        for row in cycle_runs[:20]:
            summary_lines.extend(
                [
                    "",
                    f"- Cycle Run ID: `{row['cycle_run_id']}`",
                    f"  - remote_due_before: `{row['remote_due_before_count']}`",
                    f"  - remote_due_after: `{row['remote_due_after_count']}`",
                    f"  - executed_remote: `{row['executed_remote_target_count']}`",
                    f"  - cache_hit_gain: `{row['cache_hit_gain']}`",
                ]
            )
    return summary_lines


def _build_backoff_payload(
    *,
    worker_run_id: str,
    worker_started_at: str,
    lock_path: Path,
    state_path: Path,
    previous_state: dict[str, Any],
    next_allowed_at: datetime,
) -> dict[str, Any]:
    return {
        "worker_run_id": worker_run_id,
        "status": "skipped_backoff",
        "started_at_utc": worker_started_at,
        "finished_at_utc": _utc_now_iso(),
        "lock_path": str(lock_path),
        "state_path": str(state_path),
        "next_allowed_at_utc": next_allowed_at.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "backoff_source_status": previous_state.get("status"),
        "backoff_source_worker_run_id": previous_state.get("backoff_source_worker_run_id")
        or previous_state.get("worker_run_id"),
        "message": "order refresh worker is in failure backoff window",
    }


def _check_failure_backoff(
    *,
    state_path: Path,
    worker_run_id: str,
    worker_started_at: str,
    lock_path: Path,
    failure_backoff_seconds: int,
) -> dict[str, Any] | None:
    if failure_backoff_seconds <= 0:
        return None
    previous_state = _read_json(state_path)
    if not isinstance(previous_state, dict):
        return None

    next_allowed_at: datetime | None = None
    status = previous_state.get("status")
    if status == "failed":
        finished_at = _parse_utc_iso(previous_state.get("finished_at_utc"))
        if finished_at is None:
            return None
        next_allowed_at = finished_at + timedelta(seconds=failure_backoff_seconds)
    elif status == "skipped_backoff":
        next_allowed_at = _parse_utc_iso(previous_state.get("next_allowed_at_utc"))
        if next_allowed_at is None:
            return None
    else:
        return None

    now = _parse_utc_iso(worker_started_at)
    if now is None or now >= next_allowed_at:
        return None

    return _build_backoff_payload(
        worker_run_id=worker_run_id,
        worker_started_at=worker_started_at,
        lock_path=lock_path,
        state_path=state_path,
        previous_state=previous_state,
        next_allowed_at=next_allowed_at,
    )


@contextmanager
def _acquire_lock(lock_path: Path, *, worker_run_id: str) -> Iterator[dict[str, Any]]:
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_payload = {
        "worker_run_id": worker_run_id,
        "locked_at_utc": _utc_now_iso(),
        "pid": os.getpid(),
    }
    for _ in range(2):
        try:
            fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            break
        except FileExistsError:
            existing = _read_json(lock_path)
            lock_age_seconds = _lock_age_seconds(lock_path)
            pid_alive = _pid_exists(existing.get("pid")) if existing is not None else None
            if (
                lock_age_seconds is not None
                and lock_age_seconds >= BROKEN_LOCK_STALE_SECONDS
                and (existing is None or pid_alive is False)
            ):
                try:
                    lock_path.unlink()
                except OSError:
                    pass
                continue
            raise RuntimeError(
                json.dumps(
                    existing
                    or {
                        "worker_run_id": None,
                        "locked_at_utc": None,
                        "pid": None,
                        "lock_state": "invalid_or_empty",
                        "lock_age_seconds": lock_age_seconds,
                    },
                    ensure_ascii=False,
                )
            )
    else:
        raise RuntimeError(
            json.dumps(
                {
                    "worker_run_id": None,
                    "locked_at_utc": None,
                    "pid": None,
                    "lock_state": "unrecoverable",
                },
                ensure_ascii=False,
            )
        )

    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(lock_payload, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
        yield lock_payload
    finally:
        if lock_path.exists():
            lock_path.unlink()


def run_order_refresh_worker_once(
    *,
    db_path: Path,
    output_root: Path,
    lock_path: Path,
    state_path: Path,
    region_id: int,
    orders_chunk_size: int,
    max_orders_chunks: int | None,
    profile_name: str,
    orders_config: OrderFetchConfig,
    limit: int | None,
    failure_backoff_seconds: int = 0,
    max_runtime_seconds: float | None = None,
    max_cycles_per_run: int | None = None,
) -> dict[str, Any]:
    worker_run_id = create_run_id(prefix="order-refresh-worker")
    worker_started_at = _utc_now_iso()

    backoff_payload = _check_failure_backoff(
        state_path=state_path,
        worker_run_id=worker_run_id,
        worker_started_at=worker_started_at,
        lock_path=lock_path,
        failure_backoff_seconds=failure_backoff_seconds,
    )
    if backoff_payload is not None:
        _write_state(state_path, backoff_payload)
        return backoff_payload

    try:
        with _acquire_lock(lock_path, worker_run_id=worker_run_id) as lock_info:
            worker_run_dir = ensure_run_dir(output_root, worker_run_id)
            running_state = {
                "worker_run_id": worker_run_id,
                "status": "running",
                "started_at_utc": worker_started_at,
                "lock_path": str(lock_path),
                "lock_info": lock_info,
                "db_path": str(db_path),
                "output_root": str(output_root),
                "region_id": region_id,
                "profile_name": profile_name,
                "failure_backoff_seconds": failure_backoff_seconds,
                "worker_run_dir": str(worker_run_dir),
                "max_runtime_seconds": max_runtime_seconds,
                "max_cycles_per_run": max_cycles_per_run,
                "cycle_count": 0,
                "cycle_run_ids": [],
                "aggregate": {
                    "executed_remote_target_count": 0,
                    "success_count": 0,
                    "skipped_fresh_count": 0,
                    "failed_count": 0,
                },
            }
            _write_state(state_path, running_state)

            connection = connect(db_path)
            try:
                initial_snapshot = collect_order_refresh_snapshot(
                    connection,
                    region_id=region_id,
                    profile_name=profile_name,
                    order_type=orders_config.order_type,
                    limit=limit,
                )
                aggregate = {
                    "executed_remote_target_count": 0,
                    "success_count": 0,
                    "skipped_fresh_count": 0,
                    "failed_count": 0,
                }
                cycle_runs: list[dict[str, Any]] = []
                cycle_run_ids: list[str] = []
                last_cycle_run_id: str | None = None
                last_cycle_run_dir: str | None = None
                stop_reason = "no_due_targets" if _remote_due_count(initial_snapshot) == 0 else None
                final_counts = dict(initial_snapshot["target_counts"])
                runtime_started_monotonic = time.monotonic()

                while stop_reason is None:
                    cycle_result = run_order_refresh_cycle(
                        connection,
                        output_root=output_root,
                        region_id=region_id,
                        orders_chunk_size=orders_chunk_size,
                        max_orders_chunks=max_orders_chunks,
                        limit=limit,
                        profile_name=profile_name,
                        orders_config=orders_config,
                    )
                    cycle_summary = _build_cycle_summary(cycle_result)
                    cycle_runs.append(cycle_summary)
                    cycle_run_ids.append(cycle_result["cycle_run_id"])
                    last_cycle_run_id = cycle_result["cycle_run_id"]
                    last_cycle_run_dir = str(cycle_result["run_dir"])
                    aggregate["executed_remote_target_count"] += cycle_summary["executed_remote_target_count"]
                    aggregate["success_count"] += int(
                        cycle_summary["orders_batch_totals"].get("success") or 0
                    )
                    aggregate["skipped_fresh_count"] += int(
                        cycle_summary["orders_batch_totals"].get("skipped_fresh") or 0
                    )
                    aggregate["failed_count"] += int(
                        cycle_summary["orders_batch_totals"].get("failed") or 0
                    )
                    final_counts = dict(cycle_result["payload"]["after_counts"])

                    _write_state(
                        state_path,
                        {
                            **running_state,
                            "cycle_count": len(cycle_runs),
                            "cycle_run_ids": cycle_run_ids,
                            "last_cycle_run_id": last_cycle_run_id,
                            "last_cycle_run_dir": last_cycle_run_dir,
                            "aggregate": aggregate,
                            "initial_counts": initial_snapshot["target_counts"],
                            "final_counts": final_counts,
                        },
                    )

                    due_remaining = int(final_counts.get("missing_orders_count") or 0) + int(
                        final_counts.get("refresh_due_remote_count") or 0
                    )
                    if due_remaining == 0:
                        stop_reason = "no_due_targets"
                    elif max_cycles_per_run is not None and len(cycle_runs) >= max_cycles_per_run:
                        stop_reason = "max_cycles_reached"
                    elif max_runtime_seconds is not None and (
                        time.monotonic() - runtime_started_monotonic
                    ) >= max_runtime_seconds:
                        stop_reason = "max_runtime_reached"
            finally:
                connection.close()

            payload = {
                "worker_run_id": worker_run_id,
                "status": "completed",
                "started_at_utc": worker_started_at,
                "finished_at_utc": _utc_now_iso(),
                "lock_path": str(lock_path),
                "state_path": str(state_path),
                "worker_run_dir": str(worker_run_dir),
                "region_id": region_id,
                "profile_name": profile_name,
                "max_runtime_seconds": max_runtime_seconds,
                "max_cycles_per_run": max_cycles_per_run,
                "cycle_count": len(cycle_runs),
                "cycle_run_ids": cycle_run_ids,
                "last_cycle_run_id": last_cycle_run_id,
                "last_cycle_run_dir": last_cycle_run_dir,
                "cycle_runs": cycle_runs,
                "aggregate": aggregate,
                "initial_counts": initial_snapshot["target_counts"],
                "final_counts": final_counts,
                "stop_reason": stop_reason,
                "failure_backoff_seconds": failure_backoff_seconds,
            }
            write_json(worker_run_dir / "order_refresh_worker_run.json", payload)
            write_summary(worker_run_dir / "summary.md", _build_summary_lines(payload))
            _write_state(state_path, payload)
            return payload
    except RuntimeError as exc:
        try:
            lock_info = json.loads(str(exc))
        except json.JSONDecodeError:
            lock_info = {"raw": str(exc)}
        payload = {
            "worker_run_id": worker_run_id,
            "status": "skipped_locked",
            "started_at_utc": worker_started_at,
            "finished_at_utc": _utc_now_iso(),
            "lock_path": str(lock_path),
            "state_path": str(state_path),
            "lock_info": lock_info,
            "message": "another order refresh worker run is already active",
            "failure_backoff_seconds": failure_backoff_seconds,
        }
        _write_state(state_path, payload)
        return payload
    except Exception as exc:
        payload = {
            "worker_run_id": worker_run_id,
            "status": "failed",
            "started_at_utc": worker_started_at,
            "finished_at_utc": _utc_now_iso(),
            "lock_path": str(lock_path),
            "state_path": str(state_path),
            "error_type": type(exc).__name__,
            "error_message": str(exc),
            "traceback": traceback.format_exc(),
            "failure_backoff_seconds": failure_backoff_seconds,
        }
        _write_state(state_path, payload)
        raise
