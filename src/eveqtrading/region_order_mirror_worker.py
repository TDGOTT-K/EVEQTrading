from __future__ import annotations

import json
import os
import traceback
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator

from .db import connect
from .order_fetch import OrderFetchConfig
from .region_order_mirror import run_region_order_mirror_once
from .run_artifacts import create_run_id, ensure_run_dir, write_json, write_summary

BROKEN_LOCK_STALE_SECONDS = 120


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


def _write_state(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.tmp")
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
    tmp_path.replace(path)


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


def _build_summary_lines(payload: dict[str, Any]) -> list[str]:
    return [
        "# EVEQTrading Region Order Mirror Worker",
        "",
        f"- Worker Run ID: `{payload['worker_run_id']}`",
        f"- Status: `{payload['status']}`",
        f"- Region ID: `{payload['region_id']}`",
        f"- Target Universe: `{payload['target_universe']}`",
        f"- Stop Reason: `{payload['stop_reason']}`",
        f"- Mirror Run ID: `{payload.get('mirror_run_id')}`",
        f"- Pages Fetched: `{((payload.get('orders_fetch') or {}).get('pages_fetched'))}`",
        f"- Type Success Count: `{((payload.get('orders_fetch') or {}).get('type_count_success'))}`",
    ]


def run_region_order_mirror_worker_once(
    *,
    db_path: Path,
    output_root: Path,
    lock_path: Path,
    state_path: Path,
    region_id: int,
    target_universe: str,
    refresh_interval_minutes: int,
    orders_config: OrderFetchConfig,
    limit: int | None = None,
) -> dict[str, Any]:
    worker_run_id = create_run_id(prefix="region-order-mirror-worker")
    worker_started_at = _utc_now_iso()

    try:
        with _acquire_lock(lock_path, worker_run_id=worker_run_id) as lock_info:
            previous_state = _read_json(state_path)
            now = _parse_utc_iso(worker_started_at)
            previous_finished_at = _parse_utc_iso((previous_state or {}).get("finished_at_utc"))
            next_allowed_at = None
            if now is not None and previous_finished_at is not None and refresh_interval_minutes > 0:
                next_allowed_at = previous_finished_at + timedelta(minutes=refresh_interval_minutes)
                if now < next_allowed_at and str((previous_state or {}).get("status")) == "completed":
                    payload = {
                        "worker_run_id": worker_run_id,
                        "status": "skipped_fresh",
                        "started_at_utc": worker_started_at,
                        "finished_at_utc": _utc_now_iso(),
                        "lock_path": str(lock_path),
                        "state_path": str(state_path),
                        "region_id": region_id,
                        "target_universe": target_universe,
                        "refresh_interval_minutes": refresh_interval_minutes,
                        "next_allowed_at_utc": next_allowed_at.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
                        "message": "region order mirror is still within its refresh interval",
                    }
                    _write_state(state_path, payload)
                    return payload

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
                "target_universe": target_universe,
                "refresh_interval_minutes": refresh_interval_minutes,
                "worker_run_dir": str(worker_run_dir),
            }
            _write_state(state_path, running_state)

            connection = connect(db_path)
            try:
                mirror_result = run_region_order_mirror_once(
                    connection,
                    output_root=output_root,
                    region_id=region_id,
                    target_universe=target_universe,
                    orders_config=orders_config,
                    limit=limit,
                )
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
                "target_universe": target_universe,
                "refresh_interval_minutes": refresh_interval_minutes,
                "stop_reason": "completed_single_mirror",
                "mirror_run_id": mirror_result["mirror_run_id"],
                "mirror_run_dir": str(mirror_result["run_dir"]),
                "orders_fetch": mirror_result["orders_result"]["fetch_run"],
                "mirror_payload": mirror_result["payload"],
            }
            write_json(worker_run_dir / "region_order_mirror_worker_run.json", payload)
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
            "region_id": region_id,
            "target_universe": target_universe,
            "refresh_interval_minutes": refresh_interval_minutes,
            "message": "another region order mirror worker run is already active",
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
            "region_id": region_id,
            "target_universe": target_universe,
            "refresh_interval_minutes": refresh_interval_minutes,
            "error_type": type(exc).__name__,
            "error_message": str(exc),
            "traceback": traceback.format_exc(),
        }
        _write_state(state_path, payload)
        raise
