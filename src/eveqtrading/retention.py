from __future__ import annotations

import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .run_artifacts import create_run_id, ensure_run_dir, write_json, write_summary


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_now_iso() -> str:
    return _utc_now().replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _mtime_utc(path: Path) -> datetime:
    return datetime.fromtimestamp(path.stat().st_mtime, timezone.utc)


def _path_kind(path: Path) -> str:
    if path.is_dir():
        return "dir"
    if path.is_file():
        return "file"
    return "other"


def _delete_path(path: Path) -> None:
    if path.is_dir():
        shutil.rmtree(path)
        return
    if path.exists():
        path.unlink()


def _collect_run_cleanup(
    *,
    output_root: Path,
    cleanup_run_id: str,
    retain_run_count: int,
) -> list[dict[str, Any]]:
    if retain_run_count < 0:
        raise ValueError("retain_run_count must be >= 0")

    entries = [path for path in output_root.iterdir() if path.is_dir() and path.name != cleanup_run_id]
    entries.sort(key=lambda path: (_mtime_utc(path), path.name), reverse=True)
    victims = entries[retain_run_count:]
    return [
        {
            "category": "runs",
            "path": str(path),
            "name": path.name,
            "kind": "dir",
            "mtime_utc": _mtime_utc(path).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            "reason": f"beyond retain_run_count={retain_run_count}",
        }
        for path in victims
    ]


def _collect_log_cleanup(
    *,
    log_dir: Path,
    pattern: str,
    keep_count: int,
    category: str,
) -> list[dict[str, Any]]:
    if keep_count < 0:
        raise ValueError(f"{category} keep_count must be >= 0")

    entries = [path for path in log_dir.glob(pattern) if path.is_file()]
    entries.sort(key=lambda path: (_mtime_utc(path), path.name), reverse=True)
    victims = entries[keep_count:]
    return [
        {
            "category": category,
            "path": str(path),
            "name": path.name,
            "kind": "file",
            "mtime_utc": _mtime_utc(path).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            "reason": f"beyond keep_count={keep_count} for pattern={pattern}",
        }
        for path in victims
    ]


def _collect_age_cleanup(
    *,
    root: Path,
    category: str,
    max_age_hours: int,
) -> list[dict[str, Any]]:
    if max_age_hours < 0:
        raise ValueError(f"{category} max_age_hours must be >= 0")

    cutoff = _utc_now() - timedelta(hours=max_age_hours)
    victims: list[dict[str, Any]] = []
    for path in sorted(root.iterdir(), key=lambda item: (_mtime_utc(item), item.name)):
        mtime = _mtime_utc(path)
        if mtime >= cutoff:
            continue
        victims.append(
            {
                "category": category,
                "path": str(path),
                "name": path.name,
                "kind": _path_kind(path),
                "mtime_utc": mtime.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
                "reason": f"older than {max_age_hours}h",
            }
        )
    return victims


def _collect_prev_db_cleanup(
    *,
    db_path: Path,
    max_age_hours: int,
) -> list[dict[str, Any]]:
    if max_age_hours < 0:
        raise ValueError("prev db max_age_hours must be >= 0")

    prev_path = Path(str(db_path) + ".prev")
    if not prev_path.exists():
        return []
    cutoff = _utc_now() - timedelta(hours=max_age_hours)
    mtime = _mtime_utc(prev_path)
    if mtime >= cutoff:
        return []
    return [
        {
            "category": "db_prev",
            "path": str(prev_path),
            "name": prev_path.name,
            "kind": "file",
            "mtime_utc": mtime.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            "reason": f"older than {max_age_hours}h",
        }
    ]


def run_retention_cleanup(
    *,
    output_root: Path,
    log_dir: Path | None,
    staging_dir: Path | None,
    db_path: Path | None,
    retain_run_count: int,
    retain_log_count: int,
    retain_sync_report_count: int,
    staging_max_age_hours: int | None,
    prev_db_max_age_hours: int | None,
    dry_run: bool,
) -> dict[str, Any]:
    output_root.mkdir(parents=True, exist_ok=True)
    cleanup_run_id = create_run_id(prefix="retention-cleanup")
    cleanup_run_dir = ensure_run_dir(output_root, cleanup_run_id)

    deletion_candidates: list[dict[str, Any]] = []
    deletion_candidates.extend(
        _collect_run_cleanup(
            output_root=output_root,
            cleanup_run_id=cleanup_run_id,
            retain_run_count=retain_run_count,
        )
    )

    if log_dir is not None:
        log_dir.mkdir(parents=True, exist_ok=True)
        deletion_candidates.extend(
            _collect_log_cleanup(
                log_dir=log_dir,
                pattern="eveq-*.log",
                keep_count=retain_log_count,
                category="logs",
            )
        )
        deletion_candidates.extend(
            _collect_log_cleanup(
                log_dir=log_dir,
                pattern="eveq-foreign-sync-*.json",
                keep_count=retain_sync_report_count,
                category="sync_reports",
            )
        )

    if staging_dir is not None:
        staging_dir.mkdir(parents=True, exist_ok=True)
        if staging_max_age_hours is not None:
            deletion_candidates.extend(
                _collect_age_cleanup(
                    root=staging_dir,
                    category="staging",
                    max_age_hours=staging_max_age_hours,
                )
            )

    if db_path is not None and prev_db_max_age_hours is not None:
        deletion_candidates.extend(
            _collect_prev_db_cleanup(
                db_path=db_path,
                max_age_hours=prev_db_max_age_hours,
            )
        )

    deleted: list[dict[str, Any]] = []
    for row in deletion_candidates:
        path = Path(row["path"])
        if not dry_run:
            _delete_path(path)
        deleted.append({**row, "deleted": not dry_run})

    category_counts: dict[str, int] = {}
    for row in deleted:
        category = str(row["category"])
        category_counts[category] = category_counts.get(category, 0) + 1

    payload = {
        "cleanup_run_id": cleanup_run_id,
        "cleanup_run_dir": str(cleanup_run_dir),
        "cleaned_at_utc": _utc_now_iso(),
        "dry_run": dry_run,
        "config": {
            "output_root": str(output_root),
            "log_dir": str(log_dir) if log_dir is not None else None,
            "staging_dir": str(staging_dir) if staging_dir is not None else None,
            "db_path": str(db_path) if db_path is not None else None,
            "retain_run_count": retain_run_count,
            "retain_log_count": retain_log_count,
            "retain_sync_report_count": retain_sync_report_count,
            "staging_max_age_hours": staging_max_age_hours,
            "prev_db_max_age_hours": prev_db_max_age_hours,
        },
        "summary": {
            "candidate_count": len(deletion_candidates),
            "deleted_count": len(deleted) if not dry_run else 0,
            "dry_run_candidate_count": len(deletion_candidates) if dry_run else 0,
            "category_counts": category_counts,
        },
        "deleted_entries": deleted,
    }

    summary_lines = [
        "# EVEQTrading Retention Cleanup",
        "",
        f"- Cleanup Run ID: `{cleanup_run_id}`",
        f"- Dry Run: `{dry_run}`",
        f"- Candidate Count: `{payload['summary']['candidate_count']}`",
        f"- Deleted Count: `{payload['summary']['deleted_count']}`",
    ]
    if category_counts:
        summary_lines.extend(["", "## Category Counts"])
        for category, count in sorted(category_counts.items()):
            summary_lines.append(f"- `{category}`: `{count}`")

    write_json(cleanup_run_dir / "retention_cleanup.json", payload)
    write_summary(cleanup_run_dir / "summary.md", summary_lines)
    return payload
