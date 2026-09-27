from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping


class ManagedConnection(sqlite3.Connection):
    def __exit__(self, exc_type, exc_value, traceback) -> bool:
        try:
            return super().__exit__(exc_type, exc_value, traceback)
        finally:
            self.close()


def connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(db_path, factory=ManagedConnection)
    connection.row_factory = sqlite3.Row
    return connection


def init_db(db_path: Path, schema_path: Path) -> None:
    schema_sql = schema_path.read_text(encoding="utf-8")
    with connect(db_path) as connection:
        connection.executescript(schema_sql)
        connection.commit()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def record_reference_import_run(
    connection: sqlite3.Connection,
    import_run_id: str,
    import_kind: str,
    source_path: Path,
    details: Mapping[str, Any],
    *,
    source_build: int | None = None,
    source_release_date: str | None = None,
) -> None:
    connection.execute(
        """
        INSERT INTO reference_data_import_run (
            import_run_id,
            import_kind,
            source_path,
            source_build,
            source_release_date,
            imported_at,
            details_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            import_run_id,
            import_kind,
            str(source_path),
            source_build,
            source_release_date,
            utc_now_iso(),
            json.dumps(details, ensure_ascii=False, sort_keys=True),
        ),
    )


def record_data_fetch_run(
    connection: sqlite3.Connection,
    fetch_run_id: str,
    fetch_kind: str,
    source: str,
    output_dir: Path,
    scope: Mapping[str, Any],
    details: Mapping[str, Any],
    *,
    started_at: str,
    finished_at: str | None,
    status: str,
) -> None:
    connection.execute(
        """
        INSERT INTO data_fetch_run (
            fetch_run_id,
            fetch_kind,
            started_at,
            finished_at,
            source,
            scope_json,
            output_dir,
            status,
            details_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            fetch_run_id,
            fetch_kind,
            started_at,
            finished_at,
            source,
            json.dumps(scope, ensure_ascii=False, sort_keys=True),
            str(output_dir),
            status,
            json.dumps(details, ensure_ascii=False, sort_keys=True),
        ),
    )
