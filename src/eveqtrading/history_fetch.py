from __future__ import annotations

import concurrent.futures
import json
import http.client
import multiprocessing
import socket
import sqlite3
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any

from .catalog_select import resolve_type_targets
from .db import record_data_fetch_run, utc_now_iso
from .market_scope import resolve_history_source_segments
from .run_artifacts import create_run_id, ensure_run_dir, write_json, write_summary

RETRYABLE_STATUS_CODES = {420, 429, 500, 502, 503, 504}


@dataclass
class HistoryFetchConfig:
    region_id: int
    datasource: str
    esi_base_url: str
    user_agent: str
    request_timeout_seconds: float
    max_retries: int
    pause_seconds: float
    concurrent_requests: int = 1
    request_wall_timeout_seconds: float | None = None
    refresh_mode: str = "full"
    refresh_interval_hours: float | None = None
    not_found_recheck_hours: float | None = None
    failure_backoff_seconds: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "region_id": self.region_id,
            "datasource": self.datasource,
            "esi_base_url": self.esi_base_url,
            "user_agent": self.user_agent,
            "request_timeout_seconds": self.request_timeout_seconds,
            "max_retries": self.max_retries,
            "pause_seconds": self.pause_seconds,
            "concurrent_requests": self.concurrent_requests,
            "request_wall_timeout_seconds": self.request_wall_timeout_seconds,
            "refresh_mode": self.refresh_mode,
            "refresh_interval_hours": self.refresh_interval_hours,
            "not_found_recheck_hours": self.not_found_recheck_hours,
            "failure_backoff_seconds": self.failure_backoff_seconds,
        }


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _parse_retry_after(raw_value: str | None) -> float | None:
    if not raw_value:
        return None
    raw_value = raw_value.strip()
    if not raw_value:
        return None
    if raw_value.isdigit():
        return float(raw_value)
    try:
        retry_at = parsedate_to_datetime(raw_value)
    except (TypeError, ValueError, IndexError):
        return None
    now = datetime.now(retry_at.tzinfo or timezone.utc)
    delta = (retry_at - now).total_seconds()
    return max(delta, 0.0)


def _resolve_retry_delay(attempt: int, retry_after: str | None) -> float:
    retry_after_seconds = _parse_retry_after(retry_after)
    if retry_after_seconds is not None:
        return min(retry_after_seconds, 60.0)
    return min(2**attempt, 30.0)


def _build_history_url(*, region_id: int, type_id: int, datasource: str, esi_base_url: str) -> str:
    base_url = esi_base_url.rstrip("/")
    return f"{base_url}/markets/{region_id}/history/?datasource={datasource}&type_id={type_id}"


def _request_history_once(
    *,
    url: str,
    user_agent: str,
    request_timeout_seconds: float,
) -> dict[str, Any]:
    request = urllib.request.Request(url, headers={"User-Agent": user_agent})
    try:
        with urllib.request.urlopen(request, timeout=request_timeout_seconds) as response:
            payload = json.loads(response.read().decode("utf-8"))
            return {
                "ok": True,
                "status_code": response.status,
                "url": url,
                "headers": {
                    "expires": response.headers.get("Expires"),
                    "last_modified": response.headers.get("Last-Modified"),
                    "etag": response.headers.get("ETag"),
                },
                "entries": payload,
                "error": None,
                "retryable": False,
                "retry_after": None,
            }
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        return {
            "ok": False,
            "status_code": exc.code,
            "url": url,
            "headers": {
                "expires": exc.headers.get("Expires"),
                "last_modified": exc.headers.get("Last-Modified"),
                "etag": exc.headers.get("ETag"),
            },
            "entries": [],
            "error": body,
            "retryable": exc.code in RETRYABLE_STATUS_CODES,
            "retry_after": exc.headers.get("Retry-After"),
        }
    except urllib.error.URLError as exc:
        return {
            "ok": False,
            "status_code": None,
            "url": url,
            "headers": {},
            "entries": [],
            "error": str(exc.reason),
            "retryable": True,
            "retry_after": None,
        }
    except (
        socket.timeout,
        http.client.HTTPException,
    ) as exc:
        return {
            "ok": False,
            "status_code": None,
            "url": url,
            "headers": {},
            "entries": [],
            "error": str(exc),
            "retryable": True,
            "retry_after": None,
        }


def _request_history_once_worker(connection, kwargs: dict[str, Any]) -> None:
    try:
        connection.send(_request_history_once(**kwargs))
    finally:
        connection.close()


def _request_history_once_with_deadline(
    *,
    url: str,
    user_agent: str,
    request_timeout_seconds: float,
    request_wall_timeout_seconds: float,
) -> dict[str, Any]:
    context = multiprocessing.get_context("spawn")
    parent_connection, child_connection = context.Pipe(duplex=False)
    process = context.Process(
        target=_request_history_once_worker,
        args=(
            child_connection,
            {
                "url": url,
                "user_agent": user_agent,
                "request_timeout_seconds": request_timeout_seconds,
            },
        ),
    )
    process.start()
    child_connection.close()
    try:
        if not parent_connection.poll(request_wall_timeout_seconds):
            process.terminate()
            process.join(5.0)
            return {
                "ok": False,
                "status_code": None,
                "url": url,
                "headers": {},
                "entries": [],
                "error": f"request wall timeout after {request_wall_timeout_seconds} seconds",
                "retryable": True,
                "retry_after": None,
            }
        try:
            payload = parent_connection.recv()
        except EOFError:
            return {
                "ok": False,
                "status_code": None,
                "url": url,
                "headers": {},
                "entries": [],
                "error": f"request worker exited without payload (exit_code={process.exitcode})",
                "retryable": True,
                "retry_after": None,
            }
    finally:
        parent_connection.close()
        process.join(5.0)
    return payload


def _perform_request_history_once(
    *,
    url: str,
    user_agent: str,
    request_timeout_seconds: float,
    request_wall_timeout_seconds: float | None,
) -> dict[str, Any]:
    if request_wall_timeout_seconds is None or request_wall_timeout_seconds <= 0:
        return _request_history_once(
            url=url,
            user_agent=user_agent,
            request_timeout_seconds=request_timeout_seconds,
        )
    return _request_history_once_with_deadline(
        url=url,
        user_agent=user_agent,
        request_timeout_seconds=request_timeout_seconds,
        request_wall_timeout_seconds=request_wall_timeout_seconds,
    )


def _build_unhandled_request_failure(*, url: str, error: Exception) -> dict[str, Any]:
    return {
        "ok": False,
        "status_code": None,
        "url": url,
        "headers": {},
        "entries": [],
        "error": f"{type(error).__name__}: {error}",
        "retries_used": 0,
    }


def _request_history(
    *,
    region_id: int,
    type_id: int,
    datasource: str,
    esi_base_url: str,
    user_agent: str,
    request_timeout_seconds: float,
    max_retries: int,
    request_wall_timeout_seconds: float | None = None,
) -> dict[str, Any]:
    url = _build_history_url(
        region_id=region_id,
        type_id=type_id,
        datasource=datasource,
        esi_base_url=esi_base_url,
    )
    attempt = 0
    retries_used = 0
    while True:
        response = _perform_request_history_once(
            url=url,
            user_agent=user_agent,
            request_timeout_seconds=request_timeout_seconds,
            request_wall_timeout_seconds=request_wall_timeout_seconds,
        )
        retryable = bool(response.pop("retryable", False))
        retry_after = response.pop("retry_after", None)
        if retryable and attempt < max_retries:
            delay = _resolve_retry_delay(attempt, retry_after)
            time.sleep(delay)
            attempt += 1
            retries_used += 1
            continue
        response["retries_used"] = retries_used
        return response


def resolve_history_fetch_targets(
    connection: sqlite3.Connection,
    *,
    explicit_type_ids: list[int],
    type_id_file: Path | None,
    all_published_market_types: bool,
    limit: int | None,
) -> list[dict[str, Any]]:
    return resolve_type_targets(
        connection,
        explicit_type_ids=explicit_type_ids,
        type_id_file=type_id_file,
        all_published_market_types=all_published_market_types,
        limit=limit,
    )


def _current_failure_count(connection: sqlite3.Connection, scope_key: str) -> int:
    row = connection.execute(
        "SELECT failure_count FROM sync_scope_state WHERE scope_key = ?",
        (scope_key,),
    ).fetchone()
    if row is None:
        return 0
    return int(row["failure_count"])


def _current_last_success_at(connection: sqlite3.Connection, scope_key: str) -> str | None:
    row = connection.execute(
        "SELECT last_success_at FROM sync_scope_state WHERE scope_key = ?",
        (scope_key,),
    ).fetchone()
    if row is None:
        return None
    return row["last_success_at"]


def _shift_utc_iso(*, base_time: str, hours: float | None = None, seconds: int | None = None) -> str | None:
    if hours is None and seconds is None:
        return None
    parsed = datetime.fromisoformat(base_time.replace("Z", "+00:00"))
    delta = timedelta(hours=hours or 0.0, seconds=seconds or 0)
    shifted = parsed + delta
    return shifted.replace(microsecond=0).isoformat()


def _upsert_sync_scope_state(
    connection: sqlite3.Connection,
    *,
    scope_key: str,
    scope_type: str,
    region_id: int,
    type_id: int,
    last_success_at: str | None,
    last_attempt_at: str,
    next_scheduled_at: str | None,
    failure_count: int,
    last_error: str | None,
    coverage_marker: str,
) -> None:
    connection.execute(
        """
        INSERT INTO sync_scope_state (
            scope_key,
            scope_type,
            region_id,
            type_id,
            last_success_at,
            last_attempt_at,
            next_scheduled_at,
            failure_count,
            last_error,
            coverage_marker
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(scope_key) DO UPDATE SET
            scope_type = excluded.scope_type,
            region_id = excluded.region_id,
            type_id = excluded.type_id,
            last_success_at = excluded.last_success_at,
            last_attempt_at = excluded.last_attempt_at,
            next_scheduled_at = excluded.next_scheduled_at,
            failure_count = excluded.failure_count,
            last_error = excluded.last_error,
            coverage_marker = excluded.coverage_marker
        """,
        (
            scope_key,
            scope_type,
            region_id,
            type_id,
            last_success_at,
            last_attempt_at,
            next_scheduled_at,
            failure_count,
            last_error,
            coverage_marker,
        ),
    )


def _rows_for_refresh_mode(
    entries: list[dict[str, Any]],
    *,
    refresh_mode: str,
) -> list[dict[str, Any]]:
    if refresh_mode != "latest_only" or not entries:
        return entries
    latest_market_date = max(str(entry["date"]) for entry in entries)
    return [entry for entry in entries if str(entry["date"]) == latest_market_date]


def _store_history_rows(
    connection: sqlite3.Connection,
    *,
    region_id: int,
    type_id: int,
    entries: list[dict[str, Any]],
    fetched_at: str,
) -> int:
    rows_to_write = _rows_for_refresh_mode(entries, refresh_mode="full")
    rows = [
        (
            type_id,
            int(entry.get("source_region_id", region_id)),
            entry["date"],
            entry["average"],
            entry["highest"],
            entry["lowest"],
            entry["volume"],
            entry["order_count"],
            fetched_at,
            "esi-public-history",
            "ok",
        )
        for entry in rows_to_write
    ]
    if rows:
        connection.executemany(
            """
            INSERT INTO market_history_daily (
                type_id,
                region_id,
                market_date,
                average_price,
                highest_price,
                lowest_price,
                volume,
                order_count,
                fetched_at,
                source,
                coverage_status
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(type_id, region_id, market_date) DO UPDATE SET
                average_price = excluded.average_price,
                highest_price = excluded.highest_price,
                lowest_price = excluded.lowest_price,
                volume = excluded.volume,
                order_count = excluded.order_count,
                fetched_at = excluded.fetched_at,
                source = excluded.source,
                coverage_status = excluded.coverage_status
            """,
            rows,
        )
    return len(rows)


def _store_history_rows_for_mode(
    connection: sqlite3.Connection,
    *,
    region_id: int,
    type_id: int,
    entries: list[dict[str, Any]],
    fetched_at: str,
    refresh_mode: str,
) -> int:
    rows_to_write = _rows_for_refresh_mode(entries, refresh_mode=refresh_mode)
    return _store_history_rows(
        connection,
        region_id=region_id,
        type_id=type_id,
        entries=rows_to_write,
        fetched_at=fetched_at,
    )


def _apply_history_response(
    connection: sqlite3.Connection,
    *,
    ordinal: int,
    target: dict[str, Any],
    response: dict[str, Any],
    config: HistoryFetchConfig,
    fetched_at: str,
) -> tuple[dict[str, Any], dict[str, int]]:
    type_id = int(target["type_id"])
    type_name = str(target["type_name"])
    scope_key = f"market_history:{config.region_id}:{type_id}"
    rows_written = 0
    count_updates = {
        "success": 0,
        "not_found": 0,
        "failed": 0,
        "history_rows_received": 0,
        "history_rows_written": 0,
    }

    if response["ok"]:
        entries = response["entries"]
        rows_written = _store_history_rows_for_mode(
            connection,
            region_id=config.region_id,
            type_id=type_id,
            entries=entries,
            fetched_at=fetched_at,
            refresh_mode=config.refresh_mode,
        )
        last_date = entries[-1]["date"] if entries else None
        _upsert_sync_scope_state(
            connection,
            scope_key=scope_key,
            scope_type="market_history",
            region_id=config.region_id,
            type_id=type_id,
            last_success_at=fetched_at,
            last_attempt_at=fetched_at,
            next_scheduled_at=(
                _shift_utc_iso(base_time=fetched_at, hours=config.refresh_interval_hours)
                if entries
                else _shift_utc_iso(base_time=fetched_at, hours=config.not_found_recheck_hours)
            ),
            failure_count=0,
            last_error=None,
            coverage_marker=f"ok:{len(entries)}:{last_date}" if last_date else "ok:0",
        )
        count_updates["success"] = 1
        count_updates["history_rows_received"] = len(entries)
        count_updates["history_rows_written"] = rows_written
    elif response["status_code"] == 404:
        _upsert_sync_scope_state(
            connection,
            scope_key=scope_key,
            scope_type="market_history",
            region_id=config.region_id,
            type_id=type_id,
            last_success_at=fetched_at,
            last_attempt_at=fetched_at,
            next_scheduled_at=_shift_utc_iso(base_time=fetched_at, hours=config.not_found_recheck_hours),
            failure_count=0,
            last_error=response["error"],
            coverage_marker="not_found",
        )
        count_updates["not_found"] = 1
    else:
        failure_count = _current_failure_count(connection, scope_key) + 1
        _upsert_sync_scope_state(
            connection,
            scope_key=scope_key,
            scope_type="market_history",
            region_id=config.region_id,
            type_id=type_id,
            last_success_at=_current_last_success_at(connection, scope_key),
            last_attempt_at=fetched_at,
            next_scheduled_at=_shift_utc_iso(
                base_time=fetched_at,
                seconds=config.failure_backoff_seconds,
            ),
            failure_count=failure_count,
            last_error=response["error"],
            coverage_marker=f"failed:{response['status_code']}",
        )
        count_updates["failed"] = 1

    first_date = response["entries"][0]["date"] if response["entries"] else None
    last_date = response["entries"][-1]["date"] if response["entries"] else None
    trace_row = {
        "ordinal": ordinal,
        "type_id": type_id,
        "type_name": type_name,
        "status_code": response["status_code"],
        "ok": response["ok"],
        "retries_used": response["retries_used"],
        "entry_count": len(response["entries"]),
        "rows_written": rows_written,
        "first_date": first_date,
        "last_date": last_date,
        "headers": response["headers"],
        "error": response["error"],
        "request_url": response["url"],
        "request_urls": response.get("request_urls"),
        "source_region_ids": response.get("source_region_ids"),
        "market_scopes": response.get("market_scopes"),
    }
    return trace_row, count_updates


def _request_history_target(target: dict[str, Any], config: HistoryFetchConfig) -> dict[str, Any]:
    type_id = int(target["type_id"])
    segments = resolve_history_source_segments(region_id=config.region_id, type_id=type_id)
    if len(segments) == 1 and int(segments[0].source_region_id) == int(config.region_id):
        return _request_history(
            region_id=config.region_id,
            type_id=type_id,
            datasource=config.datasource,
            esi_base_url=config.esi_base_url,
            user_agent=config.user_agent,
            request_timeout_seconds=config.request_timeout_seconds,
            max_retries=config.max_retries,
            request_wall_timeout_seconds=config.request_wall_timeout_seconds,
        )

    merged_entries_by_date: dict[str, dict[str, Any]] = {}
    request_urls: list[str] = []
    source_region_ids: list[int] = []
    market_scopes: list[str] = []
    total_retries_used = 0
    latest_headers: dict[str, Any] = {}
    latest_status_code: int | None = None

    for segment in segments:
        response = _request_history(
            region_id=int(segment.source_region_id),
            type_id=type_id,
            datasource=config.datasource,
            esi_base_url=config.esi_base_url,
            user_agent=config.user_agent,
            request_timeout_seconds=config.request_timeout_seconds,
            max_retries=config.max_retries,
            request_wall_timeout_seconds=config.request_wall_timeout_seconds,
        )
        request_urls.append(str(response["url"]))
        source_region_ids.append(int(segment.source_region_id))
        market_scopes.append(str(segment.market_scope))
        total_retries_used += int(response["retries_used"])
        if not response["ok"]:
            response["request_urls"] = request_urls
            response["source_region_ids"] = source_region_ids
            response["market_scopes"] = market_scopes
            return response
        latest_headers = dict(response["headers"])
        latest_status_code = response["status_code"]
        for entry in response["entries"]:
            market_date = str(entry["date"])
            if segment.start_date_inclusive is not None and market_date < segment.start_date_inclusive:
                continue
            if segment.end_date_exclusive is not None and market_date >= segment.end_date_exclusive:
                continue
            merged_entries_by_date[market_date] = {
                **entry,
                "source_region_id": int(segment.source_region_id),
                "market_scope": str(segment.market_scope),
            }

    merged_entries = [merged_entries_by_date[key] for key in sorted(merged_entries_by_date)]
    return {
        "ok": True,
        "status_code": latest_status_code,
        "url": request_urls[-1] if request_urls else _build_history_url(
            region_id=config.region_id,
            type_id=type_id,
            datasource=config.datasource,
            esi_base_url=config.esi_base_url,
        ),
        "request_urls": request_urls,
        "source_region_ids": source_region_ids,
        "market_scopes": market_scopes,
        "retries_used": total_retries_used,
        "headers": latest_headers,
        "entries": merged_entries,
        "error": None,
    }


def _iter_history_responses(
    type_targets: list[dict[str, Any]],
    *,
    config: HistoryFetchConfig,
) -> list[tuple[int, dict[str, Any], dict[str, Any], str]]:
    max_workers = max(int(config.concurrent_requests or 1), 1)
    if max_workers == 1:
        responses: list[tuple[int, dict[str, Any], dict[str, Any], str]] = []
        for index, target in enumerate(type_targets, start=1):
            response = _request_history_target(target, config)
            responses.append((index, target, response, utc_now_iso()))
            if config.pause_seconds > 0 and index < len(type_targets):
                time.sleep(config.pause_seconds)
        return responses

    responses = []
    pending: dict[concurrent.futures.Future[dict[str, Any]], tuple[int, dict[str, Any]]] = {}
    submitted_index = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        while submitted_index < len(type_targets) and len(pending) < max_workers:
            target = type_targets[submitted_index]
            future = executor.submit(_request_history_target, target, config)
            pending[future] = (submitted_index + 1, target)
            submitted_index += 1
            if config.pause_seconds > 0 and submitted_index < len(type_targets):
                time.sleep(config.pause_seconds)

        while pending:
            done, _ = concurrent.futures.wait(
                pending,
                return_when=concurrent.futures.FIRST_COMPLETED,
            )
            for future in done:
                ordinal, target = pending.pop(future)
                try:
                    response = future.result()
                except Exception as exc:
                    response = _build_unhandled_request_failure(
                        url=_build_history_url(
                            region_id=config.region_id,
                            type_id=int(target["type_id"]),
                            datasource=config.datasource,
                            esi_base_url=config.esi_base_url,
                        ),
                        error=exc,
                    )
                responses.append((ordinal, target, response, utc_now_iso()))

                if submitted_index < len(type_targets):
                    next_target = type_targets[submitted_index]
                    next_future = executor.submit(_request_history_target, next_target, config)
                    pending[next_future] = (submitted_index + 1, next_target)
                    submitted_index += 1
                    if config.pause_seconds > 0 and submitted_index < len(type_targets):
                        time.sleep(config.pause_seconds)
    return responses


def fetch_market_history(
    connection: sqlite3.Connection,
    *,
    output_root: Path,
    type_targets: list[dict[str, Any]],
    config: HistoryFetchConfig,
) -> dict[str, Any]:
    run_id = create_run_id(prefix="history-fetch")
    run_dir = ensure_run_dir(output_root, run_id)
    started_at = _now_utc()

    trace_rows: list[dict[str, Any]] = []
    counts = {
        "requested": len(type_targets),
        "success": 0,
        "not_found": 0,
        "failed": 0,
        "history_rows_received": 0,
        "history_rows_written": 0,
    }

    for index, target, response, fetched_at in _iter_history_responses(type_targets, config=config):
        trace_row, count_updates = _apply_history_response(
            connection,
            ordinal=index,
            target=target,
            response=response,
            config=config,
            fetched_at=fetched_at,
        )
        trace_rows.append(trace_row)
        for key, value in count_updates.items():
            counts[key] += value
        connection.commit()

    trace_rows.sort(key=lambda row: int(row["ordinal"]))

    finished_at = _now_utc()
    fetch_run = {
        "run_id": run_id,
        "fetch_kind": "market_history",
        "started_at": started_at,
        "finished_at": finished_at,
        "output_dir": str(run_dir.resolve()),
        "selection_mode": type_targets[0]["selection_mode"] if type_targets else "none",
        "type_count_requested": counts["requested"],
        "type_count_success": counts["success"],
        "type_count_not_found": counts["not_found"],
        "type_count_failed": counts["failed"],
        "history_rows_written": counts["history_rows_written"],
        "history_rows_received": counts["history_rows_received"],
        "config": config.to_dict(),
        "type_targets_sample": type_targets[:20],
    }
    fetch_trace = {
        "run_id": run_id,
        "config": config.to_dict(),
        "counts": counts,
        "type_traces": trace_rows,
    }
    summary_lines = [
        "# EVEQTrading Market History Fetch",
        "",
        f"- Run ID: `{run_id}`",
        f"- Region ID: `{config.region_id}`",
        f"- Datasource: `{config.datasource}`",
        f"- ESI Base URL: `{config.esi_base_url}`",
        f"- Requested Types: `{counts['requested']}`",
        f"- Success: `{counts['success']}`",
        f"- Not Found: `{counts['not_found']}`",
        f"- Failed: `{counts['failed']}`",
        f"- History Rows Received: `{counts['history_rows_received']}`",
        f"- History Rows Written: `{counts['history_rows_written']}`",
        "",
        "## Type Samples",
    ]
    if not trace_rows:
        summary_lines.extend(["", "No type targets were selected."])
    else:
        for row in trace_rows[:10]:
            summary_lines.extend(
                [
                    "",
                    f"- `{row['type_name']}` (`type_id={row['type_id']}`)",
                    f"  - status: `{row['status_code']}`",
                    f"  - entries: `{row['entry_count']}`",
                    f"  - rows_written: `{row['rows_written']}`",
                    f"  - window: `{row['first_date']}` -> `{row['last_date']}`",
                ]
            )

    write_json(run_dir / "history_fetch_run.json", fetch_run)
    write_json(run_dir / "history_fetch_trace.json", fetch_trace)
    write_summary(run_dir / "summary.md", summary_lines)

    record_data_fetch_run(
        connection,
        fetch_run_id=run_id,
        fetch_kind="market_history",
        source="esi-public-history",
        output_dir=run_dir.resolve(),
        scope={
            "region_id": config.region_id,
            "datasource": config.datasource,
            "selection_mode": fetch_run["selection_mode"],
        },
        details={
            "counts": counts,
            "type_targets_sample": type_targets[:20],
        },
        started_at=started_at.isoformat(),
        finished_at=finished_at.isoformat(),
        status="completed" if counts["failed"] == 0 else "completed_with_failures",
    )
    connection.commit()

    return {
        "run_id": run_id,
        "run_dir": run_dir.resolve(),
        "fetch_run": fetch_run,
        "fetch_trace": fetch_trace,
    }
