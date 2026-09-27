from __future__ import annotations

import concurrent.futures
import json
import sqlite3
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from math import ceil
from typing import Any

from .catalog_select import resolve_type_targets
from .db import record_data_fetch_run, utc_now_iso
from .history_fetch import RETRYABLE_STATUS_CODES, _current_failure_count, _resolve_retry_delay, _upsert_sync_scope_state
from .market_scope import resolve_order_source_region_id
from .run_artifacts import create_run_id, ensure_run_dir, write_json, write_summary


@dataclass
class OrderFetchConfig:
    region_id: int
    datasource: str
    esi_base_url: str
    user_agent: str
    request_timeout_seconds: float
    max_retries: int
    pause_seconds: float
    order_type: str
    concurrent_requests: int = 1
    fetch_mode: str = "per_type"
    ignore_existing_freshness: bool = False
    commit_batch_size: int = 100

    def to_dict(self) -> dict[str, Any]:
        return {
            "region_id": self.region_id,
            "datasource": self.datasource,
            "esi_base_url": self.esi_base_url,
            "user_agent": self.user_agent,
            "request_timeout_seconds": self.request_timeout_seconds,
            "concurrent_requests": self.concurrent_requests,
            "max_retries": self.max_retries,
            "pause_seconds": self.pause_seconds,
            "order_type": self.order_type,
            "fetch_mode": self.fetch_mode,
            "ignore_existing_freshness": self.ignore_existing_freshness,
            "commit_batch_size": self.commit_batch_size,
        }


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


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


def _parse_http_datetime(raw_value: Any) -> datetime | None:
    if not isinstance(raw_value, str) or not raw_value.strip():
        return None
    try:
        parsed = parsedate_to_datetime(raw_value.strip())
    except (TypeError, ValueError, IndexError):
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _datetime_to_utc_iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _resolve_orders_next_scheduled_at(headers: dict[str, Any]) -> str | None:
    return _datetime_to_utc_iso(_parse_http_datetime(headers.get("expires")))


ORDERS_PAGE_SIZE_ESTIMATE = 1000


def _first_page_headers(response: dict[str, Any]) -> dict[str, Any]:
    page_traces = response.get("page_traces")
    if isinstance(page_traces, list) and page_traces:
        first_page = page_traces[0]
        if isinstance(first_page, dict):
            headers = first_page.get("headers")
            if isinstance(headers, dict):
                return headers
    return {}


def _load_order_scope_state(connection: sqlite3.Connection, scope_key: str) -> dict[str, Any] | None:
    row = connection.execute(
        """
        SELECT last_success_at, last_attempt_at, next_scheduled_at, failure_count, last_error, coverage_marker
        FROM sync_scope_state
        WHERE scope_key = ?
        """,
        (scope_key,),
    ).fetchone()
    if row is None:
        return None
    return {
        "last_success_at": row["last_success_at"],
        "last_attempt_at": row["last_attempt_at"],
        "next_scheduled_at": row["next_scheduled_at"],
        "failure_count": int(row["failure_count"] or 0),
        "last_error": row["last_error"],
        "coverage_marker": row["coverage_marker"],
    }


def _snapshot_id_from_coverage_marker(coverage_marker: Any) -> str | None:
    marker = str(coverage_marker or "")
    if not marker.startswith("ok:"):
        return None
    snapshot_id = marker[3:]
    return snapshot_id or None


def _load_existing_snapshot_summary(
    connection: sqlite3.Connection,
    *,
    snapshot_id: str,
) -> dict[str, Any] | None:
    head_row = connection.execute(
        """
        SELECT snapshot_id, captured_at
        FROM market_order_snapshot_head
        WHERE snapshot_id = ?
        """,
        (snapshot_id,),
    ).fetchone()
    if head_row is None:
        return None

    aggregate_row = connection.execute(
        """
        SELECT
            COUNT(*) AS order_count,
            SUM(CASE WHEN is_buy_order = 1 THEN 1 ELSE 0 END) AS buy_order_count,
            SUM(CASE WHEN is_buy_order = 0 THEN 1 ELSE 0 END) AS sell_order_count,
            MAX(CASE WHEN is_buy_order = 1 THEN price END) AS best_bid,
            MIN(CASE WHEN is_buy_order = 0 THEN price END) AS best_ask
        FROM market_order_snapshot_line
        WHERE snapshot_id = ?
        """,
        (snapshot_id,),
    ).fetchone()
    return {
        "snapshot_id": snapshot_id,
        "captured_at": head_row["captured_at"],
        "order_count": int(aggregate_row["order_count"] or 0),
        "buy_order_count": int(aggregate_row["buy_order_count"] or 0),
        "sell_order_count": int(aggregate_row["sell_order_count"] or 0),
        "best_bid": float(aggregate_row["best_bid"]) if aggregate_row["best_bid"] is not None else None,
        "best_ask": float(aggregate_row["best_ask"]) if aggregate_row["best_ask"] is not None else None,
    }


def _delete_snapshot(connection: sqlite3.Connection, *, snapshot_id: str | None) -> None:
    if not snapshot_id:
        return
    connection.execute("DELETE FROM market_order_snapshot_line WHERE snapshot_id = ?", (snapshot_id,))
    connection.execute("DELETE FROM market_order_snapshot_head WHERE snapshot_id = ?", (snapshot_id,))


def _estimate_order_pages_from_snapshot_summary(snapshot_summary: dict[str, Any] | None) -> int:
    order_count = int((snapshot_summary or {}).get("order_count") or 0)
    if order_count <= 0:
        return 1
    return max(1, ceil(order_count / ORDERS_PAGE_SIZE_ESTIMATE))


def _build_fresh_skip_response(
    *,
    scope_row: dict[str, Any],
    snapshot_summary: dict[str, Any] | None,
) -> dict[str, Any]:
    next_scheduled_at = str(scope_row.get("next_scheduled_at") or "")
    snapshot_id = _snapshot_id_from_coverage_marker(scope_row.get("coverage_marker"))
    return {
        "ok": True,
        "status_code": None,
        "entries": [],
        "page_traces": [],
        "error": None,
        "page_count": 0,
        "retries_used": 0,
        "skipped_fresh": True,
        "next_scheduled_at": next_scheduled_at,
        "snapshot_summary": snapshot_summary,
        "snapshot_id": snapshot_id,
    }


def resolve_order_fetch_targets(
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


def _request_orders_page(
    *,
    region_id: int,
    type_id: int,
    datasource: str,
    esi_base_url: str,
    user_agent: str,
    request_timeout_seconds: float,
    max_retries: int,
    order_type: str,
    page: int,
) -> dict[str, Any]:
    base_url = esi_base_url.rstrip("/")
    query = urllib.parse.urlencode(
        {
            "datasource": datasource,
            "order_type": order_type,
            "type_id": type_id,
            "page": page,
        }
    )
    url = f"{base_url}/markets/{region_id}/orders/?{query}"
    attempt = 0
    retries_used = 0
    while True:
        request = urllib.request.Request(url, headers={"User-Agent": user_agent})
        try:
            with urllib.request.urlopen(request, timeout=request_timeout_seconds) as response:
                payload = json.loads(response.read().decode("utf-8"))
                x_pages_raw = response.headers.get("X-Pages")
                total_pages = int(x_pages_raw) if x_pages_raw else 1
                return {
                    "ok": True,
                    "status_code": response.status,
                    "url": url,
                    "retries_used": retries_used,
                    "headers": {
                        "expires": response.headers.get("Expires"),
                        "last_modified": response.headers.get("Last-Modified"),
                        "etag": response.headers.get("ETag"),
                        "x_pages": x_pages_raw,
                    },
                    "entries": payload,
                    "total_pages": total_pages,
                    "error": None,
                }
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            if exc.code in RETRYABLE_STATUS_CODES and attempt < max_retries:
                delay = _resolve_retry_delay(attempt, exc.headers.get("Retry-After"))
                time.sleep(delay)
                attempt += 1
                retries_used += 1
                continue
            return {
                "ok": False,
                "status_code": exc.code,
                "url": url,
                "retries_used": retries_used,
                "headers": {
                    "expires": exc.headers.get("Expires"),
                    "last_modified": exc.headers.get("Last-Modified"),
                    "etag": exc.headers.get("ETag"),
                    "x_pages": exc.headers.get("X-Pages"),
                },
                "entries": [],
                "total_pages": None,
                "error": body,
            }
        except urllib.error.URLError as exc:
            if attempt < max_retries:
                delay = _resolve_retry_delay(attempt, None)
                time.sleep(delay)
                attempt += 1
                retries_used += 1
                continue
            return {
                "ok": False,
                "status_code": None,
                "url": url,
                "retries_used": retries_used,
                "headers": {},
                "entries": [],
                "total_pages": None,
                "error": str(exc.reason),
            }


def _request_region_orders_page(
    *,
    region_id: int,
    datasource: str,
    esi_base_url: str,
    user_agent: str,
    request_timeout_seconds: float,
    max_retries: int,
    order_type: str,
    page: int,
) -> dict[str, Any]:
    base_url = esi_base_url.rstrip("/")
    query = urllib.parse.urlencode(
        {
            "datasource": datasource,
            "order_type": order_type,
            "page": page,
        }
    )
    url = f"{base_url}/markets/{region_id}/orders/?{query}"
    attempt = 0
    retries_used = 0
    while True:
        request = urllib.request.Request(url, headers={"User-Agent": user_agent})
        try:
            with urllib.request.urlopen(request, timeout=request_timeout_seconds) as response:
                payload = json.loads(response.read().decode("utf-8"))
                x_pages_raw = response.headers.get("X-Pages")
                total_pages = int(x_pages_raw) if x_pages_raw else 1
                return {
                    "ok": True,
                    "status_code": response.status,
                    "url": url,
                    "retries_used": retries_used,
                    "headers": {
                        "expires": response.headers.get("Expires"),
                        "last_modified": response.headers.get("Last-Modified"),
                        "etag": response.headers.get("ETag"),
                        "x_pages": x_pages_raw,
                    },
                    "entries": payload,
                    "total_pages": total_pages,
                    "error": None,
                }
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            if exc.code in RETRYABLE_STATUS_CODES and attempt < max_retries:
                delay = _resolve_retry_delay(attempt, exc.headers.get("Retry-After"))
                time.sleep(delay)
                attempt += 1
                retries_used += 1
                continue
            return {
                "ok": False,
                "status_code": exc.code,
                "url": url,
                "retries_used": retries_used,
                "headers": {
                    "expires": exc.headers.get("Expires"),
                    "last_modified": exc.headers.get("Last-Modified"),
                    "etag": exc.headers.get("ETag"),
                    "x_pages": exc.headers.get("X-Pages"),
                },
                "entries": [],
                "total_pages": None,
                "error": body,
            }
        except urllib.error.URLError as exc:
            if attempt < max_retries:
                delay = _resolve_retry_delay(attempt, None)
                time.sleep(delay)
                attempt += 1
                retries_used += 1
                continue
            return {
                "ok": False,
                "status_code": None,
                "url": url,
                "retries_used": retries_used,
                "headers": {},
                "entries": [],
                "total_pages": None,
                "error": str(exc.reason),
            }


def _request_all_orders_for_type(
    *,
    region_id: int,
    type_id: int,
    datasource: str,
    esi_base_url: str,
    user_agent: str,
    request_timeout_seconds: float,
    max_retries: int,
    order_type: str,
    pause_seconds: float,
) -> dict[str, Any]:
    all_entries: list[dict[str, Any]] = []
    page_traces: list[dict[str, Any]] = []
    first_page = _request_orders_page(
        region_id=region_id,
        type_id=type_id,
        datasource=datasource,
        esi_base_url=esi_base_url,
        user_agent=user_agent,
        request_timeout_seconds=request_timeout_seconds,
        max_retries=max_retries,
        order_type=order_type,
        page=1,
    )
    page_traces.append(
        {
            "page": 1,
            "status_code": first_page["status_code"],
            "ok": first_page["ok"],
            "entry_count": len(first_page["entries"]),
            "retries_used": first_page["retries_used"],
            "headers": first_page["headers"],
            "error": first_page["error"],
            "request_url": first_page["url"],
        }
    )
    if not first_page["ok"]:
        return {
            "ok": False,
            "status_code": first_page["status_code"],
            "entries": [],
            "page_traces": page_traces,
            "error": first_page["error"],
            "page_count": 1,
            "retries_used": first_page["retries_used"],
        }

    all_entries.extend(first_page["entries"])
    total_pages = int(first_page["total_pages"] or 1)
    total_retries_used = int(first_page["retries_used"])

    for page in range(2, total_pages + 1):
        if pause_seconds > 0:
            time.sleep(pause_seconds)
        page_result = _request_orders_page(
            region_id=region_id,
            type_id=type_id,
            datasource=datasource,
            esi_base_url=esi_base_url,
            user_agent=user_agent,
            request_timeout_seconds=request_timeout_seconds,
            max_retries=max_retries,
            order_type=order_type,
            page=page,
        )
        total_retries_used += int(page_result["retries_used"])
        page_traces.append(
            {
                "page": page,
                "status_code": page_result["status_code"],
                "ok": page_result["ok"],
                "entry_count": len(page_result["entries"]),
                "retries_used": page_result["retries_used"],
                "headers": page_result["headers"],
                "error": page_result["error"],
                "request_url": page_result["url"],
            }
        )
        if not page_result["ok"]:
            return {
                "ok": False,
                "status_code": page_result["status_code"],
                "entries": [],
                "page_traces": page_traces,
                "error": page_result["error"],
                "page_count": total_pages,
                "retries_used": total_retries_used,
            }
        all_entries.extend(page_result["entries"])

    return {
        "ok": True,
        "status_code": first_page["status_code"],
        "entries": all_entries,
        "page_traces": page_traces,
        "error": None,
        "page_count": total_pages,
        "retries_used": total_retries_used,
    }


def _request_region_orders_for_types(
    *,
    region_id: int,
    selected_type_ids: set[int],
    datasource: str,
    esi_base_url: str,
    user_agent: str,
    request_timeout_seconds: float,
    max_retries: int,
    order_type: str,
    concurrent_requests: int,
    pause_seconds: float,
    first_page_response: dict[str, Any] | None = None,
) -> dict[str, Any]:
    grouped_entries: dict[int, list[dict[str, Any]]] = {type_id: [] for type_id in selected_type_ids}
    page_traces: list[dict[str, Any]] = []

    def keep_entries(entries: list[dict[str, Any]]) -> None:
        for entry in entries:
            type_id = int(entry.get("type_id") or 0)
            if type_id in grouped_entries:
                grouped_entries[type_id].append(entry)

    first_page = first_page_response or _request_region_orders_page(
        region_id=region_id,
        datasource=datasource,
        esi_base_url=esi_base_url,
        user_agent=user_agent,
        request_timeout_seconds=request_timeout_seconds,
        max_retries=max_retries,
        order_type=order_type,
        page=1,
    )
    page_traces.append(
        {
            "page": 1,
            "status_code": first_page["status_code"],
            "ok": first_page["ok"],
            "entry_count": len(first_page["entries"]),
            "retries_used": first_page["retries_used"],
            "headers": first_page["headers"],
            "error": first_page["error"],
            "request_url": first_page["url"],
        }
    )
    if not first_page["ok"]:
        return {
            "ok": False,
            "status_code": first_page["status_code"],
            "entries_by_type": {},
            "page_traces": page_traces,
            "error": first_page["error"],
            "page_count": 1,
            "retries_used": first_page["retries_used"],
        }

    keep_entries(first_page["entries"])
    total_pages = int(first_page["total_pages"] or 1)
    total_retries_used = int(first_page["retries_used"])
    if total_pages == 1:
        return {
            "ok": True,
            "status_code": first_page["status_code"],
            "entries_by_type": grouped_entries,
            "page_traces": page_traces,
            "error": None,
            "page_count": 1,
            "retries_used": total_retries_used,
        }

    def request_page(page: int) -> dict[str, Any]:
        return _request_region_orders_page(
            region_id=region_id,
            datasource=datasource,
            esi_base_url=esi_base_url,
            user_agent=user_agent,
            request_timeout_seconds=request_timeout_seconds,
            max_retries=max_retries,
            order_type=order_type,
            page=page,
        )

    remaining_pages = list(range(2, total_pages + 1))
    max_workers = max(int(concurrent_requests or 1), 1)
    if max_workers == 1:
        for page in remaining_pages:
            if pause_seconds > 0:
                time.sleep(pause_seconds)
            page_result = request_page(page)
            total_retries_used += int(page_result["retries_used"])
            page_traces.append(
                {
                    "page": page,
                    "status_code": page_result["status_code"],
                    "ok": page_result["ok"],
                    "entry_count": len(page_result["entries"]),
                    "retries_used": page_result["retries_used"],
                    "headers": page_result["headers"],
                    "error": page_result["error"],
                    "request_url": page_result["url"],
                }
            )
            if not page_result["ok"]:
                page_traces.sort(key=lambda row: int(row["page"]))
                return {
                    "ok": False,
                    "status_code": page_result["status_code"],
                    "entries_by_type": {},
                    "page_traces": page_traces,
                    "error": page_result["error"],
                    "page_count": total_pages,
                    "retries_used": total_retries_used,
                }
            keep_entries(page_result["entries"])
    else:
        pending: dict[concurrent.futures.Future[dict[str, Any]], int] = {}
        submitted_index = 0
        page_results: list[tuple[int, dict[str, Any]]] = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            while submitted_index < len(remaining_pages) and len(pending) < max_workers:
                page = remaining_pages[submitted_index]
                future = executor.submit(request_page, page)
                pending[future] = page
                submitted_index += 1
                if pause_seconds > 0 and submitted_index < len(remaining_pages):
                    time.sleep(pause_seconds)

            while pending:
                done, _ = concurrent.futures.wait(
                    pending,
                    return_when=concurrent.futures.FIRST_COMPLETED,
                )
                for future in done:
                    page = pending.pop(future)
                    try:
                        page_result = future.result()
                    except Exception as exc:
                        page_result = {
                            "ok": False,
                            "status_code": None,
                            "url": _build_orders_url(
                                region_id=region_id,
                                datasource=datasource,
                                esi_base_url=esi_base_url,
                                order_type=order_type,
                                page=page,
                            ),
                            "retries_used": 0,
                            "headers": {},
                            "entries": [],
                            "total_pages": total_pages,
                            "error": f"{type(exc).__name__}: {exc}",
                        }
                    page_results.append((page, page_result))
                    if submitted_index < len(remaining_pages):
                        next_page = remaining_pages[submitted_index]
                        next_future = executor.submit(request_page, next_page)
                        pending[next_future] = next_page
                        submitted_index += 1
                        if pause_seconds > 0 and submitted_index < len(remaining_pages):
                            time.sleep(pause_seconds)

        for page, page_result in sorted(page_results, key=lambda row: row[0]):
            total_retries_used += int(page_result["retries_used"])
            page_traces.append(
                {
                    "page": page,
                    "status_code": page_result["status_code"],
                    "ok": page_result["ok"],
                    "entry_count": len(page_result["entries"]),
                    "retries_used": page_result["retries_used"],
                    "headers": page_result["headers"],
                    "error": page_result["error"],
                    "request_url": page_result["url"],
                }
            )
            if not page_result["ok"]:
                page_traces.sort(key=lambda row: int(row["page"]))
                return {
                    "ok": False,
                    "status_code": page_result["status_code"],
                    "entries_by_type": {},
                    "page_traces": page_traces,
                    "error": page_result["error"],
                    "page_count": total_pages,
                    "retries_used": total_retries_used,
                }
            keep_entries(page_result["entries"])

    page_traces.sort(key=lambda row: int(row["page"]))
    return {
        "ok": True,
        "status_code": first_page["status_code"],
        "entries_by_type": grouped_entries,
        "page_traces": page_traces,
        "error": None,
        "page_count": total_pages,
        "retries_used": total_retries_used,
    }


def _build_orders_url(
    *,
    region_id: int,
    datasource: str,
    esi_base_url: str,
    order_type: str,
    page: int = 1,
    type_id: int | None = None,
) -> str:
    base_url = esi_base_url.rstrip("/")
    params: dict[str, Any] = {
        "datasource": datasource,
        "order_type": order_type,
        "page": page,
    }
    if type_id is not None:
        params["type_id"] = type_id
    query = urllib.parse.urlencode(params)
    return f"{base_url}/markets/{region_id}/orders/?{query}"


def _build_unhandled_order_request_failure(
    *,
    url: str,
    error: Exception,
) -> dict[str, Any]:
    return {
        "ok": False,
        "status_code": None,
        "entries": [],
        "page_traces": [],
        "error": f"{type(error).__name__}: {error}",
        "page_count": 0,
        "retries_used": 0,
        "request_url": url,
    }


def _request_order_target(target: dict[str, Any], config: OrderFetchConfig) -> dict[str, Any]:
    source_region_id = resolve_order_source_region_id(region_id=config.region_id, type_id=int(target["type_id"]))
    return _request_all_orders_for_type(
        region_id=source_region_id,
        type_id=int(target["type_id"]),
        datasource=config.datasource,
        esi_base_url=config.esi_base_url,
        user_agent=config.user_agent,
        request_timeout_seconds=config.request_timeout_seconds,
        max_retries=config.max_retries,
        order_type=config.order_type,
        pause_seconds=config.pause_seconds,
    )


def _source_region_ids_for_targets(
    target_rows: list[tuple[int, dict[str, Any]]],
    *,
    default_region_id: int,
) -> list[int]:
    return sorted(
        {
            int(target.get("_source_region_id") or resolve_order_source_region_id(region_id=default_region_id, type_id=int(target["type_id"])))
            for _, target in target_rows
        }
    )


def _iter_order_responses(
    target_rows: list[tuple[int, dict[str, Any]]],
    *,
    config: OrderFetchConfig,
) -> list[tuple[int, dict[str, Any], dict[str, Any], str]]:
    max_workers = max(int(config.concurrent_requests or 1), 1)
    if max_workers == 1:
        responses: list[tuple[int, dict[str, Any], dict[str, Any], str]] = []
        for offset, (ordinal, target) in enumerate(target_rows, start=1):
            response = _request_order_target(target, config)
            responses.append((ordinal, target, response, utc_now_iso()))
            if config.pause_seconds > 0 and offset < len(target_rows):
                time.sleep(config.pause_seconds)
        return responses

    responses = []
    pending: dict[concurrent.futures.Future[dict[str, Any]], tuple[int, dict[str, Any]]] = {}
    submitted_index = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        while submitted_index < len(target_rows) and len(pending) < max_workers:
            ordinal, target = target_rows[submitted_index]
            future = executor.submit(_request_order_target, target, config)
            pending[future] = (ordinal, target)
            submitted_index += 1
            if config.pause_seconds > 0 and submitted_index < len(target_rows):
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
                    source_region_id = int(target.get("_source_region_id") or resolve_order_source_region_id(region_id=config.region_id, type_id=int(target["type_id"])))
                    response = _build_unhandled_order_request_failure(
                        url=_build_orders_url(
                            region_id=source_region_id,
                            type_id=int(target["type_id"]),
                            datasource=config.datasource,
                            esi_base_url=config.esi_base_url,
                            order_type=config.order_type,
                        ),
                        error=exc,
                    )
                responses.append((ordinal, target, response, utc_now_iso()))

                if submitted_index < len(target_rows):
                    next_ordinal, next_target = target_rows[submitted_index]
                    next_future = executor.submit(_request_order_target, next_target, config)
                    pending[next_future] = (next_ordinal, next_target)
                    submitted_index += 1
                    if config.pause_seconds > 0 and submitted_index < len(target_rows):
                        time.sleep(config.pause_seconds)
    return responses


def _iter_region_order_responses(
    target_rows: list[tuple[int, dict[str, Any]]],
    *,
    config: OrderFetchConfig,
    first_page_response: dict[str, Any] | None = None,
) -> tuple[list[tuple[int, dict[str, Any], dict[str, Any], str]], dict[str, Any] | None]:
    if not target_rows:
        return [], None

    selected_type_ids = {int(target["type_id"]) for _, target in target_rows}
    source_region_ids = _source_region_ids_for_targets(target_rows, default_region_id=config.region_id)
    if len(source_region_ids) != 1:
        raise RuntimeError("region_pages 模式不支持混合 source region 的订单抓取。")
    source_region_id = int(source_region_ids[0])
    region_response = _request_region_orders_for_types(
        region_id=source_region_id,
        selected_type_ids=selected_type_ids,
        datasource=config.datasource,
        esi_base_url=config.esi_base_url,
        user_agent=config.user_agent,
        request_timeout_seconds=config.request_timeout_seconds,
        max_retries=config.max_retries,
        order_type=config.order_type,
        concurrent_requests=config.concurrent_requests,
        pause_seconds=config.pause_seconds,
        first_page_response=first_page_response,
    )
    shared_trace = {
        "request_scope": "region_pages",
        "page_count": int(region_response["page_count"]),
        "page_traces": region_response["page_traces"],
        "retries_used": int(region_response["retries_used"]),
        "status_code": region_response["status_code"],
        "ok": bool(region_response["ok"]),
        "error": region_response["error"],
        "selected_type_count": len(selected_type_ids),
    }

    captured_at = utc_now_iso()
    responses: list[tuple[int, dict[str, Any], dict[str, Any], str]] = []
    ordered_rows = sorted(target_rows, key=lambda row: row[0])
    entries_by_type = region_response.get("entries_by_type", {})
    for offset, (ordinal, target) in enumerate(ordered_rows):
        type_id = int(target["type_id"])
        per_type_entries = entries_by_type.get(type_id, [])
        response = {
            "ok": bool(region_response["ok"]),
            "status_code": region_response["status_code"],
            "entries": per_type_entries if region_response["ok"] else [],
            "page_traces": [],
            "error": region_response["error"],
            "page_count": int(region_response["page_count"]),
            "page_count_for_counters": int(region_response["page_count"]) if offset == 0 else 0,
            "retries_used": int(region_response["retries_used"]),
            "shared_request_scope": "region_pages",
        }
        responses.append((ordinal, target, response, captured_at))
    return responses, shared_trace


def _auto_select_orders_fetch_mode(
    *,
    config: OrderFetchConfig,
    remote_target_rows: list[tuple[int, dict[str, Any]]],
    predicted_per_type_pages: int,
) -> tuple[str, dict[str, Any], dict[str, Any] | None]:
    source_region_ids = _source_region_ids_for_targets(remote_target_rows, default_region_id=config.region_id)
    target_count = len(remote_target_rows)
    conservative_target_threshold = max(int(config.concurrent_requests or 1), 1) * 8
    conservative_page_threshold = 8

    decision = {
        "requested_fetch_mode": "auto",
        "effective_fetch_mode": "per_type",
        "target_count": target_count,
        "predicted_per_type_pages": predicted_per_type_pages,
        "region_first_page_probe_used": False,
        "region_total_pages": None,
        "reason": "",
    }
    if target_count == 0:
        decision["reason"] = "no_remote_targets"
        return "per_type", decision, None

    if len(source_region_ids) > 1:
        decision["reason"] = "mixed_source_regions_require_per_type"
        return "per_type", decision, None

    if target_count <= conservative_target_threshold and predicted_per_type_pages <= conservative_page_threshold:
        decision["reason"] = "target_set_small_or_predicted_pages_low"
        return "per_type", decision, None

    first_page = _request_region_orders_page(
        region_id=int(source_region_ids[0]),
        datasource=config.datasource,
        esi_base_url=config.esi_base_url,
        user_agent=config.user_agent,
        request_timeout_seconds=config.request_timeout_seconds,
        max_retries=config.max_retries,
        order_type=config.order_type,
        page=1,
    )
    decision["region_first_page_probe_used"] = True
    if not first_page["ok"]:
        decision["reason"] = "region_probe_failed_fallback_to_per_type"
        decision["region_total_pages"] = None
        return "per_type", decision, None

    region_total_pages = int(first_page["total_pages"] or 1)
    decision["region_total_pages"] = region_total_pages
    if region_total_pages < predicted_per_type_pages:
        decision["effective_fetch_mode"] = "region_pages"
        decision["reason"] = "region_total_pages_below_predicted_per_type_pages"
        return "region_pages", decision, first_page

    decision["reason"] = "region_total_pages_not_better_than_predicted_per_type_pages"
    return "per_type", decision, None


def _store_order_snapshot(
    connection: sqlite3.Connection,
    *,
    snapshot_id: str,
    captured_at: str,
    region_id: int,
    type_id: int,
    entries: list[dict[str, Any]],
    order_type: str,
) -> dict[str, Any]:
    connection.execute("DELETE FROM market_order_snapshot_line WHERE snapshot_id = ?", (snapshot_id,))
    connection.execute("DELETE FROM market_order_snapshot_head WHERE snapshot_id = ?", (snapshot_id,))
    connection.execute(
        """
        INSERT INTO market_order_snapshot_head (
            snapshot_id,
            captured_at,
            scope_kind,
            region_id,
            location_id,
            account_id,
            source,
            refresh_mode
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            snapshot_id,
            captured_at,
            f"region_type_orders:{order_type}",
            region_id,
            None,
            None,
            "esi-public-orders",
            "replace",
        ),
    )

    line_rows: list[tuple[Any, ...]] = []
    best_bid: float | None = None
    best_ask: float | None = None
    buy_order_count = 0
    sell_order_count = 0
    seen_order_ids: set[int] = set()
    duplicate_order_id_count = 0

    for entry in entries:
        order_id = int(entry["order_id"])
        if order_id in seen_order_ids:
            duplicate_order_id_count += 1
            continue
        seen_order_ids.add(order_id)
        is_buy_order = bool(entry["is_buy_order"])
        price = float(entry["price"])
        if is_buy_order:
            buy_order_count += 1
            if best_bid is None or price > best_bid:
                best_bid = price
        else:
            sell_order_count += 1
            if best_ask is None or price < best_ask:
                best_ask = price

        line_rows.append(
            (
                snapshot_id,
                order_id,
                int(entry["type_id"]),
                1 if is_buy_order else 0,
                price,
                float(entry["volume_remain"]),
                float(entry["volume_total"]),
                float(entry["min_volume"]),
                entry["range"],
                entry["issued"],
                int(entry["duration"]),
                int(entry["location_id"]),
                region_id,
                None,
            )
        )

    connection.executemany(
        """
        INSERT INTO market_order_snapshot_line (
            snapshot_id,
            order_id,
            type_id,
            is_buy_order,
            price,
            volume_remain,
            volume_total,
            min_volume,
            order_range,
            issued_at,
            duration_days,
            location_id,
            region_id,
            account_id
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        line_rows,
    )

    return {
        "snapshot_id": snapshot_id,
        "type_id": type_id,
        "order_count": len(line_rows),
        "buy_order_count": buy_order_count,
        "sell_order_count": sell_order_count,
        "best_bid": best_bid,
        "best_ask": best_ask,
        "duplicate_order_id_count": duplicate_order_id_count,
    }


def fetch_market_orders(
    connection: sqlite3.Connection,
    *,
    output_root: Path,
    type_targets: list[dict[str, Any]],
    config: OrderFetchConfig,
) -> dict[str, Any]:
    run_id = create_run_id(prefix="orders-fetch")
    run_dir = ensure_run_dir(output_root, run_id)
    started_at = _now_utc()

    counts = {
        "requested": len(type_targets),
        "success": 0,
        "skipped_fresh": 0,
        "not_found": 0,
        "failed": 0,
        "orders_written": 0,
        "snapshots_written": 0,
        "pages_fetched": 0,
        "storage_flush_count": 0,
    }
    type_traces: list[dict[str, Any]] = []
    prepared_responses: list[tuple[int, dict[str, Any], dict[str, Any], str]] = []
    remote_target_rows: list[tuple[int, dict[str, Any]]] = []
    shared_remote_trace: dict[str, Any] | None = None
    predicted_per_type_pages = 0
    fetch_mode_decision: dict[str, Any] | None = None
    effective_fetch_mode = config.fetch_mode
    commit_batch_size = max(int(config.commit_batch_size or 1), 1)
    dirty_type_count = 0

    for index, target in enumerate(type_targets, start=1):
        captured_at = utc_now_iso()
        type_id = int(target["type_id"])
        prepared_target = dict(target)
        prepared_target["_source_region_id"] = resolve_order_source_region_id(region_id=config.region_id, type_id=type_id)
        scope_key = f"market_orders:{config.region_id}:{type_id}:{config.order_type}"
        scope_row = _load_order_scope_state(connection, scope_key)
        next_scheduled_at = (
            _parse_utc_iso(scope_row.get("next_scheduled_at"))
            if scope_row is not None
            else None
        )
        existing_snapshot_id = (
            _snapshot_id_from_coverage_marker(scope_row.get("coverage_marker"))
            if scope_row is not None
            else None
        )
        existing_snapshot_summary = (
            _load_existing_snapshot_summary(connection, snapshot_id=existing_snapshot_id)
            if existing_snapshot_id is not None
            else None
        )
        prepared_target["_existing_snapshot_id"] = existing_snapshot_id
        prepared_target["_existing_next_scheduled_at"] = (
            str(scope_row.get("next_scheduled_at") or "")
            if scope_row is not None
            else None
        )
        prepared_target["_existing_last_success_at"] = (
            str(scope_row.get("last_success_at") or "")
            if scope_row is not None
            else None
        )

        if (
            not config.ignore_existing_freshness
            and
            scope_row is not None
            and next_scheduled_at is not None
            and next_scheduled_at > _parse_utc_iso(captured_at)
            and existing_snapshot_summary is not None
        ):
            prepared_responses.append(
                (
                    index,
                    prepared_target,
                    _build_fresh_skip_response(
                        scope_row=scope_row,
                        snapshot_summary=existing_snapshot_summary,
                    ),
                    captured_at,
                )
            )
        else:
            remote_target_rows.append((index, prepared_target))
            predicted_per_type_pages += _estimate_order_pages_from_snapshot_summary(existing_snapshot_summary)

    region_probe_first_page: dict[str, Any] | None = None
    if config.fetch_mode == "auto":
        effective_fetch_mode, fetch_mode_decision, region_probe_first_page = _auto_select_orders_fetch_mode(
            config=config,
            remote_target_rows=remote_target_rows,
            predicted_per_type_pages=predicted_per_type_pages,
        )
    elif effective_fetch_mode == "region_pages":
        source_region_ids = _source_region_ids_for_targets(remote_target_rows, default_region_id=config.region_id)
        if len(source_region_ids) > 1:
            effective_fetch_mode = "per_type"
            fetch_mode_decision = {
                "requested_fetch_mode": "region_pages",
                "effective_fetch_mode": "per_type",
                "target_count": len(remote_target_rows),
                "predicted_per_type_pages": predicted_per_type_pages,
                "region_first_page_probe_used": False,
                "region_total_pages": None,
                "reason": "mixed_source_regions_require_per_type",
            }
    if effective_fetch_mode == "region_pages":
        remote_responses, shared_remote_trace = _iter_region_order_responses(
            remote_target_rows,
            config=config,
            first_page_response=region_probe_first_page,
        )
        prepared_responses.extend(remote_responses)
    else:
        prepared_responses.extend(_iter_order_responses(remote_target_rows, config=config))

    for index, target, response, captured_at in sorted(prepared_responses, key=lambda row: row[0]):
        type_id = int(target["type_id"])
        type_name = str(target["type_name"])
        snapshot_id = f"{run_id}-type-{type_id}"
        scope_key = f"market_orders:{config.region_id}:{type_id}:{config.order_type}"
        existing_snapshot_id = str(target.get("_existing_snapshot_id") or "") or None
        existing_next_scheduled_at = str(target.get("_existing_next_scheduled_at") or "") or None
        existing_last_success_at = str(target.get("_existing_last_success_at") or "") or None
        counts["pages_fetched"] += int(response.get("page_count_for_counters", response["page_count"]))

        if response.get("skipped_fresh"):
            snapshot_summary = response["snapshot_summary"]
            counts["skipped_fresh"] += 1
            preserved_cached_snapshot = True
        elif response["ok"]:
            _delete_snapshot(connection, snapshot_id=existing_snapshot_id)
            snapshot_summary = _store_order_snapshot(
                connection,
                snapshot_id=snapshot_id,
                captured_at=captured_at,
                region_id=config.region_id,
                type_id=type_id,
                entries=response["entries"],
                order_type=config.order_type,
            )
            _upsert_sync_scope_state(
                connection,
                scope_key=scope_key,
                scope_type="market_orders",
                region_id=config.region_id,
                type_id=type_id,
                last_success_at=captured_at,
                last_attempt_at=captured_at,
                next_scheduled_at=_resolve_orders_next_scheduled_at(_first_page_headers(response)),
                failure_count=0,
                last_error=None,
                coverage_marker=f"ok:{snapshot_id}",
            )
            counts["success"] += 1
            counts["orders_written"] += int((snapshot_summary or {}).get("order_count") or 0)
            counts["snapshots_written"] += 1
            preserved_cached_snapshot = False
        elif response["status_code"] == 404:
            snapshot_summary = None
            _upsert_sync_scope_state(
                connection,
                scope_key=scope_key,
                scope_type="market_orders",
                region_id=config.region_id,
                type_id=type_id,
                last_success_at=captured_at,
                last_attempt_at=captured_at,
                next_scheduled_at=_resolve_orders_next_scheduled_at(_first_page_headers(response)),
                failure_count=0,
                last_error=response["error"],
                coverage_marker="not_found",
            )
            counts["not_found"] += 1
            preserved_cached_snapshot = False
        else:
            snapshot_summary = None
            failure_count = _current_failure_count(connection, scope_key) + 1
            preserved_cached_snapshot = existing_snapshot_id is not None
            _upsert_sync_scope_state(
                connection,
                scope_key=scope_key,
                scope_type="market_orders",
                region_id=config.region_id,
                type_id=type_id,
                last_success_at=existing_last_success_at or None,
                last_attempt_at=captured_at,
                next_scheduled_at=existing_next_scheduled_at if preserved_cached_snapshot else None,
                failure_count=failure_count,
                last_error=response["error"],
                coverage_marker=(
                    f"ok:{existing_snapshot_id}"
                    if preserved_cached_snapshot
                    else f"failed:{response['status_code']}"
                ),
            )
            counts["failed"] += 1

        type_traces.append(
            {
                "ordinal": index,
                "type_id": type_id,
                "type_name": type_name,
                "source_region_id": int(target.get("_source_region_id") or config.region_id),
                "ok": response["ok"],
                "status_code": response["status_code"],
                "skipped_fresh": bool(response.get("skipped_fresh")),
                "next_scheduled_at": response.get("next_scheduled_at"),
                "page_count": response["page_count"],
                "retries_used": response["retries_used"],
                "order_count": (
                    int((snapshot_summary or {}).get("order_count") or 0)
                    if response.get("skipped_fresh")
                    else int((snapshot_summary or {}).get("order_count") or len(response["entries"]))
                ),
                "request_scope": str(response.get("shared_request_scope") or "per_type"),
                "preserved_cached_snapshot": preserved_cached_snapshot,
                "snapshot_summary": snapshot_summary,
                "page_traces": response["page_traces"],
                "error": response["error"],
            }
        )
        dirty_type_count += 1
        if dirty_type_count >= commit_batch_size:
            connection.commit()
            counts["storage_flush_count"] += 1
            dirty_type_count = 0

    if dirty_type_count > 0:
        connection.commit()
        counts["storage_flush_count"] += 1

    type_traces.sort(key=lambda row: int(row["ordinal"]))

    finished_at = _now_utc()
    fetch_run = {
        "run_id": run_id,
        "fetch_kind": "market_orders",
        "started_at": started_at,
        "finished_at": finished_at,
        "output_dir": str(run_dir.resolve()),
        "selection_mode": type_targets[0]["selection_mode"] if type_targets else "none",
        "type_count_requested": counts["requested"],
        "type_count_success": counts["success"],
        "type_count_skipped_fresh": counts["skipped_fresh"],
        "type_count_not_found": counts["not_found"],
        "type_count_failed": counts["failed"],
        "orders_written": counts["orders_written"],
        "snapshots_written": counts["snapshots_written"],
        "pages_fetched": counts["pages_fetched"],
        "storage_flush_count": counts["storage_flush_count"],
        "config": config.to_dict(),
        "effective_fetch_mode": effective_fetch_mode,
        "fetch_mode_decision": fetch_mode_decision,
        "type_targets_sample": type_targets[:20],
    }
    fetch_trace = {
        "run_id": run_id,
        "config": config.to_dict(),
        "effective_fetch_mode": effective_fetch_mode,
        "fetch_mode_decision": fetch_mode_decision,
        "counts": counts,
        "type_traces": type_traces,
        "shared_remote_trace": shared_remote_trace,
    }
    summary_lines = [
        "# EVEQTrading Market Orders Fetch",
        "",
        f"- Run ID: `{run_id}`",
        f"- Region ID: `{config.region_id}`",
        f"- Datasource: `{config.datasource}`",
        f"- Order Type: `{config.order_type}`",
        f"- Requested Types: `{counts['requested']}`",
        f"- Success: `{counts['success']}`",
        f"- Skipped Fresh: `{counts['skipped_fresh']}`",
        f"- Not Found: `{counts['not_found']}`",
        f"- Failed: `{counts['failed']}`",
        f"- Orders Written: `{counts['orders_written']}`",
        f"- Pages Fetched: `{counts['pages_fetched']}`",
        f"- Storage Flush Count: `{counts['storage_flush_count']}`",
        "",
        "## Type Samples",
    ]
    if not type_traces:
        summary_lines.extend(["", "No type targets were selected."])
    else:
        for row in type_traces[:10]:
            snapshot_summary = row.get("snapshot_summary") or {}
            summary_lines.extend(
                [
                    "",
                    f"- `{row['type_name']}` (`type_id={row['type_id']}`)",
                    f"  - status: `{'fresh-skip' if row.get('skipped_fresh') else row['status_code']}`",
                    f"  - pages: `{row['page_count']}`",
                    f"  - order_count: `{row['order_count']}`",
                    f"  - best_bid: `{snapshot_summary.get('best_bid')}`",
                    f"  - best_ask: `{snapshot_summary.get('best_ask')}`",
                ]
            )

    write_json(run_dir / "orders_fetch_run.json", fetch_run)
    write_json(run_dir / "orders_fetch_trace.json", fetch_trace)
    write_summary(run_dir / "summary.md", summary_lines)

    record_data_fetch_run(
        connection,
        fetch_run_id=run_id,
        fetch_kind="market_orders",
        source="esi-public-orders",
        output_dir=run_dir.resolve(),
        scope={
            "region_id": config.region_id,
            "datasource": config.datasource,
            "order_type": config.order_type,
            "selection_mode": fetch_run["selection_mode"],
        },
        details={
            "counts": counts,
            "effective_fetch_mode": effective_fetch_mode,
            "fetch_mode_decision": fetch_mode_decision,
            "shared_remote_trace": shared_remote_trace,
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
