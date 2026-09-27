from __future__ import annotations

import io
import http.client
import json
import os
import socket
import sys
import tempfile
import threading
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from eveqtrading.cli import main as cli_main
from eveqtrading.history_sync_worker import _acquire_lock, run_history_sync_worker_once
from eveqtrading.db import connect, init_db
from eveqtrading.history_fetch import HistoryFetchConfig, fetch_market_history
from eveqtrading.history_sync import collect_history_sync_snapshot, run_history_sync_cycle


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT / "sql" / "schema.sql"


def _build_history_entries(*, start_day: int, day_count: int, base_price: float) -> list[dict[str, object]]:
    return [
        {
            "date": f"2026-03-{start_day + day_index:02d}",
            "average": base_price + day_index,
            "highest": base_price + day_index + 1.0,
            "lowest": base_price + day_index - 1.0,
            "volume": 10_000.0 + day_index,
            "order_count": 100 + day_index,
        }
        for day_index in range(day_count)
    ]


class HistorySyncTests(unittest.TestCase):
    def setUp(self) -> None:
        self._temp_dir = tempfile.TemporaryDirectory()
        self.base_dir = Path(self._temp_dir.name)
        self.db_path = self.base_dir / "state" / "eveq.db"
        self.output_root = self.base_dir / "runs"
        self.output_root.mkdir(parents=True, exist_ok=True)
        init_db(self.db_path, SCHEMA_PATH)

    def tearDown(self) -> None:
        self._temp_dir.cleanup()

    def test_fetch_market_history_latest_only_writes_only_latest_day(self) -> None:
        connection = connect(self.db_path)
        try:
            connection.execute(
                "INSERT INTO item_catalog (type_id, type_name, market_group, is_published) VALUES (?, ?, ?, ?)",
                (9101, "Refresh Target", "Ships", 1),
            )
            connection.executemany(
                """
                INSERT INTO market_history_daily (
                    type_id, region_id, market_date, average_price, highest_price, lowest_price,
                    volume, order_count, fetched_at, source, coverage_status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (9101, 10000002, "2026-03-01", 100.0, 101.0, 99.0, 10_000.0, 100, "2026-04-01T00:00:00Z", "seed", "ok"),
                    (9101, 10000002, "2026-03-02", 101.0, 102.0, 100.0, 10_001.0, 101, "2026-04-01T00:00:00Z", "seed", "ok"),
                ],
            )
            connection.commit()

            def fake_request_history(**kwargs):
                return {
                    "ok": True,
                    "status_code": 200,
                    "url": "https://example.invalid/history/9101",
                    "retries_used": 0,
                    "headers": {},
                    "entries": [
                        {"date": "2026-03-01", "average": 900.0, "highest": 901.0, "lowest": 899.0, "volume": 1.0, "order_count": 1},
                        {"date": "2026-03-02", "average": 901.0, "highest": 902.0, "lowest": 900.0, "volume": 2.0, "order_count": 2},
                        {"date": "2026-03-03", "average": 902.0, "highest": 903.0, "lowest": 901.0, "volume": 3.0, "order_count": 3},
                    ],
                    "error": None,
                }

            with patch("eveqtrading.history_fetch._request_history", side_effect=fake_request_history):
                result = fetch_market_history(
                    connection,
                    output_root=self.output_root,
                    type_targets=[{"type_id": 9101, "type_name": "Refresh Target", "selection_mode": "explicit"}],
                    config=HistoryFetchConfig(
                        region_id=10000002,
                        datasource="tranquility",
                        esi_base_url="https://esi.evetech.net/latest",
                        user_agent="test-history-latest-only",
                        request_timeout_seconds=5.0,
                        max_retries=0,
                        pause_seconds=0.0,
                        refresh_mode="latest_only",
                    ),
                )

            rows = connection.execute(
                """
                SELECT market_date, average_price, volume
                FROM market_history_daily
                WHERE region_id = ? AND type_id = ?
                ORDER BY market_date
                """,
                (10000002, 9101),
            ).fetchall()
        finally:
            connection.close()

        self.assertEqual(3, len(rows))
        self.assertEqual("2026-03-01", rows[0]["market_date"])
        self.assertEqual(100.0, rows[0]["average_price"])
        self.assertEqual("2026-03-02", rows[1]["market_date"])
        self.assertEqual(101.0, rows[1]["average_price"])
        self.assertEqual("2026-03-03", rows[2]["market_date"])
        self.assertEqual(902.0, rows[2]["average_price"])
        self.assertEqual(3, result["fetch_run"]["history_rows_received"])
        self.assertEqual(1, result["fetch_run"]["history_rows_written"])

    def test_fetch_market_history_stitches_plex_across_transition_regions(self) -> None:
        connection = connect(self.db_path)
        try:
            connection.execute(
                "INSERT INTO item_catalog (type_id, type_name, market_group, is_published) VALUES (?, ?, ?, ?)",
                (44992, "伊甸币", "PLEX", 1),
            )
            connection.commit()

            def fake_request_history(**kwargs):
                region_id = int(kwargs["region_id"])
                if region_id == 10000002:
                    entries = [
                        {"date": "2025-07-05", "average": 5_100_000.0, "highest": 5_200_000.0, "lowest": 5_000_000.0, "volume": 100.0, "order_count": 10},
                        {"date": "2025-07-06", "average": 5_200_000.0, "highest": 5_300_000.0, "lowest": 5_100_000.0, "volume": 110.0, "order_count": 11},
                        {"date": "2025-07-07", "average": 5_300_000.0, "highest": 5_400_000.0, "lowest": 5_200_000.0, "volume": 120.0, "order_count": 12},
                    ]
                elif region_id == 19000001:
                    entries = [
                        {"date": "2025-07-07", "average": 5_350_000.0, "highest": 5_450_000.0, "lowest": 5_250_000.0, "volume": 130.0, "order_count": 13},
                        {"date": "2025-07-08", "average": 5_360_000.0, "highest": 5_460_000.0, "lowest": 5_260_000.0, "volume": 140.0, "order_count": 14},
                    ]
                else:
                    raise AssertionError(f"unexpected region_id {region_id}")
                return {
                    "ok": True,
                    "status_code": 200,
                    "url": f"https://example.invalid/history/{region_id}/44992",
                    "retries_used": 0,
                    "headers": {},
                    "entries": entries,
                    "error": None,
                }

            with patch("eveqtrading.history_fetch._request_history", side_effect=fake_request_history):
                result = fetch_market_history(
                    connection,
                    output_root=self.output_root,
                    type_targets=[{"type_id": 44992, "type_name": "伊甸币", "selection_mode": "explicit"}],
                    config=HistoryFetchConfig(
                        region_id=10000002,
                        datasource="tranquility",
                        esi_base_url="https://esi.evetech.net/latest",
                        user_agent="test-history-plex-stitch",
                        request_timeout_seconds=5.0,
                        max_retries=0,
                        pause_seconds=0.0,
                    ),
                )

            regional_rows = connection.execute(
                """
                SELECT market_date, average_price
                FROM market_history_daily
                WHERE region_id = ? AND type_id = ?
                ORDER BY market_date
                """,
                (10000002, 44992),
            ).fetchall()
            global_rows = connection.execute(
                """
                SELECT market_date, average_price
                FROM market_history_daily
                WHERE region_id = ? AND type_id = ?
                ORDER BY market_date
                """,
                (19000001, 44992),
            ).fetchall()
            scope_row = connection.execute(
                """
                SELECT coverage_marker
                FROM sync_scope_state
                WHERE scope_key = ?
                """,
                ("market_history:10000002:44992",),
            ).fetchone()
        finally:
            connection.close()

        self.assertEqual(["2025-07-05", "2025-07-06"], [row["market_date"] for row in regional_rows])
        self.assertEqual(["2025-07-07", "2025-07-08"], [row["market_date"] for row in global_rows])
        self.assertEqual(5_350_000.0, global_rows[0]["average_price"])
        self.assertEqual("ok:4:2025-07-08", scope_row["coverage_marker"])
        self.assertEqual(4, result["fetch_run"]["history_rows_received"])
        self.assertEqual(4, result["fetch_run"]["history_rows_written"])

    def test_history_sync_cycle_backfills_missing_and_refreshes_due_latest_day(self) -> None:
        connection = connect(self.db_path)
        try:
            connection.executemany(
                "INSERT INTO item_catalog (type_id, type_name, market_group, is_published) VALUES (?, ?, ?, ?)",
                [
                    (9201, "Missing History", "Ships", 1),
                    (9202, "Refresh Due", "Ships", 1),
                    (9203, "Healthy History", "Ships", 1),
                ],
            )

            refresh_due_entries = _build_history_entries(start_day=1, day_count=30, base_price=200.0)
            healthy_entries = _build_history_entries(start_day=1, day_count=30, base_price=300.0)
            for entry in refresh_due_entries:
                connection.execute(
                    """
                    INSERT INTO market_history_daily (
                        type_id, region_id, market_date, average_price, highest_price, lowest_price,
                        volume, order_count, fetched_at, source, coverage_status
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        9202,
                        10000002,
                        entry["date"],
                        entry["average"],
                        entry["highest"],
                        entry["lowest"],
                        entry["volume"],
                        entry["order_count"],
                        "2020-01-01T00:00:00Z",
                        "seed",
                        "ok",
                    ),
                )
            for entry in healthy_entries:
                connection.execute(
                    """
                    INSERT INTO market_history_daily (
                        type_id, region_id, market_date, average_price, highest_price, lowest_price,
                        volume, order_count, fetched_at, source, coverage_status
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        9203,
                        10000002,
                        entry["date"],
                        entry["average"],
                        entry["highest"],
                        entry["lowest"],
                        entry["volume"],
                        entry["order_count"],
                        "2099-01-01T00:00:00Z",
                        "seed",
                        "ok",
                    ),
                )
            connection.executemany(
                """
                INSERT INTO sync_scope_state (
                    scope_key, scope_type, region_id, type_id, last_success_at, last_attempt_at,
                    next_scheduled_at, failure_count, last_error, coverage_marker
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    ("market_history:10000002:9202", "market_history", 10000002, 9202, "2020-01-01T00:00:00Z", "2020-01-01T00:00:00Z", None, 0, None, "ok:30:2026-03-30"),
                    ("market_history:10000002:9203", "market_history", 10000002, 9203, "2099-01-01T00:00:00Z", "2099-01-01T00:00:00Z", None, 0, None, "ok:30:2026-03-30"),
                ],
            )
            connection.commit()

            snapshot_before = collect_history_sync_snapshot(
                connection,
                region_id=10000002,
                refresh_interval_hours=24.0,
                not_found_recheck_hours=168.0,
            )
            self.assertEqual([9201], snapshot_before["type_ids"]["backfill_due"])
            self.assertEqual([9202], snapshot_before["type_ids"]["refresh_due"])

            def fake_request_history(**kwargs):
                type_id = int(kwargs["type_id"])
                if type_id == 9201:
                    return {
                        "ok": True,
                        "status_code": 200,
                        "url": "https://example.invalid/history/9201",
                        "retries_used": 0,
                        "headers": {},
                        "entries": _build_history_entries(start_day=1, day_count=30, base_price=100.0),
                        "error": None,
                    }
                if type_id == 9202:
                    refreshed = _build_history_entries(start_day=1, day_count=31, base_price=500.0)
                    return {
                        "ok": True,
                        "status_code": 200,
                        "url": "https://example.invalid/history/9202",
                        "retries_used": 0,
                        "headers": {},
                        "entries": refreshed,
                        "error": None,
                    }
                raise AssertionError(f"unexpected type_id {type_id}")

            with patch("eveqtrading.history_fetch._request_history", side_effect=fake_request_history):
                result = run_history_sync_cycle(
                    connection,
                    db_path=self.db_path,
                    output_root=self.output_root,
                    region_id=10000002,
                    backfill_chunk_size=10,
                    max_backfill_chunks=1,
                    refresh_chunk_size=10,
                    max_refresh_chunks=1,
                    history_config=HistoryFetchConfig(
                        region_id=10000002,
                        datasource="tranquility",
                        esi_base_url="https://esi.evetech.net/latest",
                        user_agent="test-history-sync",
                        request_timeout_seconds=5.0,
                        max_retries=0,
                        pause_seconds=0.0,
                    ),
                    refresh_interval_hours=24.0,
                    not_found_recheck_hours=168.0,
                )

            missing_history_count = connection.execute(
                "SELECT COUNT(*) FROM market_history_daily WHERE region_id = ? AND type_id = ?",
                (10000002, 9201),
            ).fetchone()[0]
            refresh_due_rows = connection.execute(
                """
                SELECT market_date, average_price
                FROM market_history_daily
                WHERE region_id = ? AND type_id = ?
                ORDER BY market_date
                """,
                (10000002, 9202),
            ).fetchall()
        finally:
            connection.close()

        self.assertEqual(30, missing_history_count)
        self.assertEqual(31, len(refresh_due_rows))
        self.assertEqual(229.0, refresh_due_rows[-2]["average_price"])
        self.assertEqual(530.0, refresh_due_rows[-1]["average_price"])
        self.assertEqual(1, result["payload"]["backfill_target_count"])
        self.assertEqual(1, result["payload"]["backfill_executed_target_count"])
        self.assertEqual(1, result["payload"]["refresh_target_count"])
        self.assertEqual(1, result["payload"]["refresh_executed_target_count"])
        self.assertTrue((result["run_dir"] / "history_sync_cycle.json").exists())

    def test_fetch_market_history_writes_next_scheduled_at_and_preserves_last_success_on_failure(self) -> None:
        connection = connect(self.db_path)
        try:
            connection.executemany(
                "INSERT INTO item_catalog (type_id, type_name, market_group, is_published) VALUES (?, ?, ?, ?)",
                [
                    (9301, "Scheduled Success", "Ships", 1),
                    (9302, "Scheduled Not Found", "Ships", 1),
                    (9303, "Scheduled Failure", "Ships", 1),
                ],
            )
            connection.execute(
                """
                INSERT INTO sync_scope_state (
                    scope_key, scope_type, region_id, type_id, last_success_at, last_attempt_at,
                    next_scheduled_at, failure_count, last_error, coverage_marker
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "market_history:10000002:9303",
                    "market_history",
                    10000002,
                    9303,
                    "2026-04-01T00:00:00Z",
                    "2026-04-01T00:00:00Z",
                    None,
                    0,
                    None,
                    "ok:30:2026-03-30",
                ),
            )
            connection.commit()

            def fake_request_history(**kwargs):
                type_id = int(kwargs["type_id"])
                if type_id == 9301:
                    return {
                        "ok": True,
                        "status_code": 200,
                        "url": "https://example.invalid/history/9301",
                        "retries_used": 0,
                        "headers": {},
                        "entries": _build_history_entries(start_day=1, day_count=30, base_price=100.0),
                        "error": None,
                    }
                if type_id == 9302:
                    return {
                        "ok": False,
                        "status_code": 404,
                        "url": "https://example.invalid/history/9302",
                        "retries_used": 0,
                        "headers": {},
                        "entries": [],
                        "error": "not found",
                    }
                if type_id == 9303:
                    return {
                        "ok": False,
                        "status_code": 500,
                        "url": "https://example.invalid/history/9303",
                        "retries_used": 0,
                        "headers": {},
                        "entries": [],
                        "error": "server error",
                    }
                raise AssertionError(f"unexpected type_id {type_id}")

            with patch("eveqtrading.history_fetch._request_history", side_effect=fake_request_history):
                fetch_market_history(
                    connection,
                    output_root=self.output_root,
                    type_targets=[
                        {"type_id": 9301, "type_name": "Scheduled Success", "selection_mode": "explicit"},
                        {"type_id": 9302, "type_name": "Scheduled Not Found", "selection_mode": "explicit"},
                        {"type_id": 9303, "type_name": "Scheduled Failure", "selection_mode": "explicit"},
                    ],
                    config=HistoryFetchConfig(
                        region_id=10000002,
                        datasource="tranquility",
                        esi_base_url="https://esi.evetech.net/latest",
                        user_agent="test-history-schedule",
                        request_timeout_seconds=5.0,
                        max_retries=0,
                        pause_seconds=0.0,
                        refresh_interval_hours=24.0,
                        not_found_recheck_hours=168.0,
                        failure_backoff_seconds=1800,
                    ),
                )

            sync_rows = connection.execute(
                """
                SELECT type_id, last_success_at, next_scheduled_at, failure_count, coverage_marker
                FROM sync_scope_state
                WHERE scope_type = 'market_history'
                ORDER BY type_id
                """
            ).fetchall()
        finally:
            connection.close()

        by_type = {int(row["type_id"]): row for row in sync_rows}
        self.assertEqual("ok:30:2026-03-30", by_type[9301]["coverage_marker"])
        self.assertIsNotNone(by_type[9301]["next_scheduled_at"])
        self.assertEqual("not_found", by_type[9302]["coverage_marker"])
        self.assertIsNotNone(by_type[9302]["next_scheduled_at"])
        self.assertEqual("2026-04-01T00:00:00Z", by_type[9303]["last_success_at"])
        self.assertEqual(1, by_type[9303]["failure_count"])
        self.assertEqual("failed:500", by_type[9303]["coverage_marker"])
        self.assertIsNotNone(by_type[9303]["next_scheduled_at"])

    def test_collect_history_sync_snapshot_splits_waiting_backfill_and_waiting_refresh(self) -> None:
        connection = connect(self.db_path)
        try:
            connection.executemany(
                "INSERT INTO item_catalog (type_id, type_name, market_group, is_published) VALUES (?, ?, ?, ?)",
                [
                    (9401, "Backfill Wait", "Ships", 1),
                    (9402, "Refresh Wait", "Ships", 1),
                    (9403, "Backfill Due", "Ships", 1),
                    (9404, "Refresh Due", "Ships", 1),
                ],
            )
            refresh_entries = _build_history_entries(start_day=1, day_count=30, base_price=200.0)
            for entry in refresh_entries:
                for type_id in (9402, 9404):
                    connection.execute(
                        """
                        INSERT INTO market_history_daily (
                            type_id, region_id, market_date, average_price, highest_price, lowest_price,
                            volume, order_count, fetched_at, source, coverage_status
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            type_id,
                            10000002,
                            entry["date"],
                            entry["average"],
                            entry["highest"],
                            entry["lowest"],
                            entry["volume"],
                            entry["order_count"],
                            "2026-04-01T00:00:00Z",
                            "seed",
                            "ok",
                        ),
                    )
            connection.executemany(
                """
                INSERT INTO sync_scope_state (
                    scope_key, scope_type, region_id, type_id, last_success_at, last_attempt_at,
                    next_scheduled_at, failure_count, last_error, coverage_marker
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        "market_history:10000002:9401",
                        "market_history",
                        10000002,
                        9401,
                        None,
                        "2026-04-06T00:00:00Z",
                        "2099-01-01T00:00:00Z",
                        1,
                        "server error",
                        "failed:500",
                    ),
                    (
                        "market_history:10000002:9402",
                        "market_history",
                        10000002,
                        9402,
                        "2026-04-01T00:00:00Z",
                        "2026-04-06T00:00:00Z",
                        "2099-01-01T00:00:00Z",
                        1,
                        "server error",
                        "failed:500",
                    ),
                    (
                        "market_history:10000002:9403",
                        "market_history",
                        10000002,
                        9403,
                        None,
                        "2026-04-06T00:00:00Z",
                        "2020-01-01T00:00:00Z",
                        1,
                        "server error",
                        "failed:500",
                    ),
                    (
                        "market_history:10000002:9404",
                        "market_history",
                        10000002,
                        9404,
                        "2026-04-01T00:00:00Z",
                        "2026-04-06T00:00:00Z",
                        "2020-01-01T00:00:00Z",
                        1,
                        "server error",
                        "failed:500",
                    ),
                ],
            )
            connection.commit()

            snapshot = collect_history_sync_snapshot(
                connection,
                region_id=10000002,
                refresh_interval_hours=24.0,
                not_found_recheck_hours=168.0,
            )
        finally:
            connection.close()

        self.assertEqual([9403], snapshot["type_ids"]["backfill_due"])
        self.assertEqual([9401], snapshot["type_ids"]["backfill_wait"])
        self.assertEqual([9404], snapshot["type_ids"]["refresh_due"])
        self.assertEqual([9402], snapshot["type_ids"]["refresh_wait"])

    def test_collect_history_sync_snapshot_keeps_partial_history_on_backfill_path(self) -> None:
        connection = connect(self.db_path)
        try:
            connection.executemany(
                "INSERT INTO item_catalog (type_id, type_name, market_group, is_published) VALUES (?, ?, ?, ?)",
                [
                    (9411, "Partial Due", "Ships", 1),
                    (9412, "Partial Wait", "Ships", 1),
                    (9413, "Ready Healthy", "Ships", 1),
                ],
            )
            partial_entries = _build_history_entries(start_day=1, day_count=12, base_price=150.0)
            ready_entries = _build_history_entries(start_day=1, day_count=30, base_price=250.0)
            for entry in partial_entries:
                for type_id in (9411, 9412):
                    connection.execute(
                        """
                        INSERT INTO market_history_daily (
                            type_id, region_id, market_date, average_price, highest_price, lowest_price,
                            volume, order_count, fetched_at, source, coverage_status
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            type_id,
                            10000002,
                            entry["date"],
                            entry["average"],
                            entry["highest"],
                            entry["lowest"],
                            entry["volume"],
                            entry["order_count"],
                            "2026-04-01T00:00:00Z",
                            "seed",
                            "ok",
                        ),
                    )
            for entry in ready_entries:
                connection.execute(
                    """
                    INSERT INTO market_history_daily (
                        type_id, region_id, market_date, average_price, highest_price, lowest_price,
                        volume, order_count, fetched_at, source, coverage_status
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        9413,
                        10000002,
                        entry["date"],
                        entry["average"],
                        entry["highest"],
                        entry["lowest"],
                        entry["volume"],
                        entry["order_count"],
                        "2026-04-01T00:00:00Z",
                        "seed",
                        "ok",
                    ),
                )
            connection.executemany(
                """
                INSERT INTO sync_scope_state (
                    scope_key, scope_type, region_id, type_id, last_success_at, last_attempt_at,
                    next_scheduled_at, failure_count, last_error, coverage_marker
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        "market_history:10000002:9411",
                        "market_history",
                        10000002,
                        9411,
                        "2026-04-01T00:00:00Z",
                        "2026-04-06T00:00:00Z",
                        None,
                        0,
                        None,
                        "ok:12:2026-03-12",
                    ),
                    (
                        "market_history:10000002:9412",
                        "market_history",
                        10000002,
                        9412,
                        "2026-04-01T00:00:00Z",
                        "2026-04-06T00:00:00Z",
                        "2099-01-01T00:00:00Z",
                        0,
                        None,
                        "ok:12:2026-03-12",
                    ),
                    (
                        "market_history:10000002:9413",
                        "market_history",
                        10000002,
                        9413,
                        "2099-01-01T00:00:00Z",
                        "2099-01-01T00:00:00Z",
                        None,
                        0,
                        None,
                        "ok:30:2026-03-30",
                    ),
                ],
            )
            connection.commit()

            snapshot = collect_history_sync_snapshot(
                connection,
                region_id=10000002,
                refresh_interval_hours=24.0,
                not_found_recheck_hours=168.0,
            )
        finally:
            connection.close()

        self.assertEqual([9411], snapshot["type_ids"]["backfill_due"])
        self.assertEqual([9412], snapshot["type_ids"]["backfill_wait"])
        self.assertEqual([], snapshot["type_ids"]["refresh_due"])
        self.assertEqual([], snapshot["type_ids"]["refresh_wait"])
        self.assertEqual([9413], snapshot["type_ids"]["healthy"])

    def test_plan_history_sync_cli_prints_summary_payload_only(self) -> None:
        connection = connect(self.db_path)
        try:
            connection.executemany(
                "INSERT INTO item_catalog (type_id, type_name, market_group, is_published) VALUES (?, ?, ?, ?)",
                [
                    (9501, "Plan Healthy", "Ships", 1),
                    (9502, "Plan Backfill", "Ships", 1),
                ],
            )
            for entry in _build_history_entries(start_day=1, day_count=30, base_price=100.0):
                connection.execute(
                    """
                    INSERT INTO market_history_daily (
                        type_id, region_id, market_date, average_price, highest_price, lowest_price,
                        volume, order_count, fetched_at, source, coverage_status
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        9501,
                        10000002,
                        entry["date"],
                        entry["average"],
                        entry["highest"],
                        entry["lowest"],
                        entry["volume"],
                        entry["order_count"],
                        "2026-04-01T00:00:00Z",
                        "seed",
                        "ok",
                    ),
                )
            connection.execute(
                """
                INSERT INTO sync_scope_state (
                    scope_key, scope_type, region_id, type_id, last_success_at, last_attempt_at,
                    next_scheduled_at, failure_count, last_error, coverage_marker
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "market_history:10000002:9501",
                    "market_history",
                    10000002,
                    9501,
                    "2099-01-01T00:00:00Z",
                    "2099-01-01T00:00:00Z",
                    "2099-01-02T00:00:00Z",
                    0,
                    None,
                    "ok:30:2026-03-30",
                ),
            )
            connection.commit()
        finally:
            connection.close()

        stdout = io.StringIO()
        argv = [
            "eveq",
            "plan-history-sync",
            "--db-path",
            str(self.db_path),
            "--output-root",
            str(self.output_root),
            "--region-id",
            "10000002",
        ]
        with patch.object(sys, "argv", argv):
            with redirect_stdout(stdout):
                exit_code = cli_main()

        output_lines = stdout.getvalue().strip().splitlines()
        self.assertEqual(0, exit_code)
        self.assertTrue(output_lines[0].startswith("History sync plan completed:"))
        self.assertTrue(output_lines[1].startswith("Artifacts:"))
        payload = json.loads("\n".join(output_lines[2:]))
        self.assertEqual(2, payload["target_counts"]["market_type_count"])
        self.assertEqual(1, payload["target_counts"]["healthy_count"])
        self.assertEqual(1, payload["target_counts"]["backfill_due_count"])
        self.assertNotIn("rows", payload)

    def test_fetch_market_history_retries_socket_timeout(self) -> None:
        connection = connect(self.db_path)
        try:
            connection.execute(
                "INSERT INTO item_catalog (type_id, type_name, market_group, is_published) VALUES (?, ?, ?, ?)",
                (9601, "Timeout Retry", "Ships", 1),
            )
            connection.commit()

            call_counter = {"count": 0}

            def fake_request_history(**kwargs):
                raise AssertionError("patched wrong function")

            def flaky_urlopen(request, timeout):
                call_counter["count"] += 1
                if call_counter["count"] == 1:
                    raise socket.timeout("timed out")

                class FakeResponse:
                    status = 200
                    headers = {}

                    def __enter__(self):
                        return self

                    def __exit__(self, exc_type, exc, tb):
                        return False

                    def read(self):
                        return json.dumps(
                            _build_history_entries(start_day=1, day_count=30, base_price=100.0)
                        ).encode("utf-8")

                return FakeResponse()

            with patch("urllib.request.urlopen", side_effect=flaky_urlopen):
                result = fetch_market_history(
                    connection,
                    output_root=self.output_root,
                    type_targets=[{"type_id": 9601, "type_name": "Timeout Retry", "selection_mode": "explicit"}],
                    config=HistoryFetchConfig(
                        region_id=10000002,
                        datasource="tranquility",
                        esi_base_url="https://esi.evetech.net/latest",
                        user_agent="test-history-timeout-retry",
                        request_timeout_seconds=5.0,
                        max_retries=1,
                        pause_seconds=0.0,
                    ),
                )
        finally:
            connection.close()

        self.assertEqual(2, call_counter["count"])
        self.assertEqual(1, result["fetch_run"]["type_count_success"])

    def test_fetch_market_history_retries_incomplete_read(self) -> None:
        connection = connect(self.db_path)
        try:
            connection.execute(
                "INSERT INTO item_catalog (type_id, type_name, market_group, is_published) VALUES (?, ?, ?, ?)",
                (9602, "Incomplete Read Retry", "Ships", 1),
            )
            connection.commit()

            call_counter = {"count": 0}

            def flaky_urlopen(request, timeout):
                call_counter["count"] += 1
                if call_counter["count"] == 1:
                    raise http.client.IncompleteRead(b"partial", 10)

                class FakeResponse:
                    status = 200
                    headers = {}

                    def __enter__(self):
                        return self

                    def __exit__(self, exc_type, exc, tb):
                        return False

                    def read(self):
                        return json.dumps(
                            _build_history_entries(start_day=1, day_count=30, base_price=120.0)
                        ).encode("utf-8")

                return FakeResponse()

            with patch("urllib.request.urlopen", side_effect=flaky_urlopen):
                result = fetch_market_history(
                    connection,
                    output_root=self.output_root,
                    type_targets=[{"type_id": 9602, "type_name": "Incomplete Read Retry", "selection_mode": "explicit"}],
                    config=HistoryFetchConfig(
                        region_id=10000002,
                        datasource="tranquility",
                        esi_base_url="https://esi.evetech.net/latest",
                        user_agent="test-history-incomplete-read-retry",
                        request_timeout_seconds=5.0,
                        max_retries=1,
                        pause_seconds=0.0,
                    ),
                )
        finally:
            connection.close()

        self.assertEqual(2, call_counter["count"])
        self.assertEqual(1, result["fetch_run"]["type_count_success"])

    def test_fetch_market_history_retries_remote_disconnected(self) -> None:
        connection = connect(self.db_path)
        try:
            connection.execute(
                "INSERT INTO item_catalog (type_id, type_name, market_group, is_published) VALUES (?, ?, ?, ?)",
                (9603, "Remote Disconnect Retry", "Ships", 1),
            )
            connection.commit()

            call_counter = {"count": 0}

            def flaky_urlopen(request, timeout):
                call_counter["count"] += 1
                if call_counter["count"] == 1:
                    raise http.client.RemoteDisconnected("Remote end closed connection without response")

                class FakeResponse:
                    status = 200
                    headers = {}

                    def __enter__(self):
                        return self

                    def __exit__(self, exc_type, exc, tb):
                        return False

                    def read(self):
                        return json.dumps(
                            _build_history_entries(start_day=1, day_count=30, base_price=140.0)
                        ).encode("utf-8")

                return FakeResponse()

            with patch("urllib.request.urlopen", side_effect=flaky_urlopen):
                result = fetch_market_history(
                    connection,
                    output_root=self.output_root,
                    type_targets=[{"type_id": 9603, "type_name": "Remote Disconnect Retry", "selection_mode": "explicit"}],
                    config=HistoryFetchConfig(
                        region_id=10000002,
                        datasource="tranquility",
                        esi_base_url="https://esi.evetech.net/latest",
                        user_agent="test-history-remote-disconnect-retry",
                        request_timeout_seconds=5.0,
                        max_retries=1,
                        pause_seconds=0.0,
                    ),
                )
        finally:
            connection.close()

        self.assertEqual(2, call_counter["count"])
        self.assertEqual(1, result["fetch_run"]["type_count_success"])

    def test_fetch_market_history_retries_request_wall_timeout_response(self) -> None:
        connection = connect(self.db_path)
        try:
            connection.execute(
                "INSERT INTO item_catalog (type_id, type_name, market_group, is_published) VALUES (?, ?, ?, ?)",
                (9604, "Wall Timeout Retry", "Ships", 1),
            )
            connection.commit()

            call_counter = {"count": 0}

            def fake_request_once(**kwargs):
                call_counter["count"] += 1
                if call_counter["count"] == 1:
                    return {
                        "ok": False,
                        "status_code": None,
                        "url": kwargs["url"],
                        "headers": {},
                        "entries": [],
                        "error": "request wall timeout after 1.0 seconds",
                        "retryable": True,
                        "retry_after": None,
                    }
                return {
                    "ok": True,
                    "status_code": 200,
                    "url": kwargs["url"],
                    "headers": {},
                    "entries": _build_history_entries(start_day=1, day_count=30, base_price=160.0),
                    "error": None,
                    "retryable": False,
                    "retry_after": None,
                }

            with patch("eveqtrading.history_fetch._perform_request_history_once", side_effect=fake_request_once):
                result = fetch_market_history(
                    connection,
                    output_root=self.output_root,
                    type_targets=[{"type_id": 9604, "type_name": "Wall Timeout Retry", "selection_mode": "explicit"}],
                    config=HistoryFetchConfig(
                        region_id=10000002,
                        datasource="tranquility",
                        esi_base_url="https://esi.evetech.net/latest",
                        user_agent="test-history-wall-timeout-retry",
                        request_timeout_seconds=5.0,
                        request_wall_timeout_seconds=1.0,
                        max_retries=1,
                        pause_seconds=0.0,
                    ),
                )
        finally:
            connection.close()

        self.assertEqual(2, call_counter["count"])
        self.assertEqual(1, result["fetch_run"]["type_count_success"])

    def test_fetch_market_history_concurrent_requests_runs_network_fetch_in_parallel(self) -> None:
        connection = connect(self.db_path)
        try:
            connection.executemany(
                "INSERT INTO item_catalog (type_id, type_name, market_group, is_published) VALUES (?, ?, ?, ?)",
                [
                    (9701, "Concurrent One", "Ships", 1),
                    (9702, "Concurrent Two", "Ships", 1),
                ],
            )
            connection.commit()

            first_started = threading.Event()
            second_started = threading.Event()

            def fake_request_history(**kwargs):
                type_id = int(kwargs["type_id"])
                if type_id == 9701:
                    first_started.set()
                    if not second_started.wait(timeout=0.5):
                        return {
                            "ok": False,
                            "status_code": None,
                            "url": "https://example.invalid/history/9701",
                            "retries_used": 0,
                            "headers": {},
                            "entries": [],
                            "error": "second request never started concurrently",
                        }
                    return {
                        "ok": True,
                        "status_code": 200,
                        "url": "https://example.invalid/history/9701",
                        "retries_used": 0,
                        "headers": {},
                        "entries": _build_history_entries(start_day=1, day_count=30, base_price=170.0),
                        "error": None,
                    }
                if type_id == 9702:
                    self.assertTrue(first_started.wait(timeout=0.5))
                    second_started.set()
                    return {
                        "ok": True,
                        "status_code": 200,
                        "url": "https://example.invalid/history/9702",
                        "retries_used": 0,
                        "headers": {},
                        "entries": _build_history_entries(start_day=1, day_count=30, base_price=180.0),
                        "error": None,
                    }
                raise AssertionError(f"unexpected type_id {type_id}")

            with patch("eveqtrading.history_fetch._request_history", side_effect=fake_request_history):
                result = fetch_market_history(
                    connection,
                    output_root=self.output_root,
                    type_targets=[
                        {"type_id": 9701, "type_name": "Concurrent One", "selection_mode": "explicit"},
                        {"type_id": 9702, "type_name": "Concurrent Two", "selection_mode": "explicit"},
                    ],
                    config=HistoryFetchConfig(
                        region_id=10000002,
                        datasource="tranquility",
                        esi_base_url="https://esi.evetech.net/latest",
                        user_agent="test-history-concurrent-fetch",
                        request_timeout_seconds=5.0,
                        max_retries=0,
                        pause_seconds=0.0,
                        concurrent_requests=2,
                    ),
                )
        finally:
            connection.close()

        self.assertEqual(2, result["fetch_run"]["type_count_success"])
        self.assertEqual([1, 2], [row["ordinal"] for row in result["fetch_trace"]["type_traces"]])

    def test_history_sync_worker_runs_multiple_cycles_until_due_backlog_is_empty(self) -> None:
        cycle_run_dir_1 = self.output_root / "history-sync-cycle-1"
        cycle_run_dir_2 = self.output_root / "history-sync-cycle-2"

        with patch(
            "eveqtrading.history_sync_worker.collect_history_sync_snapshot",
            return_value={
                "target_counts": {
                    "market_type_count": 2,
                    "healthy_count": 0,
                    "backfill_due_count": 2,
                    "backfill_wait_count": 0,
                    "refresh_due_count": 0,
                    "refresh_wait_count": 0,
                }
            },
        ), patch(
            "eveqtrading.history_sync_worker.run_history_sync_cycle",
            side_effect=[
                {
                    "cycle_run_id": "history-sync-cycle-1",
                    "run_dir": cycle_run_dir_1,
                    "payload": {
                        "before_counts": {"backfill_due_count": 2, "refresh_due_count": 0},
                        "after_refresh_counts": {"backfill_due_count": 1, "refresh_due_count": 0},
                        "backfill_target_count": 1,
                        "refresh_target_count": 0,
                        "backfill_executed_target_count": 1,
                        "refresh_executed_target_count": 0,
                    },
                },
                {
                    "cycle_run_id": "history-sync-cycle-2",
                    "run_dir": cycle_run_dir_2,
                    "payload": {
                        "before_counts": {"backfill_due_count": 1, "refresh_due_count": 0},
                        "after_refresh_counts": {"backfill_due_count": 0, "refresh_due_count": 0},
                        "backfill_target_count": 1,
                        "refresh_target_count": 0,
                        "backfill_executed_target_count": 1,
                        "refresh_executed_target_count": 0,
                    },
                },
            ],
        ), patch("eveqtrading.history_sync_worker.time.monotonic", side_effect=[100.0, 101.0]):
            result = run_history_sync_worker_once(
                db_path=self.db_path,
                output_root=self.output_root,
                lock_path=self.base_dir / "state" / "history-sync-worker.lock",
                state_path=self.base_dir / "state" / "history-sync-worker-state.json",
                region_id=10000002,
                backfill_chunk_size=50,
                max_backfill_chunks=1,
                refresh_chunk_size=200,
                max_refresh_chunks=1,
                history_config=HistoryFetchConfig(
                    region_id=10000002,
                    datasource="tranquility",
                    esi_base_url="https://esi.evetech.net/latest",
                    user_agent="test-history-sync-worker-loop",
                    request_timeout_seconds=5.0,
                    max_retries=0,
                    pause_seconds=0.0,
                ),
                refresh_interval_hours=24.0,
                not_found_recheck_hours=168.0,
                limit=None,
                max_runtime_seconds=540.0,
                max_cycles_per_run=None,
            )

        self.assertEqual("completed", result["status"])
        self.assertEqual("no_due_targets", result["stop_reason"])
        self.assertEqual(2, result["cycle_count"])
        self.assertEqual(["history-sync-cycle-1", "history-sync-cycle-2"], result["cycle_run_ids"])
        self.assertEqual(2, result["aggregate"]["executed_backfill_target_count"])
        self.assertEqual(0, result["final_counts"]["backfill_due_count"])

    def test_history_sync_worker_skips_cycle_when_no_due_targets_exist(self) -> None:
        with patch(
            "eveqtrading.history_sync_worker.collect_history_sync_snapshot",
            return_value={
                "target_counts": {
                    "market_type_count": 10,
                    "healthy_count": 10,
                    "backfill_due_count": 0,
                    "backfill_wait_count": 0,
                    "refresh_due_count": 0,
                    "refresh_wait_count": 0,
                }
            },
        ), patch("eveqtrading.history_sync_worker.run_history_sync_cycle") as mocked_cycle:
            result = run_history_sync_worker_once(
                db_path=self.db_path,
                output_root=self.output_root,
                lock_path=self.base_dir / "state" / "history-sync-worker.lock",
                state_path=self.base_dir / "state" / "history-sync-worker-state.json",
                region_id=10000002,
                backfill_chunk_size=50,
                max_backfill_chunks=1,
                refresh_chunk_size=200,
                max_refresh_chunks=1,
                history_config=HistoryFetchConfig(
                    region_id=10000002,
                    datasource="tranquility",
                    esi_base_url="https://esi.evetech.net/latest",
                    user_agent="test-history-sync-worker-idle",
                    request_timeout_seconds=5.0,
                    max_retries=0,
                    pause_seconds=0.0,
                ),
                refresh_interval_hours=24.0,
                not_found_recheck_hours=168.0,
                limit=None,
                max_runtime_seconds=540.0,
                max_cycles_per_run=None,
            )

        mocked_cycle.assert_not_called()
        self.assertEqual("completed", result["status"])
        self.assertEqual("no_due_targets", result["stop_reason"])
        self.assertEqual(0, result["cycle_count"])
        self.assertEqual([], result["cycle_run_ids"])

    def test_history_sync_worker_stops_when_runtime_budget_is_reached(self) -> None:
        cycle_run_dir = self.output_root / "history-sync-cycle-runtime"

        with patch(
            "eveqtrading.history_sync_worker.collect_history_sync_snapshot",
            return_value={
                "target_counts": {
                    "market_type_count": 2,
                    "healthy_count": 0,
                    "backfill_due_count": 2,
                    "backfill_wait_count": 0,
                    "refresh_due_count": 0,
                    "refresh_wait_count": 0,
                }
            },
        ), patch(
            "eveqtrading.history_sync_worker.run_history_sync_cycle",
            return_value={
                "cycle_run_id": "history-sync-cycle-runtime",
                "run_dir": cycle_run_dir,
                "payload": {
                    "before_counts": {"backfill_due_count": 2, "refresh_due_count": 0},
                    "after_refresh_counts": {"backfill_due_count": 1, "refresh_due_count": 0},
                    "backfill_target_count": 1,
                    "refresh_target_count": 0,
                    "backfill_executed_target_count": 1,
                    "refresh_executed_target_count": 0,
                },
            },
        ), patch("eveqtrading.history_sync_worker.time.monotonic", side_effect=[100.0, 112.0]):
            result = run_history_sync_worker_once(
                db_path=self.db_path,
                output_root=self.output_root,
                lock_path=self.base_dir / "state" / "history-sync-worker.lock",
                state_path=self.base_dir / "state" / "history-sync-worker-state.json",
                region_id=10000002,
                backfill_chunk_size=50,
                max_backfill_chunks=1,
                refresh_chunk_size=200,
                max_refresh_chunks=1,
                history_config=HistoryFetchConfig(
                    region_id=10000002,
                    datasource="tranquility",
                    esi_base_url="https://esi.evetech.net/latest",
                    user_agent="test-history-sync-worker-runtime",
                    request_timeout_seconds=5.0,
                    max_retries=0,
                    pause_seconds=0.0,
                ),
                refresh_interval_hours=24.0,
                not_found_recheck_hours=168.0,
                limit=None,
                max_runtime_seconds=10.0,
                max_cycles_per_run=None,
            )

        self.assertEqual("completed", result["status"])
        self.assertEqual("max_runtime_reached", result["stop_reason"])
        self.assertEqual(1, result["cycle_count"])
        self.assertEqual(1, result["final_counts"]["backfill_due_count"])

    def test_history_sync_worker_reclaims_stale_empty_lock_file(self) -> None:
        lock_path = self.base_dir / "state" / "history-sync-worker.lock"
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        lock_path.write_text("", encoding="utf-8")
        stale_time = 1_700_000_000
        os.utime(lock_path, (stale_time, stale_time))

        with patch("eveqtrading.history_sync_worker.datetime") as mocked_datetime:
            real_datetime = __import__("datetime").datetime
            mocked_datetime.now.return_value = real_datetime.fromtimestamp(
                stale_time + 300,
                tz=__import__("datetime").timezone.utc,
            )
            mocked_datetime.fromisoformat = real_datetime.fromisoformat
            with _acquire_lock(lock_path, worker_run_id="history-sync-worker-test") as lock_info:
                self.assertEqual("history-sync-worker-test", lock_info["worker_run_id"])

        self.assertFalse(lock_path.exists())

    def test_history_sync_worker_reclaims_stale_dead_pid_lock_file(self) -> None:
        lock_path = self.base_dir / "state" / "history-sync-worker.lock"
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        lock_path.write_text(
            json.dumps(
                {
                    "worker_run_id": "stale-worker",
                    "locked_at_utc": "2023-11-14T00:00:00Z",
                    "pid": 999999,
                },
                ensure_ascii=False,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        stale_time = 1_700_000_000
        os.utime(lock_path, (stale_time, stale_time))

        with patch("eveqtrading.history_sync_worker.datetime") as mocked_datetime:
            real_datetime = __import__("datetime").datetime
            mocked_datetime.now.return_value = real_datetime.fromtimestamp(
                stale_time + 300,
                tz=__import__("datetime").timezone.utc,
            )
            mocked_datetime.fromisoformat = real_datetime.fromisoformat
            with _acquire_lock(lock_path, worker_run_id="history-sync-worker-test") as lock_info:
                self.assertEqual("history-sync-worker-test", lock_info["worker_run_id"])

        self.assertFalse(lock_path.exists())


if __name__ == "__main__":
    unittest.main()
