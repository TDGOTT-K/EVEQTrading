from __future__ import annotations

import io
import json
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from eveqtrading.cli import main as cli_main
from eveqtrading.db import connect, init_db
from eveqtrading.order_refresh import (
    build_order_cache_hit_targets,
    build_order_refresh_targets,
    build_order_remote_refresh_targets,
    collect_order_refresh_snapshot,
)


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT / "sql" / "schema.sql"


class OrderRefreshTests(unittest.TestCase):
    def setUp(self) -> None:
        self._temp_dir = tempfile.TemporaryDirectory()
        self.base_dir = Path(self._temp_dir.name)
        self.db_path = self.base_dir / "state" / "eveq.db"
        init_db(self.db_path, SCHEMA_PATH)
        self._seed_data()

    def tearDown(self) -> None:
        self._temp_dir.cleanup()

    def _seed_history(self, *, connection, type_id: int, type_name: str, day_count: int) -> None:
        connection.execute(
            "INSERT INTO item_catalog (type_id, type_name, market_group, is_published) VALUES (?, ?, ?, ?)",
            (type_id, type_name, "Ships", 1),
        )
        for day_index in range(day_count):
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
                    f"2026-03-{day_index + 1:02d}",
                    100.0,
                    101.0,
                    99.0,
                    1000.0,
                    100,
                    "2026-04-07T00:00:00Z",
                    "seed",
                    "ok",
                ),
            )

    def _seed_order_state(
        self,
        *,
        connection,
        type_id: int,
        last_success_at: str,
        next_scheduled_at: str,
        coverage_marker: str,
    ) -> None:
        connection.execute(
            """
            INSERT INTO sync_scope_state (
                scope_key, scope_type, region_id, type_id, last_success_at, last_attempt_at,
                next_scheduled_at, failure_count, last_error, coverage_marker
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                f"market_orders:10000002:{type_id}:all",
                "market_orders",
                10000002,
                type_id,
                last_success_at,
                last_success_at,
                next_scheduled_at,
                0,
                None,
                coverage_marker,
            ),
        )

    def _seed_data(self) -> None:
        with connect(self.db_path) as connection:
            self._seed_history(connection=connection, type_id=11, type_name="Missing Orders", day_count=30)
            self._seed_history(connection=connection, type_id=12, type_name="Due Remote", day_count=30)
            self._seed_history(connection=connection, type_id=13, type_name="Due Wait", day_count=30)
            self._seed_history(connection=connection, type_id=14, type_name="Cache Hit", day_count=30)
            self._seed_history(connection=connection, type_id=15, type_name="Missing History", day_count=10)
            self._seed_order_state(
                connection=connection,
                type_id=12,
                last_success_at="2026-04-07T10:00:00Z",
                next_scheduled_at="2026-04-07T11:00:00Z",
                coverage_marker="ok:snapshot-12",
            )
            self._seed_order_state(
                connection=connection,
                type_id=13,
                last_success_at="2026-04-07T10:30:00Z",
                next_scheduled_at="2026-04-07T12:30:00Z",
                coverage_marker="ok:snapshot-13",
            )
            self._seed_order_state(
                connection=connection,
                type_id=14,
                last_success_at="2026-04-07T11:50:00Z",
                next_scheduled_at="2026-04-07T12:20:00Z",
                coverage_marker="ok:snapshot-14",
            )
            connection.commit()

    def test_collect_snapshot_groups_targets_by_cache_state(self) -> None:
        with connect(self.db_path) as connection:
            with patch(
                "eveqtrading.order_refresh._utc_now",
                return_value=datetime(2026, 4, 7, 12, 0, tzinfo=timezone.utc),
            ):
                snapshot = collect_order_refresh_snapshot(
                    connection,
                    region_id=10000002,
                    profile_name="warm",
                    order_type="all",
                )

        self.assertEqual(5, snapshot["target_counts"]["market_type_count"])
        self.assertEqual(4, snapshot["target_counts"]["history_ready_count"])
        self.assertEqual(1, snapshot["target_counts"]["missing_orders_count"])
        self.assertEqual(1, snapshot["target_counts"]["refresh_due_remote_count"])
        self.assertEqual(1, snapshot["target_counts"]["refresh_due_wait_count"])
        self.assertEqual(1, snapshot["target_counts"]["cache_hit_count"])
        self.assertEqual([11, 12, 14, 13], snapshot["type_ids"]["priority"])

    def test_build_targets_applies_priority_order_and_limit(self) -> None:
        with connect(self.db_path) as connection:
            with patch(
                "eveqtrading.order_refresh._utc_now",
                return_value=datetime(2026, 4, 7, 12, 0, tzinfo=timezone.utc),
            ):
                snapshot, targets = build_order_refresh_targets(
                    connection,
                    region_id=10000002,
                    profile_name="warm",
                    order_type="all",
                    limit=2,
                )

        self.assertEqual([11, 12], [row["type_id"] for row in targets])
        self.assertEqual("order-refresh-warm", targets[0]["selection_mode"])
        self.assertEqual([11, 12, 14, 13], snapshot["type_ids"]["priority"])

    def test_remote_targets_exclude_cache_hit_and_waiting_rows(self) -> None:
        with connect(self.db_path) as connection:
            with patch(
                "eveqtrading.order_refresh._utc_now",
                return_value=datetime(2026, 4, 7, 12, 0, tzinfo=timezone.utc),
            ):
                snapshot, targets = build_order_remote_refresh_targets(
                    connection,
                    region_id=10000002,
                    profile_name="warm",
                    order_type="all",
                )

        self.assertEqual([11, 12], [row["type_id"] for row in targets])
        self.assertEqual("order-refresh-remote-warm", targets[0]["selection_mode"])
        self.assertEqual([11, 12, 14, 13], snapshot["type_ids"]["priority"])

    def test_cache_hit_targets_only_include_locally_fresh_rows(self) -> None:
        with connect(self.db_path) as connection:
            with patch(
                "eveqtrading.order_refresh._utc_now",
                return_value=datetime(2026, 4, 7, 12, 0, tzinfo=timezone.utc),
            ):
                snapshot, targets = build_order_cache_hit_targets(
                    connection,
                    region_id=10000002,
                    profile_name="warm",
                    order_type="all",
                )

        self.assertEqual([14], [row["type_id"] for row in targets])
        self.assertEqual("order-cache-hit-warm", targets[0]["selection_mode"])
        self.assertEqual(1, snapshot["target_counts"]["cache_hit_count"])

    def test_cli_plan_order_refresh_prints_structured_payload(self) -> None:
        fake_plan = {
            "order_refresh_plan_run_id": "order-refresh-plan-test",
            "generated_at_utc": "2026-04-07T12:00:00Z",
            "region_id": 10000002,
            "order_type": "all",
            "profile": {
                "name": "warm",
                "target_max_age_minutes": 30,
                "description": "test profile",
            },
            "target_counts": {
                "market_type_count": 5,
                "history_ready_count": 4,
                "missing_orders_count": 1,
                "refresh_due_remote_count": 1,
                "refresh_due_wait_count": 1,
                "cache_hit_count": 1,
                "priority_target_count": 4,
            },
            "selected_target_count": 2,
            "files": {
                "priority_type_ids_path": str(self.base_dir / "runs" / "priority_type_ids.txt"),
            },
            "sample_rows": {},
        }
        stdout = io.StringIO()
        argv = [
            "eveq",
            "plan-order-refresh",
            "--db-path",
            str(self.db_path),
            "--output-root",
            str(self.base_dir / "runs"),
            "--region-id",
            "10000002",
            "--orders-cache-profile",
            "warm",
        ]

        with patch("eveqtrading.cli.plan_order_refresh", return_value=fake_plan):
            with patch.object(sys, "argv", argv):
                with redirect_stdout(stdout):
                    exit_code = cli_main()

        output_lines = stdout.getvalue().strip().splitlines()
        self.assertEqual(0, exit_code)
        self.assertTrue(output_lines[0].startswith("Order refresh plan completed:"))
        payload = json.loads("\n".join(output_lines[2:]))
        self.assertEqual("warm", payload["profile"]["name"])
        self.assertEqual(4, payload["target_counts"]["history_ready_count"])


if __name__ == "__main__":
    unittest.main()
