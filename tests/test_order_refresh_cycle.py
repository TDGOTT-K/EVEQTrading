from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from eveqtrading.db import connect, init_db
from eveqtrading.order_fetch import OrderFetchConfig
from eveqtrading.order_refresh_cycle import run_order_refresh_cycle
from eveqtrading.order_refresh_worker import run_order_refresh_worker_once


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT / "sql" / "schema.sql"


class OrderRefreshCycleTests(unittest.TestCase):
    def setUp(self) -> None:
        self._temp_dir = tempfile.TemporaryDirectory()
        self.base_dir = Path(self._temp_dir.name)
        self.db_path = self.base_dir / "state" / "eveq.db"
        self.output_root = self.base_dir / "runs"
        self.lock_path = self.base_dir / "tmp" / "order-refresh-worker.lock"
        self.state_path = self.base_dir / "state" / "order-refresh-worker-state.json"
        self.output_root.mkdir(parents=True, exist_ok=True)
        init_db(self.db_path, SCHEMA_PATH)
        self._seed_data()

    def tearDown(self) -> None:
        self._temp_dir.cleanup()

    def _orders_config(self) -> OrderFetchConfig:
        return OrderFetchConfig(
            region_id=10000002,
            datasource="tranquility",
            esi_base_url="https://esi.evetech.net/latest",
            user_agent="test-order-refresh",
            request_timeout_seconds=5.0,
            max_retries=0,
            pause_seconds=0.0,
            order_type="all",
        )

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
        next_scheduled_at: str | None,
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
            self._seed_history(connection=connection, type_id=201, type_name="Missing Orders", day_count=30)
            self._seed_history(connection=connection, type_id=202, type_name="Due Remote", day_count=30)
            self._seed_history(connection=connection, type_id=203, type_name="Cache Hit", day_count=30)
            self._seed_order_state(
                connection=connection,
                type_id=202,
                last_success_at="2026-04-07T10:00:00Z",
                next_scheduled_at="2026-04-07T11:00:00Z",
                coverage_marker="ok:snapshot-202",
            )
            self._seed_order_state(
                connection=connection,
                type_id=203,
                last_success_at="2026-04-07T11:50:00Z",
                next_scheduled_at="2026-04-07T12:20:00Z",
                coverage_marker="ok:snapshot-203",
            )
            connection.commit()

    def test_cycle_only_fetches_missing_and_remote_due_targets(self) -> None:
        fake_batch_result = {
            "batch_run_id": "orders-batch-test",
            "run_dir": self.output_root / "orders-batch-test",
            "payload": {
                "totals": {
                    "success": 2,
                    "skipped_fresh": 0,
                    "failed": 0,
                }
            },
        }
        fake_batch_result["run_dir"].mkdir(parents=True, exist_ok=True)

        with connect(self.db_path) as connection:
            with patch(
                "eveqtrading.order_refresh._utc_now",
                return_value=datetime(2026, 4, 7, 12, 0, tzinfo=timezone.utc),
            ):
                with patch(
                    "eveqtrading.order_refresh_cycle.run_order_batch_fetch",
                    return_value=fake_batch_result,
                ) as mocked_batch:
                    result = run_order_refresh_cycle(
                        connection,
                        output_root=self.output_root,
                        region_id=10000002,
                        orders_chunk_size=10,
                        max_orders_chunks=1,
                        limit=None,
                        profile_name="warm",
                        orders_config=self._orders_config(),
                    )

        batch_targets = mocked_batch.call_args.kwargs["type_targets"]
        self.assertEqual([201, 202], [row["type_id"] for row in batch_targets])
        self.assertEqual(2, result["payload"]["selected_remote_target_count"])
        self.assertEqual(2, result["payload"]["executed_remote_target_count"])
        self.assertEqual(2, result["payload"]["remote_due_before_count"])
        self.assertEqual("orders-batch-test", result["payload"]["orders_batch_run_id"])

    def test_worker_runs_multiple_cycles_until_backlog_is_cleared(self) -> None:
        initial_snapshot = {
            "target_counts": {
                "missing_orders_count": 2,
                "refresh_due_remote_count": 1,
                "cache_hit_count": 0,
            }
        }
        cycle_results = [
            {
                "cycle_run_id": "order-refresh-cycle-1",
                "run_dir": self.output_root / "order-refresh-cycle-1",
                "payload": {
                    "selected_remote_target_count": 2,
                    "executed_remote_target_count": 2,
                    "remote_due_before_count": 3,
                    "remote_due_after_count": 1,
                    "cache_hit_gain": 2,
                    "after_counts": {
                        "missing_orders_count": 1,
                        "refresh_due_remote_count": 0,
                        "cache_hit_count": 2,
                    },
                    "orders_batch_run_id": "orders-batch-1",
                    "orders_batch_totals": {"success": 2, "skipped_fresh": 0, "failed": 0},
                },
            },
            {
                "cycle_run_id": "order-refresh-cycle-2",
                "run_dir": self.output_root / "order-refresh-cycle-2",
                "payload": {
                    "selected_remote_target_count": 1,
                    "executed_remote_target_count": 1,
                    "remote_due_before_count": 1,
                    "remote_due_after_count": 0,
                    "cache_hit_gain": 1,
                    "after_counts": {
                        "missing_orders_count": 0,
                        "refresh_due_remote_count": 0,
                        "cache_hit_count": 3,
                    },
                    "orders_batch_run_id": "orders-batch-2",
                    "orders_batch_totals": {"success": 1, "skipped_fresh": 0, "failed": 0},
                },
            },
        ]
        for row in cycle_results:
            Path(row["run_dir"]).mkdir(parents=True, exist_ok=True)

        with patch(
            "eveqtrading.order_refresh_worker.collect_order_refresh_snapshot",
            return_value=initial_snapshot,
        ):
            with patch(
                "eveqtrading.order_refresh_worker.run_order_refresh_cycle",
                side_effect=cycle_results,
            ) as mocked_cycle:
                result = run_order_refresh_worker_once(
                    db_path=self.db_path,
                    output_root=self.output_root,
                    lock_path=self.lock_path,
                    state_path=self.state_path,
                    region_id=10000002,
                    orders_chunk_size=50,
                    max_orders_chunks=1,
                    profile_name="warm",
                    orders_config=self._orders_config(),
                    limit=None,
                    max_runtime_seconds=60.0,
                )

        state_payload = json.loads(self.state_path.read_text(encoding="utf-8"))
        self.assertEqual(2, mocked_cycle.call_count)
        self.assertEqual("completed", result["status"])
        self.assertEqual("completed", state_payload["status"])
        self.assertEqual("no_due_targets", result["stop_reason"])
        self.assertEqual(2, result["cycle_count"])
        self.assertEqual(3, result["aggregate"]["executed_remote_target_count"])
        self.assertEqual(3, result["aggregate"]["success_count"])
        self.assertFalse(self.lock_path.exists())


if __name__ == "__main__":
    unittest.main()
