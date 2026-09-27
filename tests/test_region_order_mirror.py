from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from eveqtrading.db import connect, init_db
from eveqtrading.order_fetch import OrderFetchConfig
from eveqtrading.region_order_mirror import run_region_order_mirror_once
from eveqtrading.region_order_mirror_worker import run_region_order_mirror_worker_once


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT / "sql" / "schema.sql"


class RegionOrderMirrorTests(unittest.TestCase):
    def setUp(self) -> None:
        self._temp_dir = tempfile.TemporaryDirectory()
        self.base_dir = Path(self._temp_dir.name)
        self.db_path = self.base_dir / "state" / "eveq.db"
        self.output_root = self.base_dir / "runs"
        self.lock_path = self.base_dir / "tmp" / "region-order-mirror.lock"
        self.state_path = self.base_dir / "state" / "region-order-mirror-state.json"
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
            user_agent="test-region-order-mirror",
            request_timeout_seconds=5.0,
            max_retries=0,
            pause_seconds=0.0,
            order_type="all",
            concurrent_requests=8,
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

    def _seed_data(self) -> None:
        with connect(self.db_path) as connection:
            self._seed_history(connection=connection, type_id=9101, type_name="History Ready A", day_count=30)
            self._seed_history(connection=connection, type_id=9102, type_name="History Ready B", day_count=30)
            self._seed_history(connection=connection, type_id=9103, type_name="Partial History", day_count=5)
            connection.commit()

    def test_region_order_mirror_uses_history_ready_universe_and_forces_region_pages(self) -> None:
        fake_orders_result = {
            "run_id": "orders-fetch-region-mirror",
            "run_dir": self.output_root / "orders-fetch-region-mirror",
            "fetch_run": {
                "run_id": "orders-fetch-region-mirror",
                "effective_fetch_mode": "region_pages",
                "pages_fetched": 410,
                "type_count_success": 2,
                "type_count_failed": 0,
            },
        }
        fake_orders_result["run_dir"].mkdir(parents=True, exist_ok=True)

        with connect(self.db_path) as connection:
            with patch(
                "eveqtrading.region_order_mirror.fetch_market_orders",
                return_value=fake_orders_result,
            ) as mocked_fetch:
                result = run_region_order_mirror_once(
                    connection,
                    output_root=self.output_root,
                    region_id=10000002,
                    target_universe="history_ready",
                    orders_config=self._orders_config(),
                    limit=None,
                )

        call = mocked_fetch.call_args.kwargs
        self.assertEqual([9101, 9102], [row["type_id"] for row in call["type_targets"]])
        self.assertEqual("region_pages", call["config"].fetch_mode)
        self.assertTrue(call["config"].ignore_existing_freshness)
        self.assertEqual("history_ready", result["payload"]["target_universe"])
        self.assertEqual(2, result["payload"]["selected_target_count"])
        self.assertEqual(410, result["payload"]["orders_fetch"]["pages_fetched"])
        self.assertTrue((result["run_dir"] / "region_order_mirror.json").exists())

    def test_region_order_mirror_worker_skips_within_refresh_interval(self) -> None:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self.state_path.write_text(
            json.dumps(
                {
                    "worker_run_id": "region-order-mirror-worker-old",
                    "status": "completed",
                    "finished_at_utc": "2099-01-01T00:00:00Z",
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

        result = run_region_order_mirror_worker_once(
            db_path=self.db_path,
            output_root=self.output_root,
            lock_path=self.lock_path,
            state_path=self.state_path,
            region_id=10000002,
            target_universe="history_ready",
            refresh_interval_minutes=30,
            orders_config=self._orders_config(),
            limit=None,
        )

        self.assertEqual("skipped_fresh", result["status"])
        self.assertIn("next_allowed_at_utc", result)

    def test_region_order_mirror_worker_state_file_serializes_datetime_payloads(self) -> None:
        def fake_run_region_order_mirror_once(*args, **kwargs):
            return {
                "mirror_run_id": "region-order-mirror-serializable",
                "run_dir": self.output_root / "region-order-mirror-serializable",
                "payload": {
                    "mirror_run_id": "region-order-mirror-serializable",
                    "started_at_utc": "2026-04-07T12:00:00Z",
                },
                "orders_result": {
                    "fetch_run": {
                        "run_id": "orders-fetch-serializable",
                        "started_at": datetime(2026, 4, 7, 12, 0, 0, tzinfo=timezone.utc),
                        "finished_at": datetime(2026, 4, 7, 12, 5, 0, tzinfo=timezone.utc),
                        "pages_fetched": 10,
                        "type_count_success": 5,
                    }
                },
            }

        with patch(
            "eveqtrading.region_order_mirror_worker.run_region_order_mirror_once",
            side_effect=fake_run_region_order_mirror_once,
        ):
            result = run_region_order_mirror_worker_once(
                db_path=self.db_path,
                output_root=self.output_root,
                lock_path=self.lock_path,
                state_path=self.state_path,
                region_id=10000002,
                target_universe="history_ready",
                refresh_interval_minutes=30,
                orders_config=self._orders_config(),
                limit=None,
            )

        self.assertEqual("completed", result["status"])
        state_payload = json.loads(self.state_path.read_text(encoding="utf-8"))
        self.assertEqual("completed", state_payload["status"])
        self.assertEqual("2026-04-07 12:00:00+00:00", state_payload["orders_fetch"]["started_at"])


if __name__ == "__main__":
    unittest.main()
