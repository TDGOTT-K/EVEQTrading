from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from eveqtrading.coverage import plan_market_coverage
from eveqtrading.db import connect, init_db


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT / "sql" / "schema.sql"


class CoveragePlanTests(unittest.TestCase):
    def setUp(self) -> None:
        self._temp_dir = tempfile.TemporaryDirectory()
        self.base_dir = Path(self._temp_dir.name)
        self.db_path = self.base_dir / "state" / "eveq.db"
        self.output_root = self.base_dir / "runs"
        self.output_root.mkdir(parents=True, exist_ok=True)
        init_db(self.db_path, SCHEMA_PATH)
        self._seed_data()

    def tearDown(self) -> None:
        self._temp_dir.cleanup()

    def _seed_data(self) -> None:
        with connect(self.db_path) as connection:
            connection.executemany(
                "INSERT INTO item_catalog (type_id, type_name, market_group, is_published) VALUES (?, ?, ?, ?)",
                [
                    (7001, "Ready Item", "Minerals", 1),
                    (7002, "Needs Orders", "Minerals", 1),
                    (7003, "Needs History", "Ships", 1),
                    (7004, "Needs Both", "Modules", 1),
                ],
            )
            for day_index in range(30):
                for type_id in (7001, 7002):
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
                            10_000.0,
                            100,
                            "2026-04-06T00:00:00Z",
                            "test",
                            "ok",
                        ),
                    )
            connection.executemany(
                """
                INSERT INTO sync_scope_state (
                    scope_key, scope_type, region_id, location_id, type_id, last_success_at,
                    last_attempt_at, next_scheduled_at, failure_count, last_error, coverage_marker
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        "market-orders-7001",
                        "market_orders",
                        10000002,
                        None,
                        7001,
                        "2026-04-06T00:01:00Z",
                        "2026-04-06T00:01:00Z",
                        None,
                        0,
                        None,
                        "ok:snapshot-7001",
                    ),
                    (
                        "market-orders-7003",
                        "market_orders",
                        10000002,
                        None,
                        7003,
                        "2026-04-06T00:02:00Z",
                        "2026-04-06T00:02:00Z",
                        None,
                        0,
                        None,
                        "ok:snapshot-7003",
                    ),
                ],
            )
            connection.commit()

    def test_coverage_plan_partitions_market_universe(self) -> None:
        connection = connect(self.db_path)
        try:
            coverage_plan = plan_market_coverage(
                connection,
                output_root=self.output_root,
                region_id=10000002,
                chunk_size=2,
                limit=None,
            )
        finally:
            connection.close()

        counts = coverage_plan["target_counts"]
        self.assertEqual(4, counts["market_type_count"])
        self.assertEqual(1, counts["ready_count"])
        self.assertEqual(2, counts["missing_history_count"])
        self.assertEqual(1, counts["missing_orders_count"])
        self.assertEqual(1, counts["missing_both_count"])

        sample_rows = coverage_plan["sample_rows"]
        self.assertEqual(7001, sample_rows["ready"][0]["type_id"])
        self.assertEqual(7002, sample_rows["missing_orders"][0]["type_id"])
        self.assertEqual(7003, sample_rows["missing_history"][0]["type_id"])
        self.assertEqual(7004, sample_rows["missing_both"][0]["type_id"])

        files = coverage_plan["files"]
        ready_file = Path(files["ready_type_ids_path"])
        missing_history_file = Path(files["missing_history_type_ids_path"])
        missing_orders_file = Path(files["missing_orders_type_ids_path"])
        self.assertEqual("7001\n", ready_file.read_text(encoding="utf-8"))
        self.assertEqual("7003\n7004\n", missing_history_file.read_text(encoding="utf-8"))
        self.assertEqual("7002\n", missing_orders_file.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
