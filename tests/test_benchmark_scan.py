from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from eveqtrading.benchmark import run_scan_benchmark
from eveqtrading.db import connect, init_db


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT / "sql" / "schema.sql"


class BenchmarkScanTests(unittest.TestCase):
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
                    (5001, "Covered Item", "Minerals", 1),
                    (5002, "Missing Item", "Minerals", 1),
                ],
            )
            connection.execute(
                """
                INSERT INTO market_location_catalog (
                    location_id, location_name, location_kind, region_id, solar_system_id, is_npc, is_trade_hub_candidate
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (61000001, "Covered Station", "station", 10000002, 30000001, 1, 1),
            )
            for day_index in range(30):
                connection.execute(
                    """
                    INSERT INTO market_history_daily (
                        type_id, region_id, market_date, average_price, highest_price, lowest_price,
                        volume, order_count, fetched_at, source, coverage_status
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        5001,
                        10000002,
                        f"2026-03-{day_index + 1:02d}",
                        100.0 + (day_index % 2),
                        102.0,
                        99.0,
                        100_000.0,
                        100,
                        "2026-04-06T00:00:00Z",
                        "test",
                        "ok",
                    ),
                )

            connection.execute(
                """
                INSERT INTO market_order_snapshot_head (
                    snapshot_id, captured_at, scope_kind, region_id, location_id, account_id, source, refresh_mode
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ("benchmark-snapshot-1", "2026-04-06T00:01:00Z", "region-type", 10000002, None, None, "test", "full"),
            )
            connection.executemany(
                """
                INSERT INTO market_order_snapshot_line (
                    snapshot_id, order_id, type_id, is_buy_order, price, volume_remain, volume_total, min_volume,
                    order_range, issued_at, duration_days, location_id, region_id, account_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    ("benchmark-snapshot-1", 1, 5001, 1, 100.0, 10000.0, 10000.0, None, "station", "2026-04-06T00:00:00Z", 90, 61000001, 10000002, None),
                    ("benchmark-snapshot-1", 2, 5001, 0, 101.5, 10000.0, 10000.0, None, "station", "2026-04-06T00:00:00Z", 90, 61000001, 10000002, None),
                ],
            )
            connection.execute(
                """
                INSERT INTO sync_scope_state (
                    scope_key, scope_type, region_id, location_id, type_id, last_success_at,
                    last_attempt_at, next_scheduled_at, failure_count, last_error, coverage_marker
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "market-orders-5001",
                    "market_orders",
                    10000002,
                    None,
                    5001,
                    "2026-04-06T00:01:00Z",
                    "2026-04-06T00:01:00Z",
                    None,
                    0,
                    None,
                    "ok:benchmark-snapshot-1",
                ),
            )
            connection.commit()

    def test_benchmark_scan_reports_coverage_and_writes_artifact(self) -> None:
        connection = connect(self.db_path)
        try:
            benchmark = run_scan_benchmark(
                connection,
                db_path=self.db_path,
                output_root=self.output_root,
                region_id=10000002,
                explicit_type_ids=[],
                type_id_file=None,
                ready_market_scan_types=False,
                all_published_market_types=True,
                limit=None,
                chunk_size=1,
                max_chunks=None,
                capital_budget_isk=1_000_000_000.0,
                test_position_isk=250_000_000.0,
                top_n=10,
                strategy_set="v1-db-region",
                parameter_version="v1-db",
            )
        finally:
            connection.close()

        self.assertEqual(2, benchmark["target_selection"]["selected_target_count"])
        self.assertEqual(2, benchmark["target_selection"]["executed_chunk_count"])
        self.assertEqual(1, benchmark["coverage_snapshot"]["types_with_order_snapshot_ok"])
        self.assertEqual(1, benchmark["coverage_snapshot"]["types_with_history_30d"])
        self.assertEqual(1, benchmark["coverage_snapshot"]["types_with_both_order_and_history_30d"])
        self.assertEqual(1, benchmark["aggregate_results"]["candidate_count_built"])
        self.assertEqual(1, benchmark["aggregate_results"]["skipped_count"])
        self.assertEqual("missing_history", benchmark["aggregate_results"]["skipped_reason_counts"][0]["reason"])
        self.assertTrue((Path(benchmark["run_dir"]) / "benchmark_scan.json").exists())

    def test_benchmark_scan_ready_market_types_only_targets_covered_universe(self) -> None:
        connection = connect(self.db_path)
        try:
            benchmark = run_scan_benchmark(
                connection,
                db_path=self.db_path,
                output_root=self.output_root,
                region_id=10000002,
                explicit_type_ids=[],
                type_id_file=None,
                ready_market_scan_types=True,
                all_published_market_types=False,
                limit=None,
                chunk_size=1000,
                max_chunks=None,
                capital_budget_isk=1_000_000_000.0,
                test_position_isk=250_000_000.0,
                top_n=10,
                strategy_set="v1-db-region",
                parameter_version="v1-db",
            )
        finally:
            connection.close()

        self.assertEqual("ready-market-scan-types", benchmark["target_selection"]["selection_mode"])
        self.assertEqual(1, benchmark["target_selection"]["selected_target_count"])
        self.assertEqual(1, benchmark["target_selection"]["executed_chunk_count"])
        self.assertEqual(1, benchmark["coverage_snapshot"]["types_with_both_order_and_history_30d"])
        self.assertEqual(0, benchmark["aggregate_results"]["skipped_count"])


if __name__ == "__main__":
    unittest.main()
