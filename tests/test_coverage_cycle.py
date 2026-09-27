from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from eveqtrading.coverage_cycle import run_coverage_cycle
from eveqtrading.db import connect, init_db
from eveqtrading.history_fetch import HistoryFetchConfig
from eveqtrading.order_fetch import OrderFetchConfig


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT / "sql" / "schema.sql"


def _build_history_entries() -> list[dict[str, object]]:
    return [
        {
            "date": f"2026-03-{day_index + 1:02d}",
            "average": 100.0 + day_index,
            "highest": 101.0 + day_index,
            "lowest": 99.0 + day_index,
            "volume": 10_000.0,
            "order_count": 100,
        }
        for day_index in range(30)
    ]


def _build_order_entries(*, type_id: int) -> list[dict[str, object]]:
    return [
        {
            "order_id": type_id * 1000 + 1,
            "type_id": type_id,
            "is_buy_order": True,
            "price": 98.0,
            "volume_remain": 1000.0,
            "volume_total": 1000.0,
            "min_volume": 1.0,
            "range": "region",
            "issued": "2026-04-06T00:00:00Z",
            "duration": 90,
            "location_id": 60003760,
        },
        {
            "order_id": type_id * 1000 + 2,
            "type_id": type_id,
            "is_buy_order": False,
            "price": 101.0,
            "volume_remain": 500.0,
            "volume_total": 500.0,
            "min_volume": 1.0,
            "range": "region",
            "issued": "2026-04-06T00:00:00Z",
            "duration": 90,
            "location_id": 60003760,
        },
    ]


class CoverageCycleTests(unittest.TestCase):
    def setUp(self) -> None:
        self._temp_dir = tempfile.TemporaryDirectory()
        self.base_dir = Path(self._temp_dir.name)
        self.db_path = self.base_dir / "state" / "eveq.db"
        self.output_root = self.base_dir / "runs"
        self.output_root.mkdir(parents=True, exist_ok=True)
        init_db(self.db_path, SCHEMA_PATH)
        self._seed_catalog()

    def tearDown(self) -> None:
        self._temp_dir.cleanup()

    def _seed_catalog(self) -> None:
        with connect(self.db_path) as connection:
            connection.execute(
                "INSERT INTO item_catalog (type_id, type_name, market_group, is_published) VALUES (?, ?, ?, ?)",
                (8301, "Cycle Target", "Minerals", 1),
            )
            connection.execute(
                """
                INSERT INTO market_location_catalog (
                    location_id, location_name, location_kind, region_id, solar_system_id, is_npc, is_trade_hub_candidate
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (60003760, "Jita 4-4", "station", 10000002, 30000142, 1, 1),
            )
            connection.commit()

    def test_coverage_cycle_expands_ready_universe_and_runs_benchmark(self) -> None:
        def fake_request_history(**kwargs):
            return {
                "ok": True,
                "status_code": 200,
                "url": "https://example.invalid/history/8301",
                "retries_used": 0,
                "headers": {},
                "entries": _build_history_entries(),
                "error": None,
            }

        def fake_request_all_orders_for_type(**kwargs):
            type_id = int(kwargs["type_id"])
            return {
                "ok": True,
                "status_code": 200,
                "entries": _build_order_entries(type_id=type_id),
                "page_traces": [],
                "error": None,
                "page_count": 1,
                "retries_used": 0,
            }

        with patch("eveqtrading.history_fetch._request_history", side_effect=fake_request_history):
            with patch(
                "eveqtrading.order_fetch._request_all_orders_for_type",
                side_effect=fake_request_all_orders_for_type,
            ):
                with connect(self.db_path) as connection:
                    result = run_coverage_cycle(
                        connection,
                        db_path=self.db_path,
                        output_root=self.output_root,
                        region_id=10000002,
                        history_chunk_size=1,
                        max_history_chunks=1,
                        orders_chunk_size=1,
                        max_orders_chunks=1,
                        history_config=HistoryFetchConfig(
                            region_id=10000002,
                            datasource="tranquility",
                            esi_base_url="https://esi.evetech.net/latest",
                            user_agent="test-coverage-cycle-history",
                            request_timeout_seconds=5.0,
                            max_retries=0,
                            pause_seconds=0.0,
                        ),
                        orders_config=OrderFetchConfig(
                            region_id=10000002,
                            datasource="tranquility",
                            esi_base_url="https://esi.evetech.net/latest",
                            user_agent="test-coverage-cycle-orders",
                            request_timeout_seconds=5.0,
                            max_retries=0,
                            pause_seconds=0.0,
                            order_type="all",
                        ),
                        benchmark_chunk_size=1000,
                        benchmark_max_chunks=None,
                        capital_budget_isk=1_000_000_000.0,
                        test_position_isk=250_000_000.0,
                        top_n=10,
                        strategy_set="v1-db-region",
                        parameter_version="v1-db",
                    )

        payload = result["payload"]
        self.assertEqual(0, payload["before_counts"]["ready_count"])
        self.assertEqual(0, payload["after_history_counts"]["ready_count"])
        self.assertEqual(1, payload["after_orders_counts"]["ready_count"])
        self.assertEqual([8301], payload["newly_history_ready_type_ids"])
        self.assertEqual(1, payload["history_target_count"])
        self.assertEqual(1, payload["orders_target_count"])
        self.assertIsNotNone(payload["history_batch_run_id"])
        self.assertIsNotNone(payload["orders_batch_run_id"])
        self.assertEqual(1, payload["benchmark_summary"]["selected_target_count"])
        self.assertEqual(1, payload["benchmark_summary"]["types_with_both_order_and_history_30d"])
        self.assertIsNotNone(payload["scan_run_id"])
        self.assertIsNotNone(payload["started_at_utc"])
        self.assertIsNotNone(payload["finished_at_utc"])
        self.assertGreaterEqual(payload["duration_seconds"], 0.0)
        self.assertGreaterEqual(payload["phase_durations"]["history_batch_seconds"], 0.0)
        self.assertGreaterEqual(payload["phase_durations"]["orders_batch_seconds"], 0.0)
        self.assertGreaterEqual(payload["phase_durations"]["benchmark_seconds"], 0.0)
        self.assertGreaterEqual(payload["phase_durations"]["scan_seconds"], 0.0)
        self.assertGreaterEqual(payload["phase_durations"]["diagnosis_seconds"], 0.0)
        self.assertGreaterEqual(payload["scan_summary"]["candidate_count_built"], 1)
        self.assertEqual(0, payload["diagnosis_summary"]["selected_count"])
        self.assertTrue((result["run_dir"] / "coverage_cycle.json").exists())
        self.assertTrue((result["run_dir"] / "scan_diagnosis.json").exists())
        self.assertEqual("8301\n", (result["run_dir"] / "history_target_ids.txt").read_text(encoding="utf-8"))
        self.assertEqual("8301\n", (result["run_dir"] / "orders_target_ids.txt").read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
