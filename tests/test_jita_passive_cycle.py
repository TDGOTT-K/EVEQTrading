from __future__ import annotations

import io
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from eveqtrading.cli import main as cli_main
from eveqtrading.db import connect, init_db
from eveqtrading.jita_passive_cycle import run_jita_passive_cycle
from eveqtrading.order_fetch import OrderFetchConfig


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT / "sql" / "schema.sql"


class JitaPassiveCycleTests(unittest.TestCase):
    def setUp(self) -> None:
        self._temp_dir = tempfile.TemporaryDirectory()
        self.base_dir = Path(self._temp_dir.name)
        self.db_path = self.base_dir / "state" / "eveq.db"
        self.runs_root = self.base_dir / "runs"
        self.runs_root.mkdir(parents=True, exist_ok=True)
        init_db(self.db_path, SCHEMA_PATH)
        self._seed_data()

    def tearDown(self) -> None:
        self._temp_dir.cleanup()

    def _orders_config(self) -> OrderFetchConfig:
        return OrderFetchConfig(
            region_id=10000002,
            datasource="tranquility",
            esi_base_url="https://esi.evetech.net/latest",
            user_agent="test-jita-passive-cycle-orders",
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
            market_day = day_index + 1
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
                    f"2026-03-{market_day:02d}",
                    100.0 + market_day,
                    101.0 + market_day,
                    99.0 + market_day,
                    1000.0 + market_day,
                    100 + market_day,
                    "2026-04-06T00:00:00Z",
                    "seed",
                    "ok",
                ),
            )

    def _seed_data(self) -> None:
        connection = connect(self.db_path)
        try:
            self._seed_history(connection=connection, type_id=101, type_name="History Ready Stale", day_count=30)
            self._seed_history(connection=connection, type_id=102, type_name="History Ready Missing Orders", day_count=31)
            self._seed_history(connection=connection, type_id=103, type_name="Too Short", day_count=10)
            connection.execute(
                """
                INSERT INTO sync_scope_state (
                    scope_key, scope_type, region_id, type_id, last_success_at, last_attempt_at,
                    next_scheduled_at, failure_count, last_error, coverage_marker
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "market_orders:10000002:101:all",
                    "market_orders",
                    10000002,
                    101,
                    "2026-04-07T10:00:00Z",
                    "2026-04-07T10:00:00Z",
                    "2026-04-07T11:00:00Z",
                    0,
                    None,
                    "ok:snapshot-stale-101",
                ),
            )
            connection.commit()
        finally:
            connection.close()

    def test_cycle_prioritizes_missing_orders_before_stale_orders(self) -> None:
        fake_orders_result = {
            "batch_run_id": "orders-batch-test",
            "run_dir": self.runs_root / "orders-batch-test",
            "payload": {"totals": {"executed_target_count": 1}},
        }
        fake_orders_result["run_dir"].mkdir(parents=True, exist_ok=True)
        fake_scan_result = {
            "run_id": "jita-passive-simple-test",
            "run_dir": self.runs_root / "jita-passive-simple-test",
            "analysis_run": {"candidate_count_total": 1},
            "selected_candidates": [{"type_id": 101}],
            "skipped_rows": [],
        }
        fake_scan_result["run_dir"].mkdir(parents=True, exist_ok=True)

        connection = connect(self.db_path)
        try:
            with patch(
                "eveqtrading.order_refresh._utc_now",
                return_value=datetime(2026, 4, 7, 12, 0, tzinfo=timezone.utc),
            ):
                with patch(
                    "eveqtrading.jita_passive_cycle.run_order_batch_fetch",
                    return_value=fake_orders_result,
                ) as mocked_orders:
                    with patch(
                        "eveqtrading.jita_passive_cycle.run_jita_passive_simple_scan",
                        return_value=fake_scan_result,
                    ) as mocked_scan:
                        result = run_jita_passive_cycle(
                            connection,
                            db_path=self.db_path,
                            output_root=self.runs_root,
                            region_id=10000002,
                            orders_chunk_size=1,
                            max_orders_chunks=1,
                            limit=None,
                            orders_cache_profile="warm",
                            orders_config=self._orders_config(),
                            capital_budget_isk=1_000_000_000.0,
                            top_n=20,
                            strategy_set="jita-passive-simple",
                            parameter_version="v1-jita-passive-simple",
                        )
        finally:
            connection.close()

        orders_targets = mocked_orders.call_args.kwargs["type_targets"]
        self.assertEqual([102, 101], [int(row["type_id"]) for row in orders_targets])
        self.assertEqual(
            [102],
            mocked_scan.call_args.kwargs["explicit_type_ids"],
        )
        payload = result["payload"]
        self.assertEqual(2, payload["selected_priority_target_count"])
        self.assertEqual(1, payload["executed_orders_target_count"])
        self.assertEqual("warm", payload["config"]["orders_cache_profile"])
        self.assertEqual(1, payload["order_refresh_target_counts"]["missing_orders_count"])
        self.assertEqual(1, payload["order_refresh_target_counts"]["refresh_due_remote_count"])
        self.assertEqual("orders-batch-test", payload["orders_batch_run_id"])
        self.assertEqual("jita-passive-simple-test", payload["scan_run_id"])
        self.assertEqual(1, payload["scan_summary"]["candidate_count_selected"])
        self.assertTrue((result["run_dir"] / "jita_passive_cycle.json").exists())

    def test_cli_run_jita_passive_cycle_prints_structured_payload(self) -> None:
        fake_cycle_result = {
            "cycle_run_id": "jita-passive-cycle-test",
            "run_dir": self.runs_root / "jita-passive-cycle-test",
            "payload": {
                "selected_priority_target_count": 2,
                "executed_orders_target_count": 2,
                "scan_summary": {
                    "candidate_count_built": 2,
                    "candidate_count_selected": 1,
                    "skipped_count": 0,
                },
            },
        }
        stdout = io.StringIO()
        argv = [
            "eveq",
            "run-jita-passive-cycle",
            "--db-path",
            str(self.db_path),
            "--output-root",
            str(self.runs_root),
            "--region-id",
            "10000002",
            "--orders-chunk-size",
            "50",
            "--orders-cache-profile",
            "warm",
        ]
        with patch("eveqtrading.cli.run_jita_passive_cycle", return_value=fake_cycle_result):
            with patch.object(sys, "argv", argv):
                with redirect_stdout(stdout):
                    exit_code = cli_main()

        output_lines = stdout.getvalue().strip().splitlines()
        self.assertEqual(0, exit_code)
        self.assertTrue(output_lines[0].startswith("Jita passive cycle completed:"))
        payload = json.loads("\n".join(output_lines[2:]))
        self.assertEqual(2, payload["selected_priority_target_count"])
        self.assertEqual(1, payload["scan_summary"]["candidate_count_selected"])


if __name__ == "__main__":
    unittest.main()
