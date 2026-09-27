from __future__ import annotations

import io
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from eveqtrading.cli import main as cli_main
from eveqtrading.db import connect, init_db
from eveqtrading.jita_passive_cache_cycle import run_jita_passive_cache_cycle
from eveqtrading.order_fetch import OrderFetchConfig


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT / "sql" / "schema.sql"


class JitaPassiveCacheCycleTests(unittest.TestCase):
    def setUp(self) -> None:
        self._temp_dir = tempfile.TemporaryDirectory()
        self.base_dir = Path(self._temp_dir.name)
        self.db_path = self.base_dir / "state" / "eveq.db"
        self.runs_root = self.base_dir / "runs"
        self.runs_root.mkdir(parents=True, exist_ok=True)
        init_db(self.db_path, SCHEMA_PATH)

    def tearDown(self) -> None:
        self._temp_dir.cleanup()

    def _orders_config(self) -> OrderFetchConfig:
        return OrderFetchConfig(
            region_id=10000002,
            datasource="tranquility",
            esi_base_url="https://esi.evetech.net/latest",
            user_agent="test-jita-passive-cache-cycle-orders",
            request_timeout_seconds=5.0,
            max_retries=0,
            pause_seconds=0.0,
            order_type="all",
        )

    def test_cache_cycle_refreshes_then_scans_whole_cache_hit_pool(self) -> None:
        fake_before_snapshot = {
            "target_counts": {
                "missing_orders_count": 2,
                "refresh_due_remote_count": 1,
                "cache_hit_count": 5,
            }
        }
        fake_after_snapshot = {
            "target_counts": {
                "missing_orders_count": 1,
                "refresh_due_remote_count": 0,
                "cache_hit_count": 9,
            }
        }
        fake_refresh_result = {
            "cycle_run_id": "order-refresh-cycle-test",
            "run_dir": self.runs_root / "order-refresh-cycle-test",
            "payload": {
                "selected_remote_target_count": 3,
                "executed_remote_target_count": 2,
                "cache_hit_gain": 4,
            },
        }
        fake_refresh_result["run_dir"].mkdir(parents=True, exist_ok=True)
        fake_scan_result = {
            "run_id": "jita-passive-simple-test",
            "run_dir": self.runs_root / "jita-passive-simple-test",
            "analysis_run": {"candidate_count_total": 9},
            "selected_candidates": [
                {
                    "type_id": 3001,
                    "type_name": "Alpha Item",
                    "profit_margin": 0.1234,
                    "daily_capacity_isk_30d": 300_000_000.0,
                    "expected_turnover_days": 2.5,
                    "risk_adjusted_return": 0.08,
                    "source_order_captured_at": "2026-04-07T12:00:00Z",
                    "decision_reason": "alpha reason",
                },
                {
                    "type_id": 3002,
                    "type_name": "Beta Item",
                    "profit_margin": 0.0833,
                    "daily_capacity_isk_30d": 180_000_000.0,
                    "expected_turnover_days": 4.0,
                    "risk_adjusted_return": 0.05,
                    "source_order_captured_at": "2026-04-07T12:00:00Z",
                    "decision_reason": "beta reason",
                },
            ],
            "skipped_rows": [{"type_id": 3999}],
            "cache_selection": {
                "enabled": True,
                "profile_name": "warm",
                "selected_target_count": 9,
                "cache_hit_count": 9,
            },
        }
        fake_scan_result["run_dir"].mkdir(parents=True, exist_ok=True)

        connection = connect(self.db_path)
        try:
            with patch(
                "eveqtrading.jita_passive_cache_cycle.collect_order_refresh_snapshot",
                side_effect=[fake_before_snapshot, fake_after_snapshot],
            ):
                with patch(
                    "eveqtrading.jita_passive_cache_cycle.run_order_refresh_cycle",
                    return_value=fake_refresh_result,
                ) as mocked_refresh:
                    with patch(
                        "eveqtrading.jita_passive_cache_cycle.run_jita_passive_simple_scan",
                        return_value=fake_scan_result,
                    ) as mocked_scan:
                        result = run_jita_passive_cache_cycle(
                            connection,
                            db_path=self.db_path,
                            output_root=self.runs_root,
                            region_id=10000002,
                            orders_chunk_size=50,
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

        self.assertEqual("warm", mocked_refresh.call_args.kwargs["profile_name"])
        self.assertTrue(mocked_scan.call_args.kwargs["cache_only"])
        self.assertEqual("warm", mocked_scan.call_args.kwargs["orders_cache_profile"])
        self.assertEqual([], mocked_scan.call_args.kwargs["explicit_type_ids"])
        payload = result["payload"]
        self.assertEqual("order-refresh-cycle-test", payload["order_refresh_cycle_run_id"])
        self.assertEqual("jita-passive-simple-test", payload["scan_run_id"])
        self.assertEqual(9, payload["scan_summary"]["candidate_count_built"])
        self.assertEqual(2, payload["scan_summary"]["candidate_count_selected"])
        self.assertEqual(9, payload["after_order_cache_counts"]["cache_hit_count"])
        self.assertEqual("Alpha Item", payload["top_candidates_preview"][0]["type_name"])
        self.assertTrue(payload["files"]["scan_calc_trace_path"].endswith("calc_trace.json"))
        self.assertTrue((result["run_dir"] / "jita_passive_cache_cycle.json").exists())

    def test_cli_run_jita_passive_cache_cycle_prints_structured_payload(self) -> None:
        fake_cycle_result = {
            "cycle_run_id": "jita-passive-cache-cycle-test",
            "run_dir": self.runs_root / "jita-passive-cache-cycle-test",
            "payload": {
                "order_refresh_cycle_run_id": "order-refresh-cycle-test",
                "scan_run_id": "jita-passive-simple-test",
                "scan_summary": {
                    "candidate_count_built": 9,
                    "candidate_count_selected": 2,
                    "skipped_count": 1,
                    "cache_selection": {"profile_name": "warm"},
                },
                "top_candidates_preview": [
                    {
                        "type_id": 3001,
                        "type_name": "Alpha Item",
                    }
                ],
            },
        }
        stdout = io.StringIO()
        argv = [
            "eveq",
            "run-jita-passive-cache-cycle",
            "--db-path",
            str(self.db_path),
            "--output-root",
            str(self.runs_root),
            "--region-id",
            "10000002",
            "--orders-cache-profile",
            "warm",
        ]
        with patch("eveqtrading.cli.run_jita_passive_cache_cycle", return_value=fake_cycle_result):
            with patch.object(sys, "argv", argv):
                with redirect_stdout(stdout):
                    exit_code = cli_main()

        output_lines = stdout.getvalue().strip().splitlines()
        self.assertEqual(0, exit_code)
        self.assertTrue(output_lines[0].startswith("Jita passive cache cycle completed:"))
        payload = json.loads("\n".join(output_lines[2:]))
        self.assertEqual("order-refresh-cycle-test", payload["order_refresh_cycle_run_id"])
        self.assertEqual(2, payload["scan_summary"]["candidate_count_selected"])
        self.assertEqual("Alpha Item", payload["top_candidates_preview"][0]["type_name"])


if __name__ == "__main__":
    unittest.main()
