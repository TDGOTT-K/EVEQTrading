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
from eveqtrading.db import connect, init_db, record_data_fetch_run
from eveqtrading.jita_passive_cache_diagnose import build_jita_passive_cache_diagnosis


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT / "sql" / "schema.sql"


class JitaPassiveCacheDiagnoseTests(unittest.TestCase):
    def setUp(self) -> None:
        self._temp_dir = tempfile.TemporaryDirectory()
        self.base_dir = Path(self._temp_dir.name)
        self.db_path = self.base_dir / "state" / "eveq.db"
        self.runs_root = self.base_dir / "runs"
        self.log_dir = self.base_dir / "logs"
        self.runs_root.mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        init_db(self.db_path, SCHEMA_PATH)

    def tearDown(self) -> None:
        self._temp_dir.cleanup()

    def _seed_scan(self, *, connection, scan_run_id: str, selected_candidate_count: int) -> None:
        run_dir = self.runs_root / scan_run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        connection.execute(
            """
            INSERT INTO scan_run (
                scan_run_id, started_at, finished_at, market_scope, strategy_set,
                capital_budget_isk, parameter_version, input_path, output_dir,
                selected_candidate_count, status
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                scan_run_id,
                "2026-04-07T12:00:00Z",
                "2026-04-07T12:05:00Z",
                "region:10000002",
                "jita-passive-simple",
                1_000_000_000.0,
                "v1-jita-passive-simple",
                str(self.base_dir / "inputs" / f"{scan_run_id}.json"),
                str(run_dir),
                selected_candidate_count,
                "completed",
            ),
        )
        if selected_candidate_count > 0:
            connection.execute(
                """
                INSERT INTO opportunity_snapshot (
                    scan_run_id, opportunity_id, strategy_type, type_id, type_name, region_id,
                    buy_location_id, sell_location_id, entry_mode, exit_mode, unit_profit,
                    total_profit, profit_margin, capital_required, tradable_units,
                    expected_turnover_days, risk_adjusted_return, decision_reason
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    scan_run_id,
                    "3001-10000002-station-flip",
                    "station-flip",
                    3001,
                    "Alpha Item",
                    10000002,
                    60003760,
                    60003760,
                    "buy",
                    "sell",
                    1500.0,
                    15_000_000.0,
                    0.052,
                    280_000_000.0,
                    10_000.0,
                    1.5,
                    0.11,
                    "stable two-sided turnover and margin",
                ),
            )

        (run_dir / "analysis_run.json").write_text(
            json.dumps({"scan_run_id": scan_run_id, "strategy_set": "jita-passive-simple"}, ensure_ascii=False),
            encoding="utf-8",
        )
        (run_dir / "candidate_table.json").write_text(
            json.dumps(
                [
                    {
                        "opportunity_id": "3001-10000002-station-flip",
                        "buy_location_name": "Jita IV - Moon 4",
                        "sell_location_name": "Jita IV - Moon 4",
                    }
                ]
                if selected_candidate_count > 0
                else [],
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        (run_dir / "calc_trace.json").write_text(
            json.dumps(
                {
                    "candidate_traces": [
                        {
                            "type_id": 3001,
                            "type_name": "Alpha Item",
                            "strategy_pool": "station-flip",
                            "source_record": {
                                "opportunity_id": "3001-10000002-station-flip",
                                "type_id": 3001,
                                "source_order_snapshot_id": "orders-alpha",
                                "source_order_captured_at": "2026-04-07T12:04:00Z",
                            },
                            "derived": {
                                "screened_profit_margin": 0.052,
                                "estimated_exit_days_small_size": 1.5,
                            },
                            "filter_evaluation": {
                                "passed": selected_candidate_count > 0,
                                "rejection_reasons": [] if selected_candidate_count > 0 else ["profit margin below threshold"],
                            },
                        },
                        {
                            "type_id": 3002,
                            "type_name": "Beta Item",
                            "strategy_pool": "station-flip",
                            "source_record": {
                                "opportunity_id": "3002-10000002-station-flip",
                                "type_id": 3002,
                            },
                            "derived": {
                                "screened_profit_margin": 0.018,
                                "estimated_exit_days_small_size": 1.9,
                            },
                            "filter_evaluation": {
                                "passed": False,
                                "rejection_reasons": ["profit margin below threshold"],
                            },
                        },
                    ]
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        (run_dir / "summary.md").write_text("# summary\n", encoding="utf-8")
        (run_dir / "feature_table.json").write_text(
            json.dumps([{"type_id": 3001, "type_name": "Alpha Item"}], ensure_ascii=False),
            encoding="utf-8",
        )
        (run_dir / "skipped_table.json").write_text(
            json.dumps([{"type_id": 3999, "type_name": "Skipped Item", "reason": "missing_history"}], ensure_ascii=False),
            encoding="utf-8",
        )

    def _seed_orders_fetch_mode_artifacts(
        self,
        *,
        cycle_run_id: str,
        requested_fetch_mode: str = "auto",
        effective_fetch_mode: str = "per_type",
        decision_reason: str = "target_set_small_or_predicted_pages_low",
    ) -> None:
        refresh_run_dir = self.runs_root / f"refresh-{cycle_run_id}"
        batch_run_dir = self.runs_root / f"orders-batch-{cycle_run_id}"
        orders_run_dir = self.runs_root / f"orders-fetch-{cycle_run_id}"
        refresh_run_dir.mkdir(parents=True, exist_ok=True)
        batch_run_dir.mkdir(parents=True, exist_ok=True)
        orders_run_dir.mkdir(parents=True, exist_ok=True)
        (refresh_run_dir / "order_refresh_cycle.json").write_text(
            json.dumps(
                {
                    "cycle_run_id": f"refresh-{cycle_run_id}",
                    "orders_batch_run_id": f"orders-batch-{cycle_run_id}",
                    "orders_batch_run_dir": str(batch_run_dir),
                    "config": {"orders_fetch": {"fetch_mode": requested_fetch_mode}},
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        (batch_run_dir / "orders_batch_fetch.json").write_text(
            json.dumps(
                {
                    "batch_run_id": f"orders-batch-{cycle_run_id}",
                    "chunks": [
                        {
                            "chunk_index": 1,
                            "target_count": 5,
                            "type_ids": [3001, 3002, 3003, 3004, 3005],
                            "orders_run_id": f"orders-fetch-{cycle_run_id}",
                            "orders_run_dir": str(orders_run_dir),
                            "fetch_counts": {"success": 5, "pages_fetched": 5},
                        }
                    ],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        (orders_run_dir / "orders_fetch_run.json").write_text(
            json.dumps(
                {
                    "run_id": f"orders-fetch-{cycle_run_id}",
                    "config": {"fetch_mode": requested_fetch_mode},
                    "effective_fetch_mode": effective_fetch_mode,
                    "fetch_mode_decision": {
                        "requested_fetch_mode": requested_fetch_mode,
                        "effective_fetch_mode": effective_fetch_mode,
                        "reason": decision_reason,
                        "region_first_page_probe_used": requested_fetch_mode == "auto",
                        "predicted_per_type_pages": 5,
                        "region_total_pages": 12 if effective_fetch_mode == "per_type" else 3,
                    },
                    "type_count_requested": 5,
                    "type_count_success": 5,
                    "pages_fetched": 5,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        (orders_run_dir / "orders_fetch_trace.json").write_text(
            json.dumps(
                {
                    "run_id": f"orders-fetch-{cycle_run_id}",
                    "effective_fetch_mode": effective_fetch_mode,
                    "fetch_mode_decision": {
                        "requested_fetch_mode": requested_fetch_mode,
                        "effective_fetch_mode": effective_fetch_mode,
                        "reason": decision_reason,
                        "region_first_page_probe_used": requested_fetch_mode == "auto",
                        "predicted_per_type_pages": 5,
                        "region_total_pages": 12 if effective_fetch_mode == "per_type" else 3,
                    },
                    "shared_remote_trace": (
                        {"request_scope": "region_pages"}
                        if effective_fetch_mode == "region_pages"
                        else None
                    ),
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

    def _seed_cycle(
        self,
        *,
        connection,
        cycle_run_id: str,
        started_at: str,
        scan_run_id: str,
        candidate_count_selected: int,
    ) -> None:
        run_dir = self.runs_root / cycle_run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        refresh_run_dir = self.runs_root / f"refresh-{cycle_run_id}"
        batch_run_dir = self.runs_root / f"orders-batch-{cycle_run_id}"
        (run_dir / "summary.md").write_text("# cache cycle\n", encoding="utf-8")
        payload = {
            "cycle_run_id": cycle_run_id,
            "started_at_utc": started_at,
            "finished_at_utc": "2026-04-07T12:10:00Z",
            "duration_seconds": 600.0,
            "region_id": 10000002,
            "config": {"orders_cache_profile": "warm"},
            "after_order_cache_counts": {"cache_hit_count": 42},
            "order_refresh_cycle_run_id": f"refresh-{cycle_run_id}",
            "order_refresh_cycle_run_dir": str(refresh_run_dir),
            "scan_run_id": scan_run_id,
            "scan_summary": {
                "candidate_count_built": 20,
                "candidate_count_selected": candidate_count_selected,
                "skipped_count": 1,
            },
            "order_refresh_summary": {
                "selected_remote_target_count": 5,
                "executed_remote_target_count": 5,
                "orders_batch_run_id": f"orders-batch-{cycle_run_id}",
                "orders_batch_run_dir": str(batch_run_dir),
                "config": {"orders_fetch": {"fetch_mode": "auto"}},
            },
            "top_candidates_preview": [{"type_id": 3001, "type_name": "Alpha Item"}],
            "files": {
                "order_refresh_cycle_summary_path": str(run_dir / "order-refresh-summary.md"),
                "scan_summary_path": str(self.runs_root / scan_run_id / "summary.md"),
                "scan_analysis_run_path": str(self.runs_root / scan_run_id / "analysis_run.json"),
                "scan_calc_trace_path": str(self.runs_root / scan_run_id / "calc_trace.json"),
                "scan_candidate_table_path": str(self.runs_root / scan_run_id / "candidate_table.json"),
                "scan_skipped_table_path": str(self.runs_root / scan_run_id / "skipped_table.json"),
            },
        }
        (run_dir / "jita_passive_cache_cycle.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        record_data_fetch_run(
            connection,
            fetch_run_id=cycle_run_id,
            fetch_kind="jita_passive_cache_cycle",
            source="jita-passive-cache-cycle",
            output_dir=run_dir,
            scope={"region_id": 10000002},
            details={"scan_summary": payload["scan_summary"]},
            started_at=started_at,
            finished_at="2026-04-07T12:10:00Z",
            status="completed",
        )

    def test_cache_diagnosis_uses_cycle_scan_run_id(self) -> None:
        with connect(self.db_path) as connection:
            self._seed_scan(connection=connection, scan_run_id="scan-alpha", selected_candidate_count=0)
            self._seed_orders_fetch_mode_artifacts(cycle_run_id="jita-passive-cache-cycle-a")
            self._seed_cycle(
                connection=connection,
                cycle_run_id="jita-passive-cache-cycle-a",
                started_at="2026-04-07T12:00:00Z",
                scan_run_id="scan-alpha",
                candidate_count_selected=0,
            )
            connection.commit()
            diagnosis = build_jita_passive_cache_diagnosis(
                connection,
                db_path=self.db_path,
                runs_root=self.runs_root,
                log_dir=self.log_dir,
                cycle_run_id=None,
                prefer_latest_selected=False,
                top_rejection_reasons=5,
                top_near_passes=5,
                top_skipped_reasons=5,
            )

        self.assertEqual("jita-passive-cache-cycle-a", diagnosis["cache_cycle_summary"]["cycle_run_id"])
        self.assertEqual("scan-alpha", diagnosis["scan_summary"]["scan_run_id"])
        self.assertEqual(2, diagnosis["diagnosis_summary"]["candidate_count_total"])
        self.assertEqual("profit margin below threshold", diagnosis["top_rejection_reasons"][0]["reason"])
        self.assertEqual("missing_history", diagnosis["skipped_summary"]["reason_counts"][0]["reason"])
        self.assertEqual("per_type", diagnosis["orders_fetch_mode_summary"]["dominant_effective_fetch_mode"])
        self.assertIn("small/light", diagnosis["orders_fetch_mode_summary"]["narrative"])

    def test_cli_diagnose_jita_passive_cache_prints_payload(self) -> None:
        fake_diagnosis = {
            "cache_cycle_summary": {"cycle_run_id": "jita-passive-cache-cycle-a"},
            "scan_summary": {"scan_run_id": "scan-alpha"},
            "diagnosis_summary": {"headline": "no candidates survived into the persisted selected set"},
        }
        stdout = io.StringIO()
        argv = [
            "eveq",
            "diagnose-jita-passive-cache",
            "--db-path",
            str(self.db_path),
        ]
        with patch("eveqtrading.cli.build_jita_passive_cache_diagnosis", return_value=fake_diagnosis):
            with patch.object(sys, "argv", argv):
                with redirect_stdout(stdout):
                    exit_code = cli_main()

        output_lines = stdout.getvalue().strip().splitlines()
        self.assertEqual(0, exit_code)
        self.assertTrue(output_lines[0].startswith("Jita passive cache diagnosis ready:"))
        payload = json.loads("\n".join(output_lines[1:]))
        self.assertEqual("scan-alpha", payload["scan_summary"]["scan_run_id"])
        self.assertEqual(
            "no candidates survived into the persisted selected set",
            payload["diagnosis_summary"]["headline"],
        )


if __name__ == "__main__":
    unittest.main()
