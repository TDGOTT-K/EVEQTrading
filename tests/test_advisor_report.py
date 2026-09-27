from __future__ import annotations

import io
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from eveqtrading.advisor import _advise_candidate
from eveqtrading.advisor import build_market_advice
from eveqtrading.cli import main as cli_main
from eveqtrading.db import connect, init_db
from eveqtrading.report import _build_worker_state_summary, build_coverage_diagnosis, build_market_report, build_scan_diagnosis


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT / "sql" / "schema.sql"


class AdvisorReportTests(unittest.TestCase):
    def setUp(self) -> None:
        self._temp_dir = tempfile.TemporaryDirectory()
        self.base_dir = Path(self._temp_dir.name)
        self.db_path = self.base_dir / "data" / "eveq.db"
        self.runs_root = self.base_dir / "runs"
        self.log_dir = self.base_dir / "logs"
        self.runs_root.mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        init_db(self.db_path, SCHEMA_PATH)

    def tearDown(self) -> None:
        self._temp_dir.cleanup()

    def test_advise_candidate_respects_scan_filter_rejection_reasons(self) -> None:
        advice = _advise_candidate(
            {
                "opportunity_id": "2777-10000002-jita-passive-simple",
                "type_id": 2777,
                "type_name": "加达里控制塔蓝图",
                "strategy_type": "jita-passive-simple",
                "profit_margin": 0.25,
                "capital_required": 100_000_000.0,
                "tradable_units": 10.0,
                "expected_turnover_days": 1.0,
                "risk_adjusted_return": 0.1,
                "avg_daily_isk_volume_30d": 2_000_000_000.0,
                "active_days_ratio_30d": 1.0,
                "volatility_30d": 0.10,
                "flip_units_cv_30d": 0.20,
                "flip_units_cv_7d": 0.20,
                "scan_filter_rejection_reasons": ["blueprint category excluded"],
            }
        )

        self.assertEqual("reject", advice["action"])
        self.assertIn("blueprint category excluded", advice["blockers"]["hard"])
        self.assertEqual(["blueprint category excluded"], advice["scan_filter_rejection_reasons"])

    def test_worker_state_summary_recognizes_region_order_mirror_worker(self) -> None:
        summary = _build_worker_state_summary(
            {
                "worker_run_id": "region-order-mirror-worker-1",
                "status": "completed",
                "mirror_run_id": "region-order-mirror-1",
                "target_universe": "history_ready",
                "refresh_interval_minutes": 30,
                "started_at_utc": "2026-04-07T06:17:28Z",
                "finished_at_utc": "2026-04-07T06:24:07Z",
                "stop_reason": "completed_single_mirror",
            }
        )

        self.assertIsNotNone(summary)
        self.assertEqual("region_order_mirror_worker", summary["worker_kind"])
        self.assertEqual("completed", summary["status"])
        self.assertEqual("region-order-mirror-worker-1", summary["worker_run_id"])

    def _insert_scan(
        self,
        *,
        scan_run_id: str,
        output_dir: str,
        selected_candidate_count: int,
        opportunity_id: str = "34-10000002-cross-location",
    ) -> None:
        with connect(self.db_path) as connection:
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
                    "2026-04-06T00:00:00Z",
                    "2026-04-06T00:05:00Z",
                    "region:10000002",
                    "v1-db-region",
                    1_000_000_000.0,
                    "v1-db",
                    str(self.base_dir / "inputs" / f"{scan_run_id}.json"),
                    output_dir,
                    selected_candidate_count,
                    "completed",
                ),
            )
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
                    opportunity_id,
                    "cross-location",
                    34,
                    "Tritanium",
                    10000002,
                    60003760,
                    60008494,
                    "buy",
                    "sell",
                    2.0,
                    10_000_000.0,
                    0.031,
                    300_000_000.0,
                    5_000_000.0,
                    1.2,
                    0.19,
                    "selected for follow-up",
                ),
            )
            connection.commit()

    def _write_complete_artifacts(self, scan_run_id: str) -> None:
        run_dir = self.runs_root / scan_run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "analysis_run.json").write_text(
            json.dumps({"scan_run_id": scan_run_id, "parameter_version": "v1-db"}, ensure_ascii=False),
            encoding="utf-8",
        )
        (run_dir / "candidate_table.json").write_text(
            json.dumps(
                [
                    {
                        "opportunity_id": "34-10000002-cross-location",
                        "buy_location_name": "Jita IV - Moon 4",
                        "sell_location_name": "Amarr VIII (Oris)",
                        "buy_reference_price": 4.0,
                        "sell_reference_price": 4.3,
                        "gross_spread_pct": 0.075,
                    }
                ],
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        (run_dir / "calc_trace.json").write_text(
            json.dumps(
                {
                    "candidate_traces": [
                        {
                            "type_id": 34,
                            "type_name": "Tritanium",
                            "strategy_pool": "cross-location",
                            "source_record": {
                                "opportunity_id": "34-10000002-cross-location",
                                "type_id": 34,
                                "avg_daily_isk_volume_30d": 180_000_000.0,
                                "avg_daily_units_7d": 250_000.0,
                                "active_days_ratio_30d": 0.96,
                                "cross_location_net_spread_pct": 0.044,
                                "volatility_30d": 0.11,
                                "source_order_snapshot_id": "orders-1",
                                "source_order_captured_at": "2026-04-06T00:04:00Z",
                            },
                            "derived": {
                                "net_spread_pct": 0.044,
                                "screened_profit_margin": 0.031,
                                "price_zscore_30d": 0.20,
                                "estimated_exit_days_small_size": 1.2,
                            },
                            "filter_evaluation": {
                                "passed": True,
                                "rejection_reasons": [],
                            },
                        }
                    ]
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        (run_dir / "summary.md").write_text("# summary\n", encoding="utf-8")
        (run_dir / "feature_table.json").write_text(
            json.dumps(
                [
                    {
                        "type_id": 34,
                        "type_name": "Tritanium",
                        "estimated_exit_days_small_size": 1.2,
                    }
                ],
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        (run_dir / "skipped_table.json").write_text(
            json.dumps(
                [
                    {
                        "type_id": 35,
                        "type_name": "Pyerite",
                        "reason": "missing_history",
                    }
                ],
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

    def _seed_coverage_rows(self) -> None:
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

    def _write_coverage_cycle(
        self,
        *,
        cycle_run_id: str,
        region_id: int,
        before_ready_count: int,
        after_ready_count: int,
        history_target_count: int,
        orders_target_count: int,
        scan_run_id: str,
        diagnosis_selected_count: int = 0,
        started_at_utc: str = "2026-04-06T00:00:00Z",
        finished_at_utc: str = "2026-04-06T00:05:00Z",
        duration_seconds: float = 300.0,
        phase_durations: dict[str, float] | None = None,
    ) -> None:
        coverage_cycle_dir = self.runs_root / cycle_run_id
        coverage_cycle_dir.mkdir(parents=True, exist_ok=True)
        effective_phase_durations = phase_durations or {
            "history_batch_seconds": 120.0,
            "orders_batch_seconds": 80.0,
            "benchmark_seconds": 40.0,
            "scan_seconds": 30.0,
            "diagnosis_seconds": 10.0,
        }
        (coverage_cycle_dir / "coverage_cycle.json").write_text(
            json.dumps(
                {
                    "cycle_run_id": cycle_run_id,
                    "started_at_utc": started_at_utc,
                    "finished_at_utc": finished_at_utc,
                    "duration_seconds": duration_seconds,
                    "phase_durations": effective_phase_durations,
                    "region_id": region_id,
                    "before_counts": {"ready_count": before_ready_count},
                    "after_orders_counts": {"ready_count": after_ready_count},
                    "history_target_count": history_target_count,
                    "orders_target_count": orders_target_count,
                    "scan_run_id": scan_run_id,
                    "diagnosis_summary": {"selected_count": diagnosis_selected_count},
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

    def test_report_marks_trace_complete_scan_as_advisory_ready(self) -> None:
        scan_run_id = "scan-complete"
        self._insert_scan(
            scan_run_id=scan_run_id,
            output_dir=str(self.runs_root / scan_run_id),
            selected_candidate_count=1,
        )
        self._write_complete_artifacts(scan_run_id)

        with connect(self.db_path) as connection:
            report = build_market_report(
                connection,
                db_path=self.db_path,
                runs_root=self.runs_root,
                log_dir=self.log_dir,
                scan_run_id=scan_run_id,
                prefer_latest_selected=False,
                top_opportunities=5,
                top_near_misses=3,
            )
            advice = build_market_advice(
                connection,
                db_path=self.db_path,
                runs_root=self.runs_root,
                log_dir=self.log_dir,
                scan_run_id=scan_run_id,
                prefer_latest_selected=False,
                top_opportunities=5,
                top_near_misses=3,
            )

        artifact_health = report["primary_scan"]["artifact_health"]
        self.assertTrue(artifact_health["required_trace_complete"])
        self.assertTrue(artifact_health["advisory_ready"])
        self.assertEqual("trace-complete", advice["advice_basis"]["credibility_tier"])
        self.assertEqual([], advice["advice_basis"]["warnings"])

    def test_report_exposes_coverage_progress_summary(self) -> None:
        scan_run_id = "scan-coverage-progress"
        self._insert_scan(
            scan_run_id=scan_run_id,
            output_dir=str(self.runs_root / scan_run_id),
            selected_candidate_count=1,
        )
        self._write_complete_artifacts(scan_run_id)
        self._seed_coverage_rows()
        self._write_coverage_cycle(
            cycle_run_id="coverage-cycle-20260406T010000Z-bbbb2222",
            region_id=10000002,
            before_ready_count=0,
            after_ready_count=1,
            history_target_count=3,
            orders_target_count=2,
            scan_run_id="scan-cycle-0",
            duration_seconds=180.0,
            phase_durations={
                "history_batch_seconds": 80.0,
                "orders_batch_seconds": 50.0,
                "benchmark_seconds": 20.0,
                "scan_seconds": 20.0,
                "diagnosis_seconds": 10.0,
            },
        )
        self._write_coverage_cycle(
            cycle_run_id="coverage-cycle-20260406T020000Z-cccc3333",
            region_id=10000002,
            before_ready_count=1,
            after_ready_count=1,
            history_target_count=2,
            orders_target_count=1,
            scan_run_id="scan-cycle-1",
            duration_seconds=240.0,
            phase_durations={
                "history_batch_seconds": 90.0,
                "orders_batch_seconds": 70.0,
                "benchmark_seconds": 40.0,
                "scan_seconds": 25.0,
                "diagnosis_seconds": 15.0,
            },
        )
        self._write_coverage_cycle(
            cycle_run_id="coverage-cycle-20260406T030000Z-dddd4444",
            region_id=10000043,
            before_ready_count=50,
            after_ready_count=60,
            history_target_count=20,
            orders_target_count=10,
            scan_run_id="scan-cycle-other-region",
            duration_seconds=480.0,
        )

        with connect(self.db_path) as connection:
            report = build_market_report(
                connection,
                db_path=self.db_path,
                runs_root=self.runs_root,
                log_dir=self.log_dir,
                scan_run_id=scan_run_id,
                prefer_latest_selected=False,
                top_opportunities=5,
                top_near_misses=3,
            )

        coverage_progress = report["coverage_progress"]
        self.assertIsNotNone(coverage_progress)
        self.assertEqual(10000002, coverage_progress["region_id"])
        self.assertEqual(4, coverage_progress["target_counts"]["market_type_count"])
        self.assertEqual(1, coverage_progress["target_counts"]["ready_count"])
        self.assertEqual(3, coverage_progress["backlog_summary"]["backlog_count"])
        self.assertAlmostEqual(0.25, coverage_progress["ready_ratio"])
        self.assertEqual(0, coverage_progress["latest_cycle"]["ready_gain"])
        self.assertEqual("scan-cycle-1", coverage_progress["latest_cycle"]["scan_run_id"])
        self.assertEqual(2, coverage_progress["recent_cycle_summary"]["cycle_count"])
        self.assertEqual(1, coverage_progress["recent_cycle_summary"]["net_ready_gain"])
        self.assertEqual(1, coverage_progress["recent_cycle_summary"]["positive_gain_cycles"])
        self.assertEqual(1, coverage_progress["recent_cycle_summary"]["zero_gain_cycles"])
        self.assertEqual(1, coverage_progress["recent_cycle_summary"]["no_progress_streak"])
        self.assertEqual("flat", coverage_progress["recent_cycle_summary"]["latest_status"])
        self.assertEqual(240.0, coverage_progress["recent_cycle_summary"]["latest_duration_seconds"])
        self.assertEqual(210.0, coverage_progress["recent_cycle_summary"]["average_duration_seconds"])
        self.assertEqual(90.0, coverage_progress["recent_cycle_summary"]["latest_phase_durations"]["history_batch_seconds"])
        self.assertEqual(3, coverage_progress["recent_cycle_summary"]["latest_total_target_count"])
        self.assertEqual(4.0, coverage_progress["recent_cycle_summary"]["average_total_target_count"])
        self.assertEqual(70.0, coverage_progress["latest_cycle"]["phase_durations"]["orders_batch_seconds"])
        self.assertEqual("remote_fetch", coverage_progress["bottleneck_summary"]["suspected_bottleneck_group"])
        self.assertEqual(
            "history_batch_seconds",
            coverage_progress["bottleneck_summary"]["dominant_phase"]["phase_name"],
        )
        self.assertEqual("medium", coverage_progress["bottleneck_summary"]["dominant_phase"]["dominance_level"])
        self.assertAlmostEqual(85.0, coverage_progress["bottleneck_summary"]["average_phase_seconds"]["history_batch_seconds"])
        self.assertEqual(600.0, coverage_progress["runtime_target_summary"]["target_cycle_seconds"])
        self.assertEqual("within_target", coverage_progress["runtime_target_summary"]["latest_status"])
        self.assertEqual(-360.0, coverage_progress["runtime_target_summary"]["latest_gap_seconds"])
        self.assertEqual(-390.0, coverage_progress["runtime_target_summary"]["average_gap_seconds"])
        self.assertEqual("coverage-cycle-20260406T020000Z-cccc3333", coverage_progress["recent_cycles"][0]["cycle_run_id"])
        self.assertEqual("coverage-cycle-20260406T010000Z-bbbb2222", coverage_progress["recent_cycles"][1]["cycle_run_id"])

    def test_build_coverage_diagnosis_summarizes_focus_and_flags(self) -> None:
        scan_run_id = "scan-coverage-diagnosis"
        self._insert_scan(
            scan_run_id=scan_run_id,
            output_dir=str(self.runs_root / scan_run_id),
            selected_candidate_count=0,
        )
        self._write_complete_artifacts(scan_run_id)
        self._seed_coverage_rows()
        self._write_coverage_cycle(
            cycle_run_id="coverage-cycle-20260406T020000Z-diag3333",
            region_id=10000002,
            before_ready_count=1,
            after_ready_count=1,
            history_target_count=2,
            orders_target_count=1,
            scan_run_id="scan-cycle-diagnosis",
            duration_seconds=720.0,
            phase_durations={
                "history_batch_seconds": 320.0,
                "orders_batch_seconds": 220.0,
                "benchmark_seconds": 90.0,
                "scan_seconds": 60.0,
                "diagnosis_seconds": 30.0,
            },
        )
        self._write_coverage_cycle(
            cycle_run_id="coverage-cycle-20260406T010000Z-diag2222",
            region_id=10000002,
            before_ready_count=1,
            after_ready_count=1,
            history_target_count=3,
            orders_target_count=1,
            scan_run_id="scan-cycle-diagnosis-older",
            duration_seconds=660.0,
            phase_durations={
                "history_batch_seconds": 280.0,
                "orders_batch_seconds": 210.0,
                "benchmark_seconds": 90.0,
                "scan_seconds": 50.0,
                "diagnosis_seconds": 30.0,
            },
        )

        with connect(self.db_path) as connection:
            diagnosis = build_coverage_diagnosis(
                connection,
                db_path=self.db_path,
                runs_root=self.runs_root,
                log_dir=self.log_dir,
                scan_run_id=scan_run_id,
                prefer_latest_selected=False,
            )

        self.assertEqual(10000002, diagnosis["coverage_progress"]["region_id"])
        self.assertEqual(
            "coverage expansion is behind the runtime target and not progressing cleanly",
            diagnosis["diagnosis_summary"]["headline"],
        )
        self.assertEqual("optimize_remote_fetch", diagnosis["diagnosis_summary"]["recommended_focus"])
        self.assertIn("coverage_stalled", diagnosis["diagnosis_summary"]["attention_flags"])
        self.assertIn("runtime_over_target", diagnosis["diagnosis_summary"]["attention_flags"])
        self.assertIn("backlog_missing_both", diagnosis["diagnosis_summary"]["attention_flags"])
        self.assertEqual("remote_fetch", diagnosis["diagnosis_summary"]["suspected_bottleneck_group"])
        self.assertEqual("missing_both", diagnosis["diagnosis_summary"]["primary_backlog_bucket"]["bucket"])

    def test_cli_diagnose_coverage_prints_structured_summary(self) -> None:
        scan_run_id = "scan-cli-coverage-diagnosis"
        self._insert_scan(
            scan_run_id=scan_run_id,
            output_dir=str(self.runs_root / scan_run_id),
            selected_candidate_count=0,
        )
        self._write_complete_artifacts(scan_run_id)
        self._seed_coverage_rows()
        self._write_coverage_cycle(
            cycle_run_id="coverage-cycle-20260406T020000Z-cli3333",
            region_id=10000002,
            before_ready_count=1,
            after_ready_count=1,
            history_target_count=2,
            orders_target_count=1,
            scan_run_id="scan-cycle-cli-diagnosis",
            duration_seconds=720.0,
            phase_durations={
                "history_batch_seconds": 320.0,
                "orders_batch_seconds": 220.0,
                "benchmark_seconds": 90.0,
                "scan_seconds": 60.0,
                "diagnosis_seconds": 30.0,
            },
        )
        self._write_coverage_cycle(
            cycle_run_id="coverage-cycle-20260406T010000Z-cli2222",
            region_id=10000002,
            before_ready_count=1,
            after_ready_count=1,
            history_target_count=3,
            orders_target_count=1,
            scan_run_id="scan-cycle-cli-diagnosis-older",
            duration_seconds=660.0,
        )

        stdout = io.StringIO()
        argv = [
            "eveq",
            "diagnose-coverage",
            "--db-path",
            str(self.db_path),
            "--runs-root",
            str(self.runs_root),
            "--log-dir",
            str(self.log_dir),
            "--scan-run-id",
            scan_run_id,
        ]
        with patch.object(sys, "argv", argv):
            with redirect_stdout(stdout):
                exit_code = cli_main()

        output_lines = stdout.getvalue().strip().splitlines()
        self.assertEqual(0, exit_code)
        self.assertTrue(output_lines[0].startswith("Coverage diagnosis ready: 10000002"))
        payload = json.loads("\n".join(output_lines[1:]))
        self.assertEqual("optimize_remote_fetch", payload["diagnosis_summary"]["recommended_focus"])
        self.assertEqual("remote_fetch", payload["diagnosis_summary"]["suspected_bottleneck_group"])

    def test_report_exposes_foreign_worker_state_from_latest_sync(self) -> None:
        scan_run_id = "scan-sync-worker"
        self._insert_scan(
            scan_run_id=scan_run_id,
            output_dir=str(self.runs_root / scan_run_id),
            selected_candidate_count=1,
        )
        self._write_complete_artifacts(scan_run_id)

        (self.db_path.parent / "eveq.db.snapshot.json").write_text(
            json.dumps(
                {
                    "worker_state": {
                        "status": "completed",
                        "worker_run_id": "coverage-worker-1",
                        "cycle_run_id": "coverage-cycle-1",
                        "started_at_utc": "2026-04-06T01:00:00Z",
                        "finished_at_utc": "2026-04-06T01:05:00Z",
                    }
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        (self.log_dir / "eveq-foreign-sync-20260406T010500Z.json").write_text(
            json.dumps(
                {
                    "remote_manifest": {
                        "worker_state": {
                            "status": "completed",
                            "worker_run_id": "coverage-worker-1",
                            "cycle_run_id": "coverage-cycle-1",
                            "started_at_utc": "2026-04-06T01:00:00Z",
                            "finished_at_utc": "2026-04-06T01:05:00Z",
                        }
                    }
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

        with connect(self.db_path) as connection:
            report = build_market_report(
                connection,
                db_path=self.db_path,
                runs_root=self.runs_root,
                log_dir=self.log_dir,
                scan_run_id=scan_run_id,
                prefer_latest_selected=False,
                top_opportunities=5,
                top_near_misses=3,
            )

        latest_sync = report["latest_sync"]
        self.assertIsNotNone(latest_sync)
        self.assertEqual("completed", latest_sync["worker_state_summary"]["status"])
        self.assertEqual("coverage-worker-1", latest_sync["worker_state_summary"]["worker_run_id"])
        self.assertEqual("coverage-cycle-1", latest_sync["worker_state_summary"]["cycle_run_id"])
        self.assertEqual("coverage_worker", latest_sync["worker_state_summary"]["worker_kind"])
        self.assertEqual([], latest_sync["sync_health_summary"]["problem_flags"])

    def test_report_exposes_worker_backoff_summary_from_latest_sync(self) -> None:
        scan_run_id = "scan-sync-worker-backoff"
        self._insert_scan(
            scan_run_id=scan_run_id,
            output_dir=str(self.runs_root / scan_run_id),
            selected_candidate_count=1,
        )
        self._write_complete_artifacts(scan_run_id)

        (self.db_path.parent / "eveq.db.snapshot.json").write_text(
            json.dumps(
                {
                    "worker_state": {
                        "status": "skipped_backoff",
                        "worker_run_id": "coverage-worker-2",
                        "started_at_utc": "2026-04-06T01:10:00Z",
                        "finished_at_utc": "2026-04-06T01:10:00Z",
                        "next_allowed_at_utc": "2026-04-06T01:35:00Z",
                        "message": "coverage worker is in failure backoff window",
                    }
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

        with connect(self.db_path) as connection:
            report = build_market_report(
                connection,
                db_path=self.db_path,
                runs_root=self.runs_root,
                log_dir=self.log_dir,
                scan_run_id=scan_run_id,
                prefer_latest_selected=False,
                top_opportunities=5,
                top_near_misses=3,
            )

        latest_sync = report["latest_sync"]
        self.assertIsNotNone(latest_sync)
        self.assertEqual("skipped_backoff", latest_sync["worker_state_summary"]["status"])
        self.assertEqual("2026-04-06T01:35:00Z", latest_sync["worker_state_summary"]["next_allowed_at_utc"])
        self.assertEqual(
            "coverage worker is in failure backoff window",
            latest_sync["worker_state_summary"]["message"],
        )
        self.assertIn("worker_backoff", latest_sync["sync_health_summary"]["problem_flags"])

    def test_report_exposes_jita_passive_cache_worker_summary_from_latest_sync(self) -> None:
        scan_run_id = "scan-sync-cache-worker"
        self._insert_scan(
            scan_run_id=scan_run_id,
            output_dir=str(self.runs_root / scan_run_id),
            selected_candidate_count=1,
        )
        self._write_complete_artifacts(scan_run_id)

        worker_state = {
            "status": "completed",
            "worker_run_id": "jita-passive-cache-worker-1",
            "started_at_utc": "2026-04-07T04:17:45Z",
            "finished_at_utc": "2026-04-07T04:26:45Z",
            "cycle_count": 5,
            "last_cycle_run_id": "jita-passive-cache-cycle-1",
            "last_scan_run_id": "jita-passive-simple-1",
            "orders_cache_profile": "warm",
            "stop_reason": "max_runtime_reached",
            "aggregate": {
                "executed_remote_target_count": 250,
                "cache_hit_gain": 250,
                "candidate_count_built": 12,
                "candidate_count_selected": 3,
            },
        }
        (self.db_path.parent / "eveq.db.snapshot.json").write_text(
            json.dumps({"worker_state": worker_state}, ensure_ascii=False),
            encoding="utf-8",
        )
        (self.log_dir / "eveq-foreign-sync-20260407T042645Z.json").write_text(
            json.dumps({"remote_manifest": {"worker_state": worker_state}}, ensure_ascii=False),
            encoding="utf-8",
        )

        with connect(self.db_path) as connection:
            report = build_market_report(
                connection,
                db_path=self.db_path,
                runs_root=self.runs_root,
                log_dir=self.log_dir,
                scan_run_id=scan_run_id,
                prefer_latest_selected=False,
                top_opportunities=5,
                top_near_misses=3,
            )

        latest_sync = report["latest_sync"]
        self.assertIsNotNone(latest_sync)
        self.assertEqual("jita_passive_cache_worker", latest_sync["worker_state_summary"]["worker_kind"])
        self.assertEqual("completed", latest_sync["worker_state_summary"]["status"])
        self.assertEqual("jita-passive-cache-worker-1", latest_sync["worker_state_summary"]["worker_run_id"])
        self.assertEqual("jita-passive-cache-cycle-1", latest_sync["worker_state_summary"]["cycle_run_id"])
        self.assertEqual("jita-passive-cache-cycle-1", latest_sync["worker_state_summary"]["last_cycle_run_id"])
        self.assertEqual("jita-passive-simple-1", latest_sync["worker_state_summary"]["last_scan_run_id"])
        self.assertEqual(5, latest_sync["worker_state_summary"]["cycle_count"])
        self.assertEqual("warm", latest_sync["worker_state_summary"]["orders_cache_profile"])
        self.assertEqual("max_runtime_reached", latest_sync["worker_state_summary"]["stop_reason"])
        self.assertEqual(
            250,
            latest_sync["worker_state_summary"]["aggregate"]["executed_remote_target_count"],
        )

    def test_report_summarizes_recent_worker_health_rollup(self) -> None:
        scan_run_id = "scan-sync-rollup"
        self._insert_scan(
            scan_run_id=scan_run_id,
            output_dir=str(self.runs_root / scan_run_id),
            selected_candidate_count=1,
        )
        self._write_complete_artifacts(scan_run_id)

        for name, status, worker_run_id in [
            ("eveq-foreign-sync-20260406T011000Z.json", "completed", "coverage-worker-1"),
            ("eveq-foreign-sync-20260406T012000Z.json", "failed", "coverage-worker-2"),
            ("eveq-foreign-sync-20260406T013000Z.json", "skipped_backoff", "coverage-worker-3"),
        ]:
            (self.log_dir / name).write_text(
                json.dumps(
                    {
                        "sync_completed_at_utc": name.removeprefix("eveq-foreign-sync-").removesuffix(".json"),
                        "remote_manifest": {
                            "worker_state": {
                                "status": status,
                                "worker_run_id": worker_run_id,
                            }
                        },
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

        with connect(self.db_path) as connection:
            report = build_market_report(
                connection,
                db_path=self.db_path,
                runs_root=self.runs_root,
                log_dir=self.log_dir,
                scan_run_id=scan_run_id,
                prefer_latest_selected=False,
                top_opportunities=5,
                top_near_misses=3,
            )

        latest_sync = report["latest_sync"]
        self.assertIsNotNone(latest_sync)
        rollup = latest_sync["worker_health_rollup"]
        self.assertEqual("skipped_backoff", rollup["latest_status"])
        self.assertEqual(2, rollup["consecutive_problem_count"])
        self.assertEqual("skipped_backoff", rollup["recent_statuses"][0]["status"])
        self.assertEqual("failed", rollup["recent_statuses"][1]["status"])
        self.assertEqual("completed", rollup["recent_statuses"][2]["status"])
        self.assertEqual(2, latest_sync["sync_problem_rollup"]["recent_problem_count"])

    def test_worker_health_rollup_keeps_cache_worker_cycle_and_scan_context(self) -> None:
        scan_run_id = "scan-sync-cache-rollup"
        self._insert_scan(
            scan_run_id=scan_run_id,
            output_dir=str(self.runs_root / scan_run_id),
            selected_candidate_count=1,
        )
        self._write_complete_artifacts(scan_run_id)

        (self.log_dir / "eveq-foreign-sync-20260407T042645Z.json").write_text(
            json.dumps(
                {
                    "sync_completed_at_utc": "2026-04-07T04:26:45Z",
                    "remote_manifest": {
                        "worker_state": {
                            "status": "completed",
                            "worker_run_id": "jita-passive-cache-worker-1",
                            "cycle_count": 4,
                            "last_cycle_run_id": "jita-passive-cache-cycle-4",
                            "last_scan_run_id": "jita-passive-simple-4",
                            "orders_cache_profile": "warm",
                            "stop_reason": "max_cycles_reached",
                        }
                    },
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

        with connect(self.db_path) as connection:
            report = build_market_report(
                connection,
                db_path=self.db_path,
                runs_root=self.runs_root,
                log_dir=self.log_dir,
                scan_run_id=scan_run_id,
                prefer_latest_selected=False,
                top_opportunities=5,
                top_near_misses=3,
            )

        latest_sync = report["latest_sync"]
        self.assertIsNotNone(latest_sync)
        recent_status = latest_sync["worker_health_rollup"]["recent_statuses"][0]
        self.assertEqual("jita_passive_cache_worker", recent_status["worker_kind"])
        self.assertEqual("jita-passive-cache-cycle-4", recent_status["cycle_run_id"])
        self.assertEqual("jita-passive-cache-cycle-4", recent_status["last_cycle_run_id"])
        self.assertEqual("jita-passive-simple-4", recent_status["last_scan_run_id"])
        self.assertEqual("warm", recent_status["orders_cache_profile"])
        self.assertEqual("max_cycles_reached", recent_status["stop_reason"])

    def test_report_flags_failed_domestic_sync(self) -> None:
        scan_run_id = "scan-sync-failed"
        self._insert_scan(
            scan_run_id=scan_run_id,
            output_dir=str(self.runs_root / scan_run_id),
            selected_candidate_count=1,
        )
        self._write_complete_artifacts(scan_run_id)

        (self.log_dir / "eveq-foreign-sync-20260406T014000Z.json").write_text(
            json.dumps(
                {
                    "sync_completed_at_utc": "2026-04-06T01:40:00Z",
                    "sync_status": "failed",
                    "sync_stage": "download_snapshot",
                    "error_exit_code": 23,
                    "error_message": "foreign sync failed during download_snapshot",
                    "remote_host": "ubuntu@test-host",
                    "remote_manifest": None,
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

        with connect(self.db_path) as connection:
            report = build_market_report(
                connection,
                db_path=self.db_path,
                runs_root=self.runs_root,
                log_dir=self.log_dir,
                scan_run_id=scan_run_id,
                prefer_latest_selected=False,
                top_opportunities=5,
                top_near_misses=3,
            )

        latest_sync = report["latest_sync"]
        self.assertIsNotNone(latest_sync)
        self.assertIn("sync_failed", latest_sync["sync_health_summary"]["problem_flags"])
        self.assertEqual("failed", latest_sync["sync_health_summary"]["sync_status"])
        self.assertEqual("download_snapshot", latest_sync["sync_health_summary"]["sync_stage"])
        self.assertEqual(1, latest_sync["sync_problem_rollup"]["recent_problem_count"])
        self.assertEqual("failed", latest_sync["sync_problem_rollup"]["recent_reports"][0]["sync_status"])

    def test_report_exposes_latest_retention_cleanup_summary(self) -> None:
        scan_run_id = "scan-cleanup-summary"
        self._insert_scan(
            scan_run_id=scan_run_id,
            output_dir=str(self.runs_root / scan_run_id),
            selected_candidate_count=1,
        )
        self._write_complete_artifacts(scan_run_id)

        cleanup_dir = self.runs_root / "retention-cleanup-20260406T020000Z-aaaa1111"
        cleanup_dir.mkdir(parents=True, exist_ok=True)
        (cleanup_dir / "retention_cleanup.json").write_text(
            json.dumps(
                {
                    "cleanup_run_id": "retention-cleanup-20260406T020000Z-aaaa1111",
                    "cleaned_at_utc": "2026-04-06T02:00:00Z",
                    "dry_run": False,
                    "summary": {
                        "candidate_count": 5,
                        "deleted_count": 5,
                        "dry_run_candidate_count": 0,
                        "category_counts": {
                            "runs": 1,
                            "logs": 1,
                            "sync_reports": 1,
                            "staging": 1,
                            "db_prev": 1,
                        },
                    },
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

        with connect(self.db_path) as connection:
            report = build_market_report(
                connection,
                db_path=self.db_path,
                runs_root=self.runs_root,
                log_dir=self.log_dir,
                scan_run_id=scan_run_id,
                prefer_latest_selected=False,
                top_opportunities=5,
                top_near_misses=3,
            )

        latest_cleanup = report["latest_cleanup"]
        self.assertIsNotNone(latest_cleanup)
        self.assertEqual("retention-cleanup-20260406T020000Z-aaaa1111", latest_cleanup["cleanup_run_id"])
        self.assertEqual(5, latest_cleanup["summary"]["deleted_count"])
        self.assertEqual(1, latest_cleanup["summary"]["category_counts"]["staging"])
        self.assertEqual([], latest_cleanup["health_summary"]["problem_flags"])

    def test_report_flags_cleanup_dry_run_as_problem(self) -> None:
        scan_run_id = "scan-cleanup-dry-run"
        self._insert_scan(
            scan_run_id=scan_run_id,
            output_dir=str(self.runs_root / scan_run_id),
            selected_candidate_count=1,
        )
        self._write_complete_artifacts(scan_run_id)

        cleanup_dir = self.runs_root / "retention-cleanup-20260406T030000Z-bbbb2222"
        cleanup_dir.mkdir(parents=True, exist_ok=True)
        (cleanup_dir / "retention_cleanup.json").write_text(
            json.dumps(
                {
                    "cleanup_run_id": "retention-cleanup-20260406T030000Z-bbbb2222",
                    "cleaned_at_utc": "2026-04-06T03:00:00Z",
                    "dry_run": True,
                    "summary": {
                        "candidate_count": 3,
                        "deleted_count": 0,
                        "dry_run_candidate_count": 3,
                        "category_counts": {"runs": 1},
                    },
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

        with connect(self.db_path) as connection:
            report = build_market_report(
                connection,
                db_path=self.db_path,
                runs_root=self.runs_root,
                log_dir=self.log_dir,
                scan_run_id=scan_run_id,
                prefer_latest_selected=False,
                top_opportunities=5,
                top_near_misses=3,
            )

        latest_cleanup = report["latest_cleanup"]
        self.assertIsNotNone(latest_cleanup)
        self.assertIn("dry_run", latest_cleanup["health_summary"]["problem_flags"])
        self.assertEqual(1, latest_cleanup["health_rollup"]["recent_problem_count"])

    def test_advice_flags_missing_artifacts_as_degraded(self) -> None:
        scan_run_id = "scan-missing-artifacts"
        self._insert_scan(
            scan_run_id=scan_run_id,
            output_dir=r"D:\old-project\runs\scan-missing-artifacts",
            selected_candidate_count=1,
        )

        with connect(self.db_path) as connection:
            advice = build_market_advice(
                connection,
                db_path=self.db_path,
                runs_root=self.runs_root,
                log_dir=self.log_dir,
                scan_run_id=scan_run_id,
                prefer_latest_selected=False,
                top_opportunities=5,
                top_near_misses=3,
            )

        artifact_health = advice["artifact_health"]
        self.assertFalse(artifact_health["advisory_ready"])
        self.assertIn("analysis_run.json", artifact_health["missing_required"])
        self.assertEqual("degraded", advice["advice_basis"]["credibility_tier"])
        self.assertTrue(advice["advice_basis"]["warnings"])

    def test_scan_diagnosis_summarizes_strategy_and_skip_reasons(self) -> None:
        scan_run_id = "scan-diagnosis"
        self._insert_scan(
            scan_run_id=scan_run_id,
            output_dir=str(self.runs_root / scan_run_id),
            selected_candidate_count=1,
        )
        self._write_complete_artifacts(scan_run_id)
        run_dir = self.runs_root / scan_run_id
        (run_dir / "calc_trace.json").write_text(
            json.dumps(
                {
                    "candidate_traces": [
                        {
                            "type_id": 34,
                            "type_name": "Tritanium",
                            "strategy_pool": "cross-location",
                            "source_record": {
                                "opportunity_id": "34-10000002-cross-location",
                                "type_id": 34,
                                "avg_daily_isk_volume_30d": 180_000_000.0,
                                "avg_daily_units_7d": 250_000.0,
                                "active_days_ratio_30d": 0.96,
                                "cross_location_net_spread_pct": 0.044,
                                "volatility_30d": 0.11,
                                "buy_location_name": "Jita IV - Moon 4",
                                "sell_location_name": "Amarr VIII (Oris)",
                                "source_order_snapshot_id": "orders-1",
                                "source_order_captured_at": "2026-04-06T00:04:00Z",
                            },
                            "derived": {
                                "net_spread_pct": 0.044,
                                "screened_profit_margin": 0.031,
                                "price_zscore_30d": 0.20,
                                "estimated_exit_days_small_size": 1.2,
                            },
                            "filter_evaluation": {
                                "passed": True,
                                "rejection_reasons": [],
                            },
                        },
                        {
                            "type_id": 35,
                            "type_name": "Pyerite",
                            "strategy_pool": "station-flip",
                            "source_record": {
                                "opportunity_id": "35-10000002-station-flip",
                                "type_id": 35,
                                "avg_daily_isk_volume_30d": 90_000_000.0,
                                "avg_daily_units_7d": 120_000.0,
                                "active_days_ratio_30d": 0.88,
                                "buy_location_name": "Jita IV - Moon 4",
                                "sell_location_name": "Jita IV - Moon 4",
                                "source_order_snapshot_id": "orders-2",
                            },
                            "derived": {
                                "screened_profit_margin": 0.012,
                                "price_zscore_30d": 0.60,
                                "estimated_exit_days_small_size": 1.8,
                            },
                            "filter_evaluation": {
                                "passed": False,
                                "rejection_reasons": [
                                    "screened_profit_margin below threshold",
                                    "active_days_ratio_30d below threshold",
                                ],
                            },
                        },
                        {
                            "type_id": 36,
                            "type_name": "Mexallon",
                            "strategy_pool": "cross-location",
                            "source_record": {
                                "opportunity_id": "36-10000002-cross-location",
                                "type_id": 36,
                                "avg_daily_isk_volume_30d": 110_000_000.0,
                                "avg_daily_units_7d": 200_000.0,
                                "active_days_ratio_30d": 0.90,
                                "buy_location_name": "Perimeter",
                                "sell_location_name": "Jita",
                                "source_order_snapshot_id": "orders-3",
                            },
                            "derived": {
                                "screened_profit_margin": 0.019,
                                "price_zscore_30d": -0.20,
                                "estimated_exit_days_small_size": 1.1,
                            },
                            "filter_evaluation": {
                                "passed": False,
                                "rejection_reasons": [
                                    "screened_profit_margin below threshold",
                                ],
                            },
                        },
                    ]
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

        with connect(self.db_path) as connection:
            diagnosis = build_scan_diagnosis(
                connection,
                db_path=self.db_path,
                runs_root=self.runs_root,
                log_dir=self.log_dir,
                scan_run_id=scan_run_id,
                prefer_latest_selected=False,
                top_rejection_reasons=5,
                top_near_passes=5,
                top_skipped_reasons=5,
            )

        summary = diagnosis["diagnosis_summary"]
        self.assertEqual(3, summary["candidate_count_total"])
        self.assertEqual(1, summary["selected_count"])
        self.assertEqual(2, summary["rejected_count"])
        self.assertEqual(1, summary["skipped_count"])
        self.assertEqual("cross-location", diagnosis["strategy_summary"][0]["strategy_pool"])
        self.assertEqual(
            "screened_profit_margin below threshold",
            diagnosis["top_rejection_reasons"][0]["reason"],
        )
        self.assertEqual(
            "36-10000002-cross-location",
            diagnosis["closest_near_passes"][0]["opportunity_id"],
        )
        self.assertEqual("missing_history", diagnosis["skipped_summary"]["reason_counts"][0]["reason"])

    def test_cross_location_strong_candidate_is_deploy_small(self) -> None:
        advice = _advise_candidate(
            {
                "opportunity_id": "34-10000002-cross-location",
                "type_id": 34,
                "type_name": "Tritanium",
                "strategy_type": "cross-location",
                "profit_margin": 0.052,
                "expected_turnover_days": 1.2,
                "avg_daily_isk_volume_30d": 240_000_000.0,
                "active_days_ratio_30d": 0.97,
                "volatility_30d": 0.09,
                "price_zscore_30d": -0.05,
                "cross_location_net_spread_pct": 0.052,
            }
        )

        self.assertEqual("deploy-small", advice["action"])
        self.assertGreaterEqual(advice["confidence_score"], 82)
        self.assertEqual([], advice["blockers"]["hard"])
        self.assertIn("hauling risk and route friction are not yet modeled in v1", advice["blockers"]["soft"])

    def test_station_flip_stretched_price_becomes_watch(self) -> None:
        advice = _advise_candidate(
            {
                "opportunity_id": "35-10000002-station-flip",
                "type_id": 35,
                "type_name": "Pyerite",
                "strategy_type": "station-flip",
                "profit_margin": 0.023,
                "expected_turnover_days": 0.9,
                "avg_daily_isk_volume_30d": 210_000_000.0,
                "active_days_ratio_30d": 0.96,
                "volatility_30d": 0.11,
                "price_zscore_30d": 0.41,
            }
        )

        self.assertEqual("watch", advice["action"])
        self.assertEqual([], advice["blockers"]["hard"])
        self.assertTrue(any("zscore" in item for item in advice["blockers"]["soft"]))
        self.assertIn(advice["thesis_strength"], {"workable", "strong"})

    def test_jita_passive_simple_strong_candidate_is_deploy_small(self) -> None:
        advice = _advise_candidate(
            {
                "opportunity_id": "3001-10000002-jita-passive-simple",
                "type_id": 3001,
                "type_name": "Alpha Item",
                "strategy_type": "jita-passive-simple",
                "profit_margin": 0.022,
                "expected_turnover_days": 0.7,
                "avg_daily_isk_volume_30d": 220_000_000.0,
                "active_days_ratio_30d": 0.97,
                "volatility_30d": 0.11,
                "source_order_snapshot_id": "orders-alpha",
                "source_order_captured_at": "2026-04-07T12:04:00Z",
            }
        )

        self.assertEqual("deploy-small", advice["action"])
        self.assertGreaterEqual(advice["confidence_score"], 76)
        self.assertEqual([], advice["blockers"]["hard"])
        self.assertEqual([], advice["blockers"]["soft"])

    def test_market_advice_adds_budget_aware_position_plan(self) -> None:
        scan_run_id = "scan-budget-plan"
        self._insert_scan(
            scan_run_id=scan_run_id,
            output_dir=str(self.runs_root / scan_run_id),
            selected_candidate_count=1,
            opportunity_id="34-10000002-cross-location",
        )
        self._write_complete_artifacts(scan_run_id)
        run_dir = self.runs_root / scan_run_id
        with connect(self.db_path) as connection:
            connection.execute(
                """
                UPDATE opportunity_snapshot
                SET strategy_type = ?, profit_margin = ?, capital_required = ?, tradable_units = ?,
                    expected_turnover_days = ?, risk_adjusted_return = ?, decision_reason = ?
                WHERE scan_run_id = ?
                """,
                (
                    "jita-passive-simple",
                    0.022,
                    280_000_000.0,
                    10_000.0,
                    0.7,
                    0.11,
                    "stable two-sided turnover and margin",
                    scan_run_id,
                ),
            )
            connection.commit()
        (run_dir / "calc_trace.json").write_text(
            json.dumps(
                {
                    "candidate_traces": [
                        {
                            "type_id": 34,
                            "type_name": "Tritanium",
                            "strategy_pool": "jita-passive-simple",
                            "source_record": {
                                "opportunity_id": "34-10000002-cross-location",
                                "type_id": 34,
                                "avg_daily_isk_volume_30d": 220_000_000.0,
                                "avg_daily_units_7d": 250_000.0,
                                "active_days_ratio_30d": 0.97,
                                "volatility_30d": 0.11,
                                "source_order_snapshot_id": "orders-1",
                                "source_order_captured_at": "2026-04-06T00:04:00Z",
                            },
                            "derived": {
                                "screened_profit_margin": 0.022,
                                "estimated_exit_days_small_size": 0.7,
                            },
                            "filter_evaluation": {
                                "passed": True,
                                "rejection_reasons": [],
                            },
                        }
                    ]
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

        with connect(self.db_path) as connection:
            advice = build_market_advice(
                connection,
                db_path=self.db_path,
                runs_root=self.runs_root,
                log_dir=self.log_dir,
                scan_run_id=scan_run_id,
                prefer_latest_selected=False,
                top_opportunities=5,
                top_near_misses=3,
            )

        self.assertEqual(1_000_000_000.0, advice["budget_summary"]["total_budget_isk"])
        self.assertEqual("standard", advice["budget_summary"]["risk_profile"])
        self.assertEqual(1, advice["budget_summary"]["allocated_candidate_count"])
        self.assertEqual(1, advice["budget_summary"]["execute_now_count"])
        self.assertGreater(advice["budget_summary"]["allocated_capital_isk"], 0.0)
        self.assertLess(advice["budget_summary"]["allocated_capital_isk"], 1_000_000_000.0)
        position_plan = advice["selected_advice"][0]["position_plan"]
        self.assertEqual("allocate", position_plan["status"])
        self.assertEqual("standard", position_plan["risk_profile"])
        self.assertGreater(position_plan["suggested_capital_isk"], 0.0)
        self.assertTrue(
            any(
                item in position_plan["binding_constraints"]
                for item in {"budget_share_cap", "liquidity_cap", "capital_required"}
            )
        )
        self.assertEqual("execute_now", advice["execution_plan"][0]["execution_status"])
        self.assertEqual(1, advice["execution_plan"][0]["priority_rank"])
        self.assertGreater(advice["execution_plan"][0]["cumulative_allocated_isk"], 0.0)
        self.assertEqual("scan_budget", advice["analysis_context"]["context_mode"])
        self.assertFalse(advice["analysis_context"]["uses_character_account"])
        self.assertIn("summary_zh", advice["data_freshness"])

    def test_market_advice_risk_profile_changes_allocation_size(self) -> None:
        scan_run_id = "scan-risk-profile-plan"
        self._insert_scan(
            scan_run_id=scan_run_id,
            output_dir=str(self.runs_root / scan_run_id),
            selected_candidate_count=1,
            opportunity_id="34-10000002-cross-location",
        )
        self._write_complete_artifacts(scan_run_id)
        run_dir = self.runs_root / scan_run_id
        with connect(self.db_path) as connection:
            connection.execute(
                """
                UPDATE opportunity_snapshot
                SET strategy_type = ?, profit_margin = ?, capital_required = ?, tradable_units = ?,
                    expected_turnover_days = ?, risk_adjusted_return = ?, decision_reason = ?
                WHERE scan_run_id = ?
                """,
                (
                    "jita-passive-simple",
                    0.028,
                    900_000_000.0,
                    30_000.0,
                    0.6,
                    0.14,
                    "high-confidence sizing comparison",
                    scan_run_id,
                ),
            )
            connection.commit()
        (run_dir / "calc_trace.json").write_text(
            json.dumps(
                {
                    "candidate_traces": [
                        {
                            "type_id": 34,
                            "type_name": "Tritanium",
                            "strategy_pool": "jita-passive-simple",
                            "source_record": {
                                "opportunity_id": "34-10000002-cross-location",
                                "type_id": 34,
                                "avg_daily_isk_volume_30d": 3_000_000_000.0,
                                "avg_daily_units_7d": 400_000.0,
                                "active_days_ratio_30d": 0.98,
                                "volatility_30d": 0.09,
                                "source_order_snapshot_id": "orders-risk",
                                "source_order_captured_at": "2026-04-06T00:04:00Z",
                            },
                            "derived": {
                                "screened_profit_margin": 0.028,
                                "estimated_exit_days_small_size": 0.6,
                            },
                            "filter_evaluation": {
                                "passed": True,
                                "rejection_reasons": [],
                            },
                        }
                    ]
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

        with connect(self.db_path) as connection:
            conservative = build_market_advice(
                connection,
                db_path=self.db_path,
                runs_root=self.runs_root,
                log_dir=self.log_dir,
                scan_run_id=scan_run_id,
                prefer_latest_selected=False,
                top_opportunities=5,
                top_near_misses=3,
                risk_profile="conservative",
            )
            standard = build_market_advice(
                connection,
                db_path=self.db_path,
                runs_root=self.runs_root,
                log_dir=self.log_dir,
                scan_run_id=scan_run_id,
                prefer_latest_selected=False,
                top_opportunities=5,
                top_near_misses=3,
                risk_profile="standard",
            )
            aggressive = build_market_advice(
                connection,
                db_path=self.db_path,
                runs_root=self.runs_root,
                log_dir=self.log_dir,
                scan_run_id=scan_run_id,
                prefer_latest_selected=False,
                top_opportunities=5,
                top_near_misses=3,
                risk_profile="aggressive",
            )

        conservative_capital = conservative["selected_advice"][0]["position_plan"]["suggested_capital_isk"]
        standard_capital = standard["selected_advice"][0]["position_plan"]["suggested_capital_isk"]
        aggressive_capital = aggressive["selected_advice"][0]["position_plan"]["suggested_capital_isk"]

        self.assertEqual("conservative", conservative["budget_summary"]["risk_profile"])
        self.assertEqual("standard", standard["budget_summary"]["risk_profile"])
        self.assertEqual("aggressive", aggressive["budget_summary"]["risk_profile"])
        self.assertLess(conservative_capital, standard_capital)
        self.assertLess(standard_capital, aggressive_capital)

    def test_reservoir_stays_watch_only_even_when_metrics_are_strong(self) -> None:
        advice = _advise_candidate(
            {
                "opportunity_id": "36-10000002-reservoir",
                "type_id": 36,
                "type_name": "Mexallon",
                "strategy_type": "reservoir",
                "profit_margin": 0.082,
                "expected_turnover_days": 2.4,
                "avg_daily_isk_volume_30d": 180_000_000.0,
                "active_days_ratio_30d": 0.88,
                "volatility_30d": 0.10,
                "price_zscore_30d": -1.9,
            }
        )

        self.assertEqual("watch", advice["action"])
        self.assertIn(advice["thesis_strength"], {"workable", "strong"})
        self.assertGreaterEqual(advice["confidence_score"], 70)
        self.assertIn("does not model catalyst timing for mean reversion", advice["model_limitations"])


if __name__ == "__main__":
    unittest.main()
