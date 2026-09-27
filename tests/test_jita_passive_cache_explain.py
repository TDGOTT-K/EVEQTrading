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
from eveqtrading.jita_passive_cache_explain import build_jita_passive_cache_opportunity_explanation


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT / "sql" / "schema.sql"


class JitaPassiveCacheExplainTests(unittest.TestCase):
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

    def _seed_scan(
        self,
        *,
        connection,
        scan_run_id: str,
        selected_candidate_count: int,
        opportunity_id: str = "3001-10000002-station-flip",
        type_id: int = 3001,
        type_name: str = "Alpha Item",
    ) -> None:
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
                    opportunity_id,
                    "station-flip",
                    type_id,
                    type_name,
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

        candidate_table = (
            [
                {
                    "opportunity_id": opportunity_id,
                    "buy_location_name": "Jita IV - Moon 4",
                    "sell_location_name": "Jita IV - Moon 4",
                    "buy_reference_price": 28000.0,
                    "sell_reference_price": 29800.0,
                    "gross_spread_pct": 0.064,
                }
            ]
            if selected_candidate_count > 0
            else []
        )
        calc_trace = {
            "candidate_traces": [
                {
                    "type_id": type_id,
                    "type_name": type_name,
                    "strategy_pool": "station-flip",
                    "source_record": {
                        "opportunity_id": opportunity_id,
                        "type_id": type_id,
                        "flip_units_cv_30d": 0.12,
                        "flip_units_cv_7d": 0.21,
                        "daily_capacity_isk_30d": 180_000_000.0,
                        "avg_daily_units_7d": 8500.0,
                        "source_order_snapshot_id": "orders-alpha",
                        "source_order_captured_at": "2026-04-07T12:04:00Z",
                    },
                    "derived": {
                        "screened_profit_margin": 0.052,
                        "estimated_exit_days_small_size": 1.5,
                    },
                    "filter_evaluation": {
                        "passed": selected_candidate_count > 0,
                        "rejection_reasons": [] if selected_candidate_count > 0 else ["no selected candidates"],
                    },
                }
            ]
        }

        (run_dir / "analysis_run.json").write_text(
            json.dumps({"scan_run_id": scan_run_id, "strategy_set": "jita-passive-simple"}, ensure_ascii=False),
            encoding="utf-8",
        )
        (run_dir / "candidate_table.json").write_text(json.dumps(candidate_table, ensure_ascii=False), encoding="utf-8")
        (run_dir / "calc_trace.json").write_text(json.dumps(calc_trace, ensure_ascii=False), encoding="utf-8")
        (run_dir / "summary.md").write_text("# summary\n", encoding="utf-8")
        (run_dir / "feature_table.json").write_text(
            json.dumps([{"type_id": type_id, "type_name": type_name}], ensure_ascii=False),
            encoding="utf-8",
        )
        (run_dir / "skipped_table.json").write_text("[]", encoding="utf-8")

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
            "scan_run_id": scan_run_id,
            "scan_summary": {
                "candidate_count_built": 20,
                "candidate_count_selected": candidate_count_selected,
                "skipped_count": 3,
            },
            "order_refresh_summary": {"selected_remote_target_count": 5},
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

    def test_cache_explanation_uses_cycle_scan_run_id(self) -> None:
        with connect(self.db_path) as connection:
            self._seed_scan(connection=connection, scan_run_id="scan-alpha", selected_candidate_count=1)
            self._seed_cycle(
                connection=connection,
                cycle_run_id="jita-passive-cache-cycle-a",
                started_at="2026-04-07T12:00:00Z",
                scan_run_id="scan-alpha",
                candidate_count_selected=1,
            )
            connection.commit()
            explanation = build_jita_passive_cache_opportunity_explanation(
                connection,
                db_path=self.db_path,
                runs_root=self.runs_root,
                log_dir=self.log_dir,
                cycle_run_id=None,
                prefer_latest_selected=False,
                opportunity_id=None,
                type_id=3001,
            )

        self.assertEqual("jita-passive-cache-cycle-a", explanation["cache_cycle_summary"]["cycle_run_id"])
        self.assertEqual("scan-alpha", explanation["scan_summary"]["scan_run_id"])
        self.assertEqual("selected", explanation["matched_status"])
        self.assertEqual("Alpha Item", explanation["selected_snapshot"]["type_name"])
        self.assertEqual("2026-04-07T12:04:00Z", explanation["selected_snapshot"]["source_order_captured_at"])
        self.assertTrue(explanation["cache_cycle_artifact_health"]["report_ready"])

    def test_cache_explanation_can_prefer_latest_cycle_with_selected_candidates(self) -> None:
        with connect(self.db_path) as connection:
            self._seed_scan(connection=connection, scan_run_id="scan-old", selected_candidate_count=1)
            self._seed_cycle(
                connection=connection,
                cycle_run_id="jita-passive-cache-cycle-old",
                started_at="2026-04-07T11:00:00Z",
                scan_run_id="scan-old",
                candidate_count_selected=1,
            )
            self._seed_scan(
                connection=connection,
                scan_run_id="scan-new",
                selected_candidate_count=0,
                opportunity_id="3999-10000002-station-flip",
                type_id=3999,
                type_name="No Result Item",
            )
            self._seed_cycle(
                connection=connection,
                cycle_run_id="jita-passive-cache-cycle-new",
                started_at="2026-04-07T12:00:00Z",
                scan_run_id="scan-new",
                candidate_count_selected=0,
            )
            connection.commit()
            explanation = build_jita_passive_cache_opportunity_explanation(
                connection,
                db_path=self.db_path,
                runs_root=self.runs_root,
                log_dir=self.log_dir,
                cycle_run_id=None,
                prefer_latest_selected=True,
                opportunity_id=None,
                type_id=3001,
            )

        self.assertEqual("jita-passive-cache-cycle-old", explanation["cache_cycle_summary"]["cycle_run_id"])
        self.assertEqual("scan-old", explanation["scan_summary"]["scan_run_id"])
        self.assertEqual("selected", explanation["matched_status"])

    def test_cli_explain_jita_passive_cache_opportunity_prints_payload(self) -> None:
        fake_explanation = {
            "cache_cycle_summary": {"cycle_run_id": "jita-passive-cache-cycle-a"},
            "scan_summary": {"scan_run_id": "scan-alpha"},
            "matched_opportunity_id": "3001-10000002-station-flip",
            "selected_snapshot": {"type_name": "Alpha Item"},
        }
        stdout = io.StringIO()
        argv = [
            "eveq",
            "explain-jita-passive-cache-opportunity",
            "--db-path",
            str(self.db_path),
            "--type-id",
            "3001",
        ]
        with patch(
            "eveqtrading.cli.build_jita_passive_cache_opportunity_explanation",
            return_value=fake_explanation,
        ):
            with patch.object(sys, "argv", argv):
                with redirect_stdout(stdout):
                    exit_code = cli_main()

        output_lines = stdout.getvalue().strip().splitlines()
        self.assertEqual(0, exit_code)
        self.assertTrue(output_lines[0].startswith("Jita passive cache opportunity explanation ready:"))
        payload = json.loads("\n".join(output_lines[1:]))
        self.assertEqual("jita-passive-cache-cycle-a", payload["cache_cycle_summary"]["cycle_run_id"])
        self.assertEqual("Alpha Item", payload["selected_snapshot"]["type_name"])


if __name__ == "__main__":
    unittest.main()
