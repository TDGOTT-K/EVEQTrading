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
from eveqtrading.jita_passive_cache_report import build_jita_passive_cache_report


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT / "sql" / "schema.sql"


class JitaPassiveCacheReportTests(unittest.TestCase):
    def setUp(self) -> None:
        self._temp_dir = tempfile.TemporaryDirectory()
        self.base_dir = Path(self._temp_dir.name)
        self.db_path = self.base_dir / "state" / "eveq.db"
        self.runs_root = self.base_dir / "runs"
        self.runs_root.mkdir(parents=True, exist_ok=True)
        init_db(self.db_path, SCHEMA_PATH)

    def tearDown(self) -> None:
        self._temp_dir.cleanup()

    def _seed_cycle(
        self,
        *,
        connection,
        cycle_run_id: str,
        started_at: str,
        candidate_count_selected: int,
        top_candidates_preview: list[dict[str, object]],
    ) -> None:
        run_dir = self.runs_root / cycle_run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        scan_run_dir = self.runs_root / f"scan-{cycle_run_id}"
        refresh_run_dir = self.runs_root / f"refresh-{cycle_run_id}"
        batch_run_dir = self.runs_root / f"orders-batch-{cycle_run_id}"
        scan_run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "summary.md").write_text("# cache cycle\n", encoding="utf-8")
        (scan_run_dir / "summary.md").write_text("# scan\n", encoding="utf-8")
        (scan_run_dir / "analysis_run.json").write_text("{}", encoding="utf-8")
        (scan_run_dir / "calc_trace.json").write_text("{}", encoding="utf-8")
        (scan_run_dir / "candidate_table.json").write_text("[]", encoding="utf-8")
        (scan_run_dir / "skipped_table.json").write_text("[]", encoding="utf-8")
        payload = {
            "cycle_run_id": cycle_run_id,
            "started_at_utc": started_at,
            "finished_at_utc": "2026-04-07T12:05:00Z",
            "duration_seconds": 300.0,
            "region_id": 10000002,
            "config": {"orders_cache_profile": "warm"},
            "after_order_cache_counts": {"cache_hit_count": 12},
            "order_refresh_cycle_run_id": f"refresh-{cycle_run_id}",
            "order_refresh_cycle_run_dir": str(refresh_run_dir),
            "scan_run_id": f"scan-{cycle_run_id}",
            "scan_summary": {
                "candidate_count_built": 12,
                "candidate_count_selected": candidate_count_selected,
                "skipped_count": 3,
            },
            "order_refresh_summary": {
                "selected_remote_target_count": 5,
                "executed_remote_target_count": 5,
                "orders_batch_run_id": f"orders-batch-{cycle_run_id}",
                "orders_batch_run_dir": str(batch_run_dir),
                "config": {"orders_fetch": {"fetch_mode": "auto"}},
            },
            "top_candidates_preview": top_candidates_preview,
            "files": {
                "order_refresh_cycle_summary_path": str(run_dir / "order-refresh-summary.md"),
                "scan_summary_path": str(scan_run_dir / "summary.md"),
                "scan_analysis_run_path": str(scan_run_dir / "analysis_run.json"),
                "scan_calc_trace_path": str(scan_run_dir / "calc_trace.json"),
                "scan_candidate_table_path": str(scan_run_dir / "candidate_table.json"),
                "scan_skipped_table_path": str(scan_run_dir / "skipped_table.json"),
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
            finished_at="2026-04-07T12:05:00Z",
            status="completed",
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
                            "type_ids": [1001, 1002, 1003, 1004, 1005],
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

    def test_report_reads_latest_cycle_summary(self) -> None:
        with connect(self.db_path) as connection:
            self._seed_orders_fetch_mode_artifacts(cycle_run_id="jita-passive-cache-cycle-a")
            self._seed_cycle(
                connection=connection,
                cycle_run_id="jita-passive-cache-cycle-a",
                started_at="2026-04-07T12:00:00Z",
                candidate_count_selected=2,
                top_candidates_preview=[
                    {"type_id": 1001, "type_name": "Alpha Item"},
                    {"type_id": 1002, "type_name": "Beta Item"},
                ],
            )
            connection.commit()
            report = build_jita_passive_cache_report(
                connection,
                db_path=self.db_path,
                runs_root=self.runs_root,
                cycle_run_id=None,
                prefer_latest_selected=False,
                top_candidates=1,
            )

        self.assertEqual("jita-passive-cache-cycle-a", report["cycle_summary"]["cycle_run_id"])
        self.assertEqual(2, report["cycle_summary"]["candidate_count_selected"])
        self.assertEqual("auto", report["orders_fetch_mode_summary"]["requested_fetch_mode"])
        self.assertEqual("per_type", report["orders_fetch_mode_summary"]["dominant_effective_fetch_mode"])
        self.assertIn("small/light", report["orders_fetch_mode_summary"]["narrative"])
        self.assertEqual(
            "target_set_small_or_predicted_pages_low",
            report["orders_fetch_mode_summary"]["decision_reason_counts"][0]["reason"],
        )
        self.assertEqual("Alpha Item", report["top_candidates_preview"][0]["type_name"])
        self.assertTrue(report["artifact_health"]["report_ready"])
        self.assertTrue(
            report["orders_fetch_mode_summary"]["traceability"]["orders_batch_fetch_json_path"].endswith(
                "orders_batch_fetch.json"
            )
        )

    def test_report_can_prefer_latest_cycle_with_selected_candidates(self) -> None:
        with connect(self.db_path) as connection:
            self._seed_orders_fetch_mode_artifacts(cycle_run_id="jita-passive-cache-cycle-old")
            self._seed_orders_fetch_mode_artifacts(cycle_run_id="jita-passive-cache-cycle-new")
            self._seed_cycle(
                connection=connection,
                cycle_run_id="jita-passive-cache-cycle-old",
                started_at="2026-04-07T11:00:00Z",
                candidate_count_selected=2,
                top_candidates_preview=[{"type_id": 1001, "type_name": "Alpha Item"}],
            )
            self._seed_cycle(
                connection=connection,
                cycle_run_id="jita-passive-cache-cycle-new",
                started_at="2026-04-07T12:00:00Z",
                candidate_count_selected=0,
                top_candidates_preview=[],
            )
            connection.commit()
            report = build_jita_passive_cache_report(
                connection,
                db_path=self.db_path,
                runs_root=self.runs_root,
                cycle_run_id=None,
                prefer_latest_selected=True,
                top_candidates=5,
            )

        self.assertEqual("jita-passive-cache-cycle-old", report["cycle_summary"]["cycle_run_id"])
        self.assertEqual(2, report["cycle_summary"]["candidate_count_selected"])

    def test_cli_report_jita_passive_cache_prints_payload(self) -> None:
        fake_report = {
            "cycle_summary": {
                "cycle_run_id": "jita-passive-cache-cycle-a",
                "candidate_count_selected": 2,
            },
            "top_candidates_preview": [{"type_name": "Alpha Item"}],
        }
        stdout = io.StringIO()
        argv = [
            "eveq",
            "report-jita-passive-cache",
            "--db-path",
            str(self.db_path),
        ]
        with patch("eveqtrading.cli.build_jita_passive_cache_report", return_value=fake_report):
            with patch.object(sys, "argv", argv):
                with redirect_stdout(stdout):
                    exit_code = cli_main()

        output_lines = stdout.getvalue().strip().splitlines()
        self.assertEqual(0, exit_code)
        self.assertTrue(output_lines[0].startswith("Jita passive cache report ready:"))
        payload = json.loads("\n".join(output_lines[1:]))
        self.assertEqual("jita-passive-cache-cycle-a", payload["cycle_summary"]["cycle_run_id"])
        self.assertEqual("Alpha Item", payload["top_candidates_preview"][0]["type_name"])


if __name__ == "__main__":
    unittest.main()
