from __future__ import annotations

import io
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from eveqtrading.character_account_report import build_character_account_report
from eveqtrading.cli import main as cli_main
from eveqtrading.db import connect, init_db


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT / "sql" / "schema.sql"


class CharacterAccountReportTests(unittest.TestCase):
    def setUp(self) -> None:
        self._temp_dir = tempfile.TemporaryDirectory()
        self.base_dir = Path(self._temp_dir.name)
        self.db_path = self.base_dir / "state" / "eveq.db"
        self.runs_root = self.base_dir / "runs"
        self.output_root = self.base_dir / "reports"
        self.runs_root.mkdir(parents=True, exist_ok=True)
        self.output_root.mkdir(parents=True, exist_ok=True)
        init_db(self.db_path, SCHEMA_PATH)
        self.account_run_id = "character-account-20260411T010000Z-abcdef12"
        self.account_run_dir = self.runs_root / self.account_run_id
        self.account_run_dir.mkdir(parents=True, exist_ok=True)
        snapshot = {
            "run_id": self.account_run_id,
            "character": {
                "character_id": 12345,
                "character_name": "Report Pilot",
                "owner_hash": "owner-hash-1",
            },
            "selection_context": {
                "mode": "default_linked_character",
                "used_explicit_selection": False,
                "linked_character_count": 2,
                "requested_character_id": None,
                "requested_character_name": None,
                "requested_owner_hash": None,
                "resolved_character_id": 12345,
                "resolved_character_name": "Report Pilot",
                "resolved_owner_hash": "owner-hash-1",
            },
            "wallet": {
                "balance": 8_500_000_000.0,
                "trace": {
                    "url": "https://esi.evetech.net/latest/characters/12345/wallet?datasource=tranquility",
                    "status_code": 200,
                    "fetched_at": "2026-04-11T01:00:00Z",
                },
            },
            "assets": {
                "summary": {"count": 12, "type_count": 8, "total_quantity": 600},
                "trace": {"pages_total": 2, "requests": [{"page": 1}, {"page": 2}]},
            },
            "orders": {
                "summary": {
                    "count": 5,
                    "buy_count": 2,
                    "sell_count": 3,
                    "remaining_buy_value": 1_250_000_000.0,
                    "remaining_sell_value": 4_800_000_000.0,
                },
                "trace": {
                    "url": "https://esi.evetech.net/latest/characters/12345/orders?datasource=tranquility",
                    "status_code": 200,
                    "fetched_at": "2026-04-11T01:01:00Z",
                },
            },
            "order_history": {
                "summary": {"count": 20, "buy_count": 8, "sell_count": 12, "status_counts": {"expired": 12, "cancelled": 8}},
                "trace": {"pages_total": 3, "requests": [{"page": 1}, {"page": 2}, {"page": 3}]},
            },
            "files": {
                "wallet_path": str(self.account_run_dir / "wallet.json"),
                "assets_path": str(self.account_run_dir / "assets.json"),
                "orders_path": str(self.account_run_dir / "orders.json"),
                "order_history_path": str(self.account_run_dir / "order_history.json"),
                "summary_path": str(self.account_run_dir / "summary.md"),
            },
        }
        (self.account_run_dir / "account_snapshot.json").write_text(
            json.dumps(snapshot, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def tearDown(self) -> None:
        self._temp_dir.cleanup()

    def test_build_character_account_report_writes_artifacts(self) -> None:
        report = build_character_account_report(
            db_path=self.db_path,
            runs_root=self.runs_root,
            output_root=self.output_root,
            account_snapshot_run_id=self.account_run_id,
        )
        run_dir = Path(report["report_run_dir"])
        self.assertTrue((run_dir / "character_account_report.json").exists())
        self.assertTrue((run_dir / "character_account_report.md").exists())
        self.assertEqual(8_500_000_000.0, report["operator_summary"]["liquid_isk"])
        self.assertEqual(1_250_000_000.0, report["operator_summary"]["buy_commitment_isk"])
        markdown = (run_dir / "character_account_report.md").read_text(encoding="utf-8")
        self.assertIn("角色资金面报告", markdown)
        self.assertIn("当前可用于顾问预算: 85.00亿 ISK", markdown)
        self.assertIn("角色选择上下文", markdown)
        self.assertIn("选择模式: default_linked_character", markdown)

    def test_cli_report_character_account_prints_json(self) -> None:
        fake_report = {
            "report_run_id": "character-account-report-20260411T020000Z-aaaabbbb",
            "report_run_dir": str(self.output_root / "character-account-report-20260411T020000Z-aaaabbbb"),
        }
        stdout = io.StringIO()
        argv = [
            "eveq",
            "report-character-account",
            "--db-path",
            str(self.db_path),
            "--runs-root",
            str(self.runs_root),
            "--output-root",
            str(self.output_root),
        ]
        with patch("eveqtrading.cli.build_character_account_report", return_value=fake_report):
            with patch.object(sys, "argv", argv):
                with redirect_stdout(stdout):
                    exit_code = cli_main()

        self.assertEqual(0, exit_code)
        self.assertEqual(fake_report, json.loads(stdout.getvalue()))
