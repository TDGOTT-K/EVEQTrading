from __future__ import annotations

import json
import os
import tempfile
import time
import unittest
from pathlib import Path

from eveqtrading.retention import run_retention_cleanup


def _set_mtime(path: Path, timestamp: float) -> None:
    os.utime(path, (timestamp, timestamp))


class RetentionCleanupTests(unittest.TestCase):
    def setUp(self) -> None:
        self._temp_dir = tempfile.TemporaryDirectory()
        self.base_dir = Path(self._temp_dir.name)
        self.output_root = self.base_dir / "runs"
        self.log_dir = self.base_dir / "logs"
        self.staging_dir = self.base_dir / "tmp" / "foreign-sync"
        self.db_path = self.base_dir / "data" / "eveq.db"
        self.output_root.mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.staging_dir.mkdir(parents=True, exist_ok=True)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.db_path.write_text("db", encoding="utf-8")
        self._seed_files()

    def tearDown(self) -> None:
        self._temp_dir.cleanup()

    def _seed_files(self) -> None:
        now = time.time()
        run_old = self.output_root / "scan-old"
        run_mid = self.output_root / "scan-mid"
        run_new = self.output_root / "scan-new"
        for path in (run_old, run_mid, run_new):
            path.mkdir()
            (path / "summary.md").write_text("# summary\n", encoding="utf-8")

        log_old = self.log_dir / "eveq-coverage-worker-20260406T010000Z.log"
        log_new = self.log_dir / "eveq-coverage-worker-20260406T020000Z.log"
        sync_old = self.log_dir / "eveq-foreign-sync-20260406T010000Z.json"
        sync_new = self.log_dir / "eveq-foreign-sync-20260406T020000Z.json"
        staging_old = self.staging_dir / "eveq.db"
        prev_db = Path(str(self.db_path) + ".prev")
        for path in (log_old, log_new, sync_old, sync_new, staging_old, prev_db):
            path.write_text("x", encoding="utf-8")

        timestamps = {
            run_old: now - 300,
            run_mid: now - 200,
            run_new: now - 100,
            log_old: now - 300,
            log_new: now - 100,
            sync_old: now - 300,
            sync_new: now - 100,
            staging_old: now - 7_200,
            prev_db: now - 7_200,
        }
        for path, timestamp in timestamps.items():
            _set_mtime(path, timestamp)

    def test_cleanup_deletes_old_entries_and_writes_artifact(self) -> None:
        payload = run_retention_cleanup(
            output_root=self.output_root,
            log_dir=self.log_dir,
            staging_dir=self.staging_dir,
            db_path=self.db_path,
            retain_run_count=2,
            retain_log_count=1,
            retain_sync_report_count=1,
            staging_max_age_hours=1,
            prev_db_max_age_hours=1,
            dry_run=False,
        )

        self.assertEqual(5, payload["summary"]["deleted_count"])
        self.assertFalse((self.output_root / "scan-old").exists())
        self.assertTrue((self.output_root / "scan-mid").exists())
        self.assertTrue((self.output_root / "scan-new").exists())
        self.assertFalse((self.log_dir / "eveq-coverage-worker-20260406T010000Z.log").exists())
        self.assertFalse((self.log_dir / "eveq-foreign-sync-20260406T010000Z.json").exists())
        self.assertFalse((self.staging_dir / "eveq.db").exists())
        self.assertFalse(Path(str(self.db_path) + ".prev").exists())

        artifact_path = Path(payload["cleanup_run_dir"]) / "retention_cleanup.json"
        self.assertTrue(artifact_path.exists())
        artifact_payload = json.loads(artifact_path.read_text(encoding="utf-8"))
        self.assertEqual(payload["cleanup_run_id"], artifact_payload["cleanup_run_id"])

    def test_cleanup_dry_run_keeps_files(self) -> None:
        payload = run_retention_cleanup(
            output_root=self.output_root,
            log_dir=self.log_dir,
            staging_dir=self.staging_dir,
            db_path=self.db_path,
            retain_run_count=2,
            retain_log_count=1,
            retain_sync_report_count=1,
            staging_max_age_hours=1,
            prev_db_max_age_hours=1,
            dry_run=True,
        )

        self.assertEqual(5, payload["summary"]["candidate_count"])
        self.assertEqual(0, payload["summary"]["deleted_count"])
        self.assertTrue((self.output_root / "scan-old").exists())
        self.assertTrue((self.log_dir / "eveq-coverage-worker-20260406T010000Z.log").exists())
        self.assertTrue((self.log_dir / "eveq-foreign-sync-20260406T010000Z.json").exists())
        self.assertTrue((self.staging_dir / "eveq.db").exists())
        self.assertTrue(Path(str(self.db_path) + ".prev").exists())
