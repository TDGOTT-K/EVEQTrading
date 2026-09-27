from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from eveqtrading.coverage_worker import _write_state, run_coverage_worker_once
from eveqtrading.db import init_db
from eveqtrading.history_fetch import HistoryFetchConfig
from eveqtrading.order_fetch import OrderFetchConfig


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT / "sql" / "schema.sql"


class CoverageWorkerTests(unittest.TestCase):
    def setUp(self) -> None:
        self._temp_dir = tempfile.TemporaryDirectory()
        self.base_dir = Path(self._temp_dir.name)
        self.db_path = self.base_dir / "state" / "eveq.db"
        self.output_root = self.base_dir / "runs"
        self.lock_path = self.base_dir / "tmp" / "coverage-worker.lock"
        self.state_path = self.base_dir / "state" / "coverage-worker-state.json"
        self.output_root.mkdir(parents=True, exist_ok=True)
        init_db(self.db_path, SCHEMA_PATH)

    def tearDown(self) -> None:
        self._temp_dir.cleanup()

    def _history_config(self) -> HistoryFetchConfig:
        return HistoryFetchConfig(
            region_id=10000002,
            datasource="tranquility",
            esi_base_url="https://esi.evetech.net/latest",
            user_agent="test-worker-history",
            request_timeout_seconds=5.0,
            max_retries=0,
            pause_seconds=0.0,
        )

    def _orders_config(self) -> OrderFetchConfig:
        return OrderFetchConfig(
            region_id=10000002,
            datasource="tranquility",
            esi_base_url="https://esi.evetech.net/latest",
            user_agent="test-worker-orders",
            request_timeout_seconds=5.0,
            max_retries=0,
            pause_seconds=0.0,
            order_type="all",
        )

    def test_worker_writes_completed_state_and_artifact(self) -> None:
        fake_cycle_result = {
            "cycle_run_id": "coverage-cycle-test",
            "run_dir": self.output_root / "coverage-cycle-test",
            "payload": {
                "after_orders_counts": {"ready_count": 9},
                "scan_run_id": "scan-test",
            },
        }
        fake_cycle_result["run_dir"].mkdir(parents=True, exist_ok=True)

        with patch("eveqtrading.coverage_worker.run_coverage_cycle", return_value=fake_cycle_result):
            result = run_coverage_worker_once(
                db_path=self.db_path,
                output_root=self.output_root,
                lock_path=self.lock_path,
                state_path=self.state_path,
                region_id=10000002,
                history_chunk_size=1,
                max_history_chunks=1,
                orders_chunk_size=1,
                max_orders_chunks=1,
                benchmark_chunk_size=1000,
                benchmark_max_chunks=None,
                history_config=self._history_config(),
                orders_config=self._orders_config(),
                capital_budget_isk=1_000_000_000.0,
                test_position_isk=250_000_000.0,
                top_n=10,
                strategy_set="v1-db-region",
                parameter_version="v1-db",
            )

        state_payload = json.loads(self.state_path.read_text(encoding="utf-8"))
        self.assertEqual("completed", result["status"])
        self.assertEqual("completed", state_payload["status"])
        self.assertEqual("coverage-cycle-test", result["cycle_run_id"])
        self.assertEqual("scan-test", result["cycle_summary"]["scan_run_id"])
        self.assertFalse(self.lock_path.exists())
        self.assertTrue((Path(result["worker_run_dir"]) / "coverage_worker_run.json").exists())
        self.assertFalse((self.state_path.parent / f".{self.state_path.name}.tmp").exists())

    def test_write_state_replaces_file_atomically(self) -> None:
        _write_state(self.state_path, {"status": "running"})
        _write_state(self.state_path, {"status": "completed", "worker_run_id": "coverage-worker-test"})

        state_payload = json.loads(self.state_path.read_text(encoding="utf-8"))
        self.assertEqual("completed", state_payload["status"])
        self.assertEqual("coverage-worker-test", state_payload["worker_run_id"])
        self.assertFalse((self.state_path.parent / f".{self.state_path.name}.tmp").exists())

    def test_worker_skips_when_lock_exists(self) -> None:
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        self.lock_path.write_text(
            json.dumps(
                {
                    "worker_run_id": "other-worker",
                    "locked_at_utc": "2026-04-06T00:00:00Z",
                    "pid": 9999,
                },
                ensure_ascii=False,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )

        with patch("eveqtrading.coverage_worker.run_coverage_cycle") as mocked_cycle:
            result = run_coverage_worker_once(
                db_path=self.db_path,
                output_root=self.output_root,
                lock_path=self.lock_path,
                state_path=self.state_path,
                region_id=10000002,
                history_chunk_size=1,
                max_history_chunks=1,
                orders_chunk_size=1,
                max_orders_chunks=1,
                benchmark_chunk_size=1000,
                benchmark_max_chunks=None,
                history_config=self._history_config(),
                orders_config=self._orders_config(),
                capital_budget_isk=1_000_000_000.0,
                test_position_isk=250_000_000.0,
                top_n=10,
                strategy_set="v1-db-region",
                parameter_version="v1-db",
            )

        mocked_cycle.assert_not_called()
        state_payload = json.loads(self.state_path.read_text(encoding="utf-8"))
        self.assertEqual("skipped_locked", result["status"])
        self.assertEqual("skipped_locked", state_payload["status"])
        self.assertEqual("other-worker", result["lock_info"]["worker_run_id"])
        self.assertTrue(self.lock_path.exists())

    def test_worker_reclaims_stale_dead_pid_lock_and_runs(self) -> None:
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        self.lock_path.write_text(
            json.dumps(
                {
                    "worker_run_id": "stale-worker",
                    "locked_at_utc": "2023-11-14T00:00:00Z",
                    "pid": 999999,
                },
                ensure_ascii=False,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        stale_time = 1_700_000_000
        os.utime(self.lock_path, (stale_time, stale_time))

        fake_cycle_result = {
            "cycle_run_id": "coverage-cycle-test",
            "run_dir": self.output_root / "coverage-cycle-test",
            "payload": {
                "after_orders_counts": {"ready_count": 9},
                "scan_run_id": "scan-test",
            },
        }
        fake_cycle_result["run_dir"].mkdir(parents=True, exist_ok=True)

        with patch("eveqtrading.coverage_worker.datetime") as mocked_datetime:
            real_datetime = __import__("datetime").datetime
            mocked_datetime.now.return_value = real_datetime.fromtimestamp(
                stale_time + 300,
                tz=__import__("datetime").timezone.utc,
            )
            mocked_datetime.fromisoformat = real_datetime.fromisoformat
            with patch("eveqtrading.coverage_worker.run_coverage_cycle", return_value=fake_cycle_result):
                result = run_coverage_worker_once(
                    db_path=self.db_path,
                    output_root=self.output_root,
                    lock_path=self.lock_path,
                    state_path=self.state_path,
                    region_id=10000002,
                    history_chunk_size=1,
                    max_history_chunks=1,
                    orders_chunk_size=1,
                    max_orders_chunks=1,
                    benchmark_chunk_size=1000,
                    benchmark_max_chunks=None,
                    history_config=self._history_config(),
                    orders_config=self._orders_config(),
                    capital_budget_isk=1_000_000_000.0,
                    test_position_isk=250_000_000.0,
                    top_n=10,
                    strategy_set="v1-db-region",
                    parameter_version="v1-db",
                )

        self.assertEqual("completed", result["status"])
        self.assertFalse(self.lock_path.exists())

    def test_worker_skips_during_failure_backoff_window(self) -> None:
        _write_state(
            self.state_path,
            {
                "worker_run_id": "coverage-worker-failed",
                "status": "failed",
                "started_at_utc": "2026-04-06T02:00:00Z",
                "finished_at_utc": "2026-04-06T02:05:00Z",
            },
        )

        with patch("eveqtrading.coverage_worker._utc_now_iso", side_effect=["2026-04-06T02:10:00Z", "2026-04-06T02:10:00Z"]):
            with patch("eveqtrading.coverage_worker.run_coverage_cycle") as mocked_cycle:
                result = run_coverage_worker_once(
                    db_path=self.db_path,
                    output_root=self.output_root,
                    lock_path=self.lock_path,
                    state_path=self.state_path,
                    region_id=10000002,
                    history_chunk_size=1,
                    max_history_chunks=1,
                    orders_chunk_size=1,
                    max_orders_chunks=1,
                    benchmark_chunk_size=1000,
                    benchmark_max_chunks=None,
                    history_config=self._history_config(),
                    orders_config=self._orders_config(),
                    capital_budget_isk=1_000_000_000.0,
                    test_position_isk=250_000_000.0,
                    top_n=10,
                    strategy_set="v1-db-region",
                    parameter_version="v1-db",
                    failure_backoff_seconds=1800,
                )

        mocked_cycle.assert_not_called()
        state_payload = json.loads(self.state_path.read_text(encoding="utf-8"))
        self.assertEqual("skipped_backoff", result["status"])
        self.assertEqual("skipped_backoff", state_payload["status"])
        self.assertEqual("coverage-worker-failed", result["backoff_source_worker_run_id"])
        self.assertEqual("2026-04-06T02:35:00Z", result["next_allowed_at_utc"])
        self.assertFalse(self.lock_path.exists())


if __name__ == "__main__":
    unittest.main()
