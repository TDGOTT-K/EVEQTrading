from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from eveqtrading.db import connect, init_db
from eveqtrading.history_batch import run_history_batch_fetch
from eveqtrading.history_fetch import HistoryFetchConfig, resolve_history_fetch_targets


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


class HistoryBatchFetchTests(unittest.TestCase):
    def setUp(self) -> None:
        self._temp_dir = tempfile.TemporaryDirectory()
        self.base_dir = Path(self._temp_dir.name)
        self.db_path = self.base_dir / "state" / "eveq.db"
        self.output_root = self.base_dir / "runs"
        self.output_root.mkdir(parents=True, exist_ok=True)
        self.type_id_file = self.base_dir / "missing_history_type_ids.txt"
        init_db(self.db_path, SCHEMA_PATH)
        self._seed_catalog()
        self.type_id_file.write_text("8101\n8102\n", encoding="utf-8")

    def tearDown(self) -> None:
        self._temp_dir.cleanup()

    def _seed_catalog(self) -> None:
        with connect(self.db_path) as connection:
            connection.executemany(
                "INSERT INTO item_catalog (type_id, type_name, market_group, is_published) VALUES (?, ?, ?, ?)",
                [
                    (8101, "History Success", "Minerals", 1),
                    (8102, "History Missing", "Ships", 1),
                    (8103, "Not Selected", "Modules", 1),
                ],
            )
            connection.commit()

    def test_history_batch_fetch_writes_scope_progress_artifacts(self) -> None:
        def fake_request_history(**kwargs):
            type_id = int(kwargs["type_id"])
            if type_id == 8101:
                return {
                    "ok": True,
                    "status_code": 200,
                    "url": "https://example.invalid/history/8101",
                    "retries_used": 0,
                    "headers": {},
                    "entries": _build_history_entries(),
                    "error": None,
                }
            return {
                "ok": False,
                "status_code": 404,
                "url": "https://example.invalid/history/8102",
                "retries_used": 0,
                "headers": {},
                "entries": [],
                "error": "not found",
            }

        with patch("eveqtrading.history_fetch._request_history", side_effect=fake_request_history):
            with connect(self.db_path) as connection:
                type_targets = resolve_history_fetch_targets(
                    connection,
                    explicit_type_ids=[],
                    type_id_file=self.type_id_file,
                    all_published_market_types=False,
                    limit=None,
                )
                result = run_history_batch_fetch(
                    connection,
                    output_root=self.output_root,
                    type_targets=type_targets,
                    config=HistoryFetchConfig(
                        region_id=10000002,
                        datasource="tranquility",
                        esi_base_url="https://esi.evetech.net/latest",
                        user_agent="test-history-batch-fetch",
                        request_timeout_seconds=5.0,
                        max_retries=0,
                        pause_seconds=0.0,
                    ),
                    chunk_size=1,
                    max_chunks=None,
                )
                history_row_count = connection.execute(
                    "SELECT COUNT(*) FROM market_history_daily WHERE region_id = ? AND type_id = ?",
                    (10000002, 8101),
                ).fetchone()[0]

        payload = result["payload"]
        self.assertEqual(2, payload["totals"]["selected_target_count"])
        self.assertEqual(2, payload["totals"]["executed_target_count"])
        self.assertEqual(2, payload["totals"]["executed_chunk_count"])
        self.assertEqual(1, payload["totals"]["success"])
        self.assertEqual(1, payload["totals"]["not_found"])
        self.assertEqual(0, payload["totals"]["failed"])
        self.assertEqual(30, payload["totals"]["history_rows_written"])
        self.assertEqual(0, payload["before_executed_scope_counts"]["history_ready_count"])
        self.assertEqual(1, payload["after_executed_scope_counts"]["history_ready_count"])
        self.assertEqual(1, payload["remaining_missing_history_count_in_executed_scope"])
        self.assertEqual(30, history_row_count)

        files = payload["files"]
        self.assertEqual("8101\n8102\n", Path(files["requested_type_ids_path"]).read_text(encoding="utf-8"))
        self.assertEqual("8101\n8102\n", Path(files["executed_type_ids_path"]).read_text(encoding="utf-8"))
        self.assertEqual("8101\n", Path(files["history_ready_type_ids_path"]).read_text(encoding="utf-8"))
        self.assertEqual(
            "8102\n",
            Path(files["remaining_missing_history_type_ids_path"]).read_text(encoding="utf-8"),
        )
        self.assertTrue((result["run_dir"] / "history_batch_fetch.json").exists())


if __name__ == "__main__":
    unittest.main()
