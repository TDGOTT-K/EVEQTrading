from __future__ import annotations

import time
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from eveqtrading.db import connect, init_db
from eveqtrading.order_batch import run_order_batch_fetch
from eveqtrading.order_fetch import OrderFetchConfig, fetch_market_orders, resolve_order_fetch_targets


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT / "sql" / "schema.sql"


def _build_order_entries(*, type_id: int) -> list[dict[str, object]]:
    return [
        {
            "order_id": type_id * 1000 + 1,
            "type_id": type_id,
            "is_buy_order": True,
            "price": 98.0,
            "volume_remain": 1000.0,
            "volume_total": 1000.0,
            "min_volume": 1.0,
            "range": "region",
            "issued": "2026-04-06T00:00:00Z",
            "duration": 90,
            "location_id": 60003760,
        },
        {
            "order_id": type_id * 1000 + 2,
            "type_id": type_id,
            "is_buy_order": False,
            "price": 101.0,
            "volume_remain": 500.0,
            "volume_total": 500.0,
            "min_volume": 1.0,
            "range": "region",
            "issued": "2026-04-06T00:00:00Z",
            "duration": 90,
            "location_id": 60003760,
        },
    ]


class OrderBatchFetchTests(unittest.TestCase):
    def setUp(self) -> None:
        self._temp_dir = tempfile.TemporaryDirectory()
        self.base_dir = Path(self._temp_dir.name)
        self.db_path = self.base_dir / "state" / "eveq.db"
        self.output_root = self.base_dir / "runs"
        self.output_root.mkdir(parents=True, exist_ok=True)
        self.type_id_file = self.base_dir / "missing_orders_type_ids.txt"
        init_db(self.db_path, SCHEMA_PATH)
        self._seed_catalog_and_history()
        self.type_id_file.write_text("8201\n8202\n", encoding="utf-8")

    def tearDown(self) -> None:
        self._temp_dir.cleanup()

    def _seed_catalog_and_history(self) -> None:
        with connect(self.db_path) as connection:
            connection.executemany(
                "INSERT INTO item_catalog (type_id, type_name, market_group, is_published) VALUES (?, ?, ?, ?)",
                [
                    (8201, "Orders Success", "Minerals", 1),
                    (8202, "Orders Missing", "Ships", 1),
                    (8203, "Not Selected", "Modules", 1),
                ],
            )
            for day_index in range(30):
                for type_id in (8201, 8202):
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
            connection.commit()

    def _seed_history_for_type(self, type_id: int) -> None:
        with connect(self.db_path) as connection:
            for day_index in range(30):
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
            connection.commit()

    def _seed_order_snapshot_state(
        self,
        *,
        type_id: int,
        snapshot_id: str,
        next_scheduled_at: str = "2099-01-01T00:00:00Z",
    ) -> None:
        with connect(self.db_path) as connection:
            connection.execute(
                """
                INSERT INTO market_order_snapshot_head (
                    snapshot_id, captured_at, scope_kind, region_id, location_id, account_id, source, refresh_mode
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    snapshot_id,
                    "2026-04-06T00:01:00Z",
                    "region_type_orders:all",
                    10000002,
                    None,
                    None,
                    "test",
                    "replace",
                ),
            )
            connection.executemany(
                """
                INSERT INTO market_order_snapshot_line (
                    snapshot_id, order_id, type_id, is_buy_order, price, volume_remain, volume_total, min_volume,
                    order_range, issued_at, duration_days, location_id, region_id, account_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        snapshot_id,
                        type_id * 1000 + 1,
                        type_id,
                        1,
                        98.0,
                        1000.0,
                        1000.0,
                        1.0,
                        "region",
                        "2026-04-06T00:00:00Z",
                        90,
                        60003760,
                        10000002,
                        None,
                    ),
                    (
                        snapshot_id,
                        type_id * 1000 + 2,
                        type_id,
                        0,
                        101.0,
                        500.0,
                        500.0,
                        1.0,
                        "region",
                        "2026-04-06T00:00:00Z",
                        90,
                        60003760,
                        10000002,
                        None,
                    ),
                ],
            )
            connection.execute(
                """
                INSERT INTO sync_scope_state (
                    scope_key, scope_type, region_id, type_id, last_success_at, last_attempt_at,
                    next_scheduled_at, failure_count, last_error, coverage_marker
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    f"market_orders:10000002:{type_id}:all",
                    "market_orders",
                    10000002,
                    type_id,
                    "2026-04-06T00:01:00Z",
                    "2026-04-06T00:01:00Z",
                    next_scheduled_at,
                    0,
                    None,
                    f"ok:{snapshot_id}",
                ),
            )
            connection.commit()

    def test_orders_batch_fetch_writes_scope_progress_artifacts(self) -> None:
        def fake_request_all_orders_for_type(**kwargs):
            type_id = int(kwargs["type_id"])
            if type_id == 8201:
                return {
                    "ok": True,
                    "status_code": 200,
                    "entries": _build_order_entries(type_id=type_id),
                    "page_traces": [],
                    "error": None,
                    "page_count": 1,
                    "retries_used": 0,
                }
            return {
                "ok": False,
                "status_code": 404,
                "entries": [],
                "page_traces": [],
                "error": "not found",
                "page_count": 1,
                "retries_used": 0,
            }

        with patch("eveqtrading.order_fetch._request_all_orders_for_type", side_effect=fake_request_all_orders_for_type):
            with connect(self.db_path) as connection:
                type_targets = resolve_order_fetch_targets(
                    connection,
                    explicit_type_ids=[],
                    type_id_file=self.type_id_file,
                    all_published_market_types=False,
                    limit=None,
                )
                result = run_order_batch_fetch(
                    connection,
                    output_root=self.output_root,
                    type_targets=type_targets,
                    config=OrderFetchConfig(
                        region_id=10000002,
                        datasource="tranquility",
                        esi_base_url="https://esi.evetech.net/latest",
                        user_agent="test-orders-batch-fetch",
                        request_timeout_seconds=5.0,
                        max_retries=0,
                        pause_seconds=0.0,
                        order_type="all",
                    ),
                    chunk_size=1,
                    max_chunks=None,
                )
                orders_row_count = connection.execute(
                    "SELECT COUNT(*) FROM market_order_snapshot_line WHERE region_id = ? AND type_id = ?",
                    (10000002, 8201),
                ).fetchone()[0]

        payload = result["payload"]
        self.assertEqual(2, payload["totals"]["selected_target_count"])
        self.assertEqual(2, payload["totals"]["executed_target_count"])
        self.assertEqual(2, payload["totals"]["executed_chunk_count"])
        self.assertEqual(1, payload["totals"]["success"])
        self.assertEqual(1, payload["totals"]["not_found"])
        self.assertEqual(0, payload["totals"]["failed"])
        self.assertEqual(2, payload["totals"]["orders_written"])
        self.assertEqual(1, payload["totals"]["snapshots_written"])
        self.assertEqual(2, payload["totals"]["pages_fetched"])
        self.assertEqual(0, payload["before_executed_scope_counts"]["orders_ready_count"])
        self.assertEqual(1, payload["after_executed_scope_counts"]["orders_ready_count"])
        self.assertEqual(0, payload["before_executed_scope_counts"]["ready_count"])
        self.assertEqual(1, payload["after_executed_scope_counts"]["ready_count"])
        self.assertEqual(1, payload["remaining_missing_orders_count_in_executed_scope"])
        self.assertEqual(2, orders_row_count)

        files = payload["files"]
        self.assertEqual("8201\n8202\n", Path(files["requested_type_ids_path"]).read_text(encoding="utf-8"))
        self.assertEqual("8201\n8202\n", Path(files["executed_type_ids_path"]).read_text(encoding="utf-8"))
        self.assertEqual("8201\n", Path(files["orders_ready_type_ids_path"]).read_text(encoding="utf-8"))
        self.assertEqual("8201\n", Path(files["ready_type_ids_path"]).read_text(encoding="utf-8"))
        self.assertEqual(
            "8202\n",
            Path(files["remaining_missing_orders_type_ids_path"]).read_text(encoding="utf-8"),
        )
        self.assertTrue((result["run_dir"] / "orders_batch_fetch.json").exists())

    def test_fetch_market_orders_skips_remote_request_when_snapshot_is_still_fresh(self) -> None:
        with connect(self.db_path) as connection:
            connection.execute(
                """
                INSERT INTO market_order_snapshot_head (
                    snapshot_id, captured_at, scope_kind, region_id, location_id, account_id, source, refresh_mode
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "snapshot-fresh-8201",
                    "2026-04-06T00:01:00Z",
                    "region_type_orders:all",
                    10000002,
                    None,
                    None,
                    "test",
                    "replace",
                ),
            )
            connection.executemany(
                """
                INSERT INTO market_order_snapshot_line (
                    snapshot_id, order_id, type_id, is_buy_order, price, volume_remain, volume_total, min_volume,
                    order_range, issued_at, duration_days, location_id, region_id, account_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        "snapshot-fresh-8201",
                        8201001,
                        8201,
                        1,
                        98.0,
                        1000.0,
                        1000.0,
                        1.0,
                        "region",
                        "2026-04-06T00:00:00Z",
                        90,
                        60003760,
                        10000002,
                        None,
                    ),
                    (
                        "snapshot-fresh-8201",
                        8201002,
                        8201,
                        0,
                        101.0,
                        500.0,
                        500.0,
                        1.0,
                        "region",
                        "2026-04-06T00:00:00Z",
                        90,
                        60003760,
                        10000002,
                        None,
                    ),
                ],
            )
            connection.execute(
                """
                INSERT INTO sync_scope_state (
                    scope_key, scope_type, region_id, type_id, last_success_at, last_attempt_at,
                    next_scheduled_at, failure_count, last_error, coverage_marker
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "market_orders:10000002:8201:all",
                    "market_orders",
                    10000002,
                    8201,
                    "2026-04-06T00:01:00Z",
                    "2026-04-06T00:01:00Z",
                    "2099-01-01T00:00:00Z",
                    0,
                    None,
                    "ok:snapshot-fresh-8201",
                ),
            )
            connection.commit()

            type_targets = resolve_order_fetch_targets(
                connection,
                explicit_type_ids=[8201],
                type_id_file=None,
                all_published_market_types=False,
                limit=None,
            )

            with patch("eveqtrading.order_fetch._request_all_orders_for_type") as mocked_request:
                result = fetch_market_orders(
                    connection,
                    output_root=self.output_root,
                    type_targets=type_targets,
                    config=OrderFetchConfig(
                        region_id=10000002,
                        datasource="tranquility",
                        esi_base_url="https://esi.evetech.net/latest",
                        user_agent="test-orders-fresh-skip",
                        request_timeout_seconds=5.0,
                        max_retries=0,
                        pause_seconds=0.0,
                        order_type="all",
                    ),
                )

        mocked_request.assert_not_called()
        fetch_run = result["fetch_run"]
        self.assertEqual(1, fetch_run["type_count_requested"])
        self.assertEqual(0, fetch_run["type_count_success"])
        self.assertEqual(1, fetch_run["type_count_skipped_fresh"])
        self.assertEqual(0, fetch_run["type_count_failed"])
        self.assertEqual(0, fetch_run["pages_fetched"])
        trace_row = result["fetch_trace"]["type_traces"][0]
        self.assertTrue(trace_row["skipped_fresh"])
        self.assertEqual("2099-01-01T00:00:00Z", trace_row["next_scheduled_at"])
        self.assertEqual(2, trace_row["order_count"])

    def test_fetch_market_orders_preserves_trace_order_when_requests_finish_out_of_order(self) -> None:
        def fake_request_all_orders_for_type(**kwargs):
            type_id = int(kwargs["type_id"])
            if type_id == 8201:
                time.sleep(0.02)
            return {
                "ok": True,
                "status_code": 200,
                "entries": _build_order_entries(type_id=type_id),
                "page_traces": [],
                "error": None,
                "page_count": 1,
                "retries_used": 0,
            }

        with connect(self.db_path) as connection:
            type_targets = resolve_order_fetch_targets(
                connection,
                explicit_type_ids=[],
                type_id_file=self.type_id_file,
                all_published_market_types=False,
                limit=None,
            )

            with patch("eveqtrading.order_fetch._request_all_orders_for_type", side_effect=fake_request_all_orders_for_type):
                result = fetch_market_orders(
                    connection,
                    output_root=self.output_root,
                    type_targets=type_targets,
                    config=OrderFetchConfig(
                        region_id=10000002,
                        datasource="tranquility",
                        esi_base_url="https://esi.evetech.net/latest",
                        user_agent="test-orders-concurrent",
                        request_timeout_seconds=5.0,
                        max_retries=0,
                        pause_seconds=0.0,
                        order_type="all",
                        concurrent_requests=2,
                    ),
                )

        self.assertEqual(
            [8201, 8202],
            [row["type_id"] for row in result["fetch_trace"]["type_traces"]],
        )
        self.assertEqual(
            [1, 2],
            [row["ordinal"] for row in result["fetch_trace"]["type_traces"]],
        )

    def test_fetch_market_orders_routes_plex_to_global_market_but_stores_logical_region(self) -> None:
        with connect(self.db_path) as connection:
            connection.execute(
                "INSERT INTO item_catalog (type_id, type_name, market_group, is_published) VALUES (?, ?, ?, ?)",
                (44992, "伊甸币", "PLEX", 1),
            )
            connection.commit()

            seen_region_ids: list[int] = []

            def fake_request_all_orders_for_type(**kwargs):
                seen_region_ids.append(int(kwargs["region_id"]))
                return {
                    "ok": True,
                    "status_code": 200,
                    "entries": _build_order_entries(type_id=44992),
                    "page_traces": [],
                    "error": None,
                    "page_count": 1,
                    "retries_used": 0,
                }

            with patch("eveqtrading.order_fetch._request_all_orders_for_type", side_effect=fake_request_all_orders_for_type):
                result = fetch_market_orders(
                    connection,
                    output_root=self.output_root,
                    type_targets=[{"type_id": 44992, "type_name": "伊甸币", "selection_mode": "explicit"}],
                    config=OrderFetchConfig(
                        region_id=10000002,
                        datasource="tranquility",
                        esi_base_url="https://esi.evetech.net/latest",
                        user_agent="test-orders-plex-route",
                        request_timeout_seconds=5.0,
                        max_retries=0,
                        pause_seconds=0.0,
                        order_type="all",
                    ),
                )

            snapshot_id = str(result["fetch_trace"]["type_traces"][0]["snapshot_summary"]["snapshot_id"])
            head_row = connection.execute(
                "SELECT region_id FROM market_order_snapshot_head WHERE snapshot_id = ?",
                (snapshot_id,),
            ).fetchone()
            line_row = connection.execute(
                "SELECT region_id FROM market_order_snapshot_line WHERE snapshot_id = ? ORDER BY order_id LIMIT 1",
                (snapshot_id,),
            ).fetchone()

        self.assertEqual([19000001], seen_region_ids)
        self.assertEqual(10000002, head_row["region_id"])
        self.assertEqual(10000002, line_row["region_id"])

    def test_fetch_market_orders_batches_storage_commits(self) -> None:
        self._seed_history_for_type(8203)

        def fake_request_all_orders_for_type(**kwargs):
            type_id = int(kwargs["type_id"])
            return {
                "ok": True,
                "status_code": 200,
                "entries": _build_order_entries(type_id=type_id),
                "page_traces": [],
                "error": None,
                "page_count": 1,
                "retries_used": 0,
            }

        with connect(self.db_path) as connection:
            type_targets = resolve_order_fetch_targets(
                connection,
                explicit_type_ids=[8201, 8202, 8203],
                type_id_file=None,
                all_published_market_types=False,
                limit=None,
            )

            with patch("eveqtrading.order_fetch._request_all_orders_for_type", side_effect=fake_request_all_orders_for_type):
                result = fetch_market_orders(
                    connection,
                    output_root=self.output_root,
                    type_targets=type_targets,
                    config=OrderFetchConfig(
                        region_id=10000002,
                        datasource="tranquility",
                        esi_base_url="https://esi.evetech.net/latest",
                        user_agent="test-orders-batched-commits",
                        request_timeout_seconds=5.0,
                        max_retries=0,
                        pause_seconds=0.0,
                        order_type="all",
                        commit_batch_size=2,
                    ),
                )

        self.assertEqual(3, result["fetch_run"]["type_count_success"])
        self.assertEqual(2, result["fetch_run"]["storage_flush_count"])
        self.assertEqual(2, result["fetch_run"]["config"]["commit_batch_size"])

    def test_fetch_market_orders_region_pages_failure_preserves_existing_snapshot_marker(self) -> None:
        self._seed_order_snapshot_state(
            type_id=8201,
            snapshot_id="snapshot-old-8201",
            next_scheduled_at="2026-04-06T00:02:00Z",
        )
        self._seed_order_snapshot_state(
            type_id=8202,
            snapshot_id="snapshot-old-8202",
            next_scheduled_at="2026-04-06T00:02:00Z",
        )

        def fake_region_orders_page(**kwargs):
            return {
                "ok": False,
                "status_code": None,
                "url": "https://example.test/orders?page=1",
                "retries_used": 0,
                "headers": {},
                "entries": [],
                "total_pages": 1,
                "error": "TimeoutError: The read operation timed out",
            }

        with connect(self.db_path) as connection:
            type_targets = resolve_order_fetch_targets(
                connection,
                explicit_type_ids=[8201, 8202],
                type_id_file=None,
                all_published_market_types=False,
                limit=None,
            )

            with patch("eveqtrading.order_fetch._request_region_orders_page", side_effect=fake_region_orders_page):
                result = fetch_market_orders(
                    connection,
                    output_root=self.output_root,
                    type_targets=type_targets,
                    config=OrderFetchConfig(
                        region_id=10000002,
                        datasource="tranquility",
                        esi_base_url="https://esi.evetech.net/latest",
                        user_agent="test-orders-region-failure-preserve-cache",
                        request_timeout_seconds=5.0,
                        max_retries=0,
                        pause_seconds=0.0,
                        order_type="all",
                        concurrent_requests=4,
                        fetch_mode="region_pages",
                    ),
                )
                markers = connection.execute(
                    """
                    SELECT type_id, coverage_marker
                    FROM sync_scope_state
                    WHERE scope_type = 'market_orders'
                    ORDER BY type_id
                    """
                ).fetchall()

        self.assertEqual(2, result["fetch_run"]["type_count_failed"])
        self.assertEqual(
            [(8201, "ok:snapshot-old-8201"), (8202, "ok:snapshot-old-8202")],
            [(int(row["type_id"]), str(row["coverage_marker"])) for row in markers],
        )
        self.assertTrue(all(row["preserved_cached_snapshot"] for row in result["fetch_trace"]["type_traces"]))

    def test_fetch_market_orders_replaces_previous_snapshot_for_each_type(self) -> None:
        self._seed_order_snapshot_state(type_id=8201, snapshot_id="snapshot-old-8201")
        self._seed_order_snapshot_state(type_id=8202, snapshot_id="snapshot-old-8202")

        with connect(self.db_path) as connection:
            type_targets = resolve_order_fetch_targets(
                connection,
                explicit_type_ids=[8201, 8202],
                type_id_file=None,
                all_published_market_types=False,
                limit=None,
            )

            with patch(
                "eveqtrading.order_fetch._request_all_orders_for_type",
                side_effect=lambda **kwargs: {
                    "ok": True,
                    "status_code": 200,
                    "entries": _build_order_entries(type_id=int(kwargs["type_id"])),
                    "page_traces": [],
                    "error": None,
                    "page_count": 1,
                    "retries_used": 0,
                },
            ):
                fetch_market_orders(
                    connection,
                    output_root=self.output_root,
                    type_targets=type_targets,
                    config=OrderFetchConfig(
                        region_id=10000002,
                        datasource="tranquility",
                        esi_base_url="https://esi.evetech.net/latest",
                        user_agent="test-orders-delete-correct-old-snapshots",
                        request_timeout_seconds=5.0,
                        max_retries=0,
                        pause_seconds=0.0,
                        order_type="all",
                        ignore_existing_freshness=True,
                    ),
                )
                old_heads = connection.execute(
                    """
                    SELECT snapshot_id
                    FROM market_order_snapshot_head
                    WHERE snapshot_id IN ('snapshot-old-8201', 'snapshot-old-8202')
                    ORDER BY snapshot_id
                    """
                ).fetchall()

        self.assertEqual([], [str(row["snapshot_id"]) for row in old_heads])

    def test_fetch_market_orders_region_pages_groups_entries_by_type(self) -> None:
        def fake_region_orders_page(**kwargs):
            page = int(kwargs["page"])
            if page == 1:
                return {
                    "ok": True,
                    "status_code": 200,
                    "url": "https://example.test/orders?page=1",
                    "retries_used": 0,
                    "headers": {"x_pages": "2"},
                    "entries": [
                        {
                            "order_id": 8201001,
                            "type_id": 8201,
                            "is_buy_order": True,
                            "price": 98.0,
                            "volume_remain": 1000.0,
                            "volume_total": 1000.0,
                            "min_volume": 1.0,
                            "range": "region",
                            "issued": "2026-04-06T00:00:00Z",
                            "duration": 90,
                            "location_id": 60003760,
                        },
                        {
                            "order_id": 8202001,
                            "type_id": 8202,
                            "is_buy_order": False,
                            "price": 101.0,
                            "volume_remain": 500.0,
                            "volume_total": 500.0,
                            "min_volume": 1.0,
                            "range": "region",
                            "issued": "2026-04-06T00:00:00Z",
                            "duration": 90,
                            "location_id": 60003760,
                        },
                    ],
                    "total_pages": 2,
                    "error": None,
                }
            return {
                "ok": True,
                "status_code": 200,
                "url": "https://example.test/orders?page=2",
                "retries_used": 0,
                "headers": {"x_pages": "2"},
                "entries": [
                    {
                        "order_id": 8201002,
                        "type_id": 8201,
                        "is_buy_order": False,
                        "price": 102.0,
                        "volume_remain": 700.0,
                        "volume_total": 700.0,
                        "min_volume": 1.0,
                        "range": "region",
                        "issued": "2026-04-06T00:00:00Z",
                        "duration": 90,
                        "location_id": 60003760,
                    },
                    {
                        "order_id": 8201002,
                        "type_id": 8201,
                        "is_buy_order": False,
                        "price": 102.0,
                        "volume_remain": 700.0,
                        "volume_total": 700.0,
                        "min_volume": 1.0,
                        "range": "region",
                        "issued": "2026-04-06T00:00:00Z",
                        "duration": 90,
                        "location_id": 60003760,
                    }
                ],
                "total_pages": 2,
                "error": None,
            }

        with connect(self.db_path) as connection:
            type_targets = resolve_order_fetch_targets(
                connection,
                explicit_type_ids=[],
                type_id_file=self.type_id_file,
                all_published_market_types=False,
                limit=None,
            )

            with patch("eveqtrading.order_fetch._request_region_orders_page", side_effect=fake_region_orders_page):
                result = fetch_market_orders(
                    connection,
                    output_root=self.output_root,
                    type_targets=type_targets,
                    config=OrderFetchConfig(
                        region_id=10000002,
                        datasource="tranquility",
                        esi_base_url="https://esi.evetech.net/latest",
                        user_agent="test-orders-region-pages",
                        request_timeout_seconds=5.0,
                        max_retries=0,
                        pause_seconds=0.0,
                        order_type="all",
                        concurrent_requests=4,
                        fetch_mode="region_pages",
                    ),
                )
                counts = connection.execute(
                    """
                    SELECT type_id, COUNT(*) AS order_count
                    FROM market_order_snapshot_line
                    GROUP BY type_id
                    ORDER BY type_id
                    """
                ).fetchall()

        self.assertEqual(2, result["fetch_run"]["pages_fetched"])
        self.assertEqual(2, result["fetch_run"]["type_count_success"])
        self.assertEqual(
            [(8201, 2), (8202, 1)],
            [(int(row["type_id"]), int(row["order_count"])) for row in counts],
        )
        self.assertEqual(3, result["fetch_run"]["orders_written"])
        self.assertEqual("region_pages", result["fetch_run"]["config"]["fetch_mode"])
        self.assertEqual(
            "region_pages",
            result["fetch_trace"]["shared_remote_trace"]["request_scope"],
        )

    def test_fetch_market_orders_auto_mode_prefers_per_type_for_small_target_set(self) -> None:
        with connect(self.db_path) as connection:
            type_targets = resolve_order_fetch_targets(
                connection,
                explicit_type_ids=[8201],
                type_id_file=None,
                all_published_market_types=False,
                limit=None,
            )

            with patch("eveqtrading.order_fetch._request_region_orders_page") as mocked_region_page:
                with patch(
                    "eveqtrading.order_fetch._request_all_orders_for_type",
                    return_value={
                        "ok": True,
                        "status_code": 200,
                        "entries": _build_order_entries(type_id=8201),
                        "page_traces": [],
                        "error": None,
                        "page_count": 1,
                        "retries_used": 0,
                    },
                ):
                    result = fetch_market_orders(
                        connection,
                        output_root=self.output_root,
                        type_targets=type_targets,
                        config=OrderFetchConfig(
                            region_id=10000002,
                            datasource="tranquility",
                            esi_base_url="https://esi.evetech.net/latest",
                            user_agent="test-orders-auto-small",
                            request_timeout_seconds=5.0,
                            max_retries=0,
                            pause_seconds=0.0,
                            order_type="all",
                            concurrent_requests=4,
                            fetch_mode="auto",
                        ),
                    )

        mocked_region_page.assert_not_called()
        self.assertEqual("per_type", result["fetch_run"]["effective_fetch_mode"])
        self.assertEqual(
            "target_set_small_or_predicted_pages_low",
            result["fetch_run"]["fetch_mode_decision"]["reason"],
        )

    def test_fetch_market_orders_auto_mode_can_choose_region_pages(self) -> None:
        def fake_region_orders_page(**kwargs):
            page = int(kwargs["page"])
            if page == 1:
                return {
                    "ok": True,
                    "status_code": 200,
                    "url": "https://example.test/orders?page=1",
                    "retries_used": 0,
                    "headers": {"x_pages": "1"},
                    "entries": [
                        {
                            "order_id": 8201001,
                            "type_id": 8201,
                            "is_buy_order": True,
                            "price": 98.0,
                            "volume_remain": 1000.0,
                            "volume_total": 1000.0,
                            "min_volume": 1.0,
                            "range": "region",
                            "issued": "2026-04-06T00:00:00Z",
                            "duration": 90,
                            "location_id": 60003760,
                        },
                        {
                            "order_id": 8202001,
                            "type_id": 8202,
                            "is_buy_order": False,
                            "price": 101.0,
                            "volume_remain": 500.0,
                            "volume_total": 500.0,
                            "min_volume": 1.0,
                            "range": "region",
                            "issued": "2026-04-06T00:00:00Z",
                            "duration": 90,
                            "location_id": 60003760,
                        },
                    ],
                    "total_pages": 1,
                    "error": None,
                }
            raise AssertionError("unexpected page request")

        with connect(self.db_path) as connection:
            type_targets = resolve_order_fetch_targets(
                connection,
                explicit_type_ids=[],
                type_id_file=self.type_id_file,
                all_published_market_types=False,
                limit=None,
            )
            connection.execute(
                """
                INSERT INTO market_order_snapshot_head (
                    snapshot_id, captured_at, scope_kind, region_id, location_id, account_id, source, refresh_mode
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "snapshot-old-8201",
                    "2026-04-05T00:01:00Z",
                    "region_type_orders:all",
                    10000002,
                    None,
                    None,
                    "test",
                    "replace",
                ),
            )
            connection.executemany(
                """
                INSERT INTO market_order_snapshot_line (
                    snapshot_id, order_id, type_id, is_buy_order, price, volume_remain, volume_total, min_volume,
                    order_range, issued_at, duration_days, location_id, region_id, account_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        "snapshot-old-8201",
                        9000000 + order_id,
                        8201,
                        1 if order_id % 2 == 0 else 0,
                        100.0 + order_id,
                        1.0,
                        1.0,
                        1.0,
                        "region",
                        "2026-04-05T00:00:00Z",
                        90,
                        60003760,
                        10000002,
                        None,
                    )
                    for order_id in range(1, 9002)
                ],
            )
            connection.execute(
                """
                INSERT INTO sync_scope_state (
                    scope_key, scope_type, region_id, type_id, last_success_at, last_attempt_at,
                    next_scheduled_at, failure_count, last_error, coverage_marker
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "market_orders:10000002:8201:all",
                    "market_orders",
                    10000002,
                    8201,
                    "2026-04-05T00:01:00Z",
                    "2026-04-05T00:01:00Z",
                    "2026-04-05T01:00:00Z",
                    0,
                    None,
                    "ok:snapshot-old-8201",
                ),
            )
            connection.commit()

            with patch("eveqtrading.order_fetch._request_region_orders_page", side_effect=fake_region_orders_page):
                result = fetch_market_orders(
                    connection,
                    output_root=self.output_root,
                    type_targets=type_targets,
                    config=OrderFetchConfig(
                        region_id=10000002,
                        datasource="tranquility",
                        esi_base_url="https://esi.evetech.net/latest",
                        user_agent="test-orders-auto-region-pages",
                        request_timeout_seconds=5.0,
                        max_retries=0,
                        pause_seconds=0.0,
                        order_type="all",
                        concurrent_requests=4,
                        fetch_mode="auto",
                    ),
                )

        self.assertEqual("region_pages", result["fetch_run"]["effective_fetch_mode"])
        self.assertEqual(
            "region_total_pages_below_predicted_per_type_pages",
            result["fetch_run"]["fetch_mode_decision"]["reason"],
        )
        self.assertEqual(1, result["fetch_run"]["pages_fetched"])

    def test_fetch_market_orders_can_ignore_existing_freshness_and_replace_previous_snapshot(self) -> None:
        with connect(self.db_path) as connection:
            connection.execute(
                """
                INSERT INTO market_order_snapshot_head (
                    snapshot_id, captured_at, scope_kind, region_id, location_id, account_id, source, refresh_mode
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "snapshot-fresh-8201",
                    "2026-04-06T00:01:00Z",
                    "region_type_orders:all",
                    10000002,
                    None,
                    None,
                    "test",
                    "replace",
                ),
            )
            connection.executemany(
                """
                INSERT INTO market_order_snapshot_line (
                    snapshot_id, order_id, type_id, is_buy_order, price, volume_remain, volume_total, min_volume,
                    order_range, issued_at, duration_days, location_id, region_id, account_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        "snapshot-fresh-8201",
                        8201001,
                        8201,
                        1,
                        98.0,
                        1000.0,
                        1000.0,
                        1.0,
                        "region",
                        "2026-04-06T00:00:00Z",
                        90,
                        60003760,
                        10000002,
                        None,
                    )
                ],
            )
            connection.execute(
                """
                INSERT INTO sync_scope_state (
                    scope_key, scope_type, region_id, type_id, last_success_at, last_attempt_at,
                    next_scheduled_at, failure_count, last_error, coverage_marker
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "market_orders:10000002:8201:all",
                    "market_orders",
                    10000002,
                    8201,
                    "2026-04-06T00:01:00Z",
                    "2026-04-06T00:01:00Z",
                    "2099-01-01T00:00:00Z",
                    0,
                    None,
                    "ok:snapshot-fresh-8201",
                ),
            )
            connection.commit()

            type_targets = resolve_order_fetch_targets(
                connection,
                explicit_type_ids=[8201],
                type_id_file=None,
                all_published_market_types=False,
                limit=None,
            )

            with patch(
                "eveqtrading.order_fetch._request_all_orders_for_type",
                return_value={
                    "ok": True,
                    "status_code": 200,
                    "entries": _build_order_entries(type_id=8201),
                    "page_traces": [],
                    "error": None,
                    "page_count": 1,
                    "retries_used": 0,
                },
            ) as mocked_request:
                result = fetch_market_orders(
                    connection,
                    output_root=self.output_root,
                    type_targets=type_targets,
                    config=OrderFetchConfig(
                        region_id=10000002,
                        datasource="tranquility",
                        esi_base_url="https://esi.evetech.net/latest",
                        user_agent="test-orders-ignore-freshness",
                        request_timeout_seconds=5.0,
                        max_retries=0,
                        pause_seconds=0.0,
                        order_type="all",
                        ignore_existing_freshness=True,
                    ),
                )
                new_marker = connection.execute(
                    "SELECT coverage_marker FROM sync_scope_state WHERE scope_key = ?",
                    ("market_orders:10000002:8201:all",),
                ).fetchone()[0]
                old_head_count = connection.execute(
                    "SELECT COUNT(*) FROM market_order_snapshot_head WHERE snapshot_id = ?",
                    ("snapshot-fresh-8201",),
                ).fetchone()[0]

        mocked_request.assert_called_once()
        self.assertEqual(1, result["fetch_run"]["type_count_success"])
        self.assertEqual(0, result["fetch_run"]["type_count_skipped_fresh"])
        self.assertEqual(0, old_head_count)
        self.assertTrue(str(new_marker).startswith("ok:orders-fetch-"))


if __name__ == "__main__":
    unittest.main()
