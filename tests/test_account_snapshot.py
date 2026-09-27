from __future__ import annotations

import io
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from eve_esi_client.models import (
    EsiCharacterAssetsResult,
    EsiCharacterIdentity,
    EsiCharacterOrderHistoryResult,
    EsiCharacterOrdersResult,
    EsiRequestTrace,
    EsiWalletBalanceResult,
)
from eve_sso_core.models import LinkedCharacterRecord, LinkedCharacterSummary
from eveqtrading.account_snapshot import capture_character_account_snapshot
from eveqtrading.cli import main as cli_main


class AccountSnapshotTests(unittest.TestCase):
    def setUp(self) -> None:
        self._temp_dir = tempfile.TemporaryDirectory()
        self.base_dir = Path(self._temp_dir.name)
        self.output_root = self.base_dir / "runs"
        self.output_root.mkdir(parents=True, exist_ok=True)
        self.store_path = self.base_dir / "tokens.json"

    def tearDown(self) -> None:
        self._temp_dir.cleanup()

    def _identity(self) -> EsiCharacterIdentity:
        return EsiCharacterIdentity(
            character_id=12345,
            character_name="Snapshot Pilot",
            owner_hash="owner-hash-1",
        )

    def _trace(self, url: str) -> EsiRequestTrace:
        return EsiRequestTrace(
            method="GET",
            url=url,
            status_code=200,
            fetched_at="2026-04-10T12:00:00Z",
            response_headers={"etag": "abc"},
        )

    def _linked_record(self, *, character_id: int = 12345, character_name: str = "Snapshot Pilot") -> LinkedCharacterRecord:
        return LinkedCharacterRecord(
            character_id=character_id,
            character_name=character_name,
            owner_hash="owner-hash-1",
            scopes=[],
            access_token="token",
            access_token_expires_at="2099-01-01T00:00:00Z",
            refresh_token="refresh",
            token_type="Bearer",
            last_refreshed_at="2026-04-10T00:00:00Z",
        )

    def _linked_summary(
        self,
        *,
        character_id: int = 12345,
        character_name: str = "Snapshot Pilot",
        is_default: bool = True,
    ) -> LinkedCharacterSummary:
        return LinkedCharacterSummary(
            character_id=character_id,
            character_name=character_name,
            owner_hash="owner-hash-1",
            scopes=[],
            access_token_expires_at="2099-01-01T00:00:00Z",
            last_refreshed_at="2026-04-10T00:00:00Z",
            is_default=is_default,
        )

    def test_capture_character_account_snapshot_writes_artifacts(self) -> None:
        with patch("eveqtrading.account_snapshot.EveSsoClient.get_character_record", return_value=self._linked_record()):
            with patch(
                "eveqtrading.account_snapshot.EveSsoClient.list_characters",
                return_value=[self._linked_summary()],
            ):
                with patch("eveqtrading.account_snapshot.EveAuthenticatedEsiClient.get_wallet") as get_wallet_mock:
                    with patch("eveqtrading.account_snapshot.EveAuthenticatedEsiClient.get_assets") as get_assets_mock:
                        with patch("eveqtrading.account_snapshot.EveAuthenticatedEsiClient.get_orders") as get_orders_mock:
                            with patch("eveqtrading.account_snapshot.EveAuthenticatedEsiClient.get_order_history") as get_history_mock:
                                get_wallet_mock.return_value = EsiWalletBalanceResult(
                                    character=self._identity(),
                                    balance=987654321.0,
                                    trace=self._trace("https://esi.evetech.net/latest/characters/12345/wallet?datasource=tranquility"),
                                )
                                get_assets_mock.return_value = EsiCharacterAssetsResult(
                                    character=self._identity(),
                                    entries=[{"item_id": 1, "type_id": 34, "quantity": 100}],
                                    summary={"count": 1, "type_count": 1, "total_quantity": 100},
                                    trace={"pages_total": 1, "requests": [{"page": 1}]},
                                )
                                get_orders_mock.return_value = EsiCharacterOrdersResult(
                                    character=self._identity(),
                                    entries=[{"order_id": 1}],
                                    summary={
                                        "count": 1,
                                        "buy_count": 1,
                                        "sell_count": 0,
                                        "remaining_buy_value": 5000000.0,
                                        "remaining_sell_value": 0.0,
                                    },
                                    trace=self._trace("https://esi.evetech.net/latest/characters/12345/orders?datasource=tranquility"),
                                )
                                get_history_mock.return_value = EsiCharacterOrderHistoryResult(
                                    character=self._identity(),
                                    entries=[{"order_id": 2, "state": "expired"}],
                                    summary={"count": 1, "buy_count": 0, "sell_count": 1, "status_counts": {"expired": 1}},
                                    trace={"pages_total": 1, "requests": [{"page": 1}]},
                                )

                                snapshot = capture_character_account_snapshot(
                                    output_root=self.output_root,
                                    client_id="test-client",
                                    callback_url="http://127.0.0.1:8080/callback",
                                    store_path=self.store_path,
                                    sso_base_url="https://login.eveonline.com",
                                    esi_base_url="https://esi.evetech.net/latest",
                                    datasource="tranquility",
                                    user_agent="test-agent/1.0",
                                    request_timeout_seconds=30.0,
                                    compatibility_date=None,
                                    tenant=None,
                                    accept_language=None,
                                    character_id=12345,
                                    character_name=None,
                                    owner_hash=None,
                                )

        run_dir = Path(snapshot["run_dir"])
        self.assertTrue((run_dir / "account_snapshot.json").exists())
        self.assertTrue((run_dir / "wallet.json").exists())
        self.assertTrue((run_dir / "assets.json").exists())
        self.assertTrue((run_dir / "orders.json").exists())
        self.assertTrue((run_dir / "order_history.json").exists())
        self.assertEqual("explicit_character_id", snapshot["selection_context"]["mode"])
        self.assertEqual("Snapshot Pilot", snapshot["selection_context"]["resolved_character_name"])
        summary_text = (run_dir / "summary.md").read_text(encoding="utf-8")
        self.assertIn("Wallet Balance ISK: 987654321.00", summary_text)
        self.assertIn("Open Orders: 1", summary_text)
        self.assertIn("Selection Mode: explicit_character_id", summary_text)

    def test_capture_character_account_snapshot_uses_current_default_linked_character(self) -> None:
        beta_identity = EsiCharacterIdentity(
            character_id=2002,
            character_name="Beta Pilot",
            owner_hash="owner-hash-beta",
        )
        beta_record = self._linked_record(character_id=2002, character_name="Beta Pilot")
        alpha_summary = self._linked_summary(character_id=1001, character_name="Alpha Pilot", is_default=False)
        beta_summary = self._linked_summary(character_id=2002, character_name="Beta Pilot", is_default=True)
        with patch("eveqtrading.account_snapshot.EveSsoClient.get_character_record", return_value=beta_record):
            with patch(
                "eveqtrading.account_snapshot.EveSsoClient.list_characters",
                return_value=[alpha_summary, beta_summary],
            ):
                with patch("eveqtrading.account_snapshot.EveAuthenticatedEsiClient.get_wallet") as get_wallet_mock:
                    with patch("eveqtrading.account_snapshot.EveAuthenticatedEsiClient.get_assets") as get_assets_mock:
                        with patch("eveqtrading.account_snapshot.EveAuthenticatedEsiClient.get_orders") as get_orders_mock:
                            with patch("eveqtrading.account_snapshot.EveAuthenticatedEsiClient.get_order_history") as get_history_mock:
                                get_wallet_mock.return_value = EsiWalletBalanceResult(
                                    character=beta_identity,
                                    balance=123456789.0,
                                    trace=self._trace("https://esi.evetech.net/latest/characters/2002/wallet?datasource=tranquility"),
                                )
                                get_assets_mock.return_value = EsiCharacterAssetsResult(
                                    character=beta_identity,
                                    entries=[],
                                    summary={"count": 0, "type_count": 0, "total_quantity": 0},
                                    trace={"pages_total": 1, "requests": [{"page": 1}]},
                                )
                                get_orders_mock.return_value = EsiCharacterOrdersResult(
                                    character=beta_identity,
                                    entries=[],
                                    summary={
                                        "count": 0,
                                        "buy_count": 0,
                                        "sell_count": 0,
                                        "remaining_buy_value": 0.0,
                                        "remaining_sell_value": 0.0,
                                    },
                                    trace=self._trace("https://esi.evetech.net/latest/characters/2002/orders?datasource=tranquility"),
                                )
                                get_history_mock.return_value = EsiCharacterOrderHistoryResult(
                                    character=beta_identity,
                                    entries=[],
                                    summary={"count": 0, "buy_count": 0, "sell_count": 0, "status_counts": {}},
                                    trace={"pages_total": 1, "requests": [{"page": 1}]},
                                )

                                snapshot = capture_character_account_snapshot(
                                    output_root=self.output_root,
                                    client_id="test-client",
                                    callback_url="http://127.0.0.1:8080/callback",
                                    store_path=self.store_path,
                                    sso_base_url="https://login.eveonline.com",
                                    esi_base_url="https://esi.evetech.net/latest",
                                    datasource="tranquility",
                                    user_agent="test-agent/1.0",
                                    request_timeout_seconds=30.0,
                                    compatibility_date=None,
                                    tenant=None,
                                    accept_language=None,
                                    character_id=None,
                                    character_name=None,
                                    owner_hash=None,
                                )

        self.assertEqual("default_linked_character", snapshot["selection_context"]["mode"])
        self.assertEqual("Beta Pilot", snapshot["selection_context"]["resolved_character_name"])
        self.assertEqual(2002, snapshot["selection_context"]["resolved_character_id"])
        self.assertIsNone(snapshot["selection_context"]["requested_character_name"])

    def test_cli_capture_character_account_prints_snapshot(self) -> None:
        fake_snapshot = {
            "run_id": "character-account-20260410T120000Z-abcdef12",
            "run_dir": str(self.output_root / "character-account-20260410T120000Z-abcdef12"),
        }
        stdout = io.StringIO()
        argv = [
            "eveq",
            "capture-character-account",
            "--output-root",
            str(self.output_root),
            "--store",
            str(self.store_path),
            "--client-id",
            "test-client",
        ]
        with patch("eveqtrading.cli.capture_character_account_snapshot", return_value=fake_snapshot):
            with patch.object(sys, "argv", argv):
                with redirect_stdout(stdout):
                    exit_code = cli_main()

        self.assertEqual(0, exit_code)
        self.assertEqual(fake_snapshot, json.loads(stdout.getvalue()))
