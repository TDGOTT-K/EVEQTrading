from __future__ import annotations

import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from eve_esi_client.cli import main as eve_esi_cli_main
from eve_esi_client.client import EveAuthenticatedEsiClient
from eve_esi_client.models import EveEsiConfig
from eve_sso_core.models import EveSsoConfig, LinkedCharacterRecord, ValidatedAccessToken
from eve_sso_core.client import EveSsoClient


class EveEsiClientTests(unittest.TestCase):
    def setUp(self) -> None:
        self._temp_dir = tempfile.TemporaryDirectory()
        self.base_dir = Path(self._temp_dir.name)
        self.store_path = self.base_dir / "tokens.json"
        self.sso_client = EveSsoClient(
            EveSsoConfig(
                client_id="test-client",
                callback_url="http://127.0.0.1:8080/callback",
                store_path=self.store_path,
            )
        )
        self.sso_client.store.upsert_character(
            LinkedCharacterRecord(
                character_id=12345,
                character_name="Wallet Pilot",
                owner_hash="owner-hash",
                scopes=["esi-wallet.read_character_wallet.v1"],
                access_token="stored-access-token",
                access_token_expires_at="2099-01-01T00:00:00Z",
                refresh_token="stored-refresh-token",
                token_type="Bearer",
                last_refreshed_at="2026-04-10T00:00:00Z",
            ),
            set_default=True,
            updated_at="2026-04-10T00:00:00Z",
        )
        self.client = EveAuthenticatedEsiClient(self.sso_client, EveEsiConfig(user_agent="test-agent/1.0"))

    def tearDown(self) -> None:
        self._temp_dir.cleanup()

    def _resolved_token(self) -> ValidatedAccessToken:
        return ValidatedAccessToken(
            access_token="resolved-access-token",
            token_type="Bearer",
            character_id=12345,
            character_name="Wallet Pilot",
            owner_hash="owner-hash",
            scopes=["esi-wallet.read_character_wallet.v1"],
            expires_at="2099-01-01T00:00:00Z",
            issuer="https://login.eveonline.com",
            subject="CHARACTER:EVE:12345",
        )

    def test_wallet_uses_authorization_header(self) -> None:
        seen_requests: list[dict[str, object]] = []

        def fake_request_json(url: str, *, method: str, headers: dict[str, str], timeout_seconds: float):
            seen_requests.append(
                {
                    "url": url,
                    "method": method,
                    "headers": dict(headers),
                    "timeout_seconds": timeout_seconds,
                }
            )
            return 123456789.01, 200, {"etag": "abc"}

        with patch.object(self.sso_client, "get_access_token", return_value=self._resolved_token()):
            with patch("eve_esi_client.client.request_json", side_effect=fake_request_json):
                result = self.client.get_wallet()

        self.assertEqual(123456789.01, result.balance)
        self.assertEqual(1, len(seen_requests))
        self.assertEqual("GET", seen_requests[0]["method"])
        self.assertEqual("Bearer resolved-access-token", seen_requests[0]["headers"]["authorization"])
        self.assertIn("/characters/12345/wallet", seen_requests[0]["url"])

    def test_wallet_journal_paginates_using_x_pages(self) -> None:
        requested_urls: list[str] = []

        def fake_request_json(url: str, *, method: str, headers: dict[str, str], timeout_seconds: float):
            requested_urls.append(url)
            if "page=1" in url:
                return [{"id": 1}, {"id": 2}], 200, {"x-pages": "3"}
            if "page=2" in url:
                return [{"id": 3}], 200, {"x-pages": "3"}
            if "page=3" in url:
                return [{"id": 4}], 200, {"x-pages": "3"}
            raise AssertionError(url)

        with patch.object(self.sso_client, "get_access_token", return_value=self._resolved_token()):
            with patch("eve_esi_client.client.request_json", side_effect=fake_request_json):
                result = self.client.get_wallet_journal()

        self.assertEqual(4, len(result.entries))
        self.assertEqual(3, result.trace["pages_total"])
        self.assertEqual(3, len(result.trace["requests"]))
        self.assertTrue(any("page=3" in url for url in requested_urls))

    def test_wallet_transactions_summary_counts_buys_and_sells(self) -> None:
        def fake_request_json(url: str, *, method: str, headers: dict[str, str], timeout_seconds: float):
            return [
                {"transaction_id": 1, "is_buy": True},
                {"transaction_id": 2, "is_buy": False},
                {"transaction_id": 3, "is_buy": False},
            ], 200, {}

        with patch.object(self.sso_client, "get_access_token", return_value=self._resolved_token()):
            with patch("eve_esi_client.client.request_json", side_effect=fake_request_json):
                result = self.client.get_wallet_transactions()

        self.assertEqual(3, result.summary["count"])
        self.assertEqual(1, result.summary["buy_count"])
        self.assertEqual(2, result.summary["sell_count"])

    def test_cli_wallet_writes_output_file(self) -> None:
        output_path = self.base_dir / "wallet.json"
        stdout = io.StringIO()
        fake_result = {
            "character": {
                "character_id": 12345,
                "character_name": "Wallet Pilot",
                "owner_hash": "owner-hash",
            },
            "balance": 42.0,
            "trace": {"status_code": 200},
        }

        with patch("eve_esi_client.cli.EveAuthenticatedEsiClient.get_wallet", return_value=fake_result):
            with redirect_stdout(stdout):
                exit_code = eve_esi_cli_main(
                    ["--store", str(self.store_path), "wallet", "--output", str(output_path)]
                )

        self.assertEqual(0, exit_code)
        self.assertTrue(output_path.exists())
        self.assertEqual(fake_result, json.loads(output_path.read_text(encoding="utf-8")))
        self.assertEqual(fake_result, json.loads(stdout.getvalue()))

    def test_assets_paginates_and_builds_summary(self) -> None:
        def fake_request_json(url: str, *, method: str, headers: dict[str, str], timeout_seconds: float):
            if "page=1" in url:
                return [
                    {"item_id": 1, "type_id": 1001, "quantity": 5},
                    {"item_id": 2, "type_id": 1002, "quantity": 7},
                ], 200, {"x-pages": "2"}
            if "page=2" in url:
                return [{"item_id": 3, "type_id": 1001, "quantity": 9}], 200, {"x-pages": "2"}
            raise AssertionError(url)

        with patch.object(self.sso_client, "get_access_token", return_value=self._resolved_token()):
            with patch("eve_esi_client.client.request_json", side_effect=fake_request_json):
                result = self.client.get_assets()

        self.assertEqual(3, result.summary["count"])
        self.assertEqual(2, result.summary["type_count"])
        self.assertEqual(21, result.summary["total_quantity"])
        self.assertEqual(2, result.trace["pages_total"])

    def test_orders_build_summary(self) -> None:
        def fake_request_json(url: str, *, method: str, headers: dict[str, str], timeout_seconds: float):
            return [
                {"order_id": 1, "is_buy_order": True, "price": 10.0, "volume_remain": 5},
                {"order_id": 2, "is_buy_order": False, "price": 20.0, "volume_remain": 7},
            ], 200, {}

        with patch.object(self.sso_client, "get_access_token", return_value=self._resolved_token()):
            with patch("eve_esi_client.client.request_json", side_effect=fake_request_json):
                result = self.client.get_orders()

        self.assertEqual(2, result.summary["count"])
        self.assertEqual(1, result.summary["buy_count"])
        self.assertEqual(1, result.summary["sell_count"])
        self.assertEqual(50.0, result.summary["remaining_buy_value"])
        self.assertEqual(140.0, result.summary["remaining_sell_value"])

    def test_order_history_paginates_and_counts_states(self) -> None:
        def fake_request_json(url: str, *, method: str, headers: dict[str, str], timeout_seconds: float):
            if "page=1" in url:
                return [
                    {"order_id": 1, "is_buy_order": True, "state": "expired"},
                    {"order_id": 2, "is_buy_order": False, "state": "cancelled"},
                ], 200, {"x-pages": "2"}
            if "page=2" in url:
                return [{"order_id": 3, "is_buy_order": False, "state": "expired"}], 200, {"x-pages": "2"}
            raise AssertionError(url)

        with patch.object(self.sso_client, "get_access_token", return_value=self._resolved_token()):
            with patch("eve_esi_client.client.request_json", side_effect=fake_request_json):
                result = self.client.get_order_history()

        self.assertEqual(3, result.summary["count"])
        self.assertEqual(1, result.summary["buy_count"])
        self.assertEqual(2, result.summary["sell_count"])
        self.assertEqual({"expired": 2, "cancelled": 1}, result.summary["status_counts"])
        self.assertEqual(2, result.trace["pages_total"])
