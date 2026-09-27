from __future__ import annotations

import http.client
import io
import json
import socket
import tempfile
import threading
import time
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

import jwt
from cryptography.hazmat.primitives.asymmetric import rsa

from eve_sso_core.callback import wait_for_authorization_code
from eve_sso_core.cli import main as eve_sso_cli_main
from eve_sso_core.client import EveSsoClient
from eve_sso_core.models import EveSsoConfig, EveSsoMetadata, LinkedCharacterRecord, TokenRequest
from eve_sso_core.store import JsonTokenStore
from eve_sso_core.tokens import validate_access_token


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


class EveSsoCoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self._temp_dir = tempfile.TemporaryDirectory()
        self.base_dir = Path(self._temp_dir.name)
        self.store_path = self.base_dir / "tokens.json"

    def tearDown(self) -> None:
        self._temp_dir.cleanup()

    def test_prepare_authorization_builds_pkce_url(self) -> None:
        client = EveSsoClient(
            EveSsoConfig(
                client_id="test-client",
                callback_url="http://127.0.0.1:8080/callback",
                store_path=self.store_path,
            )
        )
        with patch(
            "eve_sso_core.client.load_metadata",
            return_value=EveSsoMetadata(
                issuer="https://login.eveonline.com",
                authorization_endpoint="https://login.eveonline.com/v2/oauth/authorize",
                token_endpoint="https://login.eveonline.com/v2/oauth/token",
                jwks_uri="https://login.eveonline.com/oauth/jwks",
            ),
        ):
            prepared = client.prepare_authorization(["esi-wallet.read_character_wallet.v1"])

        self.assertIn("response_type=code", prepared.authorization_url)
        self.assertIn("client_id=test-client", prepared.authorization_url)
        self.assertIn("code_challenge_method=S256", prepared.authorization_url)
        self.assertEqual(["esi-wallet.read_character_wallet.v1"], prepared.scopes)
        self.assertTrue(prepared.state)
        self.assertTrue(prepared.code_verifier)
        self.assertTrue(prepared.code_challenge)

    def test_json_store_round_trip_and_default_selection(self) -> None:
        store = JsonTokenStore(self.store_path)
        alpha = LinkedCharacterRecord(
            character_id=1001,
            character_name="Alpha",
            owner_hash="owner-alpha",
            scopes=["scope.one"],
            access_token="token-a",
            access_token_expires_at="2099-01-01T00:00:00Z",
            refresh_token="refresh-a",
            token_type="Bearer",
            last_refreshed_at="2026-04-10T00:00:00Z",
        )
        beta = LinkedCharacterRecord(
            character_id=1002,
            character_name="Beta",
            owner_hash="owner-beta",
            scopes=["scope.two"],
            access_token="token-b",
            access_token_expires_at="2099-01-01T00:00:00Z",
            refresh_token="refresh-b",
            token_type="Bearer",
            last_refreshed_at="2026-04-10T00:00:00Z",
        )

        store.upsert_character(alpha, set_default=True, updated_at="2026-04-10T00:00:00Z")
        store.upsert_character(beta, set_default=False, updated_at="2026-04-10T00:01:00Z")

        summaries = store.list_characters()
        self.assertEqual(2, len(summaries))
        self.assertTrue(summaries[0].is_default)
        self.assertFalse(summaries[1].is_default)

        selected = store.get_character()
        self.assertEqual(1001, selected.character_id)

        store.set_default_character(1002, updated_at="2026-04-10T00:02:00Z")
        selected_after = store.get_character()
        self.assertEqual(1002, selected_after.character_id)

    def test_validate_access_token_verifies_signed_claims(self) -> None:
        private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        public_jwk = json.loads(jwt.algorithms.RSAAlgorithm.to_jwk(private_key.public_key()))
        public_jwk["kid"] = "test-kid"

        token = jwt.encode(
            {
                "iss": "https://login.eveonline.com",
                "sub": "CHARACTER:EVE:2112625428",
                "azp": "test-client",
                "name": "Pilot One",
                "owner": "owner-hash",
                "scp": ["esi-wallet.read_character_wallet.v1"],
                "exp": int(time.time()) + 3600,
            },
            private_key,
            algorithm="RS256",
            headers={"kid": "test-kid"},
        )
        config = EveSsoConfig(client_id="test-client", store_path=self.store_path)
        metadata = EveSsoMetadata(
            issuer="https://login.eveonline.com",
            authorization_endpoint="https://login.eveonline.com/v2/oauth/authorize",
            token_endpoint="https://login.eveonline.com/v2/oauth/token",
            jwks_uri="https://login.eveonline.com/oauth/jwks",
        )

        with patch("eve_sso_core.tokens.load_signing_keys", return_value=[public_jwk]):
            validated = validate_access_token(token, token_type="Bearer", metadata=metadata, config=config)

        self.assertEqual(2112625428, validated.character_id)
        self.assertEqual("Pilot One", validated.character_name)
        self.assertEqual("owner-hash", validated.owner_hash)
        self.assertEqual(["esi-wallet.read_character_wallet.v1"], validated.scopes)

    def test_callback_listener_receives_authorization_code(self) -> None:
        port = _find_free_port()
        callback_url = f"http://127.0.0.1:{port}/callback"
        result: dict[str, str] = {}

        def run_listener() -> None:
            result["code"] = wait_for_authorization_code(callback_url, "state-123", 5.0)

        thread = threading.Thread(target=run_listener, daemon=True)
        thread.start()
        time.sleep(0.3)

        connection = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
        try:
            connection.request("GET", "/callback?code=auth-code-xyz&state=state-123")
            response = connection.getresponse()
            response.read()
            self.assertEqual(200, response.status)
        finally:
            connection.close()

        thread.join(timeout=5.0)
        self.assertEqual("auth-code-xyz", result["code"])

    def test_get_access_token_refreshes_expiring_record(self) -> None:
        client = EveSsoClient(
            EveSsoConfig(
                client_id="test-client",
                callback_url="http://127.0.0.1:8080/callback",
                store_path=self.store_path,
            )
        )
        client.store.upsert_character(
            LinkedCharacterRecord(
                character_id=9001,
                character_name="Refresh Pilot",
                owner_hash="owner-refresh",
                scopes=["esi-wallet.read_character_wallet.v1"],
                access_token="stale-access",
                access_token_expires_at="2000-01-01T00:00:00Z",
                refresh_token="refresh-token-1",
                token_type="Bearer",
                last_refreshed_at="2026-04-10T00:00:00Z",
            ),
            set_default=True,
            updated_at="2026-04-10T00:00:00Z",
        )

        refreshed = LinkedCharacterRecord(
            character_id=9001,
            character_name="Refresh Pilot",
            owner_hash="owner-refresh",
            scopes=["esi-wallet.read_character_wallet.v1"],
            access_token="fresh-access",
            access_token_expires_at="2099-01-01T00:00:00Z",
            refresh_token="refresh-token-2",
            token_type="Bearer",
            last_refreshed_at="2026-04-10T00:01:00Z",
        )
        validated = {
            "access_token": "fresh-access",
            "token_type": "Bearer",
            "character_id": 9001,
            "character_name": "Refresh Pilot",
            "owner_hash": "owner-refresh",
            "scopes": ["esi-wallet.read_character_wallet.v1"],
            "expires_at": "2099-01-01T00:00:00Z",
            "issuer": "https://login.eveonline.com",
            "subject": "CHARACTER:EVE:9001",
        }

        with patch.object(client, "refresh_character", return_value=refreshed) as refresh_mock:
            with patch(
                "eve_sso_core.client.load_metadata",
                return_value=EveSsoMetadata(
                    issuer="https://login.eveonline.com",
                    authorization_endpoint="https://login.eveonline.com/v2/oauth/authorize",
                    token_endpoint="https://login.eveonline.com/v2/oauth/token",
                    jwks_uri="https://login.eveonline.com/oauth/jwks",
                ),
            ):
                with patch("eve_sso_core.client.validate_access_token") as validate_mock:
                    validate_mock.return_value = type("TokenValue", (), validated)()
                    resolved = client.get_access_token(
                        TokenRequest(scopes=["esi-wallet.read_character_wallet.v1"], min_validity_seconds=300)
                    )

        refresh_mock.assert_called_once_with(9001)
        self.assertEqual("fresh-access", resolved.access_token)
        self.assertEqual(9001, resolved.character_id)

    def test_cli_list_outputs_json(self) -> None:
        store = JsonTokenStore(self.store_path)
        store.upsert_character(
            LinkedCharacterRecord(
                character_id=7001,
                character_name="CLI Pilot",
                owner_hash="cli-owner",
                scopes=["scope.one"],
                access_token="token-cli",
                access_token_expires_at="2099-01-01T00:00:00Z",
                refresh_token="refresh-cli",
                token_type="Bearer",
                last_refreshed_at="2026-04-10T00:00:00Z",
            ),
            set_default=True,
            updated_at="2026-04-10T00:00:00Z",
        )

        stdout = io.StringIO()
        with redirect_stdout(stdout):
            exit_code = eve_sso_cli_main(["--store", str(self.store_path), "list", "--json"])

        self.assertEqual(0, exit_code)
        payload = json.loads(stdout.getvalue())
        self.assertEqual(1, len(payload))
        self.assertEqual("CLI Pilot", payload[0]["character_name"])
