from __future__ import annotations

import webbrowser
from urllib.parse import urlencode

from .callback import wait_for_authorization_code
from .discovery import load_metadata
from .errors import EveSsoConfigError
from .http import request_json
from .models import (
    EveSsoConfig,
    LinkedCharacterRecord,
    LinkedCharacterSummary,
    PreparedAuthorization,
    TokenRequest,
    ValidatedAccessToken,
)
from .pkce import create_pkce_pair, create_state
from .store import JsonTokenStore
from .tokens import validate_access_token
from .utils import expires_within, normalize_scopes, now_utc_iso, scopes_to_string


class EveSsoClient:
    def __init__(self, config: EveSsoConfig) -> None:
        self.config = config
        self.store = JsonTokenStore(config.store_path)

    def prepare_authorization(self, scopes: list[str]) -> PreparedAuthorization:
        if not self.config.client_id:
            raise EveSsoConfigError("client_id is required for EVE SSO login.")
        normalized_scopes = normalize_scopes(scopes)
        if not normalized_scopes:
            raise EveSsoConfigError("At least one scope is required for EVE SSO login.")
        metadata = load_metadata(self.config)
        code_verifier, code_challenge = create_pkce_pair()
        state = create_state()
        query = urlencode(
            {
                "response_type": "code",
                "client_id": self.config.client_id,
                "redirect_uri": self.config.callback_url,
                "scope": scopes_to_string(normalized_scopes),
                "state": state,
                "code_challenge": code_challenge,
                "code_challenge_method": "S256",
            }
        )
        return PreparedAuthorization(
            authorization_url=f"{metadata.authorization_endpoint}?{query}",
            callback_url=self.config.callback_url,
            scopes=normalized_scopes,
            state=state,
            code_verifier=code_verifier,
            code_challenge=code_challenge,
        )

    def exchange_authorization_code(
        self,
        code: str,
        prepared_authorization: PreparedAuthorization,
        *,
        set_default: bool = False,
    ) -> LinkedCharacterRecord:
        metadata = load_metadata(self.config)
        payload, _, _ = request_json(
            metadata.token_endpoint,
            method="POST",
            headers={"user-agent": self.config.user_agent},
            form={
                "grant_type": "authorization_code",
                "client_id": self._client_id(),
                "code": code,
                "code_verifier": prepared_authorization.code_verifier,
                "redirect_uri": prepared_authorization.callback_url,
            },
            timeout_seconds=self.config.request_timeout_seconds,
        )
        validated = validate_access_token(
            str(payload["access_token"]),
            token_type=str(payload.get("token_type") or "Bearer"),
            metadata=metadata,
            config=self.config,
        )
        record = LinkedCharacterRecord(
            character_id=validated.character_id,
            character_name=validated.character_name,
            owner_hash=validated.owner_hash,
            scopes=validated.scopes or list(prepared_authorization.scopes),
            access_token=validated.access_token,
            access_token_expires_at=validated.expires_at,
            refresh_token=str(payload.get("refresh_token") or ""),
            token_type=validated.token_type,
            last_refreshed_at=now_utc_iso(),
        )
        return self.store.upsert_character(record, set_default=set_default, updated_at=now_utc_iso())

    def login_with_local_callback(
        self,
        scopes: list[str],
        *,
        set_default: bool = False,
        open_browser: bool | None = None,
        timeout_seconds: float = 180.0,
    ) -> LinkedCharacterRecord:
        prepared = self.prepare_authorization(scopes)
        should_open_browser = self.config.auto_open_browser if open_browser is None else open_browser
        if should_open_browser:
            webbrowser.open(prepared.authorization_url)
        code = wait_for_authorization_code(prepared.callback_url, prepared.state, timeout_seconds)
        return self.exchange_authorization_code(code, prepared, set_default=set_default)

    def list_characters(self) -> list[LinkedCharacterSummary]:
        return self.store.list_characters()

    def set_default_character(self, character_id: int) -> None:
        self.store.set_default_character(character_id, updated_at=now_utc_iso())

    def remove_character(self, character_id: int) -> bool:
        return self.store.remove_character(character_id, updated_at=now_utc_iso())

    def get_character_record(self, request: TokenRequest | None = None) -> LinkedCharacterRecord:
        return self.store.get_character(request)

    def refresh_character(self, character_id: int) -> LinkedCharacterRecord:
        current = self.store.get_character(TokenRequest(character_id=character_id))
        metadata = load_metadata(self.config)
        payload, _, _ = request_json(
            metadata.token_endpoint,
            method="POST",
            headers={"user-agent": self.config.user_agent},
            form={
                "grant_type": "refresh_token",
                "client_id": self._client_id(),
                "refresh_token": current.refresh_token,
            },
            timeout_seconds=self.config.request_timeout_seconds,
        )
        validated = validate_access_token(
            str(payload["access_token"]),
            token_type=str(payload.get("token_type") or current.token_type or "Bearer"),
            metadata=metadata,
            config=self.config,
        )
        updated = LinkedCharacterRecord(
            character_id=validated.character_id,
            character_name=validated.character_name,
            owner_hash=validated.owner_hash,
            scopes=normalize_scopes(validated.scopes or current.scopes),
            access_token=validated.access_token,
            access_token_expires_at=validated.expires_at,
            refresh_token=str(payload.get("refresh_token") or current.refresh_token),
            token_type=validated.token_type,
            last_refreshed_at=now_utc_iso(),
        )
        return self.store.upsert_character(updated, set_default=False, updated_at=now_utc_iso())

    def get_access_token(self, request: TokenRequest | None = None) -> ValidatedAccessToken:
        token_request = request or TokenRequest()
        record = self.store.get_character(token_request)
        if expires_within(record.access_token_expires_at, token_request.min_validity_seconds):
            record = self.refresh_character(record.character_id)
        metadata = load_metadata(self.config)
        validated = validate_access_token(
            record.access_token,
            token_type=record.token_type,
            metadata=metadata,
            config=self.config,
        )
        merged_scopes = normalize_scopes(validated.scopes or record.scopes)
        return ValidatedAccessToken(
            access_token=validated.access_token,
            token_type=validated.token_type,
            character_id=validated.character_id,
            character_name=validated.character_name,
            owner_hash=validated.owner_hash,
            scopes=merged_scopes,
            expires_at=validated.expires_at,
            issuer=validated.issuer,
            subject=validated.subject,
        )

    def _client_id(self) -> str:
        if not self.config.client_id:
            raise EveSsoConfigError("client_id is required for EVE SSO operations.")
        return self.config.client_id
