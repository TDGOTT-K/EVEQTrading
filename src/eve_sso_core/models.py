from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


@dataclass(frozen=True)
class EveSsoConfig:
    client_id: Optional[str] = None
    callback_url: str = "http://127.0.0.1:8080/callback"
    sso_base_url: str = "https://login.eveonline.com"
    user_agent: str = "eve-sso-core/0.1"
    request_timeout_seconds: float = 30.0
    store_path: Path = Path.home() / ".eve-sso" / "tokens.json"
    auto_open_browser: bool = True


@dataclass(frozen=True)
class EveSsoMetadata:
    issuer: str
    authorization_endpoint: str
    token_endpoint: str
    jwks_uri: str
    revocation_endpoint: Optional[str] = None


@dataclass(frozen=True)
class PreparedAuthorization:
    authorization_url: str
    callback_url: str
    scopes: list[str]
    state: str
    code_verifier: str
    code_challenge: str


@dataclass(frozen=True)
class ValidatedAccessToken:
    access_token: str
    token_type: str
    character_id: int
    character_name: str
    owner_hash: Optional[str]
    scopes: list[str]
    expires_at: str
    issuer: str
    subject: str


@dataclass(frozen=True)
class LinkedCharacterRecord:
    character_id: int
    character_name: str
    owner_hash: Optional[str]
    scopes: list[str]
    access_token: str
    access_token_expires_at: str
    refresh_token: str
    token_type: str
    last_refreshed_at: str


@dataclass(frozen=True)
class LinkedCharacterSummary:
    character_id: int
    character_name: str
    owner_hash: Optional[str]
    scopes: list[str]
    access_token_expires_at: str
    last_refreshed_at: str
    is_default: bool


@dataclass(frozen=True)
class TokenRequest:
    character_id: Optional[int] = None
    character_name: Optional[str] = None
    owner_hash: Optional[str] = None
    scopes: list[str] = field(default_factory=list)
    min_validity_seconds: int = 300
