from __future__ import annotations

import json
from datetime import datetime, timezone

import jwt

from .errors import EveSsoTokenValidationError
from .models import EveSsoConfig, EveSsoMetadata, ValidatedAccessToken
from .utils import normalize_issuer, normalize_scopes, parse_character_id, to_utc_iso


def _extract_scopes(claims: dict[str, object]) -> list[str]:
    if isinstance(claims.get("scp"), list):
        return normalize_scopes(str(item) for item in claims["scp"])
    if isinstance(claims.get("scope"), str):
        return normalize_scopes([str(claims["scope"])])
    return []


def _epoch_to_iso(epoch_seconds: int) -> str:
    return to_utc_iso(datetime.fromtimestamp(epoch_seconds, tz=timezone.utc))


def load_signing_keys(metadata: EveSsoMetadata, config: EveSsoConfig) -> list[dict[str, object]]:
    from .discovery import load_jwks

    payload = load_jwks(metadata, config)
    keys = payload.get("keys")
    if not isinstance(keys, list):
        raise EveSsoTokenValidationError("EVE SSO JWKS payload does not contain a valid keys array.")
    valid_keys: list[dict[str, object]] = []
    for key in keys:
        if isinstance(key, dict):
            valid_keys.append(key)
    if not valid_keys:
        raise EveSsoTokenValidationError("EVE SSO JWKS payload does not contain any usable signing keys.")
    return valid_keys


def validate_access_token(
    access_token: str,
    *,
    token_type: str,
    metadata: EveSsoMetadata,
    config: EveSsoConfig,
) -> ValidatedAccessToken:
    if not config.client_id:
        raise EveSsoTokenValidationError("client_id is required to validate an EVE SSO access token.")

    try:
        header = jwt.get_unverified_header(access_token)
    except jwt.InvalidTokenError as error:
        raise EveSsoTokenValidationError("EVE SSO access token is not a valid JWT.") from error

    algorithm = str(header.get("alg") or "")
    if algorithm != "RS256":
        raise EveSsoTokenValidationError(f"Unsupported EVE SSO token algorithm: {algorithm or 'unknown'}.")

    jwks = load_signing_keys(metadata, config)
    key_id = str(header.get("kid") or "")
    signing_key = None
    for key in jwks:
        if str(key.get("kid") or "") == key_id:
            signing_key = jwt.algorithms.RSAAlgorithm.from_jwk(json.dumps(key))
            break
    if signing_key is None:
        raise EveSsoTokenValidationError(f"Unable to find signing key for kid={key_id or 'unknown'}.")

    try:
        claims = jwt.decode(
            access_token,
            signing_key,
            algorithms=["RS256"],
            options={
                "verify_signature": True,
                "verify_exp": True,
                "verify_iss": False,
                "verify_aud": False,
                "require": ["exp", "iss", "sub"],
            },
        )
    except jwt.InvalidTokenError as error:
        raise EveSsoTokenValidationError(f"EVE SSO access token validation failed: {error}") from error

    token_issuer = normalize_issuer(str(claims.get("iss") or ""))
    expected_issuer = normalize_issuer(metadata.issuer)
    if not token_issuer or token_issuer != expected_issuer:
        raise EveSsoTokenValidationError(f"Unexpected EVE SSO token issuer: {claims.get('iss') or 'unknown'}.")

    authorized_party = claims.get("azp")
    if authorized_party and str(authorized_party) != config.client_id:
        raise EveSsoTokenValidationError(
            f"EVE SSO token was issued for a different client_id: {authorized_party}."
        )

    character_id = parse_character_id(str(claims.get("sub") or ""))
    character_name = str(claims.get("name") or f"Character {character_id}")
    owner_hash = claims.get("owner")
    expires_at = _epoch_to_iso(int(claims["exp"]))

    return ValidatedAccessToken(
        access_token=access_token,
        token_type=token_type,
        character_id=character_id,
        character_name=character_name,
        owner_hash=str(owner_hash) if owner_hash is not None else None,
        scopes=_extract_scopes(claims),
        expires_at=expires_at,
        issuer=expected_issuer or metadata.issuer,
        subject=str(claims["sub"]),
    )
