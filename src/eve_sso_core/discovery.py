from __future__ import annotations

from urllib.parse import urljoin

from .http import request_json
from .models import EveSsoConfig, EveSsoMetadata


def metadata_url(config: EveSsoConfig) -> str:
    base_url = config.sso_base_url.rstrip("/") + "/"
    return urljoin(base_url, ".well-known/oauth-authorization-server")


def load_metadata(config: EveSsoConfig) -> EveSsoMetadata:
    payload, _, _ = request_json(
        metadata_url(config),
        headers={"user-agent": config.user_agent},
        timeout_seconds=config.request_timeout_seconds,
    )
    return EveSsoMetadata(
        issuer=str(payload["issuer"]),
        authorization_endpoint=str(payload["authorization_endpoint"]),
        token_endpoint=str(payload["token_endpoint"]),
        jwks_uri=str(payload["jwks_uri"]),
        revocation_endpoint=payload.get("revocation_endpoint"),
    )


def load_jwks(metadata: EveSsoMetadata, config: EveSsoConfig) -> dict[str, object]:
    payload, _, _ = request_json(
        metadata.jwks_uri,
        headers={"user-agent": config.user_agent},
        timeout_seconds=config.request_timeout_seconds,
    )
    return payload
