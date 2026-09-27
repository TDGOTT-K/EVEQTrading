from __future__ import annotations

import hashlib
import secrets
from base64 import urlsafe_b64encode


def _encode_urlsafe(raw: bytes) -> str:
    return urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def create_pkce_pair() -> tuple[str, str]:
    verifier = _encode_urlsafe(secrets.token_bytes(32))
    challenge = _encode_urlsafe(hashlib.sha256(verifier.encode("ascii")).digest())
    return verifier, challenge


def create_state() -> str:
    return _encode_urlsafe(secrets.token_bytes(24))
