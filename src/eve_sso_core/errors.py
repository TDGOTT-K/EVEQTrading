class EveSsoError(Exception):
    """Base exception for eve_sso_core."""


class EveSsoConfigError(EveSsoError):
    """Raised when the SSO client configuration is invalid."""


class EveSsoStoreError(EveSsoError):
    """Raised when the local token store is invalid."""


class EveSsoSelectionError(EveSsoError):
    """Raised when no linked character can be uniquely selected."""


class EveSsoScopeError(EveSsoError):
    """Raised when a stored token does not satisfy required scopes."""


class EveSsoCallbackError(EveSsoError):
    """Raised when the local callback flow fails."""


class EveSsoTokenValidationError(EveSsoError):
    """Raised when an access token cannot be validated."""
