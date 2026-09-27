from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Optional


@dataclass(frozen=True)
class EveEsiConfig:
    base_url: str = "https://esi.evetech.net/latest"
    datasource: str = "tranquility"
    user_agent: str = "eve-esi-client/0.1"
    request_timeout_seconds: float = 30.0
    compatibility_date: Optional[str] = None
    tenant: Optional[str] = None
    accept_language: Optional[str] = None


@dataclass(frozen=True)
class EsiRequestTrace:
    method: str
    url: str
    status_code: int
    fetched_at: str
    response_headers: dict[str, str]
    page: Optional[int] = None
    pages_total: Optional[int] = None


@dataclass(frozen=True)
class EsiCharacterIdentity:
    character_id: int
    character_name: str
    owner_hash: Optional[str]


@dataclass(frozen=True)
class EsiWalletBalanceResult:
    character: EsiCharacterIdentity
    balance: float
    trace: EsiRequestTrace


@dataclass(frozen=True)
class EsiWalletJournalResult:
    character: EsiCharacterIdentity
    entries: list[dict[str, object]]
    trace: dict[str, object]


@dataclass(frozen=True)
class EsiWalletTransactionsResult:
    character: EsiCharacterIdentity
    entries: list[dict[str, object]]
    summary: dict[str, int]
    trace: EsiRequestTrace


@dataclass(frozen=True)
class EsiCharacterAssetsResult:
    character: EsiCharacterIdentity
    entries: list[dict[str, object]]
    summary: dict[str, int]
    trace: dict[str, object]


@dataclass(frozen=True)
class EsiCharacterOrdersResult:
    character: EsiCharacterIdentity
    entries: list[dict[str, object]]
    summary: dict[str, float | int]
    trace: EsiRequestTrace


@dataclass(frozen=True)
class EsiCharacterOrderHistoryResult:
    character: EsiCharacterIdentity
    entries: list[dict[str, object]]
    summary: dict[str, object]
    trace: dict[str, object]


def dataclass_to_jsonable(value: object) -> object:
    if hasattr(value, "__dataclass_fields__"):
        return asdict(value)  # type: ignore[arg-type]
    return value
