from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Optional

from .errors import EveSsoScopeError, EveSsoSelectionError, EveSsoStoreError
from .models import LinkedCharacterRecord, LinkedCharacterSummary, TokenRequest
from .utils import normalize_scopes


class JsonTokenStore:
    def __init__(self, path: Path) -> None:
        self.path = Path(path)

    def read_raw(self) -> dict[str, object]:
        if not self.path.exists():
            return {
                "version": 1,
                "updated_at": None,
                "default_character_id": None,
                "characters": [],
            }
        try:
            return json.loads(self.path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as error:
            raise EveSsoStoreError(f"Token store is not valid JSON: {self.path}") from error

    def write_raw(self, payload: dict[str, object]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8",
            delete=False,
            dir=str(self.path.parent),
            prefix=self.path.name,
            suffix=".tmp",
        ) as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
            temp_path = Path(handle.name)
        os.replace(temp_path, self.path)

    def _coerce_record(self, raw: object) -> LinkedCharacterRecord:
        if not isinstance(raw, dict):
            raise EveSsoStoreError("Stored character entry must be an object.")
        try:
            return LinkedCharacterRecord(
                character_id=int(raw["character_id"]),
                character_name=str(raw["character_name"]),
                owner_hash=str(raw["owner_hash"]) if raw.get("owner_hash") is not None else None,
                scopes=normalize_scopes(raw.get("scopes")),
                access_token=str(raw["access_token"]),
                access_token_expires_at=str(raw["access_token_expires_at"]),
                refresh_token=str(raw["refresh_token"]),
                token_type=str(raw.get("token_type") or "Bearer"),
                last_refreshed_at=str(raw["last_refreshed_at"]),
            )
        except KeyError as error:
            raise EveSsoStoreError(f"Stored character entry is missing field {error.args[0]!r}.") from error

    def _record_to_json(self, record: LinkedCharacterRecord) -> dict[str, object]:
        return {
            "character_id": record.character_id,
            "character_name": record.character_name,
            "owner_hash": record.owner_hash,
            "scopes": list(record.scopes),
            "access_token": record.access_token,
            "access_token_expires_at": record.access_token_expires_at,
            "refresh_token": record.refresh_token,
            "token_type": record.token_type,
            "last_refreshed_at": record.last_refreshed_at,
        }

    def list_records(self) -> list[LinkedCharacterRecord]:
        raw_store = self.read_raw()
        characters = raw_store.get("characters")
        if not isinstance(characters, list):
            raise EveSsoStoreError("Token store characters must be an array.")
        records = [self._coerce_record(entry) for entry in characters]
        records.sort(key=lambda entry: entry.character_id)
        return records

    def list_characters(self) -> list[LinkedCharacterSummary]:
        raw_store = self.read_raw()
        default_character_id = raw_store.get("default_character_id")
        return [
            LinkedCharacterSummary(
                character_id=record.character_id,
                character_name=record.character_name,
                owner_hash=record.owner_hash,
                scopes=list(record.scopes),
                access_token_expires_at=record.access_token_expires_at,
                last_refreshed_at=record.last_refreshed_at,
                is_default=record.character_id == default_character_id,
            )
            for record in self.list_records()
        ]

    def upsert_character(
        self,
        record: LinkedCharacterRecord,
        *,
        set_default: bool = False,
        updated_at: str,
    ) -> LinkedCharacterRecord:
        raw_store = self.read_raw()
        existing = [entry for entry in self.list_records() if entry.character_id != record.character_id]
        existing.append(record)
        existing.sort(key=lambda entry: entry.character_id)
        next_default = raw_store.get("default_character_id")
        if set_default or next_default is None:
            next_default = record.character_id
        self.write_raw(
            {
                "version": 1,
                "updated_at": updated_at,
                "default_character_id": next_default,
                "characters": [self._record_to_json(entry) for entry in existing],
            }
        )
        return record

    def set_default_character(self, character_id: int, *, updated_at: str) -> None:
        records = self.list_records()
        if not any(entry.character_id == character_id for entry in records):
            raise EveSsoSelectionError(f"Character {character_id} is not linked.")
        self.write_raw(
            {
                "version": 1,
                "updated_at": updated_at,
                "default_character_id": character_id,
                "characters": [self._record_to_json(entry) for entry in records],
            }
        )

    def remove_character(self, character_id: int, *, updated_at: str) -> bool:
        raw_store = self.read_raw()
        existing = self.list_records()
        remaining = [entry for entry in existing if entry.character_id != character_id]
        if len(remaining) == len(existing):
            return False
        next_default = raw_store.get("default_character_id")
        if next_default == character_id:
            next_default = remaining[0].character_id if remaining else None
        self.write_raw(
            {
                "version": 1,
                "updated_at": updated_at,
                "default_character_id": next_default,
                "characters": [self._record_to_json(entry) for entry in remaining],
            }
        )
        return True

    def get_character(self, request: Optional[TokenRequest] = None) -> LinkedCharacterRecord:
        raw_store = self.read_raw()
        records = self.list_records()
        if not records:
            raise EveSsoSelectionError("No linked EVE characters.")

        chosen = self._select_character(records, raw_store.get("default_character_id"), request)
        required_scopes = normalize_scopes(request.scopes if request else [])
        if required_scopes:
            missing_scopes = [scope for scope in required_scopes if scope not in chosen.scopes]
            if missing_scopes:
                raise EveSsoScopeError(
                    "Stored token is missing required scopes: " + ", ".join(missing_scopes)
                )
        return chosen

    def _select_character(
        self,
        records: list[LinkedCharacterRecord],
        default_character_id: object,
        request: Optional[TokenRequest],
    ) -> LinkedCharacterRecord:
        if request:
            if request.character_id is not None:
                for record in records:
                    if record.character_id == request.character_id:
                        return record
                raise EveSsoSelectionError(f"Character {request.character_id} is not linked.")
            if request.character_name:
                for record in records:
                    if record.character_name == request.character_name:
                        return record
                raise EveSsoSelectionError(f"Character {request.character_name!r} is not linked.")
            if request.owner_hash:
                for record in records:
                    if record.owner_hash == request.owner_hash:
                        return record
                raise EveSsoSelectionError("No linked character matches the requested owner hash.")

        if isinstance(default_character_id, int):
            for record in records:
                if record.character_id == default_character_id:
                    return record
        if len(records) == 1:
            return records[0]
        raise EveSsoSelectionError(
            "Multiple linked characters exist. Specify character_id, character_name, owner_hash, or set a default."
        )
