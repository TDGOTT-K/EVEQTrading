from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator

from .db import record_reference_import_run


def _iter_jsonl(path: Path) -> Iterator[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            stripped = line.strip()
            if not stripped:
                continue
            yield json.loads(stripped)


def _localized_text(payload: Any, language: str) -> str | None:
    if payload is None:
        return None
    if isinstance(payload, str):
        return payload
    if isinstance(payload, dict):
        preferred = payload.get(language)
        if isinstance(preferred, str) and preferred:
            return preferred
        fallback = payload.get("en")
        if isinstance(fallback, str) and fallback:
            return fallback
        for value in payload.values():
            if isinstance(value, str) and value:
                return value
    return None


def _load_lookup(
    sde_root: Path,
    dataset_name: str,
    key_field: str,
    value_builder,
) -> dict[int, Any]:
    lookup: dict[int, Any] = {}
    for row in _iter_jsonl(sde_root / dataset_name):
        key = row.get(key_field)
        if isinstance(key, int):
            lookup[key] = value_builder(row)
    return lookup


def _compose_npc_station_name(
    *,
    system_name: str,
    station_type_name: str | None,
    location_id: int,
) -> str:
    if not station_type_name:
        return f"{system_name} NPC Station {location_id}"
    if system_name and system_name.lower() in station_type_name.lower():
        return station_type_name
    return f"{system_name} - {station_type_name}"


def _ensure_item_catalog_name_columns(connection: sqlite3.Connection) -> None:
    rows = connection.execute("PRAGMA table_info(item_catalog)").fetchall()
    existing = {str(row[1]) for row in rows}
    if "type_name_en" not in existing:
        connection.execute("ALTER TABLE item_catalog ADD COLUMN type_name_en TEXT")
    if "type_name_zh" not in existing:
        connection.execute("ALTER TABLE item_catalog ADD COLUMN type_name_zh TEXT")
    if "market_group_id" not in existing:
        connection.execute("ALTER TABLE item_catalog ADD COLUMN market_group_id INTEGER")


def _ensure_market_group_catalog_table(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS market_group_catalog (
            market_group_id INTEGER PRIMARY KEY,
            parent_group_id INTEGER,
            market_group_name TEXT NOT NULL,
            market_group_name_en TEXT,
            market_group_name_zh TEXT,
            description TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )


def import_sde_reference_data(
    connection: sqlite3.Connection,
    sde_root: Path,
    *,
    language: str = "en",
    import_run_id: str,
) -> dict[str, Any]:
    sde_root = sde_root.resolve()
    sde_meta = next(_iter_jsonl(sde_root / "_sde.jsonl"))
    _ensure_item_catalog_name_columns(connection)
    _ensure_market_group_catalog_table(connection)

    categories = _load_lookup(
        sde_root,
        "categories.jsonl",
        "_key",
        lambda row: _localized_text(row.get("name"), language),
    )
    groups = _load_lookup(
        sde_root,
        "groups.jsonl",
        "_key",
        lambda row: {
            "name": _localized_text(row.get("name"), language),
            "category_id": row.get("categoryID"),
        },
    )
    market_groups = _load_lookup(
        sde_root,
        "marketGroups.jsonl",
        "_key",
        lambda row: {
            "name": _localized_text(row.get("name"), language),
            "name_en": _localized_text(row.get("name"), "en"),
            "name_zh": _localized_text(row.get("name"), "zh"),
            "parent_group_id": row.get("parentGroupID"),
            "description": _localized_text(row.get("description"), language),
        },
    )
    regions = _load_lookup(
        sde_root,
        "mapRegions.jsonl",
        "_key",
        lambda row: _localized_text(row.get("name"), language),
    )
    solar_systems = _load_lookup(
        sde_root,
        "mapSolarSystems.jsonl",
        "_key",
        lambda row: {
            "name": _localized_text(row.get("name"), language),
            "region_id": row.get("regionID"),
        },
    )

    market_group_rows: list[tuple[Any, ...]] = []
    for market_group_id, market_group_payload in market_groups.items():
        if not isinstance(market_group_payload, dict):
            continue
        market_group_rows.append(
            (
                market_group_id,
                market_group_payload.get("parent_group_id"),
                market_group_payload.get("name") or f"market_group:{market_group_id}",
                market_group_payload.get("name_en"),
                market_group_payload.get("name_zh"),
                market_group_payload.get("description"),
            )
        )

    type_name_lookup: dict[int, str] = {}
    item_rows: list[tuple[Any, ...]] = []
    item_counts = {
        "total_rows": 0,
        "published_rows": 0,
        "market_grouped_rows": 0,
    }
    for row in _iter_jsonl(sde_root / "types.jsonl"):
        type_id = row.get("_key")
        if not isinstance(type_id, int):
            continue

        group_id = row.get("groupID")
        group_payload = groups.get(group_id) if isinstance(group_id, int) else None
        category_id = None
        if isinstance(group_payload, dict):
            category_id = group_payload.get("category_id")

        type_name_en = _localized_text(row.get("name"), "en")
        type_name_zh = _localized_text(row.get("name"), "zh")
        type_name = (
            _localized_text(row.get("name"), language)
            or type_name_zh
            or type_name_en
            or f"type:{type_id}"
        )
        type_name_lookup[type_id] = type_name
        market_group_id = row.get("marketGroupID")
        market_group_payload = market_groups.get(market_group_id) if isinstance(market_group_id, int) else None
        market_group_name = market_group_payload.get("name") if isinstance(market_group_payload, dict) else None
        published = bool(row.get("published", False))

        item_rows.append(
            (
                type_id,
                type_name,
                type_name_en,
                type_name_zh,
                group_id,
                category_id,
                market_group_id if isinstance(market_group_id, int) else None,
                row.get("volume"),
                row.get("packagedVolume"),
                row.get("metaLevel"),
                market_group_name,
                1 if published else 0,
            )
        )
        item_counts["total_rows"] += 1
        if published:
            item_counts["published_rows"] += 1
        if market_group_name:
            item_counts["market_grouped_rows"] += 1

    region_rows: list[tuple[Any, ...]] = []
    for region_id, region_name in regions.items():
        region_rows.append(
            (
                region_id,
                region_name or f"region:{region_id}",
                "region",
                region_id,
                None,
                0,
                1 if region_name in {"The Forge"} else 0,
            )
        )

    solar_system_rows: list[tuple[Any, ...]] = []
    for solar_system_id, row in solar_systems.items():
        solar_system_rows.append(
            (
                solar_system_id,
                row.get("name") or f"solar_system:{solar_system_id}",
                "solar_system",
                row.get("region_id"),
                solar_system_id,
                0,
                1 if row.get("name") in {"Jita", "Amarr", "Dodixie", "Hek", "Rens"} else 0,
            )
        )

    npc_station_rows: list[tuple[Any, ...]] = []
    for row in _iter_jsonl(sde_root / "npcStations.jsonl"):
        location_id = row.get("_key")
        solar_system_id = row.get("solarSystemID")
        type_id = row.get("typeID")
        if not isinstance(location_id, int) or not isinstance(solar_system_id, int):
            continue
        solar_system = solar_systems.get(solar_system_id, {})
        if not isinstance(solar_system, dict):
            solar_system = {}
        system_name = solar_system.get("name") or f"solar_system:{solar_system_id}"
        location_name = _compose_npc_station_name(
            system_name=system_name,
            station_type_name=type_name_lookup.get(type_id),
            location_id=location_id,
        )
        npc_station_rows.append(
            (
                location_id,
                location_name,
                "npc_station",
                solar_system.get("region_id"),
                solar_system_id,
                1,
                1 if location_name in {"Jita Trade Hub", "Amarr VIII (Oris) - Emperor Family Academy"} else 0,
            )
        )

    connection.executemany(
        """
        INSERT INTO market_group_catalog (
            market_group_id,
            parent_group_id,
            market_group_name,
            market_group_name_en,
            market_group_name_zh,
            description
        )
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(market_group_id) DO UPDATE SET
            parent_group_id = excluded.parent_group_id,
            market_group_name = excluded.market_group_name,
            market_group_name_en = excluded.market_group_name_en,
            market_group_name_zh = excluded.market_group_name_zh,
            description = excluded.description
        """,
        market_group_rows,
    )

    connection.executemany(
        """
        INSERT INTO item_catalog (
            type_id,
            type_name,
            type_name_en,
            type_name_zh,
            group_id,
            category_id,
            market_group_id,
            volume,
            packaged_volume,
            meta_level,
            market_group,
            is_published
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(type_id) DO UPDATE SET
            type_name = excluded.type_name,
            type_name_en = excluded.type_name_en,
            type_name_zh = excluded.type_name_zh,
            group_id = excluded.group_id,
            category_id = excluded.category_id,
            market_group_id = excluded.market_group_id,
            volume = excluded.volume,
            packaged_volume = excluded.packaged_volume,
            meta_level = excluded.meta_level,
            market_group = excluded.market_group,
            is_published = excluded.is_published
        """,
        item_rows,
    )

    all_location_rows = [*region_rows, *solar_system_rows, *npc_station_rows]
    connection.executemany(
        """
        INSERT INTO market_location_catalog (
            location_id,
            location_name,
            location_kind,
            region_id,
            solar_system_id,
            is_npc,
            is_trade_hub_candidate
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(location_id) DO UPDATE SET
            location_name = excluded.location_name,
            location_kind = excluded.location_kind,
            region_id = excluded.region_id,
            solar_system_id = excluded.solar_system_id,
            is_npc = excluded.is_npc,
            is_trade_hub_candidate = excluded.is_trade_hub_candidate
        """,
        all_location_rows,
    )

    summary = {
        "source_path": str(sde_root),
        "source_build": sde_meta.get("buildNumber"),
        "source_release_date": sde_meta.get("releaseDate"),
        "language": language,
        "display_language": language,
        "item_name_localization": "bilingual_stored_display_language_selected",
        "market_group_catalog": {
            "total_rows": len(market_group_rows),
        },
        "item_catalog": item_counts,
        "market_location_catalog": {
            "regions": len(region_rows),
            "solar_systems": len(solar_system_rows),
            "npc_stations": len(npc_station_rows),
            "total_rows": len(all_location_rows),
        },
    }
    record_reference_import_run(
        connection,
        import_run_id=import_run_id,
        import_kind="sde",
        source_path=sde_root,
        source_build=sde_meta.get("buildNumber"),
        source_release_date=sde_meta.get("releaseDate"),
        details=summary,
    )
    connection.commit()
    return summary
