from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class FamilySchema:
    name: str
    schema_version: int
    record_kind: str
    primary_key: tuple[str, ...]
    required: dict[str, str]

    def validate(self, values: dict[str, Any]) -> None:
        missing = sorted(set(self.required) - set(values))
        if missing:
            raise ValueError(f"{self.name} missing required fields: {missing}")
        type_map = {"str": str, "float": (float, int), "int": int}
        for field, expected_name in self.required.items():
            expected = type_map[expected_name]
            if isinstance(values[field], bool) or not isinstance(values[field], expected):
                raise ValueError(f"{self.name}.{field} must be {expected_name}")

    def key_for(self, values: dict[str, Any]) -> str:
        return "|".join(str(values[field]) for field in self.primary_key)


@dataclass(frozen=True)
class Subscription:
    family: str
    symbols: tuple[str, ...]


@dataclass(frozen=True)
class ProjectConfig:
    registry_version: str
    manifest_version: str
    schemas: dict[str, FamilySchema]
    subscriptions: tuple[Subscription, ...]


def load_config(schema_path: Path, manifest_path: Path) -> ProjectConfig:
    registry = json.loads(schema_path.read_text(encoding="utf-8"))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    schemas = {
        name: FamilySchema(
            name=name,
            schema_version=int(spec["schema_version"]),
            record_kind=spec["record_kind"],
            primary_key=tuple(spec["primary_key"]),
            required=dict(spec["required"]),
        )
        for name, spec in registry["families"].items()
    }
    subscriptions = tuple(
        Subscription(item["family"], tuple(item["symbols"]))
        for item in manifest["subscriptions"]
        if item.get("enabled", True)
    )
    unknown = sorted({sub.family for sub in subscriptions} - set(schemas))
    if unknown:
        raise ValueError(f"subscriptions reference unknown schemas: {unknown}")
    return ProjectConfig(
        registry_version=registry["registry_version"],
        manifest_version=manifest["manifest_version"],
        schemas=schemas,
        subscriptions=subscriptions,
    )

