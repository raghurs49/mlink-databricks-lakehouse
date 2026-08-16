from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class Envelope:
    message_type: str
    payload: dict[str, Any]
    session_id: str
    sequence: int
    received_at: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class NormalizedRecord:
    family: str
    schema_version: int
    record_kind: str
    primary_key: str
    event_time: str
    received_at: str
    session_id: str
    sequence: int
    manifest_version: str
    values: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

