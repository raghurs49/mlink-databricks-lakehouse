from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .models import Envelope, NormalizedRecord


class LocalLakehouse:
    """Local JSONL analog of Bronze/Silver/Gold tables for deterministic demos/tests."""

    def __init__(self, root: Path):
        self.root = root
        self.bronze = root / "bronze" / "raw_messages.jsonl"
        self.silver = root / "silver" / "normalized_records.jsonl"
        self.gold = root / "gold" / "signals.jsonl"
        self.current = root / "silver" / "current_state.json"
        self.metrics = root / "metrics" / "run_report.json"
        for path in (self.bronze, self.silver, self.gold, self.current, self.metrics):
            path.parent.mkdir(parents=True, exist_ok=True)
        self._current_state: dict[str, dict[str, Any]] = {}
        if self.current.exists():
            self._current_state = json.loads(self.current.read_text(encoding="utf-8"))

    def append_raw(self, envelope: Envelope) -> None:
        self._append(self.bronze, envelope.to_dict())

    def append_normalized(self, record: NormalizedRecord) -> bool:
        identity = f"{record.family}:{record.session_id}:{record.sequence}"
        seen_file = self.root / "silver" / "seen_ids.txt"
        seen = set(seen_file.read_text(encoding="utf-8").splitlines()) if seen_file.exists() else set()
        if identity in seen:
            return False
        self._append(self.silver, record.to_dict())
        with seen_file.open("a", encoding="utf-8") as handle:
            handle.write(identity + "\n")
        if record.record_kind == "current_state":
            key = f"{record.family}:{record.primary_key}"
            previous = self._current_state.get(key)
            if previous is None or previous["event_time"] <= record.event_time:
                self._current_state[key] = record.to_dict()
                self.current.write_text(json.dumps(self._current_state, indent=2), encoding="utf-8")
        return True

    def append_signal(self, signal: dict[str, Any]) -> None:
        self._append(self.gold, signal)

    def write_metrics(self, report: dict[str, Any]) -> None:
        self.metrics.write_text(json.dumps(report, indent=2), encoding="utf-8")

    @staticmethod
    def _append(path: Path, item: dict[str, Any]) -> None:
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(item, separators=(",", ":")) + "\n")

