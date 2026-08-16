from __future__ import annotations

import asyncio
import statistics
import time
from dataclasses import dataclass, field
from typing import Any

from .config import ProjectConfig
from .mock_protocol import MockMlinkServer, SyntheticDisconnect
from .models import Envelope, NormalizedRecord, utc_now
from .storage import LocalLakehouse


@dataclass
class RunMetrics:
    connections: int = 0
    reconnects: int = 0
    authentication_failures: int = 0
    subscription_failures: int = 0
    checkpoints: list[str] = field(default_factory=list)
    heartbeats: int = 0
    raw_messages: int = 0
    normalized_records: int = 0
    signals: int = 0
    validation_failures: int = 0
    processing_latency_ms: list[float] = field(default_factory=list)

    def report(self) -> dict[str, Any]:
        ordered = sorted(self.processing_latency_ms)
        p95 = ordered[max(0, int(len(ordered) * 0.95) - 1)] if ordered else 0.0
        return {
            "connections": self.connections,
            "reconnects": self.reconnects,
            "authentication_failures": self.authentication_failures,
            "subscription_failures": self.subscription_failures,
            "checkpoints": self.checkpoints,
            "heartbeats": self.heartbeats,
            "raw_messages": self.raw_messages,
            "normalized_records": self.normalized_records,
            "signals": self.signals,
            "validation_failures": self.validation_failures,
            "processing_latency_ms": {
                "count": len(ordered),
                "mean": round(statistics.fmean(ordered), 4) if ordered else 0.0,
                "p95": round(p95, 4),
                "max": round(max(ordered), 4) if ordered else 0.0,
            },
        }


class StreamingPipeline:
    def __init__(self, config: ProjectConfig, server: MockMlinkServer, lakehouse: LocalLakehouse):
        self.config = config
        self.server = server
        self.lakehouse = lakehouse
        self.metrics = RunMetrics()
        self._quotes: dict[str, NormalizedRecord] = {}

    async def run(self, target_records: int, max_reconnects: int = 3) -> dict[str, Any]:
        attempts = 0
        while self.metrics.normalized_records < target_records:
            session = await self.server.connect()
            self.metrics.connections += 1
            admin = await session.authenticate("synthetic-demo-token")
            if not admin.get("authenticated"):
                self.metrics.authentication_failures += 1
                raise RuntimeError("synthetic authentication failed")
            ack = await session.subscribe([
                {"family": sub.family, "symbols": list(sub.symbols)}
                for sub in self.config.subscriptions
            ])
            if not ack.get("accepted"):
                self.metrics.subscription_failures += 1
                raise RuntimeError("synthetic subscription rejected")
            try:
                sequence = 0
                async for message in session.messages():
                    sequence += 1
                    await self._handle(session.session_id, sequence, message)
                    if self.metrics.normalized_records >= target_records:
                        break
            except SyntheticDisconnect:
                attempts += 1
                self.metrics.reconnects += 1
                if attempts > max_reconnects:
                    raise
                await asyncio.sleep(min(0.01 * (2 ** (attempts - 1)), 0.1))
        report = self.metrics.report()
        report.update({
            "registry_version": self.config.registry_version,
            "manifest_version": self.config.manifest_version,
            "mode": "synthetic",
        })
        self.lakehouse.write_metrics(report)
        return report

    async def _handle(self, session_id: str, sequence: int, message: dict[str, Any]) -> None:
        start = time.perf_counter_ns()
        received_at = utc_now()
        envelope = Envelope(message["type"], message, session_id, sequence, received_at)
        self.lakehouse.append_raw(envelope)
        self.metrics.raw_messages += 1
        if message["type"] == "CheckPoint":
            self.metrics.checkpoints.append(message["state"])
        elif message["type"] == "Heartbeat":
            self.metrics.heartbeats += 1
        elif message["type"] == "Data":
            try:
                record = self._normalize(session_id, sequence, received_at, message)
            except ValueError:
                self.metrics.validation_failures += 1
                return
            if self.lakehouse.append_normalized(record):
                self.metrics.normalized_records += 1
                self._build_signal(record)
        elapsed_ms = (time.perf_counter_ns() - start) / 1_000_000
        self.metrics.processing_latency_ms.append(elapsed_ms)

    def _normalize(
        self,
        session_id: str,
        sequence: int,
        received_at: str,
        message: dict[str, Any],
    ) -> NormalizedRecord:
        family = message["family"]
        schema = self.config.schemas[family]
        values = message["values"]
        schema.validate(values)
        return NormalizedRecord(
            family=family,
            schema_version=schema.schema_version,
            record_kind=schema.record_kind,
            primary_key=schema.key_for(values),
            event_time=values["event_time"],
            received_at=received_at,
            session_id=session_id,
            sequence=sequence,
            manifest_version=self.config.manifest_version,
            values=values,
        )

    def _build_signal(self, record: NormalizedRecord) -> None:
        if record.family != "DemoEquityQuote":
            return
        self._quotes[record.primary_key] = record
        bid, ask = record.values["bid"], record.values["ask"]
        signal = {
            "signal_type": "synthetic_midpoint",
            "signal_version": "v1",
            "symbol": record.values["symbol"],
            "value": round((bid + ask) / 2, 4),
            "decision_time": utc_now(),
            "source": {
                "family": record.family,
                "primary_key": record.primary_key,
                "event_time": record.event_time,
                "session_id": record.session_id,
                "sequence": record.sequence,
                "schema_version": record.schema_version,
                "manifest_version": record.manifest_version,
            },
            "disclaimer": "Synthetic demonstration only; not a trading recommendation.",
        }
        self.lakehouse.append_signal(signal)
        self.metrics.signals += 1

