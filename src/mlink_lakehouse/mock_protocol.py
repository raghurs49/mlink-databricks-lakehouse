from __future__ import annotations

import asyncio
import random
import uuid
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from typing import Any


class SyntheticDisconnect(ConnectionError):
    """Raised by the mock transport to exercise client recovery."""


class MockMlinkSession:
    """In-process stand-in for a WebSocket session using documented lifecycle concepts.

    Message families and payloads are deliberately synthetic and are not copied from a
    proprietary SpiderRock schema.
    """

    def __init__(self, record_count: int, disconnect_after: int | None, seed: int = 7):
        self.session_id = str(uuid.uuid4())
        self.record_count = record_count
        self.disconnect_after = disconnect_after
        self._rng = random.Random(seed)
        self._authenticated = False
        self._subscriptions: list[dict[str, Any]] = []

    async def authenticate(self, token: str) -> dict[str, Any]:
        await asyncio.sleep(0)
        if token != "synthetic-demo-token":
            return {"type": "Admin", "authenticated": False, "reason": "invalid token"}
        self._authenticated = True
        return {"type": "Admin", "authenticated": True, "session_id": self.session_id}

    async def subscribe(self, subscriptions: list[dict[str, Any]]) -> dict[str, Any]:
        await asyncio.sleep(0)
        if not self._authenticated:
            return {"type": "StreamAck", "accepted": False, "reason": "not authenticated"}
        self._subscriptions = subscriptions
        return {"type": "StreamAck", "accepted": True, "count": len(subscriptions)}

    async def messages(self) -> AsyncIterator[dict[str, Any]]:
        if not self._subscriptions:
            raise RuntimeError("subscribe before reading messages")
        yield {"type": "CheckPoint", "state": "Begin"}
        yield self._quote("DEMO", 99.9, 100.1, bootstrap=True)
        yield {"type": "CheckPoint", "state": "Active"}
        yield {"type": "CheckPoint", "state": "Complete"}
        for index in range(self.record_count):
            await asyncio.sleep(0)
            if self.disconnect_after is not None and index == self.disconnect_after:
                raise SyntheticDisconnect("deliberate synthetic network interruption")
            if index % 5 == 0:
                yield {"type": "Heartbeat", "server_time": self._timestamp(index)}
            symbol = "DEMO" if index % 2 == 0 else "SYNTH"
            mid = 100 + self._rng.uniform(-1.0, 1.0)
            yield self._quote(symbol, mid - 0.05, mid + 0.05, bootstrap=False, index=index)
            if index % 3 == 0:
                yield {
                    "type": "Data",
                    "family": "DemoTradePrint",
                    "values": {
                        "symbol": "DEMO",
                        "trade_id": f"T-{index}",
                        "price": round(mid, 4),
                        "size": 10 + index,
                        "event_time": self._timestamp(index),
                    },
                }

    def _quote(
        self,
        symbol: str,
        bid: float,
        ask: float,
        bootstrap: bool,
        index: int = 0,
    ) -> dict[str, Any]:
        return {
            "type": "Data",
            "family": "DemoEquityQuote",
            "bootstrap": bootstrap,
            "values": {
                "symbol": symbol,
                "bid": round(bid, 4),
                "ask": round(ask, 4),
                "event_time": self._timestamp(index),
            },
        }

    @staticmethod
    def _timestamp(offset_ms: int) -> str:
        now = datetime.now(timezone.utc)
        return now.isoformat(timespec="milliseconds")


class MockMlinkServer:
    def __init__(self, record_count: int = 25, disconnect_once_after: int | None = 8):
        self.record_count = record_count
        self.disconnect_once_after = disconnect_once_after
        self.connections = 0

    async def connect(self) -> MockMlinkSession:
        self.connections += 1
        disconnect_after = self.disconnect_once_after if self.connections == 1 else None
        return MockMlinkSession(self.record_count, disconnect_after, seed=7 + self.connections)

