from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

from .config import load_config
from .mock_protocol import MockMlinkServer
from .pipeline import StreamingPipeline
from .storage import LocalLakehouse


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the synthetic MLink lakehouse demo")
    parser.add_argument("--schemas", type=Path, default=Path("configs/schema_registry.json"))
    parser.add_argument("--subscriptions", type=Path, default=Path("configs/subscriptions.json"))
    parser.add_argument("--output", type=Path, default=Path("demo-output"))
    parser.add_argument("--records", type=int, default=25)
    parser.add_argument("--disconnect-after", type=int, default=8)
    return parser


async def _run(args: argparse.Namespace) -> dict:
    config = load_config(args.schemas, args.subscriptions)
    server = MockMlinkServer(record_count=args.records, disconnect_once_after=args.disconnect_after)
    pipeline = StreamingPipeline(config, server, LocalLakehouse(args.output))
    return await pipeline.run(target_records=args.records)


def main() -> None:
    args = build_parser().parse_args()
    report = asyncio.run(_run(args))
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()

