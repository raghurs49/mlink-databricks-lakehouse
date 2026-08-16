import asyncio
import json
import tempfile
import unittest
from pathlib import Path

from mlink_lakehouse.config import load_config
from mlink_lakehouse.mock_protocol import MockMlinkServer
from mlink_lakehouse.pipeline import StreamingPipeline
from mlink_lakehouse.storage import LocalLakehouse


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class PipelineTests(unittest.TestCase):
    def test_reconnects_and_writes_all_layers(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = load_config(
                PROJECT_ROOT / "configs/schema_registry.json",
                PROJECT_ROOT / "configs/subscriptions.json",
            )
            lakehouse = LocalLakehouse(Path(tmp))
            server = MockMlinkServer(record_count=12, disconnect_once_after=3)
            pipeline = StreamingPipeline(config, server, lakehouse)
            report = asyncio.run(pipeline.run(target_records=12))
            self.assertEqual(report["reconnects"], 1)
            self.assertGreaterEqual(report["connections"], 2)
            self.assertIn("Complete", report["checkpoints"])
            self.assertGreaterEqual(report["normalized_records"], 12)
            self.assertGreater(report["signals"], 0)
            self.assertTrue(lakehouse.bronze.exists())
            self.assertTrue(lakehouse.silver.exists())
            self.assertTrue(lakehouse.current.exists())
            self.assertTrue(lakehouse.gold.exists())
            saved = json.loads(lakehouse.metrics.read_text())
            self.assertIn("p95", saved["processing_latency_ms"])


if __name__ == "__main__":
    unittest.main()

