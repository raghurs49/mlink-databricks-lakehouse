import json
import tempfile
import unittest
from pathlib import Path

from mlink_lakehouse.config import FamilySchema, load_config


class ConfigTests(unittest.TestCase):
    def test_schema_validation_and_key(self):
        schema = FamilySchema("Quote", 1, "current_state", ("symbol",), {"symbol": "str", "bid": "float"})
        schema.validate({"symbol": "DEMO", "bid": 1.5})
        self.assertEqual(schema.key_for({"symbol": "DEMO", "bid": 1.5}), "DEMO")
        with self.assertRaises(ValueError):
            schema.validate({"symbol": "DEMO"})

    def test_unknown_subscription_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            schemas = root / "schemas.json"
            manifest = root / "manifest.json"
            schemas.write_text(json.dumps({"registry_version": "1", "families": {}}))
            manifest.write_text(json.dumps({"manifest_version": "1", "subscriptions": [
                {"family": "Missing", "symbols": ["X"], "enabled": True}
            ]}))
            with self.assertRaises(ValueError):
                load_config(schemas, manifest)


if __name__ == "__main__":
    unittest.main()

