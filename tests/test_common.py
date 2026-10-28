import io
import json
import sys
import unittest
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "lambdas"))
from common import ShadowResolver, estimated_shadow_units, shadow_record_bytes, units_for_bytes  # noqa: E402


class FakeShadowClient:
    def __init__(self, reported, version=1):
        self.calls = 0
        self.body = json.dumps({"state": {"reported": reported}, "version": version}).encode()

    def get_thing_shadow(self, thingName):
        self.calls += 1
        return {"payload": io.BytesIO(self.body)}


class FakeTable:
    def __init__(self):
        self.items = {}
        self.fail_reads = False

    def get_item(self, Key):
        if self.fail_reads:
            raise RuntimeError("DynamoDB unavailable")
        item = self.items.get(Key["device_id"])
        return {"Item": item} if item else {}

    def put_item(self, Item, **kwargs):
        self.items[Item["device_id"]] = Item


class ResolverTests(unittest.TestCase):
    def test_payload_and_null_are_values(self):
        client = FakeShadowClient({"sku": "origin"})
        resolver = ShadowResolver("demo", shadow_client=client,
            initial_shadow={"state": {"reported": {"sku": None}}})
        self.assertEqual(resolver.get("sku"), (None, "payload"))
        self.assertEqual(client.calls, 0)

    def test_miss_then_object_then_cache(self):
        client = FakeShadowClient({"sku": "A", "soldAsSku": None, "batteryPercent": 80})
        table = FakeTable()
        first = ShadowResolver("demo", shadow_client=client, table=table, now=lambda: 100)
        self.assertEqual(first.get("sku"), ("A", "iot"))
        self.assertEqual(first.get("batteryPercent"), (80, "object"))
        self.assertEqual(client.calls, 1)
        second = ShadowResolver("demo", shadow_client=client, table=table, now=lambda: 101)
        self.assertEqual(second.get("sku"), ("A", "dynamodb"))
        self.assertEqual(second.get("sku"), ("A", "object"))
        self.assertEqual(second.counts["CacheReads"], 1)
        self.assertEqual(client.calls, 1)

    def test_expired_item_and_cache_error_fall_back(self):
        client = FakeShadowClient({"sku": "origin"})
        table = FakeTable()
        table.items["demo:sku"] = {"value": "stale", "expires_at": 99}
        resolver = ShadowResolver("demo", shadow_client=client, table=table, now=lambda: 100)
        self.assertEqual(resolver.get("sku"), ("origin", "iot"))
        self.assertEqual(resolver.counts["CacheMisses"], 1)
        table.fail_reads = True
        another = ShadowResolver("demo", shadow_client=client, table=table, now=lambda: 100)
        self.assertEqual(another.get("sku"), ("origin", "iot"))
        self.assertEqual(another.counts["CacheErrors"], 1)

    def test_missing_field_does_not_refetch_same_object(self):
        client = FakeShadowClient({"sku": "A"})
        resolver = ShadowResolver("demo", shadow_client=client)
        self.assertEqual(resolver.get("batteryPercent"), (None, "iot"))
        self.assertEqual(resolver.get("batteryPercent"), (None, "object"))
        self.assertEqual(client.calls, 1)

    def test_billing_boundaries(self):
        self.assertEqual(units_for_bytes(1025, 1024), 2)
        self.assertEqual(units_for_bytes(5 * 1024, 5 * 1024), 1)
        self.assertEqual(units_for_bytes(5 * 1024 + 1, 5 * 1024), 2)
        self.assertEqual(estimated_shadow_units({"state": {"reported": {"sku": "A"}}, "metadata": {"filler": "x" * 4000}}), 1)
        self.assertEqual(shadow_record_bytes({"state": {"reported": {"sku": "A"}, "delta": {"filler": "x" * 4000}}}),
                         shadow_record_bytes({"state": {"reported": {"sku": "A"}}}))


if __name__ == "__main__":
    unittest.main()
