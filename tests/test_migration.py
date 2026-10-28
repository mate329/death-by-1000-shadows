import copy
import io
import json
import sys
import unittest
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "lambdas"))
from migration_lab import ShadowCalls, dual_write_child_lock, migration_step, read_child_lock, write_child_lock  # noqa: E402


class MissingShadow(Exception):
    response = {"Error": {"Code": "ResourceNotFoundException"}}


class FakeIoTData:
    def __init__(self):
        self.documents = {
            None: {
                "version": 1,
                "state": {
                    "desired": {"childLock": False, "settingsPadding": "s" * 1300},
                    "reported": {"cap": {"ns": 0}, "nsmip": 0, "sku": "DEMO",
                                 "softwareVersion": "1.0", "softwarePadding": "w" * 4500},
                },
            },
        }

    def get_thing_shadow(self, *, thingName, shadowName=None):
        if shadowName not in self.documents:
            raise MissingShadow()
        return {"payload": io.BytesIO(json.dumps(self.documents[shadowName]).encode())}

    def update_thing_shadow(self, *, thingName, payload, shadowName=None):
        update = json.loads(payload)
        document = self.documents.setdefault(shadowName, {"version": 0, "state": {}})
        if "version" in update and update["version"] != document["version"]:
            raise ValueError("version conflict")
        for section, values in update["state"].items():
            dest = document["state"].setdefault(section, {})
            for field, value in values.items():
                if value is None:
                    dest.pop(field, None)
                else:
                    dest[field] = copy.deepcopy(value)
        document["version"] += 1
        return {"payload": io.BytesIO(json.dumps(document).encode())}


class MigrationTests(unittest.TestCase):
    def test_pause_resume_copy_verify_clear_and_idempotence(self):
        client = FakeIoTData()
        first = ShadowCalls(client, "demo")
        self.assertEqual(migration_step(first, pause_after_copy=True)["stage"], "in_progress")
        classic = client.documents[None]["state"]
        self.assertEqual(classic["reported"]["nsmip"], 1)
        self.assertEqual(classic["reported"]["cap"]["ns"], 0)
        self.assertIn("childLock", classic["desired"])
        self.assertEqual(read_child_lock(ShadowCalls(client, "demo"))["source"], "classic")
        dual = dual_write_child_lock(ShadowCalls(client, "demo"), value=True)
        self.assertEqual(dual["updates"], 2)
        self.assertTrue(dual["classic_value"])
        self.assertTrue(dual["named_value"])

        second = ShadowCalls(client, "demo")
        self.assertEqual(migration_step(second)["stage"], "complete")
        classic = client.documents[None]["state"]
        self.assertEqual(classic["reported"]["nsmip"], 2)
        self.assertEqual(classic["reported"]["cap"]["ns"], 1)
        self.assertNotIn("softwarePadding", classic["reported"])
        self.assertNotIn("childLock", classic["desired"])
        self.assertEqual(client.documents["rw-settings"]["state"]["desired"]["childLock"], True)
        self.assertEqual(client.documents["rw-software"]["state"]["reported"]["softwareVersion"], "1.0")

        third = ShadowCalls(client, "demo")
        self.assertTrue(migration_step(third)["already_complete"])
        self.assertEqual(third.updates, 0)

    def test_staged_read_falls_back_and_lookup_adds_units(self):
        client = FakeIoTData()
        client.documents[None]["state"]["reported"]["nsmip"] = 1
        self.assertEqual(read_child_lock(ShadowCalls(client, "demo"))["source"], "classic")
        migration_step(ShadowCalls(client, "demo"))
        known = write_child_lock(ShadowCalls(client, "demo"), value=True, routing="known", named_capable=True)
        lookup = write_child_lock(ShadowCalls(client, "demo"), value=False, routing="lookup")
        self.assertEqual(known["target"], "rw-settings")
        self.assertLess(known["touched_units_estimated"], 6)
        self.assertGreater(lookup["workload_units_estimated"], known["workload_units_estimated"])
        self.assertEqual(lookup["routing_lookup_gets"], 1)

    def test_concurrent_classic_change_is_not_cleared(self):
        class ChangingClient(FakeIoTData):
            def __init__(self):
                super().__init__()
                self.classic_gets = 0

            def get_thing_shadow(self, *, thingName, shadowName=None):
                if shadowName is None:
                    self.classic_gets += 1
                    if self.classic_gets == 2:
                        self.documents[None]["state"]["desired"]["childLock"] = True
                        self.documents[None]["version"] += 1
                return super().get_thing_shadow(thingName=thingName, shadowName=shadowName)

        client = ChangingClient()
        with self.assertRaisesRegex(ValueError, "changed during copy"):
            migration_step(ShadowCalls(client, "demo"))
        self.assertEqual(client.documents[None]["state"]["reported"]["nsmip"], 1)
        self.assertTrue(client.documents[None]["state"]["desired"]["childLock"])
        self.assertEqual(migration_step(ShadowCalls(client, "demo"))["stage"], "complete")
        self.assertTrue(client.documents["rw-settings"]["state"]["desired"]["childLock"])


if __name__ == "__main__":
    unittest.main()
