import copy
import io
import json
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from named_shadow_benchmark import (  # noqa: E402
    MemoryIoT, MeteredIoT, benchmark_metrics, cleanup, encode, main, merge, preset, presets,
    run_scenario, validate_scenario,
)
from migration_lab import ShadowCalls


def run(scenario, **kwargs):
    validate_scenario(scenario)
    return run_scenario(MemoryIoT(), scenario, things=("classic", "named"), repeats=1, **kwargs)


class BenchmarkTests(unittest.TestCase):
    def test_presets_include_savings_ties_and_losses(self):
        results = {scenario["name"]: run(scenario) for scenario in presets()}
        self.assertEqual(results["small-shadow"]["economics"]["conclusion"], "equal_estimated_units")
        self.assertEqual(results["hot-settings"]["classic"]["estimated_units_per_action"], 6)
        self.assertEqual(results["hot-settings"]["named"]["estimated_units_per_action"], 2)
        self.assertEqual(results["whole-state-reads"]["named"]["gets"], 40)
        self.assertLess(results["whole-state-reads"]["economics"]["saving_percent"], 0)
        self.assertEqual(results["cross-domain-writes"]["classic"]["updates"], 20)
        self.assertEqual(results["cross-domain-writes"]["named"]["updates"], 40)
        self.assertEqual(results["lookup-overhead"]["named"]["gets"], 20)
        self.assertIsNone(results["hot-settings"]["named"]["action_api_ms_p95"])

    def test_overlap_charges_only_extra_named_writes_and_delays_payback(self):
        base = run(preset("base"))
        overlap = run(preset("overlap", dual_write_actions=100))
        self.assertEqual(overlap["economics"]["overlap_extra_units_per_device_estimated"], 200)
        self.assertGreater(overlap["economics"]["break_even_logical_actions_per_device"],
                           base["economics"]["break_even_logical_actions_per_device"])
        self.assertGreater(overlap["phases"]["transition_reset"]["updates"], 0)
        self.assertTrue(overlap["migration_trace"]["retry"]["already_complete"])

    def test_user_partitions_nested_updates_and_mixed_workload(self):
        scenario = {
            "name": "custom",
            "groups": {
                "settings": {"state": {"desired": {"mode": {"speed": 1}}},
                             "write": {"desired": {"mode": {"speed": 2, "extra": True}}}},
                "status": {"state": {"reported": {"battery": 90}},
                           "write": {"reported": {"battery": 89}}},
                "software": {"state": {"reported": {"versions": [1, 2]}},
                             "write": {"reported": {"versions": [2, 3]}}},
            },
            "workload": [
                {"kind": "read", "groups": ["settings", "status", "software"], "count": 2},
                {"kind": "write", "groups": ["settings", "status"], "count": 3},
            ],
            "dual_write_actions": 3,
        }
        result = run(scenario, named_first=True, fleet=10, rate=1)
        self.assertEqual(result["classic"]["logical_actions"], 5)
        self.assertEqual(result["named"]["logical_actions"], 5)
        self.assertEqual(result["architecture_order"], ["named", "classic"])
        self.assertIn("monthly_shadow_operation_usd_saved_at_supplied_rate", result["economics"])

    def test_partial_update_response_does_not_shrink_record_estimate(self):
        meter = MeteredIoT(MemoryIoT())
        calls = ShadowCalls(meter, "thing")
        calls.update({"reported": {"padding": "x" * 3000, "value": 1}})
        meter.phase = "workload"
        calls.update({"reported": {"value": 2}})
        self.assertEqual(meter.counters()["estimated_units"], 3)
        self.assertLess(meter.counters()["response_payload_bytes"], 100)
        meter.phase = "verification"
        self.assertGreater(meter.verify("thing"), 3000)

    def test_delta_response_is_excluded_from_model_and_verification(self):
        class WithDelta(MemoryIoT):
            def get_thing_shadow(self, **kwargs):
                document = json.load(super().get_thing_shadow(**kwargs)["payload"])
                document["state"]["delta"] = {"padding": "d" * 6000}
                return {"payload": io.BytesIO(encode(document))}
        meter = MeteredIoT(WithDelta())
        ShadowCalls(meter, "thing").update({"desired": {"value": False}})
        self.assertLess(meter.verify("thing"), 100)
        self.assertNotIn("delta", meter.states[("thing", None)])
        ShadowCalls(meter, "thing").update({"desired": {"value": True}})
        self.assertLess(meter.verify("thing"), 100)

    def test_invalid_custom_inputs_fail_before_aws_calls(self):
        bad = preset("bad")
        bad["groups"]["rw-software"]["state"] = {"desired": {"childLock": False}}
        with self.assertRaisesRegex(ValueError, "two partitions"):
            validate_scenario(bad)
        for mutate in (
            lambda s: s["workload"][0].update(count=-1),
            lambda s: s.update(routing="guess"),
            lambda s: s["groups"]["rw-settings"]["write"]["desired"].update(childLock=None),
            lambda s: s["groups"]["rw-settings"]["state"]["desired"].update(settingsPadding="x" * 8192),
        ):
            bad = preset("bad")
            mutate(bad)
            with self.assertRaises(ValueError):
                validate_scenario(bad)

    def test_independent_growth_cannot_bypass_size_limit(self):
        scenario = preset("growth", settings=100, software=7000)
        scenario["groups"]["rw-settings"]["write"]["desired"]["settingsPadding"] = "x" * 7000
        scenario["groups"]["rw-software"]["write"]["reported"]["softwarePadding"] = "x" * 100
        # All-before and all-after fit, but writing settings alone does not.
        with self.assertRaisesRegex(ValueError, "8 KiB"):
            validate_scenario(scenario)

    def test_cleanup_continues_after_a_shadow_delete_failure(self):
        class Control:
            def __init__(self):
                self.deleted = []
            def delete_thing(self, *, thingName):
                self.deleted.append(thingName)
        class Data:
            def delete_thing_shadow(self, **kwargs):
                if kwargs.get("shadowName") == "settings":
                    raise RuntimeError("delete failed")
        control = Control()
        errors = cleanup(control, Data(), ["a", "b"], ["settings", "software"])
        self.assertEqual(control.deleted, ["b", "a"])
        self.assertEqual(len(errors), 2)

    def test_null_removal_and_nested_merge(self):
        state = {"reported": {"nested": {"a": 1, "b": 2}, "array": [1]}}
        result = merge(copy.deepcopy(state), {"reported": {"nested": {"a": None}, "array": [2, 3]}})
        self.assertEqual(result, {"reported": {"nested": {"b": 2}, "array": [2, 3]}})

    def test_live_cli_with_fake_sdk_reports_latency_and_cleans_resources(self):
        class Control:
            def __init__(self):
                self.created, self.deleted = [], []
            def describe_endpoint(self, **kwargs):
                return {"endpointAddress": "fake-ats.iot.us-east-1.amazonaws.com"}
            def create_thing(self, *, thingName):
                self.created.append(thingName)
            def delete_thing(self, *, thingName):
                self.deleted.append(thingName)
        class Data(MemoryIoT):
            def update_thing_shadow(self, **kwargs):
                result = super().update_thing_shadow(**kwargs)
                result["ResponseMetadata"] = {"RetryAttempts": 1}
                return result
            def delete_thing_shadow(self, *, thingName, shadowName=None):
                self.documents.pop((thingName, shadowName), None)
        class CloudWatch:
            def __init__(self):
                self.calls = []
            def put_metric_data(self, *, Namespace, MetricData):
                self.calls.append((Namespace, MetricData))
        control, data, cloudwatch = Control(), Data(), CloudWatch()
        clients = {"iot": control, "iot-data": data, "cloudwatch": cloudwatch}
        session = types.SimpleNamespace(client=lambda name, **kwargs: clients[name])
        modules = {
            "boto3": types.SimpleNamespace(Session=lambda **kwargs: session),
            "botocore.config": types.SimpleNamespace(Config=lambda **kwargs: kwargs),
        }
        with tempfile.TemporaryDirectory() as directory, patch.dict(sys.modules, modules), patch("named_shadow_benchmark.time.sleep"):
            output = Path(directory) / "live.json"
            code = main(["--mode", "live", "--scenario", "hot-settings", "--repeats", "1",
                         "--publish-cloudwatch", "--output", str(output)])
            report = json.loads(output.read_text())
        self.assertEqual(code, 0)
        self.assertCountEqual(control.created, control.deleted)
        self.assertFalse(data.documents)
        self.assertEqual(report["resources_requested"], report["resources_created"])
        result = report["scenarios"][0]
        self.assertIsNotNone(result["named"]["action_api_ms_p95"])
        self.assertEqual(result["named"]["gets"], 0)
        self.assertEqual(result["named"]["sdk_retries"], 20)
        self.assertTrue(report["cloudwatch_published"])
        self.assertEqual(report["cloudwatch_errors"], [])
        published = [datum for _, batch in cloudwatch.calls for datum in batch]
        self.assertTrue(all(namespace == "ShadowTalkDemo" for namespace, _ in cloudwatch.calls))
        self.assertTrue(all(len(batch) <= 20 for _, batch in cloudwatch.calls))
        self.assertEqual(len({d["Timestamp"] for d in published}), 1)  # one bucket for the bar widgets
        by_key = {(d["MetricName"], tuple(x["Value"] for x in d["Dimensions"])): d["Value"] for d in published}
        self.assertEqual(by_key[("BenchmarkUnitsPerAction", ("hot-settings", "classic"))], 6.0)
        self.assertEqual(by_key[("BenchmarkUnitsPerAction", ("hot-settings", "named"))], 2.0)
        self.assertIn(("BenchmarkPaybackActions", ("hot-settings",)), by_key)
        self.assertIn(("BenchmarkActionApiMsP95", ("hot-settings", "named")), by_key)

    def test_publish_requires_live_mode_and_skips_null_payback(self):
        with self.assertRaises(SystemExit):
            main(["--publish-cloudwatch", "--scenario", "hot-settings", "--output", "/dev/null"])
        result = run(preset("loss", settings=100, software=100, groups=["rw-settings", "rw-software"]))
        names = {datum["MetricName"] for datum in benchmark_metrics(result)}
        self.assertNotIn("BenchmarkPaybackActions", names)  # never pays back: no bar rather than a fake zero
        self.assertNotIn("BenchmarkActionApiMsP95", names)  # offline latency is null
        self.assertIn("BenchmarkSavingPercent", names)
        ratio = next(d["Value"] for d in benchmark_metrics(result) if d["MetricName"] == "BenchmarkNamedToClassicUnitRatio")
        self.assertEqual(ratio, 2.0)  # two named writes per action against one classic write


if __name__ == "__main__":
    unittest.main()
