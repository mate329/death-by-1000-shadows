"""Experiment 2 only: replay matched workloads against classic and named shadows.

Offline mode requires only Python. Live mode requires boto3 and AWS credentials.
Run from the repository root; see EXPERIMENT_2.md for the configuration
schema, cost scope, IAM permissions, and presentation notes.
"""

import argparse
import copy
import io
import json
import math
import re
import statistics
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "lambdas"))
from common import estimated_shadow_units, shadow_record_bytes  # noqa: E402
from migration_lab import ShadowCalls, migration_step  # noqa: E402


def encode(value):
    return json.dumps(value, separators=(",", ":"), allow_nan=False).encode()


def merge(destination, patch):
    """AWS shadow object merge: null deletes, arrays/scalars replace."""
    for key, value in patch.items():
        if value is None:
            destination.pop(key, None)
        elif isinstance(value, dict):
            if not isinstance(destination.get(key), dict):
                destination[key] = {}
            merge(destination[key], value)
            if not destination[key]:
                destination.pop(key, None)
        else:
            destination[key] = copy.deepcopy(value)
    return destination


def combined(states):
    result = {}
    for state in states:
        merge(result, state)
    return result


def stored_state(document):
    """Computed delta is a response view, not stored desired/reported data."""
    state = document.get("state", {})
    return {section: copy.deepcopy(state[section]) for section in ("desired", "reported")
            if state.get(section)}


class MissingShadow(Exception):
    response = {"Error": {"Code": "ResourceNotFoundException"}}


class MemoryIoT:
    """State/version emulator, not a latency, concurrency, or billing emulator."""

    def __init__(self):
        self.documents = {}

    def get_thing_shadow(self, *, thingName, shadowName=None):
        key = (thingName, shadowName)
        if key not in self.documents:
            raise MissingShadow()
        return {"payload": io.BytesIO(encode(self.documents[key]))}

    def update_thing_shadow(self, *, thingName, payload, shadowName=None):
        request = json.loads(payload)
        key = (thingName, shadowName)
        document = self.documents.setdefault(key, {"state": {}, "version": 0})
        if request.get("version", document["version"]) != document["version"]:
            raise ValueError("Shadow version conflict")
        next_state = merge(copy.deepcopy(document["state"]), request["state"])
        if len(encode(next_state)) > 8192:
            raise ValueError("Shadow exceeds the 8 KiB state limit")
        document["state"] = next_state
        document["version"] += 1
        # Return a patch, as AWS does, to avoid assuming update replies contain
        # the complete stored shadow when estimating update units.
        return {"payload": io.BytesIO(encode({
            "state": request["state"], "version": document["version"],
        }))}


def empty_metrics():
    return {"gets": 0, "updates": 0, "estimated_units": 0,
            "request_payload_bytes": 0, "response_payload_bytes": 0,
            "sdk_retries": 0, "not_found_gets": 0, "errors": 0,
            "api_ms": 0.0}


class MeteredIoT:
    """Measure SDK calls by phase; pacing and verification are outside timings."""

    def __init__(self, client, *, live=False, interval=0.12):
        self.client = client
        self.live = live
        self.interval = interval
        self.last_call = 0.0
        self.phase = "setup"
        self.metrics = {}
        self.states = {}

    def counters(self):
        return self.metrics.setdefault(self.phase, empty_metrics())

    def _call(self, method, kwargs):
        counters = self.counters()
        operation = "gets" if method == "get_thing_shadow" else "updates"
        counters[operation] += 1
        counters["request_payload_bytes"] += len(kwargs.get("payload", b""))
        if self.live:
            time.sleep(max(0, self.interval - (time.monotonic() - self.last_call)))
        started = time.perf_counter()
        try:
            response = getattr(self.client, method)(**kwargs)
            raw = response["payload"].read()
            document = json.loads(raw)
            counters["response_payload_bytes"] += len(raw)
            counters["sdk_retries"] += response.get("ResponseMetadata", {}).get("RetryAttempts", 0)
            return document, raw
        except Exception as error:
            code = getattr(error, "response", {}).get("Error", {}).get("Code")
            if method == "get_thing_shadow" and code == "ResourceNotFoundException":
                counters["not_found_gets"] += 1
                counters["estimated_units"] += 1  # explicit minimum-unit proxy
            else:
                counters["errors"] += 1
            raise
        finally:
            if self.live:
                counters["api_ms"] += (time.perf_counter() - started) * 1000
                self.last_call = time.monotonic()

    def get_thing_shadow(self, **kwargs):
        document, raw = self._call("get_thing_shadow", kwargs)
        key = (kwargs["thingName"], kwargs.get("shadowName"))
        self.states[key] = stored_state(document)
        self.counters()["estimated_units"] += estimated_shadow_units(document)
        return {"payload": io.BytesIO(raw)}

    def update_thing_shadow(self, **kwargs):
        document, raw = self._call("update_thing_shadow", kwargs)
        key = (kwargs["thingName"], kwargs.get("shadowName"))
        next_state = merge(copy.deepcopy(self.states.get(key, {})), json.loads(kwargs["payload"])["state"])
        self.states[key] = next_state
        self.counters()["estimated_units"] += estimated_shadow_units({"state": next_state})
        return {"payload": io.BytesIO(raw)}

    def verify(self, thing, name=None):
        expected = copy.deepcopy(self.states[(thing, name)])
        calls = ShadowCalls(self, thing)
        observed = stored_state(calls.get(name))
        if observed != expected:
            raise ValueError(f"Stored state differs from the benchmark model: {thing}/{name or 'classic'}")
        return shadow_record_bytes({"state": observed})


def preset(name, settings=1300, software=4500, *, kind="write", groups=None,
           routing="known", dual_write_actions=0):
    return {
        "name": name,
        "groups": {
            "rw-settings": {"state": {"desired": {
                "childLock": False, "settingsPadding": "s" * settings,
            }}, "write": {"desired": {"childLock": True}}},
            "rw-software": {"state": {"reported": {
                "softwareVersion": "1.0", "softwarePadding": "w" * software,
            }}, "write": {"reported": {"softwareVersion": "1.1"}}},
        },
        "workload": [{"kind": kind, "groups": groups or ["rw-settings"], "count": 20}],
        "routing": routing,
        "dual_write_actions": dual_write_actions,
        "logical_actions_per_device_per_day": 1000,
    }


def presets():
    return [
        preset("small-shadow", settings=100, software=100),
        preset("hot-settings"),
        preset("targeted-reads", kind="read"),
        preset("whole-state-reads", kind="read", groups=["rw-settings", "rw-software"]),
        preset("cross-domain-writes", settings=100, software=100,
               groups=["rw-settings", "rw-software"]),
        preset("lookup-overhead", settings=100, software=100, routing="lookup"),
        preset("migration-overlap", dual_write_actions=100),
    ]


def integer(value, label, minimum=0):
    if type(value) is not int or value < minimum:
        raise ValueError(f"{label} must be an integer >= {minimum}")


def validate_state(state, label):
    if not isinstance(state, dict) or not state or set(state) - {"desired", "reported"}:
        raise ValueError(f"{label} must contain desired/reported objects")
    def walk(value, depth=0):
        if depth > 6:
            raise ValueError(f"{label}: nested payload is too deep for this benchmark")
        if value is None or value == {}:
            raise ValueError(f"{label}: null and empty objects are unsupported in workload input")
        if isinstance(value, dict):
            for key, child in value.items():
                if not isinstance(key, str) or not key:
                    raise ValueError(f"{label}: field names must be nonempty strings")
                walk(child, depth + 1)
        elif isinstance(value, list):
            for child in value:
                walk(child, depth + 1)
    for section, fields in state.items():
        if not isinstance(fields, dict) or not fields:
            raise ValueError(f"{label}.{section} must be a nonempty object")
        if section == "reported" and set(fields) & {"cap", "nsmip"}:
            raise ValueError(f"{label}: reported cap/nsmip are reserved migration markers")
        walk(fields)
    encode(state)  # reject NaN and non-JSON values


def validate_scenario(scenario):
    if not isinstance(scenario, dict):
        raise ValueError("Each scenario must be an object")
    if not isinstance(scenario.get("name"), str) or not re.fullmatch(r"[a-zA-Z0-9_-]{1,64}", scenario["name"]):
        raise ValueError("Scenario names must use 1-64 letters, digits, hyphens or underscores")
    groups = scenario.get("groups")
    if not isinstance(groups, dict) or not 1 <= len(groups) <= 10:
        raise ValueError("groups must contain 1-10 named shadow partitions")
    fields = set()
    for name, group in groups.items():
        if not isinstance(name, str) or not re.fullmatch(r"[a-zA-Z0-9_-]{1,64}", name) or name.startswith("AWS"):
            raise ValueError(f"Invalid or reserved shadow name: {name}")
        if not isinstance(group, dict):
            raise ValueError(f"{name} must be an object")
        validate_state(group.get("state"), f"{name}.state")
        validate_state(group.get("write"), f"{name}.write")
        for section, values in group["state"].items():
            for field in values:
                key = (section, field)
                if key in fields:
                    raise ValueError(f"A top-level field cannot belong to two partitions: {key}")
                fields.add(key)
        for section, values in group["write"].items():
            if set(values) - set(group["state"].get(section, {})):
                raise ValueError(f"{name}.write must update existing top-level fields")
    if scenario.get("routing", "known") not in ("known", "lookup"):
        raise ValueError("routing must be known or lookup")
    integer(scenario.get("dual_write_actions", 0), "dual_write_actions")
    daily = scenario.get("logical_actions_per_device_per_day", 1000)
    if isinstance(daily, bool) or not isinstance(daily, (int, float)) or not math.isfinite(daily) or daily <= 0:
        raise ValueError("logical_actions_per_device_per_day must be finite and positive")
    workload = scenario.get("workload")
    if not isinstance(workload, list) or not workload:
        raise ValueError("workload must be a nonempty list")
    for action in workload:
        if not isinstance(action, dict) or action.get("kind") not in ("read", "write"):
            raise ValueError("Every workload action must have kind read or write")
        names = action.get("groups")
        if not isinstance(names, list) or not names or any(not isinstance(name, str) or name not in groups for name in names) or len(names) != len(set(names)):
            raise ValueError("Every action needs unique groups that exist in the scenario")
        integer(action.get("count"), "workload count", 1)
    # A workload may independently grow one partition and shrink another. Use
    # each field's largest reachable value rather than just checking all-before
    # and all-after; nested merges can retain keys added by a previous write.
    largest = seed_state(scenario)
    for group in groups.values():
        changed = merge(copy.deepcopy(group["state"]), group["write"])
        restored = merge(copy.deepcopy(changed), patches({"groups": {"g": group}}, ["g"], False)["g"])
        for section, values in group["state"].items():
            for field, value in values.items():
                largest[section][field] = max(
                    (value, changed[section][field], restored[section][field]), key=lambda v: len(encode(v)))
    # Include a conservative outer state envelope; marker values stay one digit.
    if len(encode({"state": largest})) > 8192:
        raise ValueError("Seed request exceeds this benchmark's conservative 8 KiB request limit")


def seed_state(scenario):
    return combined([*(g["state"] for g in scenario["groups"].values()),
                     {"reported": {"cap": {"ns": 0}, "nsmip": 0}}])


def patches(scenario, names, changed):
    result = {}
    for name in names:
        group = scenario["groups"][name]
        result[name] = group["write"] if changed else {
            section: {key: group["state"][section][key] for key in values}
            for section, values in group["write"].items()
        }
    return result


def percentile(values, fraction):
    return sorted(values)[max(0, math.ceil(len(values) * fraction) - 1)] if values else None


def replay(meter, thing, scenario, *, named, repeats):
    meter.phase = "workload_named" if named else "workload_classic"
    calls = ShadowCalls(meter, thing)
    latencies = []
    actions = 0
    for repeat in range(repeats):
        # Alternate order each cycle to reduce consistent first/last bias.
        workload = scenario["workload"] if repeat % 2 == 0 else list(reversed(scenario["workload"]))
        for action in workload:
            for _ in range(action["count"]):
                before = meter.counters()["api_ms"]
                if named and scenario.get("routing", "known") == "lookup":
                    if calls.get()["state"]["reported"]["nsmip"] != 2:
                        raise ValueError("Named workload started before migration completed")
                if action["kind"] == "read":
                    for name in action["groups"] if named else [None]:
                        calls.get(name)
                else:
                    updates = patches(scenario, action["groups"], changed=(actions % 2 == 0))
                    if named:
                        for name, patch in updates.items():
                            calls.update(patch, name)
                    else:
                        calls.update(combined(updates.values()))
                if meter.live:
                    latencies.append(meter.counters()["api_ms"] - before)
                actions += 1
    result = copy.deepcopy(meter.counters())
    result.update({"logical_actions": actions,
                   "estimated_units_per_action": result["estimated_units"] / actions,
                   "action_api_ms_p50": statistics.median(latencies) if latencies else None,
                   "action_api_ms_p95": percentile(latencies, 0.95)})
    return result


def economics(classic, named, migration_units, overlap_extra_units, scenario, fleet, rate):
    saved = classic["estimated_units_per_action"] - named["estimated_units_per_action"]
    overhead = migration_units + overlap_extra_units
    daily = saved * scenario.get("logical_actions_per_device_per_day", 1000)
    result = {
        "conclusion": "lower_estimated_units" if saved > 0 else "higher_estimated_units" if saved < 0 else "equal_estimated_units",
        "estimated_units_saved_per_action": saved,
        "saving_percent": 100 * saved / classic["estimated_units_per_action"],
        "migration_units_per_device_estimated": migration_units,
        "overlap_extra_units_per_device_estimated": overlap_extra_units,
        "break_even_logical_actions_per_device": math.ceil(overhead / saved) if saved > 0 else None,
        "break_even_days": overhead / daily if saved > 0 else None,
        "fleet_size": fleet,
        "steady_state_monthly_units_saved_estimated": daily * 30 * fleet,
    }
    if rate is not None:
        result["monthly_shadow_operation_usd_saved_at_supplied_rate"] = daily * 30 * fleet * rate / 1_000_000
        result["one_time_shadow_operation_usd_at_supplied_rate"] = overhead * fleet * rate / 1_000_000
    return result


def run_scenario(client, scenario, *, things, live=False, interval=0.12, repeats=3,
                 fleet=1, rate=None, named_first=False, meter=None):
    meter = meter if meter is not None else MeteredIoT(client, live=live, interval=interval)
    classic_thing, named_thing = things
    initial = seed_state(scenario)
    for thing in things:
        ShadowCalls(meter, thing).update(initial)
    meter.phase = "verification"
    initial_bytes = meter.verify(classic_thing)
    meter.verify(named_thing)
    fields = {name: {section: tuple(values) for section, values in group["state"].items()}
              for name, group in scenario["groups"].items()}
    meter.phase = "migration"
    paused = migration_step(ShadowCalls(meter, named_thing), pause_after_copy=True, named_fields=fields)
    meter.phase = "overlap"
    overlap_baseline_units = 0
    groups = next((a["groups"] for a in scenario["workload"] if a["kind"] == "write"), [next(iter(fields))])
    transition = ShadowCalls(meter, named_thing)
    overlap_count = scenario.get("dual_write_actions", 0)
    for index in range(overlap_count):
        updates = patches(scenario, groups, changed=(index % 2 == 0))
        transition.update(combined(updates.values()))
        overlap_baseline_units += estimated_shadow_units({"state": meter.states[(named_thing, None)]})
        for name, patch in updates.items():
            transition.update(patch, name)
    if overlap_count:
        # Restore exact seeded partitions, including removal of nested keys
        # introduced by updates. These artificial reset calls are benchmark
        # overhead, excluded from migration/transition payback arithmetic.
        meter.phase = "transition_reset"
        clear = {section: {field: None for field in values}
                 for section, values in combined(scenario["groups"][name]["state"] for name in groups).items()}
        transition.update(clear)
        transition.update(combined(scenario["groups"][name]["state"] for name in groups))
        for name in groups:
            transition.update({section: {field: None for field in values}
                               for section, values in scenario["groups"][name]["state"].items()}, name)
            transition.update(scenario["groups"][name]["state"], name)
    meter.phase = "migration"
    completed = migration_step(ShadowCalls(meter, named_thing), named_fields=fields)
    migration_units = meter.counters()["estimated_units"]
    overlap_units = meter.metrics.get("overlap", empty_metrics())["estimated_units"]
    meter.phase = "verification"
    retry_calls = ShadowCalls(meter, named_thing)
    retried = migration_step(retry_calls, named_fields=fields)
    if not retried["already_complete"] or retry_calls.updates:
        raise ValueError("Migration was not idempotent")
    record_sizes = {"classic_before": initial_bytes,
                    "classic_routing_stub_after": meter.verify(named_thing)}
    record_sizes.update({name: meter.verify(named_thing, name) for name in fields})
    # Alternate which architecture runs first across scenarios. All actions are
    # sequential: the cost/latency of multiple named-shadow calls stays visible.
    results = {}
    for is_named in ([True, False] if named_first else [False, True]):
        label = "named" if is_named else "classic"
        results[label] = replay(meter, named_thing if is_named else classic_thing,
                                scenario, named=is_named, repeats=repeats)
    classic, named = results["classic"], results["named"]
    meter.phase = "verification"
    meter.verify(classic_thing)
    for name in fields:
        meter.verify(named_thing, name)
    for section in ("desired", "reported"):
        observed_classic = meter.states[(classic_thing, None)].get(section, {})
        for name, sections in fields.items():
            observed_named = meter.states[(named_thing, name)].get(section, {})
            for field in sections.get(section, ()):
                if observed_classic.get(field) != observed_named.get(field):
                    raise ValueError(f"Final classic/named field differs: {section}.{field}")
    return {
        "name": scenario["name"], "status": "ok", "routing": scenario.get("routing", "known"),
        "architecture_order": ["named", "classic"] if named_first else ["classic", "named"],
        "record_bytes": record_sizes, "classic": classic, "named": named,
        "migration_trace": {"pause": paused, "resume": completed, "retry": retried},
        "phases": meter.metrics,
        "economics": economics(classic, named, migration_units,
                               overlap_units - overlap_baseline_units, scenario, fleet, rate),
    }


def benchmark_metrics(result, timestamp=None):
    """Flatten one scenario result into CloudWatch metric data for the dashboard.

    Dimensions are Scenario plus, for per-architecture series, Architecture.
    The deployed dashboard finds these with SEARCH expressions, so custom
    scenario names appear without a redeploy. Values remain unit proxies.
    All scenarios of one run share a timestamp: the dashboard's bar views
    render one time bucket, so scattered timestamps would hide scenarios.
    """
    scenario = result["name"]
    stamp = {"Timestamp": timestamp} if timestamp is not None else {}
    data = []
    for architecture in ("classic", "named"):
        stats = result[architecture]
        dimensions = [{"Name": "Scenario", "Value": scenario},
                      {"Name": "Architecture", "Value": architecture}]
        actions = stats["logical_actions"]
        series = {
            "BenchmarkUnitsPerAction": (stats["estimated_units_per_action"], "Count"),
            "BenchmarkApiCallsPerAction": ((stats["gets"] + stats["updates"]) / actions, "Count"),
            "BenchmarkResponseBytesPerAction": (stats["response_payload_bytes"] / actions, "Bytes"),
            "BenchmarkActionApiMsP50": (stats["action_api_ms_p50"], "Milliseconds"),
            "BenchmarkActionApiMsP95": (stats["action_api_ms_p95"], "Milliseconds"),
        }
        for name, (value, unit) in series.items():
            if value is not None:
                data.append({"MetricName": name, "Dimensions": dimensions, "Value": float(value), "Unit": unit, **stamp})
    econ = result["economics"]
    record = result["record_bytes"]
    named_bytes = sum(size for key, size in record.items() if key != "classic_before")
    dimensions = [{"Name": "Scenario", "Value": scenario}]
    series = {
        # Signed saving is kept for the metrics explorer; the dashboard bar view
        # clamps at zero, so it charts the always-positive ratio (1.0 = break-even).
        "BenchmarkSavingPercent": (econ["saving_percent"], "Percent"),
        "BenchmarkNamedToClassicUnitRatio": (
            result["named"]["estimated_units_per_action"] / result["classic"]["estimated_units_per_action"], "None"),
        "BenchmarkPaybackActions": (econ["break_even_logical_actions_per_device"], "Count"),
        "BenchmarkMigrationUnits": (econ["migration_units_per_device_estimated"], "Count"),
        "BenchmarkOverlapExtraUnits": (econ["overlap_extra_units_per_device_estimated"], "Count"),
        "BenchmarkClassicRecordBytes": (record["classic_before"], "Bytes"),
        "BenchmarkNamedRecordBytes": (named_bytes, "Bytes"),
    }
    for name, (value, unit) in series.items():
        if value is not None:
            data.append({"MetricName": name, "Dimensions": dimensions, "Value": float(value), "Unit": unit, **stamp})
    return data


def publish_metrics(cloudwatch, results, namespace="ShadowTalkDemo", timestamp=None):
    """Publish every successful scenario of a run under one shared timestamp."""
    timestamp = timestamp if timestamp is not None else datetime.now(timezone.utc)
    data = [datum for result in results for datum in benchmark_metrics(result, timestamp)]
    for start in range(0, len(data), 20):  # PutMetricData accepts 20 per request
        cloudwatch.put_metric_data(Namespace=namespace, MetricData=data[start:start + 20])
    return len(data)


def cleanup(control, data, created, groups):
    errors = []
    for thing in reversed(created):
        for name in [None, *groups]:
            kwargs = {"thingName": thing}
            if name:
                kwargs["shadowName"] = name
            try:
                data.delete_thing_shadow(**kwargs)
            except Exception as error:
                if getattr(error, "response", {}).get("Error", {}).get("Code") != "ResourceNotFoundException":
                    errors.append(f"Delete shadow {thing}/{name or 'classic'}: {error}")
        try:
            control.delete_thing(thingName=thing)
        except Exception as error:
            if getattr(error, "response", {}).get("Error", {}).get("Code") != "ResourceNotFoundException":
                errors.append(f"Delete Thing {thing}: {error}")
    return errors


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("offline", "live"), default="offline")
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--profile")
    parser.add_argument("--scenario", action="append", help="Select names; repeat to select several")
    parser.add_argument("--config", type=Path, help="JSON object containing a scenarios list")
    parser.add_argument("--write-example", type=Path, help="Write all editable presets and exit")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--fleet-size", type=int, default=1)
    parser.add_argument("--shadow-rate-per-million", type=float, help="Supplied regional USD rate; no default price")
    parser.add_argument("--interval-seconds", type=float, default=0.12, help="Minimum gap between live shadow calls")
    parser.add_argument("--output", type=Path, default=Path("experiment-2-results.json"))
    parser.add_argument("--publish-cloudwatch", action="store_true",
                        help="Live mode only: put per-scenario results into the ShadowTalkDemo "
                             "CloudWatch namespace for the deployed dashboard's Experiment 2 widgets")
    args = parser.parse_args(argv)
    try:
        integer(args.repeats, "repeats", 1)
        integer(args.fleet_size, "fleet-size", 1)
        if args.publish_cloudwatch and args.mode != "live":
            raise ValueError("--publish-cloudwatch requires --mode live; offline estimates stay off the dashboard")
        if not math.isfinite(args.interval_seconds) or args.interval_seconds < 0.06:
            raise ValueError("interval-seconds must be finite and >= 0.06")
        if args.shadow_rate_per_million is not None and (not math.isfinite(args.shadow_rate_per_million) or args.shadow_rate_per_million < 0):
            raise ValueError("shadow-rate-per-million must be finite and nonnegative")
        if args.write_example:
            args.write_example.write_text(json.dumps({"scenarios": presets()}, indent=2) + "\n")
            print(f"Editable presets: {args.write_example.resolve()}")
            return 0
        config = json.loads(args.config.read_text()) if args.config else {"scenarios": presets()}
        scenarios = config["scenarios"]
        if not isinstance(scenarios, list) or not scenarios:
            raise ValueError("scenarios must be a nonempty list")
        for scenario in scenarios:
            validate_scenario(scenario)
        names = [s["name"] for s in scenarios]
        if len(names) != len(set(names)):
            raise ValueError("Scenario names must be unique")
        if args.scenario:
            unknown = set(args.scenario) - set(names)
            if unknown:
                raise ValueError(f"Unknown scenarios: {', '.join(sorted(unknown))}")
            scenarios = [s for s in scenarios if s["name"] in args.scenario]
    except (ValueError, KeyError, TypeError, OSError) as error:
        parser.error(str(error))

    control = None
    cloudwatch = None
    client = MemoryIoT()
    if args.mode == "live":
        try:
            import boto3
            from botocore.config import Config
        except ImportError:
            parser.error("Live mode requires boto3: python -m pip install 'boto3>=1.34,<2'")
        session = boto3.Session(profile_name=args.profile, region_name=args.region)
        control = session.client("iot")
        endpoint = control.describe_endpoint(endpointType="iot:Data-ATS")["endpointAddress"]
        client = session.client("iot-data", endpoint_url=f"https://{endpoint}",
                                config=Config(retries={"mode": "standard", "total_max_attempts": 3},
                                              connect_timeout=10, read_timeout=30))
        if args.publish_cloudwatch:
            cloudwatch = session.client("cloudwatch")
    token = uuid.uuid4().hex[:12]
    report = {
        "experiment": 2, "mode": args.mode, "region": args.region,
        "created_at_utc": datetime.now(timezone.utc).isoformat(), "run_id": token,
        "configuration": scenarios, "repeats": args.repeats,
        "shadow_rate_per_million_usd": args.shadow_rate_per_million,
        "scope": "Shadow operation unit proxies, not billing exports. Units use complete desired/reported records, rounded to 1024 bytes. Missing migration GETs use one minimum unit. SDK retries are reported but not included in unit estimates. Setup, verification, transition_reset and cleanup are benchmark overhead. Migration and extra transition work enter break-even. Excludes MQTT, rules, compute, logging, engineering cost, and cross-shadow atomicity. Offline payload bytes are simulated; latency is null. Live bytes are SDK JSON payloads, excluding HTTP/TLS overhead. Latency sums sequential SDK-call durations per logical action, excluding pacing and verification.",
        "scenarios": [], "resources_requested": [], "resources_created": [], "cleanup_errors": [],
        "cloudwatch_published": args.publish_cloudwatch, "cloudwatch_errors": [],
    }
    # Verify the destination and persist names before mutations, so a stopped
    # run leaves enough information for manual cleanup of uncertain outcomes.
    try:
        args.output.write_text(json.dumps(report, indent=2, allow_nan=False) + "\n")
    except OSError as error:
        parser.error(f"Cannot write report: {error}")
    print(f"Experiment 2 | {args.mode} | {args.region} | run {token}")
    print("Scenario                 Classic u/action  Named u/action   Saving     Payback actions")
    failed = False
    for index, scenario in enumerate(scenarios):
        things = tuple(f"shadow-talk-bench-{token}-{index}-{label}" for label in ("classic", "named"))
        created = []
        meter = MeteredIoT(client, live=(args.mode == "live"), interval=args.interval_seconds)
        try:
            if control:
                for thing in things:
                    report["resources_requested"].append(thing)
                    args.output.write_text(json.dumps(report, indent=2, allow_nan=False) + "\n")
                    # A timed-out Create may have succeeded remotely. All names
                    # are freshly generated; try cleaning this name either way.
                    created.append(thing)
                    control.create_thing(thingName=thing)
                    report["resources_created"].append(thing)
            result = run_scenario(client, scenario, things=things, live=(args.mode == "live"),
                                  interval=args.interval_seconds, repeats=args.repeats,
                                  fleet=args.fleet_size, rate=args.shadow_rate_per_million,
                                  named_first=bool(index % 2), meter=meter)
            report["scenarios"].append(result)
            econ = result["economics"]
            payback = econ["break_even_logical_actions_per_device"]
            print(f"{scenario['name']:<25}{result['classic']['estimated_units_per_action']:>14.2f}"
                  f"{result['named']['estimated_units_per_action']:>16.2f}"
                  f"{econ['saving_percent']:>9.1f}%  {str(payback) if payback is not None else 'never':>17}", flush=True)
        except Exception as error:
            failed = True
            report["scenarios"].append({"name": scenario["name"], "status": "error",
                                        "error": str(error), "partial_phases": meter.metrics})
            print(f"{scenario['name']}: ERROR: {error}", file=sys.stderr, flush=True)
        finally:
            if control:
                report["cleanup_errors"].extend(cleanup(control, client, created, scenario["groups"]))
            args.output.write_text(json.dumps(report, indent=2, allow_nan=False) + "\n")
    if cloudwatch is not None:
        # Publishing is reporting, not measurement: a failure here must not
        # change scenario status, and it runs after every cleanup has finished.
        successful = [s for s in report["scenarios"] if s["status"] == "ok"]
        try:
            published_at = datetime.now(timezone.utc)
            count = publish_metrics(cloudwatch, successful, timestamp=published_at)
            report["cloudwatch_published_at_utc"] = published_at.isoformat()
            print(f"CloudWatch: published {count} datapoints for {len(successful)} scenario(s) to ShadowTalkDemo "
                  f"at {published_at.strftime('%H:%M:%S')} UTC; open the ShadowTalkDemo dashboard's Experiment 2 "
                  "section with a time range covering only this run.")
        except Exception as error:
            report["cloudwatch_errors"].append(str(error))
            print(f"CLOUDWATCH ERROR: {error}", file=sys.stderr, flush=True)
        args.output.write_text(json.dumps(report, indent=2, allow_nan=False) + "\n")
    print(f"Detailed report: {args.output.resolve()}")
    print("Unit estimates only; positive saving does not establish overall migration value.")
    for error in report["cleanup_errors"]:
        print(f"CLEANUP ERROR: {error}", file=sys.stderr)
    return 1 if failed or report["cleanup_errors"] or report["cloudwatch_errors"] else 0


if __name__ == "__main__":
    sys.exit(main())
