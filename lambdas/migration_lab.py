"""Small, explicit classic-to-named-shadow migration experiment."""

import json
import os
import time

from common import MISSING, estimated_shadow_units, make_iot_data_client, metric_log, shadow_record_bytes


NAMED_FIELDS = {
    "rw-settings": {"desired": ("childLock", "settingsPadding")},
    "rw-software": {"reported": ("softwareVersion", "softwarePadding")},
}


def _is_missing(error):
    return getattr(error, "response", {}).get("Error", {}).get("Code") == "ResourceNotFoundException"


def _state_bytes(document):
    return shadow_record_bytes(document)


class ShadowCalls:
    def __init__(self, client, thing_name):
        self.client = client
        self.thing_name = thing_name
        self.gets = 0
        self.updates = 0
        self.get_units = 0

    def get(self, shadow_name=None, *, missing_ok=False):
        kwargs = {"thingName": self.thing_name}
        if shadow_name:
            kwargs["shadowName"] = shadow_name
        self.gets += 1
        try:
            document = json.loads(self.client.get_thing_shadow(**kwargs)["payload"].read())
        except Exception as error:
            if missing_ok and _is_missing(error):
                return None
            raise
        self.get_units += estimated_shadow_units(document)
        return document

    def update(self, state, shadow_name=None, *, version=None):
        body = {"state": state}
        if version is not None:
            body["version"] = version
        kwargs = {"thingName": self.thing_name, "payload": json.dumps(body, separators=(",", ":")).encode()}
        if shadow_name:
            kwargs["shadowName"] = shadow_name
        response = json.loads(self.client.update_thing_shadow(**kwargs)["payload"].read())
        self.updates += 1
        return response


def migration_step(calls, *, pause_after_copy=False, named_fields=None):
    """Copy, verify, clear, complete. A paused invocation can be repeated safely."""
    # The community benchmark can supply its own partition map. The deployed
    # talk Lambda retains the original settings/software map by default.
    fields_by_shadow = NAMED_FIELDS if named_fields is None else named_fields
    classic = calls.get()
    reported = classic.get("state", {}).get("reported", {})
    stage = int(reported.get("nsmip", 0))
    if stage == 2:
        return {"stage": "complete", "already_complete": True}
    if stage not in (0, 1):
        raise ValueError(f"Unexpected migration stage {stage}")

    if stage == 0:
        calls.update({"reported": {"nsmip": 1}}, version=classic["version"])

    # The original classic data remains available until named copies are verified.
    source = classic.get("state", {})
    for shadow_name, sections in fields_by_shadow.items():
        existing = calls.get(shadow_name, missing_ok=True)
        patch = {}
        for section, fields in sections.items():
            values = source.get(section, {})
            if values:
                patch[section] = {field: values[field] for field in fields if field in values}
        if patch:
            current = (existing or {}).get("state", {})
            if any(current.get(section, {}).get(field, object()) != value
                   for section, values in patch.items() for field, value in values.items()):
                calls.update(patch, shadow_name)
                existing = calls.get(shadow_name)
        if not existing:
            raise ValueError(f"Missing {shadow_name} after copy")
        for section, fields in sections.items():
            values = existing.get("state", {}).get(section, {})
            for field in fields:
                if field not in values:
                    raise ValueError(f"Missing {shadow_name}.{section}.{field} after copy")
                if field in source.get(section, {}) and values[field] != source[section][field]:
                    raise ValueError(f"Mismatch in {shadow_name}.{section}.{field}")

    if pause_after_copy:
        return {"stage": "in_progress", "paused_after_copy": True}

    latest = calls.get()
    for sections in fields_by_shadow.values():
        for section, fields in sections.items():
            old_values = source.get(section, {})
            new_values = latest.get("state", {}).get(section, {})
            for field in fields:
                if old_values.get(field, MISSING) != new_values.get(field, MISSING):
                    raise ValueError(f"Classic {section}.{field} changed during copy; retry migration")
    clear = {}
    for sections in fields_by_shadow.values():
        for section, fields in sections.items():
            present = latest.get("state", {}).get(section, {})
            values = {field: None for field in fields if field in present}
            if values:
                clear.setdefault(section, {}).update(values)
    # One version-checked classic update switches routing and removes the old
    # fields. A failed update leaves the in-progress classic copy intact.
    capability = dict(latest.get("state", {}).get("reported", {}).get("cap") or {})
    capability["ns"] = 1
    clear.setdefault("reported", {}).update({"cap": capability, "nsmip": 2})
    calls.update(clear, version=latest["version"])
    return {"stage": "complete", "already_complete": False}


def read_child_lock(calls):
    classic = calls.get()
    stage = int(classic.get("state", {}).get("reported", {}).get("nsmip", 0))
    if stage in (0, 1) and "childLock" in classic.get("state", {}).get("desired", {}):
        return {"stage": stage, "source": "classic", "value": classic.get("state", {}).get("desired", {}).get("childLock")}
    named = calls.get("rw-settings", missing_ok=(stage == 1))
    if named and "childLock" in named.get("state", {}).get("desired", {}):
        return {"stage": stage, "source": "rw-settings", "value": named["state"]["desired"]["childLock"]}
    if stage == 1:
        raise ValueError("Migration in progress but childLock is absent from both classic and rw-settings")
    raise ValueError("Migration is complete but rw-settings.childLock is missing")


def write_child_lock(calls, *, value, routing, named_capable=None):
    """The probe GET measures record size and is excluded from workload units."""
    lookup_gets = 0
    lookup_units = 0
    if routing == "lookup":
        classic = calls.get()
        lookup_gets = 1
        lookup_units = estimated_shadow_units(classic)
        named_capable = int(classic.get("state", {}).get("reported", {}).get("nsmip", 0)) == 2
    elif routing != "known" or not isinstance(named_capable, bool):
        raise ValueError("Use routing='known' with named_capable, or routing='lookup'")

    target = "rw-settings" if named_capable else None
    calls.update({"desired": {"childLock": value}}, target)
    touched = calls.get(target)  # measurement probe; real API cost, shown separately
    units = estimated_shadow_units(touched)
    return {
        "pattern": "After" if named_capable else "Before",
        "routing": routing,
        "target": target or "classic",
        "value": touched.get("state", {}).get("desired", {}).get("childLock"),
        "touched_record_bytes": _state_bytes(touched),
        "touched_units_estimated": units,
        "routing_lookup_gets": lookup_gets,
        "routing_lookup_units_estimated": lookup_units,
        "workload_units_estimated": units + lookup_units,
        "measurement_probe_gets": 1,
    }


def dual_write_child_lock(calls, *, value):
    """The caller writes both independent shadows while old/new consumers coexist."""
    calls.update({"desired": {"childLock": value}})
    calls.update({"desired": {"childLock": value}}, "rw-settings")
    classic = calls.get()
    named = calls.get("rw-settings")
    old_value = classic.get("state", {}).get("desired", {}).get("childLock", MISSING)
    new_value = named.get("state", {}).get("desired", {}).get("childLock", MISSING)
    if old_value is MISSING or new_value is MISSING or old_value != new_value:
        raise ValueError("Dual-write values differ; retry before completing migration")
    return {
        "classic_value": old_value,
        "named_value": new_value,
        "updates": 2,
        "measurement_probe_gets": 2,
        "update_units_estimated": estimated_shadow_units(classic) + estimated_shadow_units(named),
    }


_client = None


def handler(event, context):
    global _client
    device_id = event.get("device_id", os.environ["MIGRATION_THING_NAME"])
    if device_id != os.environ["MIGRATION_THING_NAME"]:
        raise ValueError("This demo function is restricted to its configured Thing")
    if _client is None:
        _client = make_iot_data_client()
    calls = ShadowCalls(_client, device_id)
    action = event.get("action")
    started = time.perf_counter()
    if action == "migrate":
        result = migration_step(calls, pause_after_copy=event.get("pause_after_copy", False))
        metric_log("Migration", {
            "MigrationGets": calls.gets, "MigrationUpdates": calls.updates,
            "MigrationPaused": int(result.get("paused_after_copy", False)),
            "MigrationMs": round((time.perf_counter() - started) * 1000, 2),
        }, kind="migration", stage=result["stage"])
    elif action == "read":
        result = read_child_lock(calls)
        metric_log("Migration", {"MigrationReadGets": calls.gets}, kind="migration_read", stage=result["stage"])
    elif action == "dual_write":
        result = dual_write_child_lock(calls, value=event.get("value"))
        metric_log("Migration", {
            "DualWriteUpdates": result["updates"],
            "DualWriteUnitsEstimated": result["update_units_estimated"],
            "MeasurementProbeGets": result["measurement_probe_gets"],
        }, kind="migration_dual_write")
    elif action == "write":
        result = write_child_lock(
            calls, value=event.get("value"), routing=event.get("routing", "known"),
            named_capable=event.get("named_capable"),
        )
        metrics = {
            "TouchedRecordBytes": result["touched_record_bytes"],
            "TouchedUnitsEstimated": result["touched_units_estimated"],
            "RoutingLookupGets": result["routing_lookup_gets"],
            "MeasurementProbeGets": result["measurement_probe_gets"],
            "HotWriteMs": round((time.perf_counter() - started) * 1000, 2),
        }
        metric_name = "KnownRouteUnitsEstimated" if result["routing"] == "known" else "LookupRouteUnitsEstimated"
        metrics[metric_name] = result["workload_units_estimated"]
        metric_log(result["pattern"], metrics, kind="hot_write", target=result["target"], routing=result["routing"])
    else:
        raise ValueError("action must be migrate, read, dual_write, or write")
    return {"result": result, "api_calls": {"gets": calls.gets, "updates": calls.updates}}
