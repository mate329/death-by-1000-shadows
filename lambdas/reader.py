import json
import os
import time

import boto3

from common import ShadowResolver, estimated_shadow_units, make_iot_data_client, metric_log


# Warm Lambda environments reuse clients. Shadow values live only in a resolver instance.
_shadow_client = make_iot_data_client()
_dynamodb = boto3.resource("dynamodb")


def handler(event, context):
    started = time.perf_counter()
    pattern = os.environ["PATTERN"]
    device_id = event.get("device_id", os.environ["THING_NAME"])
    if device_id != os.environ["THING_NAME"]:
        raise ValueError("This demo function is restricted to its configured Thing")
    fields = event.get("fields", ["sku", "soldAsSku", "batteryPercent", "sku"])
    if not isinstance(fields, list) or not all(isinstance(field, str) for field in fields):
        raise ValueError("fields must be a list of strings")
    initial_shadow = event.get("initial_shadow")
    if initial_shadow is not None and not isinstance(initial_shadow, dict):
        raise ValueError("initial_shadow must be a shadow document object")

    values = []
    counts = {name: 0 for name in (
        "ShadowGets", "ShadowUnitsEstimated", "PayloadHits", "ObjectHits", "CacheHits",
        "CacheMisses", "CacheReads", "CacheWrites", "CacheErrors",
    )}
    try:
        if pattern == "Before":
            for field in fields:
                response = _shadow_client.get_thing_shadow(thingName=device_id)
                raw = response["payload"].read()
                shadow = json.loads(raw)
                counts["ShadowGets"] += 1
                counts["ShadowUnitsEstimated"] += estimated_shadow_units(shadow)
                values.append({"field": field, "value": shadow.get("state", {}).get("reported", {}).get(field), "source": "iot"})
        else:
            table = _dynamodb.Table(os.environ["CACHE_TABLE"])
            resolver = ShadowResolver(
                device_id, shadow_client=_shadow_client, table=table,
                initial_shadow=initial_shadow,
            )
            for field in fields:
                value, source = resolver.get(field)
                values.append({"field": field, "value": value, "source": source})
            counts = resolver.counts
        return {"device_id": device_id, "pattern": pattern, "values": values, "counts": counts}
    finally:
        counts["InvocationMs"] = round((time.perf_counter() - started) * 1000, 2)
        metric_log(pattern, counts, kind="read")
