import json
import math
import time


NAMESPACE = "ShadowTalkDemo"
CACHEABLE_FIELDS = frozenset({"sku", "soldAsSku"})
CACHE_TTL_SECONDS = 3600
MISSING = object()


def make_iot_data_client():
    import boto3

    endpoint = boto3.client("iot").describe_endpoint(endpointType="iot:Data-ATS")["endpointAddress"]
    return boto3.client("iot-data", endpoint_url=f"https://{endpoint}")


def metric_log(pattern, values, **properties):
    """One CloudWatch Embedded Metric Format event, with bounded dimensions."""
    units = {
        "InvocationMs": "Milliseconds",
        "WriteInvocationMs": "Milliseconds",
        "MigrationMs": "Milliseconds",
        "HotWriteMs": "Milliseconds",
        "RoutedBytes": "Bytes",
        "TouchedRecordBytes": "Bytes",
    }
    document = {
        "_aws": {
            "Timestamp": int(time.time() * 1000),
            "CloudWatchMetrics": [{
                "Namespace": NAMESPACE,
                "Dimensions": [["Pattern"]],
                "Metrics": [
                    {"Name": name, "Unit": units.get(name, "Count")}
                    for name in values
                ],
            }],
        },
        "Pattern": pattern,
        **values,
        **properties,
    }
    print(json.dumps(document, separators=(",", ":"), default=str))


def units_for_bytes(size, block_size):
    return max(1, math.ceil(size / block_size))


def estimated_shadow_units(shadow):
    return units_for_bytes(shadow_record_bytes(shadow), 1024)


def shadow_record_bytes(shadow):
    # Response metadata and computed delta are excluded from the stored record.
    state = shadow.get("state", {})
    stored = {section: state[section] for section in ("desired", "reported") if section in state}
    return len(json.dumps({"state": stored}, separators=(",", ":")).encode())


def cache_key(device_id, field):
    return f"{device_id}:{field}"


class ShadowResolver:
    """Resolve one reported field through payload, object, DynamoDB, then IoT Core."""

    def __init__(self, device_id, *, shadow_client, table=None, initial_shadow=None, now=time.time):
        self.device_id = device_id
        self.shadow_client = shadow_client
        self.table = table
        self.initial_shadow = initial_shadow or {}
        self.now = now
        self.fetched_shadow = None
        self.local_values = {}
        self.counts = {name: 0 for name in (
            "ShadowGets", "ShadowUnitsEstimated", "PayloadHits", "ObjectHits",
            "CacheHits", "CacheMisses", "CacheReads", "CacheWrites", "CacheErrors",
        )}

    @staticmethod
    def field_from(shadow, field):
        reported = shadow.get("state", {}).get("reported", {})
        return reported[field] if field in reported else MISSING

    def get(self, field):
        if field in self.local_values:
            self.counts["ObjectHits"] += 1
            return self.local_values[field], "object"

        value = self.field_from(self.initial_shadow, field)
        if value is not MISSING:
            self.counts["PayloadHits"] += 1
            self.local_values[field] = value
            return value, "payload"

        if self.fetched_shadow is not None:
            value = self.field_from(self.fetched_shadow, field)
            self.counts["ObjectHits"] += 1
            self.local_values[field] = None if value is MISSING else value
            return self.local_values[field], "object"

        if field in CACHEABLE_FIELDS and self.table is not None and self.fetched_shadow is None:
            try:
                self.counts["CacheReads"] += 1
                response = self.table.get_item(Key={"device_id": cache_key(self.device_id, field)})
                item = response.get("Item")
                if item and int(item["expires_at"]) > int(self.now()) and "value" in item:
                    self.counts["CacheHits"] += 1
                    self.local_values[field] = item["value"]
                    return item["value"], "dynamodb"
                self.counts["CacheMisses"] += 1
            except Exception:
                # The cache is an optimization. The origin still answers the request.
                self.counts["CacheErrors"] += 1

        response = self.shadow_client.get_thing_shadow(thingName=self.device_id)
        raw = response["payload"].read()
        self.counts["ShadowGets"] += 1
        self.fetched_shadow = json.loads(raw)
        self.counts["ShadowUnitsEstimated"] += estimated_shadow_units(self.fetched_shadow)
        value = self.field_from(self.fetched_shadow, field)

        if field in CACHEABLE_FIELDS and value is not MISSING and self.table is not None:
            version = int(self.fetched_shadow.get("version", 0))
            try:
                self.table.put_item(
                    Item={
                        "device_id": cache_key(self.device_id, field),
                        "value": value,
                        "shadow_version": version,
                        "expires_at": int(self.now()) + CACHE_TTL_SECONDS,
                    },
                    ConditionExpression="attribute_not_exists(shadow_version) OR shadow_version <= :version",
                    ExpressionAttributeValues={":version": version},
                )
                self.counts["CacheWrites"] += 1
            except Exception as error:
                if getattr(error, "response", {}).get("Error", {}).get("Code") != "ConditionalCheckFailedException":
                    self.counts["CacheErrors"] += 1

        self.local_values[field] = None if value is MISSING else value
        return self.local_values[field], "iot"
