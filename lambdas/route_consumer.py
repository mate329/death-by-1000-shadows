import base64
import json
import os
import time

import boto3

from common import CACHE_TTL_SECONDS, cache_key, metric_log, units_for_bytes


_dynamodb = boto3.resource("dynamodb")


def handler(event, context):
    pattern = os.environ["PATTERN"]
    payloads = (
        [base64.b64decode(record["kinesis"]["data"]) for record in event["Records"]]
        if "Records" in event else [json.dumps(event, separators=(",", ":")).encode()]
    )
    count = 0
    total_bytes = 0
    mqtt_units = 0
    for raw in payloads:
        item = json.loads(raw)
        count += 1
        total_bytes += len(raw)
        mqtt_units += units_for_bytes(len(raw), 5 * 1024)
        if pattern == "After" and item.get("sku") is not None and item.get("device_id") and item.get("version"):
            version = int(item["version"])
            try:
                _dynamodb.Table(os.environ["CACHE_TABLE"]).put_item(
                    Item={
                        "device_id": cache_key(item["device_id"], "sku"),
                        "value": item["sku"],
                        "shadow_version": version,
                        "expires_at": int(time.time()) + CACHE_TTL_SECONDS,
                    },
                    ConditionExpression="attribute_not_exists(shadow_version) OR shadow_version <= :version",
                    ExpressionAttributeValues={":version": version},
                )
            except Exception as error:
                if getattr(error, "response", {}).get("Error", {}).get("Code") != "ConditionalCheckFailedException":
                    raise
    metric_log(pattern, {
        "RoutedEvents": count,
        "RoutedBytes": total_bytes,
        "MqttUnitsIfRepublished": mqtt_units,
    }, kind="route")
    return {"records": count, "bytes": total_bytes}
