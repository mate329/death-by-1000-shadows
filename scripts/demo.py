"""Run the deck's three before/after experiments against real AWS resources."""

import argparse
import json
import socket
import statistics
import time

import boto3
from botocore.exceptions import ClientError


def stack_outputs(cloudformation, stack_name):
    response = cloudformation.describe_stacks(StackName=stack_name)
    return {entry["OutputKey"]: entry["OutputValue"] for entry in response["Stacks"][0]["Outputs"]}


def shadow_endpoint_url(session, region):
    """Prefer the account-specific ATS endpoint; fall back to the regional one when local DNS cannot resolve it."""
    account_endpoint = session.client("iot").describe_endpoint(endpointType="iot:Data-ATS")["endpointAddress"]
    regional_endpoint = f"data-ats.iot.{region}.amazonaws.com"
    try:
        socket.getaddrinfo(account_endpoint, 443)
        return f"https://{account_endpoint}"
    except socket.gaierror:
        print(f"Local DNS cannot resolve {account_endpoint}; using {regional_endpoint}")
        return f"https://{regional_endpoint}"


def invoke(lambda_client, function_name, event):
    started = time.perf_counter()
    response = lambda_client.invoke(
        FunctionName=function_name,
        InvocationType="RequestResponse",
        Payload=json.dumps(event).encode(),
    )
    result = json.load(response["Payload"])
    if response.get("FunctionError"):
        raise RuntimeError(f"{function_name}: {result}")
    return result, round((time.perf_counter() - started) * 1000, 1)


def reset_migration_shadow(shadow, thing_name):
    for shadow_name in (None, "rw-settings", "rw-software"):
        kwargs = {"thingName": thing_name}
        if shadow_name:
            kwargs["shadowName"] = shadow_name
        try:
            shadow.delete_thing_shadow(**kwargs)
        except ClientError as error:
            if error.response["Error"]["Code"] != "ResourceNotFoundException":
                raise
    legacy = {
        "state": {
            "desired": {"childLock": False, "settingsPadding": "s" * 1300},
            "reported": {
                "cap": {"ns": 0}, "nsmip": 0, "sku": "MIGRATION-DEMO",
                "softwareVersion": "1.0", "softwarePadding": "w" * 4500,
            },
        },
    }
    shadow.update_thing_shadow(thingName=thing_name, payload=json.dumps(legacy).encode())
    return len(json.dumps(legacy["state"], separators=(",", ":")).encode())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stack", default="ShadowTalkDemo")
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--runs", type=int, default=8)
    parser.add_argument("--padding-bytes", type=int, default=7500)
    parser.add_argument("--route-timeout-seconds", type=int, default=180)
    parser.add_argument("--skip-seed", action="store_true", help="Read comparison only; keep the existing shadow and skip migration reset")
    args = parser.parse_args()
    if args.runs < 1 or not 0 <= args.padding_bytes <= 7800 or args.route_timeout_seconds < 1:
        parser.error("runs and route-timeout-seconds must be positive; padding-bytes must be between 0 and 7800")

    session = boto3.Session(region_name=args.region)
    outputs = stack_outputs(session.client("cloudformation"), args.stack)
    thing_name = outputs["ThingName"]
    shadow = session.client("iot-data", endpoint_url=shadow_endpoint_url(session, args.region))
    table = session.resource("dynamodb").Table(outputs["CacheTableName"])
    if not args.skip_seed:
        latest_version = None
        for index in (1, 2):
            request = {
                "state": {"reported": {
                    "sku": f"TALK-{index}", "soldAsSku": None,
                    "batteryPercent": 70 + index, "bin": {"full": bool(index % 2)},
                    "demoPadding": "x" * args.padding_bytes,
                }}
            }
            state_size = len(json.dumps(request["state"], separators=(",", ":")).encode())
            if state_size > 8192:
                parser.error(f"seeded shadow state is {state_size} bytes, above the 8 KiB limit; reduce --padding-bytes")
            response = shadow.update_thing_shadow(thingName=thing_name, payload=json.dumps(request).encode())
            latest_version = json.load(response["payload"])["version"]
            print(f"Shadow update {index}: {len(json.dumps(request).encode())} request bytes")
        deadline = time.monotonic() + args.route_timeout_seconds
        while time.monotonic() < deadline:
            item = table.get_item(Key={"device_id": f"{thing_name}:sku"}).get("Item", {})
            if int(item.get("shadow_version", 0)) >= latest_version:
                break
            time.sleep(1)
        else:
            raise RuntimeError(
                f"Projected Kinesis consumer did not refresh the SKU cache within {args.route_timeout_seconds} seconds; "
                "inspect the ProjectedKinesisRule action, AfterRouteConsumer event source mapping, and Lambda logs"
            )

    # Clear cache so the first After read visibly falls through to IoT Core.
    for field in ("sku", "soldAsSku"):
        table.delete_item(Key={"device_id": f"{thing_name}:{field}"})

    lambda_client = session.client("lambda")
    event = {"device_id": thing_name, "fields": ["sku", "soldAsSku", "batteryPercent", "sku"]}
    summary = {}
    sku_value = None
    for label, name in (("Before", outputs["BeforeReaderName"]), ("After", outputs["AfterReaderName"])):
        results = [invoke(lambda_client, name, event) for _ in range(args.runs)]
        sku_value = results[-1][0]["values"][0]["value"]
        counts = [result[0]["counts"] for result in results]
        summary[label] = {
            "shadow_gets": sum(item["ShadowGets"] for item in counts),
            "estimated_shadow_units": sum(item["ShadowUnitsEstimated"] for item in counts),
            "ddb_reads": sum(item["CacheReads"] for item in counts),
            "ddb_writes": sum(item["CacheWrites"] for item in counts),
            "median_client_round_trip_ms": round(statistics.median(result[1] for result in results), 1),
            "last_sources": [entry["source"] for entry in results[-1][0]["values"]],
        }

    # A caller can forward a value it already has. This demonstration needs no IoT read.
    carried = {"device_id": thing_name, "fields": ["sku"],
               "initial_shadow": {"state": {"reported": {"sku": sku_value}}}}
    carried_result, _ = invoke(lambda_client, outputs["AfterReaderName"], carried)
    summary["CarriedPayload"] = carried_result["counts"]

    if not args.skip_seed:
        migration_thing = outputs["MigrationThingName"]
        migration_function = outputs["MigrationFunctionName"]
        seeded_bytes = reset_migration_shadow(shadow, migration_thing)
        def migration(action, **details):
            result, _ = invoke(lambda_client, migration_function, {
                "device_id": migration_thing, "action": action, **details,
            })
            return result

        summary["NamedShadowMigration"] = {
            "seeded_classic_state_bytes": seeded_bytes,
            "before_read": migration("read"),
            "before_hot_update": migration("write", routing="known", named_capable=False, value=True),
            "pause_after_copy": migration("migrate", pause_after_copy=True),
            "dual_write_during_transition": migration("dual_write", value=False),
            "read_while_in_progress": migration("read"),
            "resume": migration("migrate"),
            "after_read": migration("read"),
            "after_hot_update": migration("write", routing="known", named_capable=True, value=True),
            "after_with_capability_lookup": migration("write", routing="lookup", value=False),
            "retry_when_complete": migration("migrate"),
        }
    print(json.dumps(summary, indent=2))
    print(f"Dashboard: https://{args.region}.console.aws.amazon.com/cloudwatch/home?region={args.region}#dashboards/dashboard/{outputs['DashboardName']}")
    print("CloudWatch EMF extraction and the asynchronous route consumers can take a few minutes to appear.")


if __name__ == "__main__":
    main()
