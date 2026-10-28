"""Hold a steady before workload, then a steady after workload, so 1-minute CloudWatch graphs show two plateaus.

Both halves use the same cadence. IoT rules are switched so route metrics follow the same split:
the full-document republish runs only during the before half, and the Kinesis projection only during the after half.
"""

import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from demo import invoke, reset_migration_shadow, shadow_endpoint_url, stack_outputs

import boto3


def topic_rules(iot):
    rules = iot.list_topic_rules()["rules"]

    def one(prefix):
        matches = [rule["ruleName"] for rule in rules if rule["ruleName"].startswith(prefix)]
        if len(matches) != 1:
            raise RuntimeError(f"expected one IoT rule starting with {prefix}, found {matches}")
        return matches[0]

    return one("FullDocumentRule_"), one("FullDocumentConsumerRule_"), one("ProjectedKinesisRule_")


def set_rules(iot, names, *, full, projected):
    full_document, full_consumer, projected_rule = names
    for name in (full_document, full_consumer):
        (iot.enable_topic_rule if full else iot.disable_topic_rule)(ruleName=name)
    (iot.enable_topic_rule if projected else iot.disable_topic_rule)(ruleName=projected_rule)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stack", default="ShadowTalkDemo")
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--minutes-each", type=int, default=10)
    parser.add_argument("--reader-seconds", type=float, default=10)
    parser.add_argument("--write-seconds", type=float, default=20)
    parser.add_argument("--route-seconds", type=float, default=20)
    args = parser.parse_args()
    if args.minutes_each < 1:
        parser.error("minutes-each must be positive")

    session = boto3.Session(region_name=args.region)
    outputs = stack_outputs(session.client("cloudformation"), args.stack)
    thing_name = outputs["ThingName"]
    migration_thing = outputs["MigrationThingName"]
    shadow = session.client("iot-data", endpoint_url=shadow_endpoint_url(session, args.region))
    lambda_client = session.client("lambda")
    iot = session.client("iot")
    rules = topic_rules(iot)
    table = session.resource("dynamodb").Table(outputs["CacheTableName"])

    reader_event = {"device_id": thing_name, "fields": ["sku", "soldAsSku", "batteryPercent", "sku"]}
    duration = args.minutes_each * 60
    counts = {"before_reads": 0, "after_reads": 0, "classic_writes": 0, "named_writes": 0, "routes": 0, "errors": 0}

    def safe(label, fn):
        try:
            fn()
            return True
        except Exception as error:
            counts["errors"] += 1
            print(f"  {label} failed: {error}", flush=True)
            return False

    def read(function_name):
        safe("read", lambda: invoke(lambda_client, function_name, reader_event))

    def hot_write(*, named, lookup=False):
        event = {"device_id": migration_thing, "action": "write", "value": int(time.time()) % 2 == 0}
        if lookup:
            event["routing"] = "lookup"
        else:
            event["routing"] = "known"
            event["named_capable"] = named
        safe("write", lambda: invoke(lambda_client, outputs["MigrationFunctionName"], event))

    def route(step):
        payload = {"state": {"reported": {"batteryPercent": 50 + (step % 40)}}}
        safe("route", lambda: shadow.update_thing_shadow(thingName=thing_name, payload=json.dumps(payload).encode()))

    def phase(name, seconds, *, named_writes):
        print(f"PHASE {name} for {seconds // 60} min at {time.strftime('%H:%M:%S')}", flush=True)
        started = time.monotonic()
        next_reader = started
        next_write = started
        next_route = started
        next_report = started + 60
        routes = 0
        lookups = 0
        while True:
            now = time.monotonic()
            elapsed = now - started
            if elapsed >= seconds:
                break
            if now >= next_reader:
                read(outputs["BeforeReaderName"] if not named_writes else outputs["AfterReaderName"])
                counts["before_reads" if not named_writes else "after_reads"] += 1
                next_reader = now + args.reader_seconds
            if now >= next_write:
                # One lookup each minute during the after half, so that series is a line rather than a dot.
                lookup = named_writes and lookups < int(elapsed // 60) + 1 and (now - started) >= lookups * 60
                hot_write(named=named_writes, lookup=lookup)
                if lookup:
                    lookups += 1
                counts["classic_writes" if not named_writes else "named_writes"] += 1
                next_write = now + args.write_seconds
            if now >= next_route:
                routes += 1
                route(routes)
                counts["routes"] += 1
                next_route = now + args.route_seconds
            if now >= next_report:
                minute = int(elapsed // 60)
                print(f"  {name} minute {minute} complete {counts}", flush=True)
                next_report += 60
            time.sleep(0.5)
        print(f"PHASE {name} done {counts}", flush=True)

    print(
        f"Sustained demo: {args.minutes_each} min before, then {args.minutes_each} min after. "
        f"Readers every {args.reader_seconds:g}s, hot updates and shadow routes every {args.write_seconds:g}s/{args.route_seconds:g}s.",
        flush=True,
    )
    try:
        seeded = reset_migration_shadow(shadow, migration_thing)
        print(f"Reset classic migration shadow to {seeded} state bytes", flush=True)
        set_rules(iot, rules, full=True, projected=False)
        phase("before", duration, named_writes=False)

        print(f"TRANSITION migrate at {time.strftime('%H:%M:%S')}", flush=True)
        migration = outputs["MigrationFunctionName"]
        invoke(lambda_client, migration, {"device_id": migration_thing, "action": "migrate", "pause_after_copy": True})
        invoke(lambda_client, migration, {"device_id": migration_thing, "action": "dual_write", "value": False})
        invoke(lambda_client, migration, {"device_id": migration_thing, "action": "read"})
        invoke(lambda_client, migration, {"device_id": migration_thing, "action": "migrate"})
        print("TRANSITION complete: nsmip=2, reads and hot updates now use rw-settings", flush=True)

        for field in ("sku", "soldAsSku"):
            table.delete_item(Key={"device_id": f"{thing_name}:{field}"})
        set_rules(iot, rules, full=False, projected=True)
        phase("after", duration, named_writes=True)

        carried = {
            "device_id": thing_name,
            "fields": ["sku"],
            "initial_shadow": {"state": {"reported": {"sku": "TALK-2"}}},
        }
        safe("carried", lambda: invoke(lambda_client, outputs["AfterReaderName"], carried))
    finally:
        set_rules(iot, rules, full=True, projected=True)
        print("Restored all three IoT rules to enabled", flush=True)

    print(f"DONE {counts}", flush=True)
    print(
        "Dashboard: https://us-east-1.console.aws.amazon.com/cloudwatch/home?region=us-east-1"
        "#dashboards/dashboard/ShadowTalkDemo",
        flush=True,
    )
    print("Set the time range to the last 30 minutes or 1 hour, period 1 minute. EMF can lag the last few minutes.", flush=True)


if __name__ == "__main__":
    main()
