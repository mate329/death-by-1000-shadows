"""Turn CloudWatch unit counters into a clearly scoped demo cost worksheet."""

import argparse
import json
from datetime import datetime, timedelta, timezone

import boto3


def total(cloudwatch, metric, pattern, start, end):
    response = cloudwatch.get_metric_statistics(
        Namespace="ShadowTalkDemo", MetricName=metric,
        Dimensions=[{"Name": "Pattern", "Value": pattern}],
        StartTime=start, EndTime=end, Period=60, Statistics=["Sum"],
    )
    return sum(point["Sum"] for point in response["Datapoints"])


def average(cloudwatch, metric, pattern, start, end):
    response = cloudwatch.get_metric_statistics(
        Namespace="ShadowTalkDemo", MetricName=metric,
        Dimensions=[{"Name": "Pattern", "Value": pattern}],
        StartTime=start, EndTime=end, Period=60, Statistics=["Average", "SampleCount"],
    )
    points = response["Datapoints"]
    samples = sum(point["SampleCount"] for point in points)
    return sum(point["Average"] * point["SampleCount"] for point in points) / samples if samples else None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--minutes", type=int, default=30)
    parser.add_argument("--shadow-rate-per-million", type=float, required=True)
    parser.add_argument("--mqtt-rate-per-million", type=float, required=True)
    args = parser.parse_args()
    if args.minutes <= 0 or args.shadow_rate_per_million < 0 or args.mqtt_rate_per_million < 0:
        parser.error("minutes must be positive and rates must be nonnegative")
    end = datetime.now(timezone.utc)
    start = end - timedelta(minutes=args.minutes)
    cloudwatch = boto3.client("cloudwatch", region_name=args.region)
    read_units = {p: total(cloudwatch, "ShadowUnitsEstimated", p, start, end) for p in ("Before", "After")}
    route_units = {p: total(cloudwatch, "MqttUnitsIfRepublished", p, start, end) for p in ("Before", "After")}
    route_events = {p: total(cloudwatch, "RoutedEvents", p, start, end) for p in ("Before", "After")}
    named_units = {
        "classic_known_route": average(cloudwatch, "KnownRouteUnitsEstimated", "Before", start, end),
        "named_known_route": average(cloudwatch, "KnownRouteUnitsEstimated", "After", start, end),
        "named_with_classic_lookup": average(cloudwatch, "LookupRouteUnitsEstimated", "After", start, end),
    }
    result = {
        "region": args.region, "window_minutes": args.minutes,
        "shadow_units_estimated": read_units,
        "shadow_read_cost_at_supplied_rate_usd": {
            p: round(read_units[p] * args.shadow_rate_per_million / 1_000_000, 6) for p in read_units
        },
        "pattern_2_units_per_hot_update_estimated": named_units,
        "pattern_2_usd_per_million_hot_updates_at_supplied_rate": {
            case: round(units * args.shadow_rate_per_million, 4) if units is not None else None
            for case, units in named_units.items()
        },
        "routed_events": route_events,
        "mqtt_units_if_republished": route_units,
        "mqtt_cost_if_both_routes_republished_usd": {
            p: round(route_units[p] * args.mqtt_rate_per_million / 1_000_000, 6) for p in route_units
        },
        "scope": "Demo unit arithmetic only. Pattern 2 excludes migration and measurement-probe GETs. After routing actually targets Kinesis. Excludes rules, Kinesis, Lambda, DynamoDB, and CloudWatch charges.",
    }
    print(json.dumps(result, indent=2))
    if route_events["Before"] != route_events["After"]:
        print("CAUTION: route event counts differ, so the two route totals are not a matched comparison.")


if __name__ == "__main__":
    main()
