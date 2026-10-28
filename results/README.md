# Recorded benchmark evidence

These are historical live AWS IoT runs in `us-east-1`, copied from the talk demo's saved reports. They have not been rerun as part of preparing this repository. The JSON reports retain the measured values, run IDs, generated temporary resource names, and cleanup records. Product-specific scenario and partition labels have been replaced with generic device labels; the partition-name replacement preserves its byte length. These are historical reports with anonymized labels.

| Report | UTC start | Run ID | Workload |
| --- | --- | --- | --- |
| [presets-live-2026-09-30.json](presets-live-2026-09-30.json) | 2026-09-30 21:03:57 | `50177fa1c7b8` | Seven presets; three repeats, 60 actions per architecture per scenario |
| [device-live-2026-10-01.json](device-live-2026-10-01.json) | 2026-10-01 08:42:01 | `4f000ab5137f` | Seeded Device-shaped state; three repeats, 300 actions per architecture |

Both reports record successful scenarios, zero SDK retries in the workloads, and empty cleanup error lists. Workload operation counters exclude setup, migration, transition/reset, and verification; these additional phases are recorded separately. Cleanup traffic is not included in the unit estimates.

## Reproduce the presentation's unit comparison

From the repository root:

```bash
python3 scripts/named_shadow_benchmark.py \
  --config examples/device-workload.json --repeats 3 \
  --output device-offline.json
```

`examples/device-workload.json` is the `configuration` array from the recorded report, wrapped in the runner's `scenarios` object without changing its fields or workload. The default seven presets are a separate experiment. Add `--mode live` and your AWS profile to collect new live measurements. New latencies and response sizes will vary.

## Interpret the measurements

- `estimated_units_per_action` prices each touched stored record with the repository's 1,024-byte rounding proxy. It is not an AWS billing measurement.
- `logical_actions` counts business reads or changes. One named action can require several API calls.
- `gets` and `updates` count workload calls; consult `phases` for other work.
- `action_api_ms_p95` sums SDK durations within each action, excluding deliberate pacing and verification. It does not measure a device-to-cloud round trip.
- `record_bytes` describes serialized stored state; `response_payload_bytes` describes SDK response bodies. Neither is a measurement of total network transfer or total AWS cost.
- `economics` estimates migration/transition payback under the same action mix, excluding engineering effort and other AWS services.

For the presentation workload, 2,400 classic versus 1,293 named estimated units across 300 actions gives 8.00 versus 4.31 units/action. The named workload makes 522 Gets and 282 Updates; classic makes 156 Gets and 144 Updates. The measured action p95 values are about 719 ms and 141 ms respectively. Smaller touched records and increased call counts coexist in this workload.

The two-partition hot-settings preset has different document sizes, traffic, and migration work. It must be presented separately from the ten-partition, 300-action comparison.
