# Death by a Thousand Shadows: AWS demo

This deployable Python CDK example follows the three patterns in `MRasetina - 1000 shadows.pptx`: **fewer reads, smaller touched shadow records, and smaller routed events**. It uses real AWS IoT Core Device Shadows, Python 3.12 Lambdas, DynamoDB, IoT Rules, Kinesis, and a CloudWatch dashboard in `us-east-1`. This is an isolated demo, independent of production infrastructure.

## Architecture and experiments

| Talk pattern | Before | After | Live experiment |
| --- | --- | --- | --- |
| 1. Layered reads | Four field requests produce four `GetThingShadow` calls | A fresh resolver checks supplied payload, object state, selected DynamoDB fields, then IoT Core | Cache miss, object reuse, cache hit, and an already-carried SKU |
| 2. Classic → named shadows | `childLock` update touches a roughly 6 KB classic record | The same update touches a roughly 2 KB `rw-settings` record | Pause after copying, dual-write during transition, resume, and compare touched bytes and estimated 1 KB units |
| 3. Boundary projection | Full `/update/documents` event is republished to MQTT | An IoT Topic Rule projects selected fields into Kinesis | Compare observed consumer payload bytes and 5 KB MQTT-unit arithmetic |

The stack creates **two Things**. `shadow-talk-demo` is for reads and routing; `shadow-talk-demo-migrate` is for the named-shadow experiment. Separate Things keep migration from changing the payload used by Pattern 3.

### Pattern 2 migration sequence

The migration Thing starts with `cap.ns=0` and `nsmip=0` in its classic shadow. Its desired `childLock` and `settingsPadding` move to `rw-settings`; its reported `softwareVersion` and `softwarePadding` move to `rw-software`.

1. Mark `nsmip=1`, keeping classic routing active.
2. Copy the fields into named shadows and verify them.
3. Pause here for the live demo. A read still resolves from classic. The demo producer updates `childLock` in **both** independent shadows while old and new consumers coexist. Both updates and their estimated units appear in CloudWatch.
4. Resume. The migration rereads and verifies the classic source, then one version-checked classic update clears the old fields, sets `cap.ns=1`, and sets `nsmip=2`.
5. A later invocation sees completion and performs no writes. If a classic field changed while copies were being made, migration stops and the next invocation copies the new value before switching.

This is a bounded demonstration of the migration mechanism. The two shadow writes are separate API calls, so a production producer needs retry/reconciliation logic if one succeeds and the other fails. A fleet rollout also needs a worker queue, a defined consumer cutover, and coordination with concurrent device writers. The production migration architecture discussed in the talk uses a registration trigger, an SQS queue, and a migration Lambda; that proprietary implementation is not included here.

## Deploy and run

**Run only experiment 2, without deploying the stack:**

```bash
python3 scripts/named_shadow_benchmark.py
python3 scripts/named_shadow_benchmark.py --mode live --scenario hot-settings
python3 scripts/named_shadow_benchmark.py --mode live --publish-cloudwatch  # feeds the deployed dashboard
```

The default offline matrix needs no dependencies or credentials. Live mode uses
boto3 and fresh temporary Things, then cleans them up. It does not invoke the
read-cache or routing experiments. `--publish-cloudwatch` puts the run's results
into the `ShadowTalkDemo` namespace for the dashboard's Experiment 2 bar widgets. See [the benchmark guide](../EXPERIMENT_2.md) for the
seven scenarios, editable workload configuration, required permissions, metric
scope, migration payback, and speaking bullet points. Share this folder rather
than the runner alone, because it imports the shared Lambda helpers.

For the full three-experiment CDK demo, prerequisites are Python 3.12, Node.js, AWS CDK CLI v2, AWS credentials for `us-east-1`, and a bootstrapped account. The stack creates billable resources, including a Kinesis stream and CloudWatch dashboard.

```bash
# From the repository root:
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
export AWS_DEFAULT_REGION=us-east-1
cdk bootstrap aws://YOUR_ACCOUNT_ID/us-east-1
cdk deploy
python scripts/demo.py --runs 8
```

If either Thing name exists, change `demo_thing_name` in `cdk.json`; the migration name adds `-migrate`. The runner discovers the account's `iot:Data-ATS` endpoint, seeds both Things, waits up to 180 seconds for the projected route, and prints the three experiment traces and dashboard link. Use `--route-timeout-seconds` if the rule or Kinesis consumer takes longer to become active. `--skip-seed` repeats only the read comparison against the existing read Thing. `--padding-bytes` controls Pattern 3's illustrative shadow size; the runner checks the 8 KiB state limit before sending. Destroy the stack with `cdk destroy` when finished.

## What the metrics mean

The dashboard uses CloudWatch Embedded Metric Format from Lambda logs. `Pattern=Before|After|Migration` is the only metric dimension for the Lambda experiments; the Experiment 2 benchmark publishes `Benchmark*` metrics directly with `Scenario` and `Architecture` dimensions. CloudWatch extracts metrics asynchronously; wait a few minutes before capturing graphs.

- **Pattern 1:** `ShadowGets`, handler p95, payload/object/cache hits, DynamoDB replacement traffic, and estimated shadow-operation units.
- **Pattern 2:** observed bytes of the touched record and estimated 1 KB units for one hot update. `KnownRouteUnitsEstimated` compares a caller that already knows the device capability. `LookupRouteUnitsEstimated` includes the extra classic Get needed when capability must be discovered. Migration Gets, Updates, the deliberate pause, and transition dual writes are shown separately.
- **Pattern 3:** bytes observed by each consumer and 5 KB MQTT units **if republished**. Before actually republishes to MQTT; After writes to Kinesis, so its MQTT series is a what-if comparison.

For Pattern 2, the Lambda makes a **measurement probe Get** after each hot update to observe the complete touched record. The transition dual write probes both shadows. `MeasurementProbeGets` reports this real demo overhead. It is excluded from the hypothetical steady-state `KnownRouteUnitsEstimated` and `LookupRouteUnitsEstimated` numbers. Migration and dual writes also cost shadow operations, which must be amortized over later traffic. Record sizes and metered units are measured/estimated on this seeded demo workload; the deck's 6 KB/2 KB values are illustrative until the real run confirms them.

`ShadowUnitsEstimated` and `TouchedUnitsEstimated` serialize stored `desired` and `reported` state, exclude response metadata and computed delta, and round to 1 KB. They are **teaching proxies**, not AWS billing exports. MQTT-unit estimates round observed payload bytes to 5 KB. IoT Rules, Kinesis, Lambda, DynamoDB, CloudWatch, and migration all have separate cost. Use current regional rates and a Cost and Usage Report for a total dollar claim.

For slide 19's calculator, `python scripts/cost_estimate.py --shadow-rate-per-million RATE --mqtt-rate-per-million RATE` reads the short-window CloudWatch counters and applies rates you supply from the current AWS IoT Core pricing page. It keeps Pattern 2 migration and Pattern 3 Kinesis costs outside its scoped arithmetic.

## Presentation use

The presentation can be added to the repository root. The numbered patterns below describe the demo independently of slide numbering. Capture static dashboard screenshots before the talk as a venue-network fallback; the live dashboard can remain the primary demo.

## Local validation

```bash
python3.12 -m unittest discover -s tests -v
python3.12 -m compileall -q app.py stack.py lambdas scripts tests
cdk synth
```

The tests cover null versus missing values, cache fallback, migration pause/dual-write/resume, idempotence, staged reads, and a classic-field change during copying. `cdk synth` validates the stack shape. The included historical benchmark reports record direct live IoT runs. They do not verify deployment of the full three-pattern CDK stack; rerun and validate that stack in your own account.
