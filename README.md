# Death by a Thousand Shadows

Runnable AWS IoT Device Shadow experiments and the benchmark evidence accompanying the talk. Add the presentation PowerPoint or PDF directly to this repository's root.

## Start here: no AWS account required

From the repository root, using Python 3.12 (the reference version):

```bash
python3 scripts/named_shadow_benchmark.py
```

This runs seven offline scenarios and writes `experiment-2-results.json`. It uses only the Python standard library. The scenarios demonstrate when partitioning a classic shadow into named shadows wins, ties, or increases estimated operation units.

Reproduce the presentation workload's unit estimates:

```bash
python3 scripts/named_shadow_benchmark.py \
  --config examples/device-workload.json --repeats 3 \
  --output device-offline.json
```

The workload contains 100 logical actions per cycle: 48 writes and 52 reads across ten proposed partitions. Three repeats produce 300 actions per architecture. It uses seeded business fields, not a connection to a physical robot or a production fleet. Offline mode cannot reproduce measured latency or SDK response bytes.

## Measure it in your AWS account

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install 'boto3>=1.34,<2'
python3 scripts/named_shadow_benchmark.py \
  --mode live --region us-east-1 --profile YOUR_PROFILE \
  --config examples/device-workload.json --repeats 3 \
  --output device-live.json
```

Live mode creates temporary Things and shadows, replays identical logical workloads, verifies business-state equality, and attempts cleanup. It creates billable traffic. See [EXPERIMENT_2.md](EXPERIMENT_2.md) for IAM permissions, cleanup behavior, custom workloads, retry accounting, and metric definitions. No CDK deployment is needed for this benchmark.

## Evidence behind the talk

The [recorded results](results/README.md) include the original live JSON reports, timestamps, complete configurations, phase accounting, retries, and cleanup status.

| Presentation workload, 2026-10-01 | Classic | Named |
| --- | ---: | ---: |
| Logical actions | 300 | 300 |
| Estimated shadow units/action | 8.00 | 4.31 |
| Gets / Updates | 156 / 144 | 522 / 282 |
| Action API duration p95 | 141 ms | 719 ms |

This run had 46.125% fewer estimated shadow units, with more API calls and higher action p95. Estimated migration work was 85 units per device, with unit payback at 24 later actions of the same mix. These are workload-specific results from a short seeded demonstration, not a production latency study or total AWS cost saving.

Unit estimates serialize complete stored `desired` and `reported` state and round each touched record to 1,024 bytes. They are teaching proxies, not billing exports. Stored-record bytes, SDK response bytes, and estimated units are different measurements. The separate two-partition hot-settings example must not be used to derive the 8.00 versus 4.31 result.

## Other talk patterns

The optional [three-pattern CDK demo](docs/DEMO.md) includes layered reads, staged classic-to-named migration, and IoT SQL projection to Kinesis. `app.py`, `stack.py`, `lambdas/`, and the other scripts provide the implementation. Its dependencies are in `requirements.txt`; install them only if using that demo. The historical direct-IoT benchmark reports do not validate deployment of the complete CDK stack.

## Repository contents

- `scripts/`: standalone benchmark, full demo runner, sustained demo traffic, and scoped cost arithmetic.
- `lambdas/`: shared estimators, migration helpers, read resolver, and route consumer.
- `examples/`: editable minimal workload and the exact configuration extracted from the presentation's live report.
- `results/`: recorded live reports and provenance.
- `EXPERIMENT_2.md`: detailed benchmark instructions and interpretation.
- `docs/DEMO.md`: optional CDK deployment and all three patterns.
- `tests/`: unit tests and fake-SDK checks.

## Local verification

```bash
python3 -m unittest discover -s tests -v
```

These checks use simulation and a fake SDK; they make no AWS calls. Use your own repeated live runs to investigate latency, throttling, and account-specific behavior. Supply a current regional rate with `--shadow-rate-per-million` for scoped dollar arithmetic; no pricing rate is hard-coded.
