# Soup training job queue

A Go HTTP API backed by PostgreSQL with four independent workers by default. A worker can retry a job after its lease expires. Each successful job commits exactly one simulated effect in the same transaction as its final state. See [DESIGN.md](DESIGN.md) for the claim, cancellation and recovery rules.

**Results:** 6 of 6 chaos runs passed, with 10,000 jobs and eight workers in each. The [raw logs](runs/) include the earlier startup failure.

## Start

```sh
docker compose up --build
```

The API listens on localhost:8080. Rebuilding or restarting preserves jobs and effects in the PostgreSQL volume.

| Method and path | Behavior |
| --- | --- |
| `POST /jobs` | Requires `Idempotency-Key` and a JSON payload with `duration_ms` from 1 to 1000 and optional `fail`. Returns 201 on create, 200 on matching replay, 409 on payload mismatch. |
| `GET /jobs/{id}` | Returns the current job or 404. |
| `GET /jobs?limit=50&after=0` | Lists jobs by increasing ID. Pass `next_after` as `after` until it is null. Each page reflects current data. Concurrent inserts can commit out of ID order. |
| `POST /jobs/{id}/cancel` | Cancels a queued or running job. Returns the terminal state if the job is already terminal. |
| `GET /healthz` | Checks database connectivity. |

```sh
curl -fsS -H 'Idempotency-Key: demo-1' -H 'Content-Type: application/json' \
  -d '{"duration_ms":150,"fail":false}' http://localhost:8080/jobs
curl -fsS http://localhost:8080/jobs
```

## Reproduce

### GitHub Actions

The [Queue checks workflow](.github/workflows/chaos.yml) runs on a push to
`main` and can also be started from **Actions → Queue checks → Run workflow**.
The three seeds run independently. Each job builds the image, runs the database
probes and starts eight workers for the chaos test. Its `queue-logs-seed-*`
artifact contains the raw runner log, including on test failure. The logs used
for this report are also committed under [runs/](runs/).

### Local Docker

Run on a Linux environment with Docker Compose v2, Bash, curl and Python 3. The runner creates an isolated Compose project on port 18080, runs probes against an empty database, then starts eight workers for the 10,000-job harness. It preserves output and tears down its test project and volume. Set `QUEUE_PORT` if 18080 is occupied.

```sh
./run-checks.sh 42
./run-checks.sh 731
./run-checks.sh 2026
```

Each invocation creates a new file in [runs/](runs/) containing stdout and stderr. The harness prints `START` when it begins and `PASS all invariants and pagination` when its checks finish successfully. Seeds make victim selection reproducible for a given worker list and number of kills. Thread and process timing still varies.

## Failure checks

| Silent failure | How the code checks it |
| --- | --- |
| Double claim | A second connection tries to claim a row while a transaction holds its lock. After the first claim, another claim must still return no job until lease expiry. |
| Zombie worker | A probe sets the lease into the past using database time. The old attempt must fail to complete both before and after another attempt reclaims it. Only the current attempt may write the effect. |
| Cancel/complete race | Probes exercise both lock order outcomes and launch concurrent finish and cancel calls. Each outcome is checked against the actual effect row. The chaos harness also issues cancels under load. |
| Duplicate request | Concurrent HTTP POSTs use the same key. They must return one 201 and one 200 with the same job ID and payload. The harness checks a different payload returns 409 and audits the keys and IDs in PostgreSQL. |
| Isolation level | The probe checks the session is READ COMMITTED. The second statement after an idempotency conflict uses a fresh snapshot. |
| Clock skew | Claim and completion SQL refer to PostgreSQL time. The probe moves the lease across the database's current time and exercises the expiry decision. The test does not alter system clocks. |

The harness checks that every acknowledged key and ID survives, that all jobs reach terminal states, and that effects match successful jobs and the winning attempt. The six runs used jobs lasting 20–40 ms, a concurrent replay for every key, and a cancellation request for every fourth job. It requires evidence of at least one reclaim after worker kills. It does not simulate a PostgreSQL outage, failover, disk exhaustion, a database clock jump, an external side effect, or every possible ordering around COMMIT. It also does not measure sustained throughput at a fixed arrival rate. The deterministic probes isolate a few failure sequences that random kills alone may miss.

## Run results

**Six completed chaos runs passed.** The [push run](https://github.com/AsatovDias/Soup/actions/runs/42792793581) and the [manual run](https://github.com/AsatovDias/Soup/actions/runs/42793129016) each used three seeds. Each row links to the original runner log. Time covers the harness, including submission, drain and the database audit.

| Workflow | Seed and raw log | Worker kills | Jobs reclaimed | Succeeded / effects | Failed jobs | Cancelled jobs | Time |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| #1 | [42](runs/run-20261110T094124Z-seed-42-7qW3Nh.log) | 17 | 14 | 7060 / 7060 | 441 | 2499 | 48.00 s |
| #1 | [731](runs/run-20261110T094124Z-seed-731-nLfI8t.log) | 18 | 16 | 7059 / 7059 | 441 | 2500 | 51.02 s |
| #1 | [2026](runs/run-20261110T094125Z-seed-2026-jixDYC.log) | 20 | 14 | 7060 / 7060 | 441 | 2499 | 65.24 s |
| #2 | [42](runs/run-20261110T094358Z-seed-42-8yxruZ.log) | 17 | 15 | 7060 / 7060 | 441 | 2499 | 44.51 s |
| #2 | [731](runs/run-20261110T094352Z-seed-731-wbr5Zp.log) | 24 | 16 | 7059 / 7059 | 442 | 2499 | 75.56 s |
| #2 | [2026](runs/run-20261110T094352Z-seed-2026-rMk55y.log) | 21 | 18 | 7062 / 7062 | 442 | 2496 | 61.69 s |

Across 60,000 jobs, the harness recorded 117 worker kills and 93 jobs with a second or later claim. All 42,360 successful jobs had exactly one effect. Every other job had zero effects. The failed jobs in the table are intentional simulated failures that reached their expected terminal state. The harness found no missing jobs, duplicate effects, or jobs left queued or running.

The [first local attempt](runs/run-20261110T081812Z-seed-42-GJtOTe.log) exited with code 127 because Docker was unavailable. It stopped before starting the harness. GitHub Actions provided the Linux environment used for the six completed runs.

## Fixed-rate check at 100 requests/s

The [fixed-rate workflow](https://github.com/AsatovDias/Soup/actions/runs/42796708735)
sent 6,000 new create requests over 60 seconds to the default four-worker setup.
Simulated job durations ranged from 20 to 40 ms. No workers were killed during
this test. The client and the Compose stack ran on the same CI machine.

| Metric | Observed |
| --- | ---: |
| Offered rate / accepted / HTTP errors | 100.02 requests/s / 6,000 / 0 |
| Client HTTP latency p50 / p95 / p99 | 1.12 / 3.07 / 12.63 ms |
| Client dispatch lag p99 | 0.94 ms |
| Peak queued jobs in samples taken about every two seconds | 5 |
| Terminal jobs / effects after drain | 6,000 / 6,000 |

See the [raw output](runs/fixed-rate-42796708735.log). Run the **Fixed-rate queue
check** workflow from the Actions tab to reproduce it. Locally, start
`docker compose up -d --build` in a fresh Compose project with an empty
database, then run `python3 -u rate.py` once the API is healthy.

## Production verdict

**DON'T SHIP at 100 requests/s for the full supported workload.** The four-worker setup handled the one-minute fixed-rate check for short simulated jobs. With one-second jobs, those same workers can complete at most about four jobs per second before overhead. If all 100 requests/s create jobs of that duration, the queue grows by at least about 96 jobs/s. The fixed-rate check also leaves recovery at a steady arrival rate unmeasured.

First, cap accepted work based on worker capacity and expose the age of the oldest queued job. Before raising that cap, run a longer fixed-rate test with the expected mix of creates, retries, reads and cancels, a representative job-duration distribution and worker kills. Compare error rate, p95/p99 latency, queue age and recovery against explicit targets.

## Reflection and AI use

The case that surprised me most was a worker returning after its lease expired, before another worker had picked up the job. Its attempt number would still match. A check of that number alone would let it publish, so the completion transaction checks both the attempt and the lease after locking the row. My main remaining concern is what happens when jobs get longer or the database is under contention. The short simulated jobs give us a reproducible workload, but they cannot tell us how quickly a real training queue will recover under load.

I used Codex to generate the initial design, Go implementation, and test harness. I kept its suggestions to use PostgreSQL row locks, track claims with attempt numbers, and commit the effect and final state together. I chose GitHub Actions because I didn't have Docker on my laptop. I triggered the workflow myself and checked that all three jobs in that run passed and produced log artifacts. Codex reviewed and archived the raw logs from both chaos workflows and added the fixed-rate check.
