# Soup training job queue

Go HTTP API, PostgreSQL, and four separate worker processes by default. The design is in [DESIGN.md](DESIGN.md).

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
artifact contains the raw runner log, including on test failure. Download all
three artifacts from the run summary, keep each new log in `runs/`, and record
the pass/fail counts before submission.

### Local Docker

Run on a Linux environment with Docker Compose v2, Bash, curl and Python 3. The runner creates an isolated Compose project on port 18080, runs probes against an empty database, then starts eight workers for the 10,000-job harness. It preserves output and tears down its test project and volume. Set `QUEUE_PORT` if 18080 is occupied.

```sh
./run-checks.sh 42
./run-checks.sh 731
./run-checks.sh 2026
```

Each invocation creates a new file in [runs/](runs/) containing stdout and stderr. A run started by the harness prints `START` and a successful one ends with `PASS all invariants and pagination` before cleanup. Use seeds to reproduce the victim selection sequence. Thread and process timing will vary.

| Silent failure | How the code checks it |
| --- | --- |
| Double claim | A second connection tries to claim a row while a transaction holds its lock. After the first claim, another claim must still return no job until lease expiry. |
| Zombie worker | A probe sets the lease into the past using database time. The old attempt must fail to complete both before and after another attempt reclaims it. Only the current attempt may write the effect. |
| Cancel/complete race | Probes exercise both lock order outcomes and launch concurrent finish and cancel calls. Each outcome is checked against the actual effect row. The chaos harness also issues cancels under load. |
| Duplicate request | Concurrent HTTP POSTs use the same key. They must return one 201 and one 200 with the same job ID and payload. The harness checks a different payload returns 409 and audits the keys and IDs in PostgreSQL. |
| Isolation level | The probe checks the session is READ COMMITTED. The second statement after an idempotency conflict uses a fresh snapshot. |
| Clock skew | Claim and completion SQL refer to PostgreSQL time. The probe moves the lease across the database's current time and exercises the expiry decision. The test does not alter system clocks. |

The harness checks that every acknowledged key and ID survives, that all jobs reach terminal states, and that effects match successful jobs and the winning attempt. It requires evidence of at least one reclaim after worker kills. It does not simulate a PostgreSQL outage, failover, disk exhaustion, a database clock jump, an external side effect, or every possible ordering around COMMIT. It also does not measure sustained throughput at a fixed arrival rate. The deterministic probes isolate a few failure sequences that random kills alone may miss.

## Evidence and verdict

**DON'T SHIP at 100 requests/s.** Zero completed chaos runs and one startup attempt. The supplied raw output records Docker being unavailable. No database or worker behavior, Go build, or 100 requests/s result has been verified in this environment.

First, run the probes and the three recorded seeds on a machine with Docker. Then measure a sustained, fixed rate of 100 requests/s with a stated mix of creates, retries, reads, and cancels, both with and without worker kills. Record latency percentiles, HTTP errors, queue depth, oldest queued job age, recovery time and database saturation. If requests can all create one-second jobs, eight workers can complete at most about eight such jobs per second before overhead, so the first production capacity fix would be to bound intake or raise processing capacity for that workload.

## Reflection and AI use

The design review surfaced a subtle expiry window. Checking only the attempt number would let a worker with an expired lease complete while no replacement had claimed the job. Completion therefore checks the lease after locking the row. The main open concern is whether PostgreSQL contention under the intended request mix delays claims and completion enough to create lease churn. The supplied run could not reach that point.

Codex assisted with the design note, Go code, Compose setup, probes, harness, and this draft. The design adopted its suggestions to use row locks, fencing tokens and an effect in the completion transaction. Codex ran shell and Python syntax checks, parsed the Compose YAML, and preserved the startup failure. Candidate verification and any changes or conclusions from local runs should be recorded here before submission.
