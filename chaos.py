#!/usr/bin/env python3
"""Run against a fresh disposable Compose project with eight workers."""
import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import random
import subprocess
import threading
import time
from urllib.error import HTTPError
from urllib.request import Request, urlopen


def command(*args, timeout=30):
    return subprocess.check_output(args, text=True, timeout=timeout).strip()


def query(sql):
    return command("docker", "compose", "exec", "-T", "db", "psql", "-XAt",
                   "-v", "ON_ERROR_STOP=1", "-U", "queue", "-d", "queue", "-c", sql)


def require(condition, detail):
    if not condition:
        raise RuntimeError(detail)


def request(base, method, path, statuses, body=None, key=None):
    headers = {"Content-Type": "application/json"}
    if key is not None:
        headers["Idempotency-Key"] = key
    req = Request(base + path, None if body is None else json.dumps(body).encode(),
                  headers, method=method)
    try:
        with urlopen(req, timeout=8) as response:
            status, data = response.status, response.read()
    except HTTPError as error:
        status, data = error.code, error.read()
    require(status in statuses, f"{method} {path} key={key}: HTTP {status}: {data!r}")
    return status, json.loads(data)


def audit():
    return json.loads(query("""
        SELECT coalesce(json_agg(row_to_json(result)), '[]'::json)
        FROM (
            SELECT j.request_key AS key, j.id, j.state, j.attempt,
                   j.duration_ms, j.should_fail,
                   count(e.job_id) AS effects, max(e.attempt) AS effect_attempt
            FROM jobs j LEFT JOIN job_effects e ON e.job_id=j.id
            GROUP BY j.id ORDER BY j.id
        ) result
    """))


def run(args):
    print(f"START jobs={args.jobs} seed={args.seed} interval={args.kill_interval}s")
    workers = sorted(command("docker", "compose", "ps", "-q", "worker").splitlines())
    require(len(workers) == 8, f"expected 8 running workers, got {len(workers)}")
    require(query("SELECT count(*) FROM jobs") == "0", "expected an empty database")
    request(args.url, "GET", "/healthz", {200})

    stop = threading.Event()
    chaos_errors = []
    kills = []
    rng = random.Random(args.seed)

    def kill_workers():
        while not stop.wait(args.kill_interval):
            victim = rng.choice(workers)
            try:
                command("docker", "kill", "--signal=KILL", victim)
                kills.append(victim)
                print(f"SIGKILL elapsed={time.monotonic()-started:.3f}s worker={victim}")
                command("docker", "start", victim)
            except Exception as error:
                chaos_errors.append(repr(error))
                print(f"CHAOS ERROR {error!r}")
                return

    def submit(i, io_pool):
        key = f"chaos-{args.seed}-{i}"
        body = {"duration_ms": 20 + i % 21, "fail": i % 17 == 0}
        # Duplicate POSTs are launched before waiting for either response.
        replay = io_pool.submit(request, args.url, "POST", "/jobs", {200, 201}, body, key)
        status, first = request(args.url, "POST", "/jobs", {200, 201}, body, key)
        job_id = first["id"]
        terminal = None
        if i % 4 == 0:
            _, cancelled = request(args.url, "POST", f"/jobs/{job_id}/cancel", {200})
            terminal = cancelled["state"]
            require(terminal in {"cancelled", "succeeded", "failed"}, cancelled)
        second_status, second = replay.result()
        require(sorted([status, second_status]) == [200, 201], f"{key}: duplicate create status")
        require(second["id"] == job_id and first["payload"] == second["payload"] == body,
                f"{key}: inconsistent replay")
        if i % 500 == 0:
            request(args.url, "POST", "/jobs", {409}, {**body, "duration_ms": 100}, key)
            _, found = request(args.url, "GET", f"/jobs/{job_id}", {200})
            require(found["id"] == job_id, f"{key}: inconsistent GET")
        return key, (job_id, body, terminal)

    started = time.monotonic()
    killer = threading.Thread(target=kill_workers)
    killer.start()
    expected = {}
    errors = []
    try:
        with ThreadPoolExecutor(max_workers=24) as io_pool:
            with ThreadPoolExecutor(max_workers=24) as client_pool:
                pending = {client_pool.submit(submit, i, io_pool): i for i in range(args.jobs)}
                for future in as_completed(pending):
                    try:
                        key, result = future.result()
                        expected[key] = result
                    except Exception as error:
                        errors.append(pending[future])
                        print(f"CLIENT ERROR job={pending[future]} {error!r}")
        print(f"SUBMITTED ok={len(expected)} failed={len(errors)}")
        deadline = time.monotonic() + args.drain_timeout
        while time.monotonic() < deadline:
            if query("SELECT count(*) FROM jobs WHERE state IN ('queued','running')") == "0":
                break
            if chaos_errors:
                break
            time.sleep(1)
    finally:
        stop.set()
        killer.join()

    rows = audit()
    counts = Counter(row["state"] for row in rows)
    print(f"AUDIT rows={len(rows)} states={dict(counts)} kills={len(kills)} "
          f"effects={sum(r['effects'] for r in rows)} "
          f"retried={sum(r['attempt'] > 1 for r in rows)} elapsed={time.monotonic()-started:.2f}s")
    problems = []
    if not any(row["attempt"] > 1 for row in rows):
        problems.append("no reclaimed job observed after SIGKILL")
    if errors or chaos_errors or not kills:
        problems.append(f"client_errors={len(errors)} chaos_errors={chaos_errors} kills={len(kills)}")
    if len(rows) != args.jobs or {row["key"] for row in rows} != expected.keys():
        problems.append("persisted keys differ from acknowledged requests")
    for row in rows:
        key, state = row["key"], row["state"]
        if key not in expected:
            continue
        job_id, body, terminal = expected[key]
        wanted = terminal or ("failed" if body["fail"] else "succeeded")
        if (state == ("succeeded" if body["fail"] else "failed")
                or row["id"] != job_id or state != wanted or row["duration_ms"] != body["duration_ms"]
                or row["should_fail"] != body["fail"] or row["effects"] != int(state == "succeeded")
                or (state == "succeeded" and row["effect_attempt"] != row["attempt"])):
            problems.append(f"{key}: expected state={wanted}, actual={row}")
    for problem in problems:
        print(f"INVARIANT ERROR {problem}")
    require(not problems, f"{len(problems)} invariant failures")

    listed = []
    cursor = 0
    for _ in range((args.jobs + 99) // 100 + 1):
        _, page = request(args.url, "GET", f"/jobs?limit=100&after={cursor}", {200})
        listed.extend((j["id"], j["state"]) for j in page["items"])
        if page["next_after"] is None:
            break
        require(page["next_after"] > cursor, "pagination cursor did not advance")
        cursor = page["next_after"]
    require(listed == [(r["id"], r["state"]) for r in rows], "API listing disagrees with database")
    print("PASS all invariants and pagination")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:8080")
    parser.add_argument("--jobs", type=int, default=10_000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--kill-interval", type=float, default=2)
    parser.add_argument("--drain-timeout", type=float, default=180)
    run(parser.parse_args())
