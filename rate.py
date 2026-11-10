#!/usr/bin/env python3
"""Drive a fixed create request rate against a fresh Compose project."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import math
import threading
import time

from chaos import query, request, require


def queue_stats():
    return json.loads(query("""
        SELECT json_build_object(
            'queued', count(*) FILTER (WHERE state='queued'),
            'running', count(*) FILTER (WHERE state='running'),
            'succeeded', count(*) FILTER (WHERE state='succeeded'),
            'other_terminal', count(*) FILTER (WHERE state IN ('failed','cancelled')),
            'effects', (SELECT count(*) FROM job_effects))
        FROM jobs
    """))


def percentile(values, quantile):
    return sorted(values)[math.ceil(len(values) * quantile) - 1] * 1000


def main(args):
    require(args.rate > 0 and args.seconds > 0, "rate and seconds must be positive")
    request(args.url, "GET", "/healthz", {200})
    require(query("SELECT count(*) FROM jobs") == "0", "expected an empty database")
    count = args.rate * args.seconds
    print(f"START rate={args.rate}/s seconds={args.seconds} requests={count} "
          "workers=4 mix=100% new creates work=20..40ms faults=none")

    def create(i, scheduled):
        start = time.monotonic()
        _, result = request(args.url, "POST", "/jobs", {201},
                            {"duration_ms": 20 + i % 21}, f"rate-{i}")
        return result["id"], start - scheduled, time.monotonic() - start

    done = threading.Event()
    samples = []
    sample_errors = []
    started = time.monotonic()

    def sample():
        try:
            while not done.is_set():
                stats = queue_stats()
                samples.append(stats)
                print(f"QUEUE elapsed={time.monotonic()-started:.2f}s {stats}")
                done.wait(2)
        except Exception as error:
            sample_errors.append(error)

    monitor = threading.Thread(target=sample)
    monitor.start()
    ids, lag, latency = set(), [], []
    errors = 0
    try:
        with ThreadPoolExecutor(max_workers=32) as pool:
            pending = []
            for i in range(count):
                scheduled = started + i / args.rate
                time.sleep(max(0, scheduled - time.monotonic()))
                pending.append(pool.submit(create, i, scheduled))
            last_submission = time.monotonic()
            for i, future in enumerate(pending):
                try:
                    job_id, delayed, elapsed = future.result()
                    ids.add(job_id)
                    lag.append(delayed)
                    latency.append(elapsed)
                except Exception as error:
                    errors += 1
                    print(f"REQUEST ERROR key=rate-{i} {error!r}")
    finally:
        done.set()
        monitor.join()

    print(f"HTTP requested={count} accepted={len(latency)} distinct_ids={len(ids)} "
          f"errors={errors} scheduled_rate={count/(last_submission-started):.2f}/s")
    if latency:
        print(f"LATENCY p50={percentile(latency, .50):.2f}ms "
              f"p95={percentile(latency, .95):.2f}ms p99={percentile(latency, .99):.2f}ms "
              f"dispatch_lag_p99={percentile(lag, .99):.2f}ms")
    require(samples and not sample_errors, f"queue sampling failed: {sample_errors}")
    print(f"QUEUE peak_sampled_queued={max(s['queued'] for s in samples)} "
          f"first_queued={samples[0]['queued']} last_queued={samples[-1]['queued']}")

    drain_started = time.monotonic()
    while True:
        stats = queue_stats()
        if stats["queued"] + stats["running"] == 0 or time.monotonic() - drain_started >= 30:
            break
        time.sleep(0.25)
    print(f"DRAIN elapsed={time.monotonic()-drain_started:.2f}s {stats}")
    require(errors == 0 and len(ids) == count, "create requests failed or returned duplicate IDs")
    require(len(latency) == count and percentile(lag, .99) < 100,
            "load generator missed the target dispatch schedule")
    require(stats == dict(queued=0, running=0, succeeded=count, other_terminal=0, effects=count),
            f"unexpected final database counts: {stats}")
    print("PASS fixed-rate workload and final counts")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:8080")
    parser.add_argument("--rate", type=int, default=100)
    parser.add_argument("--seconds", type=int, default=60)
    main(parser.parse_args())
