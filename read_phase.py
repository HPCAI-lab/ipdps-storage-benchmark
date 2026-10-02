"""Fixed-count, synchronized warm range queries after insertion."""
import multiprocessing as mp
import queue
import random
import time
from datetime import datetime, timedelta, timezone

from process_metrics import process_snapshot, process_delta, summarize_resources
from backend_factory import connect_backend
from mixed_workloads import query_ids, query_workload


def read_worker(worker_id, cfg, db_path, start_event, messages):
    backend = None
    try:
        backend = connect_backend(cfg, db_path, read_only=True)
        base = datetime(2024, 1, 1, tzinfo=timezone.utc)
        latencies = []
        queries_by_workload = {"metadata": 0, "telemetry": 0}
        messages.put({"kind": "ready", "worker": worker_id})
        if not start_event.wait(cfg["timeout"]):
            raise TimeoutError("Read phase start timed out")

        usage_start = process_snapshot()
        population = cfg["records"] // 2 if cfg["workload"] == "mixed" else cfg["records"]
        for query_id in query_ids(cfg, worker_id):
            current_workload = query_workload(cfg, query_id)
            rng = random.Random(cfg["seed"] + 1000000000 + query_id)
            first = rng.randrange(population - cfg["query_window"] + 1)
            last = first + cfg["query_window"]
            start_ts = base + timedelta(milliseconds=first * 100)
            end_ts = base + timedelta(milliseconds=(last - 1) * 100)

            started = time.perf_counter()
            if current_workload == "metadata":
                rows = backend.query_scientific_metadata_range(first, last)
            else:
                rows = backend.query_telemetry_range(start_ts, end_ts)
            latencies.append((time.perf_counter() - started) * 1000)

            if len(rows) != cfg["query_window"]:
                raise RuntimeError(f"Wrong row count for query {query_id}")
            if current_workload == "metadata":
                valid = [row[0] for row in rows] == list(range(first, last))
            else:
                valid = all(
                    (row[0] if isinstance(row[0], datetime) else datetime.fromisoformat(row[0]))
                    == base + timedelta(milliseconds=(first + i) * 100)
                    for i, row in enumerate(rows)
                )
            if not valid:
                raise RuntimeError(f"Wrong returned keys for query {query_id}")
            queries_by_workload[current_workload] += 1

        finished = time.perf_counter()
        metrics = process_delta(usage_start)
        backend.close()
        backend = None
        messages.put({
            "kind": "done", "worker": worker_id,
            "finished": finished, "latencies_ms": latencies, "resources": metrics,
            "queries_by_workload": queries_by_workload,
        })
    except Exception as exc:
        messages.put({
            "kind": "error", "worker": worker_id, "error": repr(exc),
        })
    finally:
        if backend is not None:
            backend.close()


def run_read_phase(cfg, db_path):
    ctx = mp.get_context("spawn")
    event, messages = ctx.Event(), ctx.Queue()
    processes = []
    deadline = time.monotonic() + cfg["timeout"]

    def receive(expected):
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("Read phase exceeded timeout")
            try:
                message = messages.get(timeout=min(1, remaining))
            except queue.Empty:
                if any(p.exitcode not in (None, 0) for p in processes):
                    raise RuntimeError("Read worker crashed")
                continue
            if message["kind"] != expected:
                raise RuntimeError(f"Unexpected read-worker message: {message}")
            return message

    try:
        for worker_id in range(cfg["clients"]):
            p = ctx.Process(
                target=read_worker,
                args=(worker_id, cfg, db_path, event, messages),
            )
            p.start()
            processes.append(p)

        for _ in processes:
            receive("ready")
        started = time.perf_counter()
        event.set()
        done = [receive("done") for _ in processes]

        for p in processes:
            p.join(timeout=max(0, deadline - time.monotonic()))
            if p.is_alive() or p.exitcode != 0:
                raise RuntimeError("Read worker did not exit successfully")

        elapsed = max(item["finished"] for item in done) - started
        values = sorted(v for item in done for v in item["latencies_ms"])
        if len(values) != cfg["read_queries"]:
            raise RuntimeError("Read query count mismatch")

        def percentile(percent):
            position = (len(values) - 1) * percent / 100
            lo = int(position)
            hi = min(lo + 1, len(values) - 1)
            return values[lo] + (values[hi] - values[lo]) * (position - lo)

        return {
            **summarize_resources(done, "read", elapsed),
            "read_cache_state": "warm_after_write_checkpoint_and_validation",
            "read_query_type": (
                "alternating_metadata_telemetry_pairs" if cfg["workload"] == "mixed" else
                "record_id_range" if cfg["workload"] == "metadata"
                else "timestamp_range"
            ),
            "read_queries_completed": len(values),
            "metadata_read_queries": sum(item["queries_by_workload"]["metadata"] for item in done),
            "telemetry_read_queries": sum(item["queries_by_workload"]["telemetry"] for item in done),
            "read_worker_queries_by_workload": [
                {"worker": item["worker"], **item["queries_by_workload"]}
                for item in sorted(done, key=lambda x: x["worker"])
            ],
            "read_rows_returned": len(values) * cfg["query_window"],
            "read_wall_s": elapsed,
            "read_queries_per_s": len(values) / elapsed,
            "read_latency_mean_ms": sum(values) / len(values),
            "read_latency_p50_ms": percentile(50),
            "read_latency_p95_ms": percentile(95),
            "read_latency_p99_ms": percentile(99),
            "read_latency_samples_ms": values,
            "worker_read_queries": [
                len(item["latencies_ms"])
                for item in sorted(done, key=lambda x: x["worker"])
            ],
        }
    finally:
        for p in processes:
            if p.is_alive():
                p.terminate()
                p.join(timeout=5)
                if p.is_alive():
                    p.kill()
                    p.join()
        messages.close()
