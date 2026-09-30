"""Fixed-work concurrent telemetry pilot using the existing SQLite adapter."""

import argparse
import json
import math
import multiprocessing as mp
import os
import queue
import random
import socket
import sqlite3
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src"))

from db_backends.sqlite_backend import SQLiteBackend
from workloads.telemetry_workload import TelemetryWorkload
from storage import StorageManager


def worker(worker_id, cfg, db_path, start_event, messages):
    backend = None
    try:
        backend = SQLiteBackend(db_path, "WAL", "NORMAL")
        backend.connect()
        backend.conn.execute("PRAGMA busy_timeout=60000")

        generator = TelemetryWorkload(
            backend, batch_size=cfg["batch_size"],
            interval_ms=100, seed=cfg["seed"],
        )
        base_ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
        committed = 0
        latencies = []

        messages.put({"kind": "ready", "worker": worker_id})
        if not start_event.wait(cfg["timeout"]):
            raise TimeoutError("Timed out waiting for synchronized start")

        batches = math.ceil(cfg["records"] / cfg["batch_size"])
        for batch_id in range(worker_id, batches, cfg["clients"]):
            first = batch_id * cfg["batch_size"]
            last = min(first + cfg["batch_size"], cfg["records"])

            # The same batch has the same data at every client count.
            records = generator._generate_batch(
                first, last, base_ts,
                random.Random(cfg["seed"] + batch_id),
            )
            started = time.perf_counter()
            backend.insert_telemetry_batch(records)
            latencies.append((time.perf_counter() - started) * 1000)
            committed += len(records)

        finished = time.perf_counter()
        backend.close()
        backend = None
        messages.put({
            "kind": "done", "worker": worker_id,
            "committed": committed, "finished": finished,
            "latencies_ms": latencies,
        })
    except Exception as exc:
        messages.put({
            "kind": "error", "worker": worker_id,
            "error": repr(exc),
        })
    finally:
        if backend is not None:
            backend.close()


def percentile(sorted_values, percent):
    position = (len(sorted_values) - 1) * percent / 100
    lo = int(position)
    hi = math.ceil(position)
    return (
        sorted_values[lo]
        + (sorted_values[hi] - sorted_values[lo]) * (position - lo)
    )


def run(cfg, result):
    manager = StorageManager({
        "lustre": "/pscratch/sd/m/makhatri",
        "tmpfs": "/tmp",
    })
    storage = manager.prepare(cfg["storage"])
    result["storage_info"] = storage
    db_path = str(Path(storage["path"]) / "benchmark.db")

    # Schema creation occurs before workers and outside workload timing.
    backend = SQLiteBackend(db_path, "WAL", "NORMAL")
    try:
        backend.connect()
        backend.initialize_schema()
        result["journal_mode"] = backend.journal_mode
        result["synchronous"] = backend.synchronous
    finally:
        backend.close()

    ctx = mp.get_context("spawn")
    start_event = ctx.Event()
    messages = ctx.Queue()
    processes = []
    deadline = time.monotonic() + cfg["timeout"]

    def receive():
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("Trial exceeded timeout")
            try:
                message = messages.get(timeout=min(1, remaining))
            except queue.Empty:
                crashed = [
                    p.pid for p in processes
                    if p.exitcode is not None and p.exitcode != 0
                ]
                if crashed:
                    raise RuntimeError(f"Workers crashed: {crashed}")
                continue
            if message["kind"] == "error":
                raise RuntimeError(
                    f"Worker {message['worker']}: {message['error']}"
                )
            return message

    try:
        for worker_id in range(cfg["clients"]):
            process = ctx.Process(
                target=worker,
                args=(worker_id, cfg, db_path, start_event, messages),
            )
            process.start()
            processes.append(process)

        for _ in processes:
            message = receive()
            if message["kind"] != "ready":
                raise RuntimeError(f"Unexpected startup message: {message}")

        started = time.perf_counter()
        start_event.set()
        completed = []
        for _ in processes:
            message = receive()
            if message["kind"] != "done":
                raise RuntimeError(f"Unexpected result message: {message}")
            completed.append(message)

        for process in processes:
            process.join(timeout=max(0, deadline - time.monotonic()))
            if process.is_alive() or process.exitcode != 0:
                raise RuntimeError("Worker did not exit successfully")

        elapsed = max(item["finished"] for item in completed) - started
        committed = sum(item["committed"] for item in completed)
        latencies = sorted(
            value for item in completed for value in item["latencies_ms"]
        )

        # Validate independently against the requested workload.
        with sqlite3.connect(db_path) as conn:
            stored, unique_timestamps = conn.execute(
                "SELECT COUNT(*), COUNT(DISTINCT ts) FROM telemetry"
            ).fetchone()
            check = conn.execute("PRAGMA quick_check").fetchall()

            checkpoint_started = time.perf_counter()
            checkpoint = conn.execute(
                "PRAGMA wal_checkpoint(TRUNCATE)"
            ).fetchone()
            checkpoint_s = time.perf_counter() - checkpoint_started

        if committed != cfg["records"] or stored != cfg["records"]:
            raise RuntimeError(
                f"Count mismatch: requested={cfg['records']}, "
                f"committed={committed}, stored={stored}"
            )
        if unique_timestamps != cfg["records"] or check != [("ok",)]:
            raise RuntimeError("Uniqueness or database integrity check failed")
        if checkpoint[0] != 0:
            raise RuntimeError(f"Checkpoint was busy: {checkpoint}")

        result.update({
            "success": True,
            "committed_records": committed,
            "stored_records": stored,
            "workload_wall_s": elapsed,
            "throughput_records_s": committed / elapsed,
            "transactions": len(latencies),
            "transaction_latency_mean_ms": sum(latencies) / len(latencies),
            "transaction_latency_p50_ms": percentile(latencies, 50),
            "transaction_latency_p95_ms": percentile(latencies, 95),
            "transaction_latency_p99_ms": percentile(latencies, 99),
            "post_validation_checkpoint_s": checkpoint_s,
            "worker_committed_records": [
                item["committed"]
                for item in sorted(completed, key=lambda x: x["worker"])
            ],
        })

        # Clean up only this successful trial's known database files.
        for suffix in ("", "-wal", "-shm"):
            Path(db_path + suffix).unlink(missing_ok=True)
        Path(storage["path"]).rmdir()
    finally:
        for process in processes:
            if process.is_alive():
                process.terminate()
                process.join(timeout=5)
                if process.is_alive():
                    process.kill()
                    process.join()
        messages.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--storage", choices=["lustre", "tmpfs"], required=True)
    parser.add_argument("--clients", type=int, default=1)
    parser.add_argument("--records", type=int, default=100000)
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--seed", type=int, default=20260814)
    parser.add_argument("--timeout", type=int, default=300)
    args = parser.parse_args()
    cfg = vars(args)

    if min(args.clients, args.records, args.batch_size, args.timeout) < 1:
        parser.error("Counts and timeout must be positive")
    batches = math.ceil(args.records / args.batch_size)
    if batches < args.clients:
        parser.error("Need at least one batch per client")
    if batches > 100000:
        parser.error("This pilot limits stored latency samples to 100000")
    if not os.environ.get("SLURM_JOB_ID") or not socket.gethostname().startswith("nid"):
        parser.error("Run this pilot on an allocated compute node")

    output = ROOT / "results" / "smoke"
    output.mkdir(parents=True, exist_ok=True)
    result_path = output / f"{uuid.uuid4().hex}.json"
    result = {
        "config": cfg, "success": False,
        "hostname": socket.gethostname(),
        "slurm_job_id": os.environ["SLURM_JOB_ID"],
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "python_version": sys.version,
        "sqlite_version": sqlite3.sqlite_version,
    }

    try:
        run(cfg, result)
    except Exception as exc:
        result["error"] = repr(exc)
    finally:
        result_path.write_text(json.dumps(result, indent=2) + "\n")
        print(json.dumps(result, indent=2))
        print(f"RESULT_FILE={result_path}")

    return 0 if result["success"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
