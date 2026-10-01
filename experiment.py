"""Configuration-driven native and Shifter SQLite controller."""
import argparse
import csv
import hashlib
import itertools
import json
import os
import platform
import random
import socket
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path

import yaml
from process_metrics import SUMMARY_FIELDS
from server_metrics import SERVER_FIELDS

ROOT = Path(__file__).resolve().parent
FIELDS = [
    "trial", "phase", "repetition", "workload", "storage", "clients", "records", "seed",
    "batch_size", "success", "workload_wall_s", "throughput_records_s",
    "transaction_latency_mean_ms", "transaction_latency_p50_ms",
    "transaction_latency_p95_ms", "transaction_latency_p99_ms",
    "committed_records", "stored_records", "transactions",
    "final_checkpoint_s", "completion_wall_s", "completion_throughput_records_s",
    "read_queries", "query_window", "read_queries_completed", "read_rows_returned",
    "read_wall_s", "read_queries_per_s", "read_latency_mean_ms",
    "read_latency_p50_ms", "read_latency_p95_ms", "read_latency_p99_ms",
    "error", "raw_file",
]
FIELDS += SUMMARY_FIELDS
FIELDS += SERVER_FIELDS
FIELDS += ["pair_id", "profiling_comparison"]
FIELDS += ["database", "runtime", "python_version", "sqlite_version", "shifter_image_id"]
FIELDS += ["client_runtime", "server_runtime", "server_image_id", "postgresql_version",
           "driver_version", "libpq_version", "worker_metrics_scope", "server_resource_metrics_status"]


def _build_single_mode_plan(cfg):
    if type(cfg.get("server_profiling", False)) is not bool:
        raise ValueError("server_profiling must be a boolean")
    if cfg.get("server_profiling", False) and cfg.get("database") != "postgresql":
        raise ValueError("Server profiling currently requires PostgreSQL")
    if cfg.get("workload") not in ("metadata", "telemetry"):
        raise ValueError("workload must be metadata or telemetry")
    if cfg.get("database") not in ("sqlite", "postgresql"):
        raise ValueError("database must be sqlite or postgresql")
    if cfg["database"] == "postgresql":
        if cfg.get("runtime") != "native" or cfg.get("server_runtime") != "shifter":
            raise ValueError("Initial PostgreSQL integration requires native clients and a Shifter server")
        image_id = cfg.get("server_image_id", "")
        if not isinstance(image_id, str) or len(image_id) != 64 or any(
            c not in "0123456789abcdef" for c in image_id
        ):
            raise ValueError("PostgreSQL requires a pinned server_image_id")
        if any(type(c) is not int or c < 1 or c > 64 for c in cfg["clients"]):
            raise ValueError("Initial PostgreSQL pilot supports 1 to 64 clients")
    if cfg.get("runtime") not in ("native", "shifter"):
        raise ValueError("runtime must be native or shifter")
    if cfg["runtime"] == "shifter":
        image_id = cfg.get("shifter_image_id", "")
        if not isinstance(image_id, str) or len(image_id) != 64 or any(
            c not in "0123456789abcdef" for c in image_id
        ):
            raise ValueError("Shifter requires a pinned 64-character image ID")
    for key in ("records", "batch_size", "repetitions", "timeout"):
        if type(cfg[key]) is not int or cfg[key] < 1:
            raise ValueError(f"{key} must be a positive integer")
    if type(cfg["warmup_repetitions"]) is not int or cfg["warmup_repetitions"] < 0:
        raise ValueError("warmup_repetitions must be a nonnegative integer")
    if type(cfg["seed"]) is not int:
        raise ValueError("seed must be an integer")
    for key in ("storage", "clients"):
        if not isinstance(cfg[key], list) or not cfg[key]:
            raise ValueError(f"{key} must be a nonempty list")
        if len(cfg[key]) != len(set(cfg[key])):
            raise ValueError(f"Duplicate entries in {key}")
    if set(cfg["storage"]) - {"lustre", "tmpfs"}:
        raise ValueError("Only verified lustre/tmpfs tiers are supported")
    batches = (cfg["records"] + cfg["batch_size"] - 1) // cfg["batch_size"]
    if batches > 100000:
        raise ValueError("Pilot latency collection supports at most 100000 transactions")
    if any(type(c) is not int or c < 1 or c > batches for c in cfg["clients"]):
        raise ValueError("Each client must receive at least one batch")
    queries, window = cfg["read_queries"], cfg["query_window"]
    if type(queries) is not int or not 0 <= queries <= 100000:
        raise ValueError("read_queries must be an integer from 0 to 100000")
    if type(window) is not int or not 1 <= window <= cfg["records"]:
        raise ValueError("query_window must be between 1 and records")
    if queries and queries < max(cfg["clients"]):
        raise ValueError("Each client must receive at least one query")
    rng = random.Random(cfg["seed"])
    plan = []
    for phase, count in (("warmup", cfg["warmup_repetitions"]),
                         ("measured", cfg["repetitions"])):
        for repetition in range(1, count + 1):
            conditions = list(itertools.product(cfg["storage"], cfg["clients"]))
            rng.shuffle(conditions)
            seed = cfg["seed"] + (repetition if phase == "measured" else -repetition)
            for storage, clients in conditions:
                plan.append(dict(phase=phase, repetition=repetition,
                                 storage=storage, clients=clients, seed=seed))
    return plan


def verify_runtime(cfg):
    image_id = os.environ.get("SHIFTER_IMAGE", "")
    marker = os.environ.get("IPDPS_RUNTIME", "native")
    if cfg["runtime"] == "shifter":
        if marker != "shifter" or image_id != cfg["shifter_image_id"]:
            raise ValueError("Shifter runtime/image mismatch; use hpc/ipdps_shifter.sh")
        return image_id
    if marker != "native" or image_id:
        raise ValueError("Native run has container markers; check the launch environment")
    return ""



def build_plan(cfg):
    compare = cfg.get("profiling_comparison", False)
    if type(compare) is not bool:
        raise ValueError("profiling_comparison must be a boolean")
    if compare and cfg.get("database") != "postgresql":
        raise ValueError("Profiling comparison requires PostgreSQL")

    base = _build_single_mode_plan(cfg)
    if not compare:
        return base

    conditions = list(itertools.product(cfg["storage"], cfg["clients"]))
    paired = []
    for item in base:
        index = conditions.index((item["storage"], item["clients"]))
        first = bool((item["repetition"] + index) % 2)
        pair_id = ":".join(str(v) for v in (
            cfg["workload"], item["phase"], item["repetition"],
            item["storage"], item["clients"]))
        for enabled in (first, not first):
            paired.append(dict(
                item, server_profiling=enabled, pair_id=pair_id))
    return paired


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--output-root", default=str(ROOT / "results"))
    args = parser.parse_args()
    cfg = yaml.safe_load(Path(args.config).read_text())
    plan = build_plan(cfg)
    print(f"Planned: {len(plan)} trials ({sum(p['phase'] == 'measured' for p in plan)} measured)", flush=True)
    if args.dry_run:
        for item in plan:
            print(json.dumps(item))
        return 0
    if not os.environ.get("SLURM_JOB_ID") or not socket.gethostname().startswith("nid"):
        parser.error("Execute on an allocated compute node; --dry-run works on login nodes")
    if len(os.sched_getaffinity(0)) < max(cfg["clients"]):
        parser.error("Too few available logical CPUs for the requested client count")

    image_id = verify_runtime(cfg)
    from concurrent_sqlite import run
    if cfg["database"] == "postgresql":
        from concurrent_postgres import run as run_postgres
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output = Path(args.output_root).resolve() / f"{stamp}-{uuid.uuid4().hex[:8]}"
    output.mkdir(parents=True)
    (output / "config.yaml").write_text(yaml.safe_dump(cfg))
    (output / "plan.json").write_text(json.dumps(plan, indent=2) + "\n")
    paths = [ROOT / "experiment.py", ROOT / "concurrent_sqlite.py", ROOT / "storage.py", ROOT / "pilot_workloads.py", ROOT / "read_phase.py", ROOT / "process_metrics.py"]
    paths += [ROOT / name for name in ("backend_factory.py", "pg_server.py", "concurrent_postgres.py")]
    paths += [ROOT / "server_metrics.py"]
    paths += sorted((ROOT / "src").rglob("*.py"))
    paths += sorted((ROOT / "hpc").glob("*.sh"))
    hashes = {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}
    (output / "source_hashes.json").write_text(json.dumps(hashes, indent=2) + "\n")
    print(f"RESULTS={output}", flush=True)

    with (output / "results.csv").open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=FIELDS)
        writer.writeheader()
        stream.flush()
        os.fsync(stream.fileno())
        for trial, item in enumerate(plan, 1):
            trial_cfg = {k: cfg[k] for k in ("records", "batch_size", "timeout", "workload", "read_queries", "query_window")}
            trial_cfg.update({k: item[k] for k in ("storage", "clients", "seed")})
            if cfg["database"] == "postgresql":
                trial_cfg["server_profiling"] = item.get("server_profiling", cfg.get("server_profiling", False))
            if cfg["database"] == "postgresql":
                trial_cfg.update({k: cfg[k] for k in ("database", "server_runtime", "server_image_id")})
            result = dict(config=trial_cfg, success=False, trial=trial,
                          pair_id=item.get("pair_id", ""),
                          profiling_comparison=cfg.get("profiling_comparison", False),
                          phase=item["phase"], repetition=item["repetition"],
                          database=cfg["database"], runtime=cfg["runtime"], workload=cfg["workload"],
                          client_runtime=cfg["runtime"],
                          server_runtime=cfg.get("server_runtime", "embedded"),
                          shifter_image_id=image_id,
                          hostname=socket.gethostname(), slurm_job_id=os.environ["SLURM_JOB_ID"],
                          python_version=platform.python_version(), sqlite_version=(sqlite3.sqlite_version if cfg["database"] == "sqlite" else ""),
                          available_cpus=sorted(os.sched_getaffinity(0)),
                          timestamp_utc=datetime.now(timezone.utc).isoformat())
            try:
                if cfg["database"] == "postgresql":
                    run_postgres(trial_cfg, result, output / f"trial-{trial:04d}-postgres.log")
                else:
                    run(trial_cfg, result)
            except Exception as exc:
                result.update(success=False, error=repr(exc))
            raw_name = f"trial-{trial:04d}.json"
            with (output / raw_name).open("w") as raw:
                json.dump(result, raw, indent=2)
                raw.write("\n")
                raw.flush()
                os.fsync(raw.fileno())
            row = {key: result.get(key, "") for key in FIELDS}
            row.update(trial_cfg)
            row.pop("timeout")
            row["raw_file"] = raw_name
            writer.writerow(row)
            stream.flush()
            os.fsync(stream.fileno())
            state = "PASS" if result["success"] else "FAIL"
            print(f"[{trial}/{len(plan)}] {state} {item['phase']} {item['storage']} clients={item['clients']}", flush=True)
            if not result["success"]:
                print(result.get("error", "Trial validation failed"), flush=True)
                return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
