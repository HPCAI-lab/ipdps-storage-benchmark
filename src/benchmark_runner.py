"""Reproducible SQLite benchmark runner for native and containerized HPC trials."""

import csv
import os
import platform
import socket
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

try:
    import yaml
except ImportError:
    yaml = None


class BenchmarkRunner:
    """Run warmups and measured repetitions with a fresh SQLite DB per trial."""

    CSV_COLUMNS = [
        "timestamp_utc", "hostname", "slurm_job_id", "runtime", "backend",
        "storage_backend", "storage_path", "workload", "workload_size",
        "repetition", "seed", "journal_mode", "synchronous", "batch_size",
        "preparation_time_s", "write_time_s", "read_time_s", "total_time_s",
        "records_written", "records_read", "write_throughput_rps",
        "read_throughput_rps", "database_size_bytes", "python_version",
        "sqlite_version", "success", "errors",
    ]

    def __init__(self, config_path: str = "benchmark_config.yaml"):
        self.config_path = config_path
        self.config = self._load_config()
        benchmark_cfg = self.config.get("benchmark", {})
        self.repetitions = int(os.environ.get(
            "REPETITIONS", benchmark_cfg.get("repetitions", 5)
        ))
        self.warmup_repetitions = int(os.environ.get(
            "WARMUP_REPETITIONS", benchmark_cfg.get("warmup_repetitions", 1)
        ))
        self.base_seed = int(benchmark_cfg.get("random_seed", 20260814))
        self.cleanup_databases = bool(benchmark_cfg.get("cleanup_databases", True))
        self.runtime_label = os.environ.get("RUNTIME_LABEL", "unknown")
        self.storage_label = os.environ.get("STORAGE_BACKEND", "unknown")
        self.results = []
        self.failures = 0

        if self.repetitions < 1:
            raise ValueError("repetitions must be at least 1")
        if self.warmup_repetitions < 0:
            raise ValueError("warmup_repetitions cannot be negative")

    def _load_config(self) -> dict:
        if yaml is None:
            raise ImportError("PyYAML is required: pip install PyYAML")
        cfg_path = Path(self.config_path)
        if not cfg_path.exists():
            raise FileNotFoundError(f"Benchmark config not found: {cfg_path}")
        with open(cfg_path, encoding="utf-8") as fh:
            config = yaml.safe_load(fh)
        if not isinstance(config, dict):
            raise ValueError("benchmark configuration must be a YAML mapping")
        return config

    def run(self):
        print("\n" + "=" * 78)
        print("REPRODUCIBLE SQLITE STORAGE BENCHMARK")
        print("=" * 78)
        print(f"Runtime: {self.runtime_label} | Storage: {self.storage_label}")
        print(f"Measured repetitions: {self.repetitions} | Warmups: {self.warmup_repetitions}")

        workloads = self.config.get("workloads", ["metadata", "telemetry"])
        xyz_files = self._collect_xyz_files()

        if "metadata" in workloads:
            if not xyz_files:
                raise FileNotFoundError(
                    f"metadata workload requested but no .xyz files were found under {self._input_dir()}"
                )
            self._run_condition("metadata", None, xyz_files)

        if "telemetry" in workloads:
            for size in self._telemetry_sizes():
                self._run_condition("telemetry", size, xyz_files)

        csv_path = self._write_csv()
        self._print_summary()
        print(f"\nResults written to: {csv_path}")

        if self.failures:
            raise RuntimeError(f"{self.failures} measured benchmark trial(s) failed validation")

    def _run_condition(self, workload_name, workload_size, xyz_files):
        condition = workload_name if workload_size is None else f"{workload_name}:{workload_size}"
        print(f"\n--- Condition: {condition} ---")

        for warmup in range(1, self.warmup_repetitions + 1):
            self._run_trial(workload_name, workload_size, xyz_files, -warmup, measured=False)

        for repetition in range(1, self.repetitions + 1):
            result = self._run_trial(
                workload_name, workload_size, xyz_files, repetition, measured=True
            )
            self.results.append(result)
            self._print_result(result)
            if not result.success:
                self.failures += 1

    def _run_trial(self, workload_name, workload_size, xyz_files, repetition, measured):
        from workloads.metadata_workload import MetadataWorkload, WorkloadResult
        from workloads.telemetry_workload import TelemetryWorkload

        trial_label = "warmup" if not measured else f"r{repetition}"
        size_label = workload_size if workload_size is not None else "xyz"
        db_path = self._trial_db_path(workload_name, size_label, trial_label)
        self._remove_db_files(db_path)
        backend = self._build_backend(db_path)
        seed = self.base_seed + max(repetition, 0) + int(workload_size or 0)

        try:
            backend.connect()
            backend.initialize_schema()
            if workload_name == "metadata":
                cfg = self.config.get("metadata", {})
                workload = MetadataWorkload(
                    backend,
                    storage_backend_label=self.storage_label,
                    batch_size=int(cfg.get("batch_size", 1000)),
                )
                result = workload.run(xyz_files)
            else:
                cfg = self.config.get("telemetry", {})
                workload = TelemetryWorkload(
                    backend,
                    storage_backend_label=self.storage_label,
                    num_points=int(workload_size),
                    interval_ms=int(cfg.get("interval_ms", 100)),
                    batch_size=int(cfg.get("batch_size", 1000)),
                    seed=seed,
                    repetition=max(repetition, 0),
                )
                result = workload.run()

            backend.checkpoint()
            result.runtime_label = self.runtime_label
            result.repetition = max(repetition, 0)
            result.seed = seed
            result.journal_mode = backend.journal_mode
            result.synchronous = backend.synchronous
        except Exception as exc:
            result = WorkloadResult(
                backend_name="sqlite",
                storage_backend=self.storage_label,
                workload_name=workload_name,
                workload_size=int(workload_size or 0),
                repetition=max(repetition, 0),
                seed=seed,
                runtime_label=self.runtime_label,
                success=False,
                errors=[repr(exc)],
            )
        finally:
            try:
                backend.close()
            except Exception:
                pass

        result.database_size_bytes = self._database_size(db_path)
        if self.cleanup_databases:
            self._remove_db_files(db_path)
        return result

    def _build_backend(self, db_path):
        from db_backends.sqlite_backend import SQLiteBackend

        sqlite_cfg = self.config.get("sqlite", {})
        return SQLiteBackend(
            db_path=str(db_path),
            journal_mode=sqlite_cfg.get("journal_mode", "WAL"),
            synchronous=sqlite_cfg.get("synchronous", "NORMAL"),
        )

    def _input_dir(self) -> Path:
        explicit = os.environ.get("INPUT_DIR")
        if explicit:
            return Path(explicit)
        data_dir = os.environ.get("DATA_DIR", self.config.get("data_dir", "./data"))
        return Path(data_dir) / "input"

    def _collect_xyz_files(self) -> list:
        input_dir = self._input_dir()
        if not input_dir.exists():
            return []
        return sorted(input_dir.rglob("*.xyz"))

    def _telemetry_sizes(self) -> list[int]:
        override = os.environ.get("TELEMETRY_SIZES")
        if override:
            values = [int(value.strip()) for value in override.split(",") if value.strip()]
        else:
            values = self.config.get("telemetry", {}).get("num_points", [100_000])
            if isinstance(values, int):
                values = [values]
            values = [int(value) for value in values]
        if not values or any(value <= 0 for value in values):
            raise ValueError("telemetry num_points values must be positive")
        return values

    def _base_db_path(self) -> Path:
        configured = self.config.get("sqlite", {}).get("path", "./data/benchmark.db")
        return Path(os.environ.get("DB_PATH", configured))

    def _trial_db_path(self, workload, size, trial_label) -> Path:
        base = self._base_db_path()
        base.parent.mkdir(parents=True, exist_ok=True)
        return base.parent / f"{base.stem}_{workload}_{size}_{trial_label}{base.suffix or '.db'}"

    @staticmethod
    def _remove_db_files(db_path: Path):
        for candidate in (db_path, Path(str(db_path) + "-wal"), Path(str(db_path) + "-shm")):
            if candidate.exists() and candidate.is_file():
                candidate.unlink()

    @staticmethod
    def _database_size(db_path: Path) -> int:
        total = 0
        for candidate in (db_path, Path(str(db_path) + "-wal"), Path(str(db_path) + "-shm")):
            if candidate.exists() and candidate.is_file():
                total += candidate.stat().st_size
        return total

    def _output_dir(self) -> Path:
        output = os.environ.get("OUTPUT_DIR", self.config.get("output_dir", "./data/output"))
        path = Path(output)
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _write_csv(self) -> Path:
        timestamp = datetime.now(timezone.utc)
        safe_runtime = self.runtime_label.replace("/", "-")
        safe_storage = self.storage_label.replace("/", "-")
        csv_path = self._output_dir() / (
            f"benchmark_results_{safe_runtime}_{safe_storage}_"
            f"{timestamp.strftime('%Y%m%dT%H%M%S_%fZ')}.csv"
        )
        common = {
            "timestamp_utc": timestamp.isoformat(),
            "hostname": socket.gethostname(),
            "slurm_job_id": os.environ.get("SLURM_JOB_ID", ""),
            "runtime": self.runtime_label,
            "storage_path": str(self._base_db_path().parent.resolve()),
            "python_version": platform.python_version(),
            "sqlite_version": sqlite3.sqlite_version,
        }
        with open(csv_path, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=self.CSV_COLUMNS)
            writer.writeheader()
            for result in self.results:
                row = {
                    **common,
                    "backend": result.backend_name,
                    "storage_backend": result.storage_backend,
                    "workload": result.workload_name,
                    "workload_size": result.workload_size,
                    "repetition": result.repetition,
                    "seed": result.seed,
                    "journal_mode": result.journal_mode,
                    "synchronous": result.synchronous,
                    "batch_size": result.batch_size,
                    "preparation_time_s": f"{result.preparation_time_s:.6f}",
                    "write_time_s": f"{result.write_time_s:.6f}",
                    "read_time_s": f"{result.read_time_s:.6f}",
                    "total_time_s": f"{result.total_time_s:.6f}",
                    "records_written": result.records_written,
                    "records_read": result.records_read,
                    "write_throughput_rps": f"{result.write_throughput:.3f}",
                    "read_throughput_rps": f"{result.read_throughput:.3f}",
                    "database_size_bytes": result.database_size_bytes,
                    "success": result.success,
                    "errors": " | ".join(result.errors),
                }
                writer.writerow(row)
        return csv_path

    @staticmethod
    def _print_result(result):
        state = "OK" if result.success else "FAILED"
        print(
            f"  r{result.repetition}: {state} size={result.workload_size} "
            f"write={result.write_time_s:.3f}s read={result.read_time_s:.3f}s "
            f"total={result.total_time_s:.3f}s"
        )
        for error in result.errors:
            print(f"    ERROR: {error}", file=sys.stderr)

    def _print_summary(self):
        print("\n" + "=" * 94)
        print(
            f"{'RUNTIME':<10} {'STORAGE':<10} {'WORKLOAD':<10} {'SIZE':<10} "
            f"{'REP':<5} {'WRITE(s)':<10} {'READ(s)':<10} {'STATUS':<8}"
        )
        print("-" * 94)
        for result in self.results:
            print(
                f"{self.runtime_label:<10} {result.storage_backend:<10} "
                f"{result.workload_name:<10} {result.workload_size:<10} "
                f"{result.repetition:<5} {result.write_time_s:<10.3f} "
                f"{result.read_time_s:<10.3f} "
                f"{('OK' if result.success else 'FAILED'):<8}"
            )
        print("=" * 94)
