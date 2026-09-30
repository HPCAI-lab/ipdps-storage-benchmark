"""Telemetry workload — generates synthetic time-series data and writes/reads it.

Simulates a sensor that emits readings at a configurable rate.  Measures
write throughput, read latency (full range query), and records counts.
"""

import time
import random
from datetime import datetime, timedelta, timezone

from .metadata_workload import WorkloadResult  # reuse the shared dataclass


class TelemetryWorkload:
    """Generate N telemetry points, write them, then query back a time range."""

    BATCH_SIZE = 1000
    METRICS = ["temperature", "pressure", "velocity", "energy", "density"]

    def __init__(
        self,
        backend,
        storage_backend_label: str = "unknown",
        num_points: int = 100_000,
        interval_ms: int = 100,
        batch_size: int = 1000,
        seed: int = 20260814,
        repetition: int = 1,
    ):
        self.backend = backend
        self.storage_backend_label = storage_backend_label
        self.num_points = num_points
        self.interval_ms = interval_ms
        self.batch_size = batch_size
        self.seed = seed
        self.repetition = repetition

    def run(self) -> WorkloadResult:
        result = WorkloadResult(
            backend_name=self.backend.get_backend_name(),
            storage_backend=self.storage_backend_label,
            workload_name="telemetry",
            workload_size=self.num_points,
            repetition=self.repetition,
            seed=self.seed,
            batch_size=self.batch_size,
        )

        if self.num_points <= 0:
            result.success = False
            result.errors.append("num_points must be positive")
            return result

        base_ts = datetime(2024, 1, 1, tzinfo=timezone.utc) + timedelta(days=self.repetition)
        start_ts = base_ts
        end_ts = base_ts + timedelta(milliseconds=(self.num_points - 1) * self.interval_ms)
        rng = random.Random(self.seed)

        total_start = time.perf_counter()

        # --- WRITE PHASE ---
        write_time = 0.0
        for start in range(0, self.num_points, self.batch_size):
            stop = min(start + self.batch_size, self.num_points)
            prepare_start = time.perf_counter()
            batch = self._generate_batch(start, stop, base_ts, rng)
            result.preparation_time_s += time.perf_counter() - prepare_start
            write_start = time.perf_counter()
            self.backend.insert_telemetry_batch(batch)
            write_time += time.perf_counter() - write_start
            result.records_written += len(batch)
        result.write_time_s = write_time

        # --- READ PHASE (full range query) ---
        read_start = time.perf_counter()
        rows = self.backend.query_telemetry_range(start_ts, end_ts)
        result.records_read = len(rows)
        result.read_time_s = time.perf_counter() - read_start

        result.total_time_s = time.perf_counter() - total_start
        if result.records_read != result.records_written:
            result.success = False
            result.errors.append(
                f"record-count mismatch: wrote {result.records_written}, read {result.records_read}"
            )
        return result

    # ------------------------------------------------------------------

    def _generate_batch(self, start: int, stop: int, base_ts, rng) -> list[dict]:
        records = []
        for i in range(start, stop):
            records.append({
                "ts": base_ts + timedelta(milliseconds=i * self.interval_ms),
                "metric": rng.choice(self.METRICS),
                "value": rng.gauss(0, 1),
                "tags": {"sensor_id": f"sensor_{i % 10}", "node": f"node_{i % 4}"},
            })
        return records
