"""Batch workloads for the fixed-work concurrency pilot."""
from workloads.telemetry_workload import TelemetryWorkload


class TelemetryBatchWorkload:
    def __init__(self, backend, batch_size, seed):
        self.backend = backend
        self.generator = TelemetryWorkload(
            backend, batch_size=batch_size, interval_ms=100, seed=seed
        )

    def generate_batch(self, first, last, base_ts, rng):
        return self.generator._generate_batch(first, last, base_ts, rng)

    def insert_batch(self, records):
        self.backend.insert_telemetry_batch(records)


class MetadataBatchWorkload:
    VARIABLES = ("temperature", "pressure", "density", "energy")

    def __init__(self, backend, batch_size, seed):
        self.backend = backend

    def generate_batch(self, first, last, base_ts, rng):
        return [
            (i, i // 64, rng.choice(self.VARIABLES),
             f"node_{i % 64}", rng.gauss(0, 1))
            for i in range(first, last)
        ]

    def insert_batch(self, records):
        self.backend.insert_scientific_metadata_batch(records)


def build_workload(name, backend, batch_size, seed):
    from mixed_workloads import MixedBatchWorkload
    classes = {"telemetry": TelemetryBatchWorkload, "metadata": MetadataBatchWorkload, "mixed": MixedBatchWorkload}
    if name not in classes:
        raise ValueError(f"Unsupported workload: {name}")
    return classes[name](backend, batch_size, seed)
