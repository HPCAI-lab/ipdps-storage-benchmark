"""Metadata workload — reads XYZ files, stores coordinates, queries them back.

This mirrors the original FileDataStorage pipeline but routes through the
abstract DatabaseBackend interface so it works with any backend.
"""

import time
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class WorkloadResult:
    backend_name: str
    storage_backend: str
    workload_name: str
    write_time_s: float = 0.0
    read_time_s: float = 0.0
    total_time_s: float = 0.0
    records_written: int = 0
    records_read: int = 0
    workload_size: int = 0
    repetition: int = 0
    seed: int = 0
    preparation_time_s: float = 0.0
    database_size_bytes: int = 0
    runtime_label: str = "unknown"
    journal_mode: str = "unknown"
    synchronous: str = "unknown"
    batch_size: int = 0
    success: bool = True
    errors: list = field(default_factory=list)

    @property
    def write_throughput(self) -> float:
        """Records/second during write phase."""
        return self.records_written / self.write_time_s if self.write_time_s > 0 else 0.0

    @property
    def read_throughput(self) -> float:
        """Records/second during read phase."""
        return self.records_read / self.read_time_s if self.read_time_s > 0 else 0.0


class MetadataWorkload:
    """Store XYZ coordinates in the DB and read them back; measure both phases."""

    def __init__(self, backend, storage_backend_label: str = "unknown", batch_size: int = 1000):
        self.backend = backend
        self.storage_backend_label = storage_backend_label
        self.batch_size = batch_size

    def run(self, xyz_files: list) -> WorkloadResult:
        result = WorkloadResult(
            backend_name=self.backend.get_backend_name(),
            storage_backend=self.storage_backend_label,
            workload_name="metadata",
            batch_size=self.batch_size,
        )

        total_start = time.perf_counter()

        # --- WRITE PHASE ---
        write_start = time.perf_counter()
        file_ids = []
        for xyz_path in xyz_files:
            fid = self._store_xyz(xyz_path, result)
            if fid is not None:
                file_ids.append(fid)
        result.write_time_s = time.perf_counter() - write_start

        # --- READ PHASE ---
        read_start = time.perf_counter()
        for fid in file_ids:
            rows = self.backend.query_coordinates(fid)
            result.records_read += len(rows)
        result.read_time_s = time.perf_counter() - read_start

        result.total_time_s = time.perf_counter() - total_start
        result.workload_size = result.records_written
        if result.records_written == 0 or result.records_read != result.records_written:
            result.success = False
            result.errors.append(
                f"record-count mismatch: wrote {result.records_written}, read {result.records_read}"
            )
        return result

    # ------------------------------------------------------------------

    def _store_xyz(self, xyz_path, result: WorkloadResult):
        """Parse one XYZ file and batch-insert coordinates. Returns file_id."""
        try:
            path = Path(xyz_path)
            prepare_start = time.perf_counter()
            with open(path) as fh:
                lines = fh.readlines()
            result.preparation_time_s += time.perf_counter() - prepare_start

            num_atoms = int(lines[0].strip())
            num_frames = len(lines) // (num_atoms + 2)

            file_id = self.backend.insert_file_metadata(path.name, num_atoms, num_frames)

            batch = []
            for frame in range(num_frames):
                start_idx = frame * (num_atoms + 2) + 2
                for atom_idx in range(num_atoms):
                    parts = lines[start_idx + atom_idx].split()
                    batch.append({
                        "file_id": file_id,
                        "frame": frame,
                        "atom_index": atom_idx,
                        "x": float(parts[1]),
                        "y": float(parts[2]),
                        "z": float(parts[3]),
                    })
                    if len(batch) >= self.batch_size:
                        self.backend.insert_coordinates_batch(batch)
                        result.records_written += len(batch)
                        batch = []

            if batch:
                self.backend.insert_coordinates_batch(batch)
                result.records_written += len(batch)

            return file_id
        except Exception as exc:
            result.errors.append(str(exc))
            return None
