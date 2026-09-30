"""Verified storage locations and unique per-trial directories."""

import getpass
import os
import subprocess
import tempfile
from pathlib import Path


class StorageManager:
    EXPECTED_FILESYSTEMS = {
        "lustre": "lustre",
        "tmpfs": "tmpfs",
    }

    def __init__(self, roots):
        self.roots = {
            label: Path(path).expanduser().resolve()
            for label, path in roots.items()
        }

    def inspect(self, label):
        if label not in self.EXPECTED_FILESYSTEMS:
            raise ValueError(f"Storage tier not implemented: {label}")
        if label not in self.roots:
            raise ValueError(f"No storage root configured for {label}")

        root = self.roots[label]
        if not root.is_dir():
            raise ValueError(f"Storage root does not exist: {root}")

        command = subprocess.run(
            ["findmnt", "--noheadings", "--raw",
             "--output", "FSTYPE", "--target", str(root)],
            check=True, capture_output=True, text=True,
        )

        # A job namespace can report multiple mounts for the same target.
        filesystems = set(command.stdout.split())
        expected = self.EXPECTED_FILESYSTEMS[label]
        if filesystems != {expected}:
            raise ValueError(
                f"{label}: expected {expected}, found "
                f"{sorted(filesystems)} at {root}"
            )

        stats = os.statvfs(root)
        return {
            "label": label,
            "root": str(root),
            "filesystem": expected,
            "available_bytes": stats.f_bavail * stats.f_frsize,
        }

    def prepare(self, label):
        info = self.inspect(label)
        job = os.environ.get("SLURM_JOB_ID", "local")
        prefix = f"ipdps-{getpass.getuser()}-{job}-"

        # mkdtemp creates a private, unique directory and verifies that
        # this user can create directories on the selected filesystem.
        trial_path = Path(tempfile.mkdtemp(
            prefix=prefix, dir=info["root"]
        ))

        try:
            # Verify ordinary file creation as well.
            with tempfile.TemporaryFile(dir=trial_path) as probe:
                probe.write(b"storage-check\n")
                probe.flush()
                os.fsync(probe.fileno())
        except Exception:
            trial_path.rmdir()
            raise

        return {**info, "path": str(trial_path)}
