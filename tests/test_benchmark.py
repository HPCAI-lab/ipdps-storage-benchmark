import csv
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class BenchmarkSmokeTest(unittest.TestCase):
    def test_fresh_database_repetitions_and_counts(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            input_dir = tmp_path / "input"
            output_dir = tmp_path / "output"
            data_dir = tmp_path / "data"
            input_dir.mkdir()
            output_dir.mkdir()
            data_dir.mkdir()
            (input_dir / "sample.xyz").write_bytes(
                (ROOT / "tests" / "data" / "sample.xyz").read_bytes()
            )

            env = os.environ.copy()
            env.update({
                "BENCHMARK_MODE": "1",
                "BENCHMARK_CONFIG": str(ROOT / "tests" / "smoke_config.yaml"),
                "DATA_DIR": str(data_dir),
                "INPUT_DIR": str(input_dir),
                "OUTPUT_DIR": str(output_dir),
                "DB_PATH": str(data_dir / "benchmark.db"),
                "STORAGE_BACKEND": "test-storage",
                "RUNTIME_LABEL": "test-runtime",
            })
            completed = subprocess.run(
                [sys.executable, str(ROOT / "src" / "app.py")],
                env=env,
                cwd=ROOT,
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)

            csv_files = list(output_dir.glob("benchmark_results_*.csv"))
            self.assertEqual(len(csv_files), 1)
            with open(csv_files[0], newline="", encoding="utf-8") as fh:
                rows = list(csv.DictReader(fh))

            self.assertEqual(len(rows), 6)
            self.assertTrue(all(row["success"] == "True" for row in rows))
            for row in rows:
                self.assertEqual(row["records_written"], row["records_read"])
            telemetry_counts = sorted(
                int(row["records_read"]) for row in rows if row["workload"] == "telemetry"
            )
            self.assertEqual(telemetry_counts, [100, 100, 1000, 1000])
            self.assertFalse(list(data_dir.glob("*.db*")))


if __name__ == "__main__":
    unittest.main()
