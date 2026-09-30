# CANOPIE-HPC Storage Benchmark

Corrected experiment package derived from Sakthi Muthuswamy's file-storage-app.
It measures a SQLite-based scientific metadata pipeline and synthetic telemetry
workload under native Python and Shifter, with databases placed on Perlmutter
Lustre scratch or compute-node DRAM.

## Corrections made

- Copies the benchmark configuration into the container image.
- Uses one image tag rather than incompatible per-volume tags.
- Creates a fresh database for every warmup and measured repetition.
- Deletes the database, WAL, and SHM files after each trial.
- Uses deterministic telemetry seeds and bounded-memory batch generation.
- Tests 100K, 1M, and 5M telemetry records.
- Runs one discarded warmup and five measured repetitions per condition.
- Validates that every trial reads exactly the number of records it wrote.
- Records runtime, storage, host, Slurm job, Python, SQLite, database size, and
  timing metadata in CSV.
- Adds the native-versus-Shifter and Lustre-versus-node-DRAM matrix.
- Adds automated smoke testing and result aggregation.
- Excludes virtual environments, generated results, and test data from images.

## Local validation

```bash
python3 -m venv .venv-benchmark
.venv-benchmark/bin/pip install -r requirements-benchmark.txt
.venv-benchmark/bin/python -m unittest tests/test_benchmark.py -v
```

## Prepare the real XYZ input

```bash
mkdir -p data/input/1H9T
cp /path/to/1h9t_traj.xyz data/input/1H9T/
```

Confirm the trajectory's provenance and redistribution rights before publishing
it as an artifact.

## Build and push the corrected image from WSL2

```bash
docker login
./scripts/build_and_push.sh YOUR_DOCKERHUB_USERNAME sc26
```

The command prints the exact image name. Do not use Sakthi's older
`named-volumes`, `bind-volumes`, or `tmpfs` tags for this experiment.

For the urgent pilot only, `hpc/run_shifter.sh` overlays the corrected source
directory onto `/app). Therefore Sakthi's already-imported `latest` image can
serve as the dependency base without rebuilding. The measured code remains the
corrected source in this package.

## Prepare Perlmutter

Copy this clean project to `$SCRATCH`, then create the lightweight native
environment:

```bash
cd $SCRATCH/canopie-hpc-benchmark
python3 -m venv .venv-benchmark
.venv-benchmark/bin/pip install -r requirements-benchmark.txt
shifterimg pull docker:YOUR_DOCKERHUB_USERNAME/canopie-storage-benchmark:sc26
```

## Pilot run

Submit a small pilot first. Replace `ACCOUNT` and the image name:

```bash
sbatch \
  -A ACCOUNT \
  --export=ALL,SHIFTER_IMAGE=YOUR_DOCKERHUB_USERNAME/canopie-storage-benchmark:sc26,REPETITIONS=1,WARMUP_REPETITIONS=0,TELEMETRY_SIZES=100000 \
  hpc/perlmutter_job.sh
```

## Full run

```bash
sbatch \
  -A ACCOUNT \
  --export=ALL,SHIFTER_IMAGE=YOUR_DOCKERHUB_USERNAME/canopie-storage-benchmark:sc26 \
  hpc/perlmutter_job.sh
```

Each run stores raw CSVs, `combined_raw.csv`, and `summary.csv` under:

```text
$SCRATCH/canopie-results/<job-id>_<UTC-timestamp>/
```

The expected full matrix contains 80 measured rows:

- 2 runtimes: native and Shifter
- 2 storage locations: Lustre and node-local DRAM
- 4 workload conditions: one XYZ metadata workload and three telemetry sizes
- 5 measured repetitions

## Interpretation boundary

The metadata write measurement is end-to-end ingestion: file reading, XYZ
parsing, and SQLite insertion. The telemetry write measurement times only
SQLite batch insertion; synthetic record generation is reported separately as
`preparation_time_s`. These measurements must not be described as equivalent
pure-device bandwidth tests.
