#!/bin/bash
# Run the 2x2 native/Shifter x Lustre/node-DRAM matrix on one Perlmutter CPU node.

set -euo pipefail

if [[ -z "${SLURM_JOB_ID:-}" ]]; then
    echo "Run this script inside a Slurm job." >&2
    exit 2
fi
if [[ "$(hostname)" != nid* ]]; then
    echo "This script must execute on a Perlmutter compute node, not $(hostname)." >&2
    exit 2
fi
if [[ -z "${SCRATCH:-}" ]]; then
    echo "SCRATCH is not set." >&2
    exit 2
fi

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INPUT_ROOT="${INPUT_ROOT:-$ROOT/data/input}"
NATIVE_PYTHON="${NATIVE_PYTHON:-$ROOT/.venv-benchmark/bin/python3}"
IMAGE="${SHIFTER_IMAGE:?Set SHIFTER_IMAGE to the corrected Docker image tag}"
RUN_ID="${SLURM_JOB_ID}_$(date -u +%Y%m%dT%H%M%SZ)"
RESULT_ROOT="${RESULT_ROOT:-$SCRATCH/canopie-results/$RUN_ID}"
SCRATCH_DATA="$SCRATCH/canopie-data/$RUN_ID"
LOCAL_DATA="$(mktemp -d "/tmp/${USER}-canopie-${SLURM_JOB_ID}-XXXXXX")"

mkdir -p "$RESULT_ROOT" "$SCRATCH_DATA"
NATIVE_PYTHON="$NATIVE_PYTHON" "$ROOT/hpc/capture_environment.sh" > "$RESULT_ROOT/environment.txt" 2>&1

cleanup_local() {
    find "$LOCAL_DATA" -type f -delete 2>/dev/null || true
    find "$LOCAL_DATA" -depth -type d -empty -delete 2>/dev/null || true
}
trap cleanup_local EXIT

if [[ ! -x "$NATIVE_PYTHON" ]]; then
    echo "Native benchmark Python not found: $NATIVE_PYTHON" >&2
    echo "Create it using: python3 -m venv .venv-benchmark && .venv-benchmark/bin/pip install -r requirements-benchmark.txt" >&2
    exit 2
fi
if ! find "$INPUT_ROOT" -type f -name '*.xyz' -print -quit | grep -q .; then
    echo "No XYZ input found under $INPUT_ROOT" >&2
    exit 2
fi

export OMP_NUM_THREADS=1
export OPENBLAS_NUM_THREADS=1
export MKL_NUM_THREADS=1

run_native() {
    local storage_label="$1"
    local data_dir="$2"
    local output_dir="$RESULT_ROOT/native-$storage_label"
    mkdir -p "$data_dir" "$output_dir"

    BENCHMARK_MODE=1 \
    BENCHMARK_CONFIG="$ROOT/benchmark_config.yaml" \
    DATA_DIR="$data_dir" \
    INPUT_DIR="$INPUT_ROOT" \
    OUTPUT_DIR="$output_dir" \
    DB_PATH="$data_dir/benchmark.db" \
    STORAGE_BACKEND="$storage_label" \
    RUNTIME_LABEL=native \
    REPETITIONS="${REPETITIONS:-5}" \
    WARMUP_REPETITIONS="${WARMUP_REPETITIONS:-1}" \
    TELEMETRY_SIZES="${TELEMETRY_SIZES:-100000,1000000,5000000}" \
    "$NATIVE_PYTHON" "$ROOT/src/app.py"
}

run_shifter() {
    local storage_label="$1"
    local data_dir="$2"
    local output_dir="$RESULT_ROOT/shifter-$storage_label"
    mkdir -p "$data_dir" "$output_dir"

    REPETITIONS="${REPETITIONS:-5}" \
    WARMUP_REPETITIONS="${WARMUP_REPETITIONS:-1}" \
    TELEMETRY_SIZES="${TELEMETRY_SIZES:-100000,1000000,5000000}" \
    "$ROOT/hpc/run_shifter.sh" \
        --image "$IMAGE" \
        --datadir "$data_dir" \
        --inputdir "$INPUT_ROOT" \
        --outputdir "$output_dir" \
        --storage "$storage_label"
}

echo "Results: $RESULT_ROOT"
echo "Node-local DRAM: $LOCAL_DATA"

run_native lustre "$SCRATCH_DATA/native-lustre"
run_native node-dram "$LOCAL_DATA/native"
run_shifter lustre "$SCRATCH_DATA/shifter-lustre"
run_shifter node-dram "$LOCAL_DATA/shifter"

"$NATIVE_PYTHON" "$ROOT/scripts/summarize_results.py" "$RESULT_ROOT"
echo "COMPLETE_RESULTS=$RESULT_ROOT"
