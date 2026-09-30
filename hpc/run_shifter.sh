#!/bin/bash
# Run one corrected benchmark matrix inside Shifter.

set -euo pipefail

IMAGE="${SHIFTER_IMAGE:-sakthi22/file-storage-app:latest}"
DATA_DIR="${SCRATCH:-$PWD}/file-storage-data"
INPUT_DIR=""
OUTPUT_DIR=""
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CODE_DIR="$ROOT"
STORAGE_LABEL="${STORAGE_BACKEND:-unknown}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --datadir) DATA_DIR="$2"; shift 2 ;;
        --inputdir) INPUT_DIR="$2"; shift 2 ;;
        --outputdir) OUTPUT_DIR="$2"; shift 2 ;;
        --codedir) CODE_DIR="$2"; shift 2 ;;
        --storage) STORAGE_LABEL="$2"; shift 2 ;;
        --image) IMAGE="$2"; shift 2 ;;
        *) echo "Unknown option: $1" >&2; exit 2 ;;
    esac
done

if [[ -z "${SLURM_JOB_ID:-}" && "${ALLOW_LOGIN_NODE:-0}" != "1" ]]; then
    echo "Refusing to benchmark on a login node. Start a Slurm allocation first." >&2
    exit 2
fi

INPUT_DIR="${INPUT_DIR:-$DATA_DIR/input}"
OUTPUT_DIR="${OUTPUT_DIR:-$DATA_DIR/output}"
mkdir -p "$DATA_DIR" "$INPUT_DIR" "$OUTPUT_DIR"
mkdir -p "$DATA_DIR/output"
cp "$ROOT/benchmark_config.yaml" "$DATA_DIR/benchmark_config.yaml"

if [[ ! -f "$CODE_DIR/src/app.py" || ! -f "$CODE_DIR/src/benchmark_runner.py" ]]; then
    echo "Corrected source files not found under $CODE_DIR" >&2
    exit 2
fi

echo "Image:   $IMAGE"
echo "Data:    $DATA_DIR"
echo "Input:   $INPUT_DIR"
echo "Output:  $OUTPUT_DIR"
echo "Code:    $CODE_DIR"
echo "Storage: $STORAGE_LABEL"

shifter \
    --image="$IMAGE" \
    --volume="$DATA_DIR:/data" \
    --volume="$CODE_DIR:/app" \
    --env BENCHMARK_MODE=1 \
    --env BENCHMARK_CONFIG=/app/benchmark_config.yaml \
    --env DATA_DIR=/data \
    --env INPUT_DIR=/app/data/input \
    --env OUTPUT_DIR=/data/output \
    --env DB_PATH=/data/benchmark.db \
    --env STORAGE_BACKEND="$STORAGE_LABEL" \
    --env RUNTIME_LABEL=shifter \
    --env REPETITIONS="${REPETITIONS:-5}" \
    --env WARMUP_REPETITIONS="${WARMUP_REPETITIONS:-1}" \
    --env TELEMETRY_SIZES="${TELEMETRY_SIZES:-100000,1000000,5000000}" \
    python3 /app/src/app.py
cp "$DATA_DIR"/output/benchmark_results_*.csv "$OUTPUT_DIR"/
