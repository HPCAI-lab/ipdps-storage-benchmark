#!/bin/bash
#SBATCH --account=m5289
#SBATCH --constraint=cpu
#SBATCH --qos=debug
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=256
#SBATCH --time=00:10:00
#SBATCH --job-name=ipdps-smoke
#SBATCH --output=ipdps-smoke-%j.out

set -euo pipefail

module load python/3.11-24.1.0
cd "$SLURM_SUBMIT_DIR"

export OMP_NUM_THREADS=1
export OPENBLAS_NUM_THREADS=1
export MKL_NUM_THREADS=1
export PYTHONUNBUFFERED=1

srun --nodes=1 --ntasks=1 --cpus-per-task=256 --cpu-bind=none \
    .venv/bin/python experiment.py \
    --config configs/telemetry-smoke.yaml

echo "CONTROLLER_SMOKE_COMPLETE"
