#!/bin/bash
#SBATCH --account=m5289
#SBATCH --constraint=cpu
#SBATCH --qos=regular
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=256
#SBATCH --exclusive
#SBATCH --time=01:30:00
#SBATCH --no-requeue
#SBATCH --job-name=ipdps-sqlite-size
#SBATCH --output=ipdps-sqlite-size-%j.out

set -euo pipefail
snapshot="${1:?Specify the prepared source snapshot}"
cd "$snapshot"
test -f SOURCE_SHA256.json
module load python/3.11-24.1.0
export OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1
export PYTHONUNBUFFERED=1
printf 'SOURCE_COMMIT=%s\n' "$(cat SOURCE_COMMIT)"
srun --nodes=1 --ntasks=1 --cpus-per-task=256 --cpu-bind=none \
    .venv/bin/python scripts/sqlite_mechanism_study.py \
    --config configs/sqlite-mechanism/size-sweep.json
