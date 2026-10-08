#!/bin/bash
#SBATCH --account=m5289
#SBATCH --constraint=cpu
#SBATCH --qos=regular
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=256
#SBATCH --exclusive
#SBATCH --time=01:00:00
#SBATCH --no-requeue
#SBATCH --job-name=ipdps-copy-out
#SBATCH --output=ipdps-copy-out-%j.out

set -euo pipefail
snapshot="${1:?Specify prepared snapshot}"
cd "$snapshot"
test -f SOURCE_SHA256.json
module load python/3.11-24.1.0
export OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1
export PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
pointer="$snapshot/results/copy-out-${SLURM_JOB_ID:?}.path"
log="$snapshot/results/copy-out-$SLURM_JOB_ID.log"
srun --nodes=1 --ntasks=1 --cpus-per-task=256 --cpu-bind=none \
    .venv/bin/python scripts/sqlite_lock_revision.py run --suite copy-out \
    --result-pointer "$pointer" 2>&1 | tee "$log"
run="$(cat "$pointer")"
.venv/bin/python scripts/sqlite_lock_revision.py export --run "$run" --log "$log"
echo "SQLITE_COPY_OUT_COMPLETE: 48 trials validated and archived"
