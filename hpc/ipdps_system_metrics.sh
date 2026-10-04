#!/bin/bash
#SBATCH --account=m5289
#SBATCH --constraint=cpu
#SBATCH --qos=regular
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=256
#SBATCH --exclusive
#SBATCH --time=01:00:00
#SBATCH --job-name=ipdps-system-metrics
#SBATCH --output=ipdps-system-metrics-%j.out
set -euo pipefail
snapshot="${1:?Specify committed source snapshot}"
cd "$snapshot"
module load python/3.11-24.1.0
export OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1
export PYTHONUNBUFFERED=1 IPDPS_RUNTIME=native
export SLURM_SUBMIT_DIR="$PWD"
echo "SOURCE_COMMIT=$(cat SOURCE_COMMIT)"
echo "SOURCE_SNAPSHOT=$PWD"
srun --nodes=1 --ntasks=1 --cpus-per-task=256 --cpu-bind=none \
    .venv-postgres/bin/python scripts/collect_system_metrics.py
