#!/bin/bash
#SBATCH --account=m5289
#SBATCH --constraint=cpu
#SBATCH --qos=debug
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --time=00:10:00
#SBATCH --job-name=ipdps-influx-check
#SBATCH --output=ipdps-influx-check-%j.out

set -euo pipefail
module load python/3.11-24.1.0
cd "$SLURM_SUBMIT_DIR"
export PYTHONUNBUFFERED=1
srun --nodes=1 --ntasks=1 --cpus-per-task=4 \
    .venv-postgres/bin/python hpc/influx_lifecycle_check.py
