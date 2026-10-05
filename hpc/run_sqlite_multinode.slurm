#!/bin/bash
#SBATCH --account=m5289
#SBATCH --constraint=cpu
#SBATCH --qos=regular
#SBATCH --nodes=4
#SBATCH --ntasks=4
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=256
#SBATCH --exclusive
#SBATCH --time=00:30:00
#SBATCH --job-name=ipdps-sqlite-multinode
#SBATCH --output=ipdps-sqlite-multinode-%j.out

set -euo pipefail
snapshot="${1:?Specify the prepared source snapshot}"
cd "$snapshot"
test -f SOURCE_SHA256.json
module load python/3.11-24.1.0
export OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1
export PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1

# The coordinator runs on the batch node and launches sequential srun steps.
# Each step has one node controller per ACTIVE node, with local Python workers.
.venv/bin/python scripts/sqlite_multinode.py run
