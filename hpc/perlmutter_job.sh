#!/bin/bash
#SBATCH --constraint=cpu
#SBATCH --qos=regular
#SBATCH --nodes=1
#SBATCH --time=02:00:00
#SBATCH --job-name=canopie-io
#SBATCH --output=canopie-%j.out

set -euo pipefail
module load python/3.9-24.1.0
ROOT="${SLURM_SUBMIT_DIR:-$PWD}"
"$ROOT/hpc/run_perlmutter_matrix.sh"
