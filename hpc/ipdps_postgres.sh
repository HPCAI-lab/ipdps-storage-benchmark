#!/bin/bash
#SBATCH --account=m5289
#SBATCH --constraint=cpu
#SBATCH --qos=debug
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=256
#SBATCH --time=00:20:00
#SBATCH --job-name=ipdps-postgres
#SBATCH --output=ipdps-postgres-%j.out

set -euo pipefail
module load python/3.11-24.1.0
cd "$SLURM_SUBMIT_DIR"
export OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1
export PYTHONUNBUFFERED=1
if [[ "$#" -eq 0 ]]; then
    set -- configs/metadata-postgres-check.yaml configs/telemetry-postgres-check.yaml
fi
for config in "$@"; do
    echo "CONFIG=$config"
    srun --nodes=1 --ntasks=1 --cpus-per-task=256 --cpu-bind=none \
        .venv-postgres/bin/python experiment.py --config "$config"
done
echo "POSTGRES_RUN_COMPLETE"
