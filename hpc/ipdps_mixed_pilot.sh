#!/bin/bash
#SBATCH --account=m5289
#SBATCH --constraint=cpu
#SBATCH --qos=regular
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=256
#SBATCH --time=00:45:00
#SBATCH --job-name=ipdps-mixed-pilot
#SBATCH --output=ipdps-mixed-pilot-%j.out

set -euo pipefail
module load python/3.11-24.1.0
cd "${SLURM_SUBMIT_DIR:?}"
export OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1
export PYTHONUNBUFFERED=1

deployment="${1:?Specify a deployment}"
case "$deployment" in
    sqlite-native) launcher=hpc/ipdps_smoke.sh ;;
    sqlite-shifter) launcher=hpc/ipdps_shifter.sh ;;
    postgres) launcher=hpc/ipdps_postgres.sh ;;
    influx) launcher=hpc/ipdps_influx.sh ;;
    *) echo "Unsupported deployment: $deployment" >&2; exit 2 ;;
esac

config="configs/mixed-$deployment-pilot.yaml"
logs="results/mixed-pilot-${SLURM_JOB_ID:?}"
mkdir "$logs"

echo "DEPLOYMENT=$deployment CONFIG=$config"
bash "$launcher" "$config" 2>&1 | tee "$logs/controller.log"

.venv-postgres/bin/python scripts/validate_mixed_results.py \
    --log "$logs/controller.log" --job "$SLURM_JOB_ID"

echo "MIXED_PILOT_COMPLETE: $deployment — 30 measured + 6 warmups verified"
