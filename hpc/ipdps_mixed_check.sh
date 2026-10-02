#!/bin/bash
#SBATCH --account=m5289
#SBATCH --constraint=cpu
#SBATCH --qos=debug
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=256
#SBATCH --time=00:20:00
#SBATCH --job-name=ipdps-mixed-check
#SBATCH --output=ipdps-mixed-check-%j.out
set -euo pipefail
module load python/3.11-24.1.0
cd "${SLURM_SUBMIT_DIR:?}"
export OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1 PYTHONUNBUFFERED=1
logs="results/mixed-check-${SLURM_JOB_ID:?}"
mkdir "$logs"

run_and_verify() {
    local deployment="$1" launcher="$2" config="$3"
    echo "DEPLOYMENT=$deployment CONFIG=$config"
    bash "$launcher" "$config" 2>&1 | tee "$logs/$deployment.log"
    .venv-postgres/bin/python scripts/validate_mixed_results.py \
        --log "$logs/$deployment.log" --job "$SLURM_JOB_ID"
}

run_and_verify sqlite-native hpc/ipdps_smoke.sh configs/mixed-sqlite-native-check.yaml
run_and_verify sqlite-shifter hpc/ipdps_shifter.sh configs/mixed-sqlite-shifter-check.yaml
run_and_verify postgres hpc/ipdps_postgres.sh configs/mixed-postgres-check.yaml
run_and_verify influx hpc/ipdps_influx.sh configs/mixed-influx-check.yaml

echo "MIXED_CHECK_COMPLETE: 24 mixed trials verified across four deployments"
