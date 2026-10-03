#!/bin/bash
#SBATCH --account=m5289
#SBATCH --constraint=cpu
#SBATCH --qos=regular
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=256
#SBATCH --time=02:00:00
#SBATCH --job-name=ipdps-size-cal
#SBATCH --output=ipdps-size-cal-%j.out

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

logs="results/size-calibration-${SLURM_JOB_ID:?}"
mkdir "$logs"

for workload in metadata telemetry mixed; do
    config="configs/size-calibration/$workload-$deployment-10000000.yaml"
    echo "DEPLOYMENT=$deployment WORKLOAD=$workload CONFIG=$config"
    bash "$launcher" "$config" 2>&1 | tee "$logs/$workload.log"

    if [[ "$workload" == mixed ]]; then
        .venv-postgres/bin/python scripts/validate_mixed_results.py \
            --log "$logs/$workload.log" --job "$SLURM_JOB_ID"
    fi
done

echo "SIZE_CALIBRATION_COMPLETE: $deployment"
