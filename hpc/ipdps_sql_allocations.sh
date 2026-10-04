#!/bin/bash
#SBATCH --account=m5289
#SBATCH --constraint=cpu
#SBATCH --qos=regular
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=256
#SBATCH --time=00:45:00
#SBATCH --job-name=ipdps-sql-repeat
#SBATCH --output=ipdps-sql-repeat-%j.out

set -euo pipefail
snapshot="${1:?Specify frozen source directory}"
workload="${2:?Specify metadata or telemetry}"
block="${3:?Specify block 1, 2, or 3}"

case "$workload" in metadata|telemetry) ;; *) exit 2 ;; esac
case "$block" in
    1) order=(sqlite-native sqlite-shifter postgres) ;;
    2) order=(sqlite-shifter postgres sqlite-native) ;;
    3) order=(postgres sqlite-native sqlite-shifter) ;;
    *) exit 2 ;;
esac

cd "$snapshot"
export SLURM_SUBMIT_DIR="$PWD"
module load python/3.11-24.1.0

logs="results/sql-repeat-${SLURM_JOB_ID:?}"
mkdir "$logs"
{
    printf 'WORKLOAD=%s BLOCK=%s JOB=%s NODE=%s\n' \
        "$workload" "$block" "$SLURM_JOB_ID" "$(hostname)"
    printf 'SOURCE_COMMIT=%s\n' "$(cat SOURCE_COMMIT)"
    printf 'SOURCE_SNAPSHOT=%s\n' "$PWD"
    printf 'ORDER=%s\n' "${order[*]}"
    date -u '+START_UTC=%Y-%m-%dT%H:%M:%SZ'
} | tee "$logs/allocation.txt"

for deployment in "${order[@]}"; do
    case "$deployment" in
        sqlite-native) launcher=hpc/ipdps_smoke.sh ;;
        sqlite-shifter) launcher=hpc/ipdps_shifter.sh ;;
        postgres) launcher=hpc/ipdps_postgres.sh ;;
    esac
    config="configs/$workload-$deployment-read1024.yaml"
    echo "DEPLOYMENT=$deployment CONFIG=$config"
    bash "$launcher" "$config" 2>&1 | tee "$logs/$deployment.log"
done

printf 'SQL_ALLOCATION_COMPLETE workload=%s block=%s\n' "$workload" "$block"
