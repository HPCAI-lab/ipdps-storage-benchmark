#!/bin/bash
#SBATCH --account=m5289
#SBATCH --constraint=cpu
#SBATCH --qos=regular
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=256
#SBATCH --time=00:45:00
#SBATCH --job-name=ipdps-read1024
#SBATCH --output=ipdps-read1024-%j.out

set -euo pipefail
workload="${1:?Specify metadata or telemetry}"
case "$workload" in
    metadata|telemetry) ;;
    *) echo "Unsupported workload: $workload" >&2; exit 2 ;;
esac

cd "${SLURM_SUBMIT_DIR:?}"
echo "READ1024_WORKLOAD=$workload NODE=$(hostname) JOB=$SLURM_JOB_ID"

echo "DEPLOYMENT=sqlite-native"
bash hpc/ipdps_smoke.sh "configs/$workload-sqlite-native-read1024.yaml"

echo "DEPLOYMENT=sqlite-shifter"
bash hpc/ipdps_shifter.sh "configs/$workload-sqlite-shifter-read1024.yaml"

echo "DEPLOYMENT=postgres"
bash hpc/ipdps_postgres.sh "configs/$workload-postgres-read1024.yaml"

echo "READ1024_COMPLETE workload=$workload"
