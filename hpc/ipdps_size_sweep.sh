#!/bin/bash
#SBATCH --account=m5289
#SBATCH --constraint=cpu
#SBATCH --qos=regular
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=256
#SBATCH --time=03:00:00
#SBATCH --array=0-23%4
#SBATCH --no-requeue
#SBATCH --job-name=ipdps-size-sweep
#SBATCH --output=ipdps-size-sweep-%A_%a.out

set -euo pipefail
snapshot="${1:?Specify frozen source directory}"
cd "$snapshot"
export SLURM_SUBMIT_DIR="$PWD"
module load python/3.11-24.1.0
export OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1
export PYTHONUNBUFFERED=1 IPDPS_RUNTIME=native

task="${SLURM_ARRAY_TASK_ID:?}"
[[ "$task" =~ ^[0-9]+$ ]] && (( task < 24 ))
line="$(sed -n "$((task + 1))p" configs/size-sweep/manifest.tsv)"
IFS=$'\t' read -r deployment workload records config <<< "$line"
test -n "$config" && test -f "$config"
case "$deployment" in
    sqlite-native) launcher=hpc/ipdps_smoke.sh ;;
    sqlite-shifter) launcher=hpc/ipdps_shifter.sh ;;
    postgres) launcher=hpc/ipdps_postgres.sh ;;
    influx) launcher=hpc/ipdps_influx.sh ;;
    *) echo "Invalid deployment" >&2; exit 2 ;;
esac
logs="results/size-sweep-${SLURM_ARRAY_JOB_ID:?}-$task"
mkdir "$logs"
{
    printf 'ARRAY=%s TASK=%s JOB=%s NODE=%s\n' "$SLURM_ARRAY_JOB_ID" "$task" "$SLURM_JOB_ID" "$(hostname)"
    printf 'DEPLOYMENT=%s WORKLOAD=%s RECORDS=%s CONFIG=%s\n' "$deployment" "$workload" "$records" "$config"
    printf 'SOURCE_COMMIT=%s\nSOURCE_SNAPSHOT=%s\n' "$(cat SOURCE_COMMIT)" "$PWD"
    date -u '+START_UTC=%Y-%m-%dT%H:%M:%SZ'
} | tee "$logs/allocation.txt"
bash "$launcher" "$config" 2>&1 | tee "$logs/controller.log"
.venv-postgres/bin/python scripts/validate_size_sweep.py \
    --log "$logs/controller.log" --config "$config" --job "$SLURM_JOB_ID"
printf 'SIZE_SWEEP_TASK_COMPLETE task=%s deployment=%s workload=%s records=%s\n' "$task" "$deployment" "$workload" "$records"
