#!/bin/bash
#SBATCH --account=m5289
#SBATCH --constraint=cpu
#SBATCH --qos=debug
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --time=00:10:00
#SBATCH --job-name=ipdps-pg-check
#SBATCH --output=ipdps-pg-check-%j.out

set -euo pipefail
umask 077
[[ "$(hostname)" == nid* ]] || { echo "Compute node required"; exit 2; }

ROOT="${SLURM_SUBMIT_DIR:?}"
cd "$ROOT"
result_dir="$ROOT/results/pg-lifecycle-${SLURM_JOB_ID:?}"
mkdir "$result_dir"
cp hpc/pg_check_inner.sh hpc/pg_lifecycle.sh "$result_dir/"
printf 'RESULTS=%s\n' "$result_dir"

image_id=e4842c8a99ca99339e1693e6fe5fe62c7becb31991f066f989047dfb2fbf47af

for tier in lustre tmpfs; do
    if [[ "$tier" == lustre ]]; then
        base="${SCRATCH:?}"
    else
        base=/tmp
    fi

    trial_dir="$(mktemp -d "$base/ipdps-pg-${SLURM_JOB_ID}-XXXXXX")"
    socket_dir="$(mktemp -d "/tmp/ipdps-pgsock-${SLURM_JOB_ID}-XXXXXX")"
    printf '%s data=%s socket=%s\n' "$tier" "$trial_dir" "$socket_dir" \
        >> "$result_dir/paths.txt"

    if srun --nodes=1 --ntasks=1 --cpus-per-task=4 --cpu-bind=none \
        shifter --image="id:$image_id" \
        --volume="$ROOT:/app" --volume="$trial_dir:/pgwork" \
        --volume="$socket_dir:/pgsocket" --volume="$result_dir:/pgresults" \
        /bin/bash /app/hpc/pg_check_inner.sh "$tier" \
        2>&1 | tee "$result_dir/$tier.txt"; then
        rm -rf -- "$trial_dir" "$socket_dir"
    else
        echo "FAILED: retained data at $trial_dir and logs at $result_dir" >&2
        exit 1
    fi
done

echo "POSTGRES_LIFECYCLE_COMPLETE"
