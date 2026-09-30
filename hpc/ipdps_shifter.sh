#!/bin/bash
#SBATCH --account=m5289
#SBATCH --constraint=cpu
#SBATCH --qos=debug
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=256
#SBATCH --time=00:10:00
#SBATCH --job-name=ipdps-shifter
#SBATCH --output=ipdps-shifter-%j.out

set -euo pipefail
module load python/3.11-24.1.0
ROOT="${SLURM_SUBMIT_DIR:?}"
cd "$ROOT"

if [[ $# -eq 0 ]]; then
    echo "Usage: sbatch hpc/ipdps_shifter.sh configs/<config>.yaml [...]" >&2
    exit 2
fi

export OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1
export PYTHONUNBUFFERED=1

for config in "$@"; do
    image_id="$(.venv/bin/python - "$config" <<'PY'
import sys
from pathlib import Path
import yaml
from experiment import build_plan

cfg = yaml.safe_load(Path(sys.argv[1]).read_text())
build_plan(cfg)
assert cfg["runtime"] == "shifter", "This launcher requires runtime: shifter"
print(cfg["shifter_image_id"])
PY
)"
    echo "CONFIG=$config IMAGE_ID=$image_id"

    srun --nodes=1 --ntasks=1 --cpus-per-task=256 --cpu-bind=none \
        shifter --image="id:$image_id" --volume="$ROOT:/app" \
        --env=IPDPS_RUNTIME=shifter \
        --env=OMP_NUM_THREADS=1 --env=OPENBLAS_NUM_THREADS=1 \
        --env=MKL_NUM_THREADS=1 --env=PYTHONUNBUFFERED=1 \
        /usr/local/bin/python3 /app/experiment.py \
        --config "$ROOT/$config" --output-root "$ROOT/results"
done

echo "SHIFTER_RUN_COMPLETE"
