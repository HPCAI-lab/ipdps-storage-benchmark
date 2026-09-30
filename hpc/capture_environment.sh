#!/bin/bash
# Capture non-sensitive system metadata needed for the paper and artifact.

set -u

echo "captured_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "hostname=$(hostname)"
echo "slurm_job_id=${SLURM_JOB_ID:-}"
echo "shifter_image=${SHIFTER_IMAGE:-}"
uname -a
lscpu
df -Th "${SCRATCH:-$PWD}" /tmp
python3 --version
python3 -c 'import sqlite3; print("sqlite=" + sqlite3.sqlite_version)'
if [[ -n "${NATIVE_PYTHON:-}" && -x "$NATIVE_PYTHON" ]]; then
    "$NATIVE_PYTHON" --version
    "$NATIVE_PYTHON" -c 'import sqlite3; print("native_sqlite=" + sqlite3.sqlite_version)'
fi
shifter --version 2>&1 || true
if command -v lfs >/dev/null 2>&1 && [[ -n "${SCRATCH:-}" ]]; then
    lfs getstripe -d "$SCRATCH" 2>&1 || true
fi
if command -v scontrol >/dev/null 2>&1 && [[ -n "${SLURM_JOB_ID:-}" ]]; then
    scontrol show job "$SLURM_JOB_ID" 2>&1 || true
fi
