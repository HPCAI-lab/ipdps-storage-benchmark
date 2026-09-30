# Perlmutter execution

Use `perlmutter_job.sh` to obtain a CPU compute node and
`run_perlmutter_matrix.sh` to execute the four conditions. The job refuses to
run on a login node. See the project-level `README.md` for exact commands.

Perlmutter's documented storage tiers used here are:

- `$SCRATCH`: shared all-flash Lustre
- `/tmp`: compute-node-local DRAM

The old package's assumed `/nvme/$USER` compute-node path is intentionally not
used.
