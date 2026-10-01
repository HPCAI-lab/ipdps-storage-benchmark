# Validation record

Completed locally on August 14, 2026:

- Python byte-compilation of source, tests, and result scripts
- Bash syntax validation of every HPC and build script
- Automated fresh-database test with two measured repetitions
- Metadata record-count validation
- Telemetry sizes of 100 and 1,000 records with no cross-trial accumulation
- Database, WAL, and SHM cleanup validation
- Simulated four-condition native/Shifter and Lustre/node-DRAM aggregation
- Combined raw CSV and descriptive summary CSV generation

Still required on the user's systems:

- Docker build and WSL2 smoke test
- Shifter image pull
- One small Perlmutter pilot job
- Full Perlmutter matrix

The native and container environments may use different Python and SQLite
builds. Their exact versions are captured in result metadata and must be treated
as part of the compared execution environments, not as a perfectly isolated
measurement of runtime overhead.

## PostgreSQL lifecycle profiling — job 59148624

- Eight trials completed successfully: metadata/telemetry, Lustre/tmpfs,
  and 1/64 clients; one million records and 64,000 queries per trial.
- CSV resource fields verified against raw JSON and wait4 accounting files.
- Memory summaries verified against retained raw sampling records.
- All trials reported lifecycle_collected.
- Complete samples observed up to 8 PostgreSQL processes with one client
  and 71 processes with 64 clients.
- CPU and kernel I/O accounting cover the server launch lifecycle,
  including initialization and shutdown; Python clients are excluded.
- Sampled launch-tree PSS includes the profiler supervisor. Incomplete
  samples are excluded; sampling can miss memory peaks.
- Profiling is opt-in. Existing pilot configurations leave it disabled.
- These are instrumentation-validation runs with one trial per condition.
  Profiling overhead and repeatability remain to be evaluated.

Results:
- results/20261001T043405Z-8deb7035
- results/20261001T043524Z-847ae5c3
