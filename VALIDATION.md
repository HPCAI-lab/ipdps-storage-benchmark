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
