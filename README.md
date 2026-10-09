# IPDPS Storage Benchmark

**Storage placement, database concurrency, and write coordination on NERSC Perlmutter.**

This repository contains the experiment framework, validation tools, and research
artifacts developed from the original CANOPIE-HPC storage benchmark. It compares
SQLite, PostgreSQL, and InfluxDB on Lustre and memory-backed tmpfs, then uses
focused SQLite experiments to investigate workload size, shared-file contention,
partitioning, multi-node placement, busy-timeout policies, and persistence costs.

The central question is **when faster storage translates into faster application
completion—and when coordination, checkpointing, or copying results back to
persistent storage limits that benefit.**

[Completed studies](#completed-studies) · [Findings](#selected-findings) ·
[Artifacts](#reports-data-and-artifact-availability) ·
[Reproduction](#getting-started) · [Limitations](#interpretation-and-limitations)

## What the framework provides

- **Four deployments:** native SQLite, SQLite in Shifter, and native Python
  clients communicating with PostgreSQL or InfluxDB servers in Shifter.
- **Verified storage placement:** Lustre scratch and compute-node tmpfs, with
  fresh databases for each condition and saved environment information.
- **Configurable workloads:** deterministic metadata, telemetry, and a 50/50
  metadata–telemetry composition; controlled batch sizes, query counts, seeds,
  concurrency, warmups, and repetitions.
- **Coordinated execution:** workers report ready before timed execution;
  condition ordering is randomized and server lifecycle management is automated.
- **Correctness checks:** stored counts, unique keys, query results, mixed-type
  composition, and shard ownership are checked where applicable.
- **Traceable measurements:** CSV summaries, raw JSON and latency samples,
  source hashes, runtime versions, pinned container identities, and validation
  receipts. Follow-up studies freeze committed source before submission.
- **Mechanism experiments:** shared versus partitioned SQLite, per-batch timing,
  multi-node coordination, direct versus queued writers, staggered starts, and
  timed tmpfs-to-Lustre copy-out.

The broad database comparisons use 1, 16, and 64 clients. The SQLite partitioning
study extends this to 1, 2, 4, 8, 16, 32, and 64 clients. Mixed workloads combine
both data types in the **same database**, with two separate backend calls per
logical batch; writes and warm reads remain separate phases. They are not a
partitioned database or simultaneous read/write experiment.

## Completed studies

The evidence below reflects completed experiments through **8 October 2026**.
Counts are kept by study because workloads, timing boundaries, and statistical
units differ. Warmups are retained but excluded from effect estimates.

| Study | Experimental coverage | Measured observations | Warmups |
| --- | --- | ---: | ---: |
| Original database pilots | Four deployments; metadata/telemetry; two tiers; 1/16/64 clients; 1M records | 240 | 48 |
| SQL read-count follow-up | Native SQLite, Shifter SQLite, PostgreSQL; 1,024 warm queries per trial | 180 | 36 |
| Mixed-workload pilots | Four deployments; 50/50 metadata–telemetry; 1M total records | 120 | 24 |
| SQL allocation repeats | Three allocations per workload; all three SQL deployments on the same node within each allocation; rotated deployment order | 540 | 108 |
| SQLite size sweep | 100K/1M/10M records; shared file; 1/64 clients; Lustre/tmpfs | 60 | 12 |
| SQLite partitioning | 1M total records; shared/per-client files; seven client counts; Lustre/tmpfs | 140 | 28 |
| Multi-node partitioned SQLite | 10M total records; 64 total writers/files distributed over 1/2/4 active nodes; Lustre/tmpfs | 30 | 6 |
| SQLite lock policies | 1M records; 64 producers; five direct-writer timeouts, single-writer queue, and staggered starts; both tiers | 70 attempts: 50 complete, 20 BUSY | 14: 11 complete, 3 BUSY |
| SQLite copy-out | 1M/10M records; one file/one writer or 64 files/64 writers; both tiers | 40 | 8 |

The first four studies contain **1,080 measured trials and 216 warmups**. Their
consolidated analysis additionally includes 48 ten-million-record calibration
observations and 48 system-diagnostic observations. Dedicated PostgreSQL/InfluxDB
server profiling, PostgreSQL profiling-overhead comparisons, and integration
checks are separate supporting studies.

**Size-coverage distinction:** the focused native SQLite size sweep above is
complete. The broader 24-configuration sweep across all four deployments and
three workloads was prepared but **not executed** after allocation-budget
rejection. Its files in [configs/size-sweep](configs/size-sweep/) are plans,
not completed results. It is different from
[configs/sqlite-mechanism/size-sweep.json](configs/sqlite-mechanism/size-sweep.json).

## Selected findings

These are results for the recorded workloads and measurement boundaries, not
universal engine or filesystem rankings. Ratios greater than one favor the
numerator. Intervals below are exploratory 95% confidence intervals.

| Question | Observed result | Interpretation |
| --- | --- | --- |
| Does SQLite retain its tmpfs advantage as concurrency increases? | Across the SQL allocation repeats, native SQLite metadata-write tmpfs/Lustre throughput was **1.87× [1.82, 1.92]** at one client and **1.03× [0.93, 1.13]** at 64. | The large single-client advantage largely disappears; the 64-client interval includes parity. |
| Is that pattern shared by PostgreSQL? | Metadata-write tmpfs/Lustre ratios were **1.42×** at one client and **1.38×** at 64 in the SQL allocation repeats. | Storage sensitivity depends on the database and workload. These are within-deployment comparisons. |
| Does Lustre win for small, concurrent SQLite bursts? | The focused size sweep gave **1.13× [0.96, 1.33]** tmpfs/Lustre at 100K records and 64 clients. | The proposed small-size Lustre crossover was **not established**. |
| Does partitioning help? | At 1M records and 64 clients, partitioned/shared completion throughput was **5.44× on Lustre** and **34.19× on tmpfs**. | Partitioning is an effective intervention for independently shardable data; file sizes, caches, and final checkpoints also change. |
| Does distributing shards across nodes help? | Moving the same 10M records and 64 writers/files from one to four active nodes improved completion throughput by **1.68× on Lustre** and **1.49× on tmpfs**. | This is fixed-work node-distribution scaling. Checkpoints also become concurrent across nodes. |
| Can write coordination recover a tmpfs benefit? | At 64 producers and 1M records, a single-writer queue gave **2.43× [2.40, 2.47]** tmpfs/Lustre, versus approximately parity for the 60-second direct-writer control. | Coordination policy materially changes performance; this does not uniquely identify unfair busy-handler waiting as the cause. |
| Does the gain survive persistence? | For 10M records in 64 files, including tmpfs copy-out and fsync reduced the tmpfs/Lustre completion ratio to **0.98× [0.93, 1.04]**. | The interval includes parity: a benefit before copy-out need not survive the full persistence workflow. |

SQL allocation-repeat estimates use contrasts across three allocation blocks
per workload. The later SQLite studies use five paired measured blocks within
one allocation per suite. These uncertainty scopes must not be pooled. Unless
otherwise stated, write throughput above includes explicit final checkpoints.
See the artifact index below for the evidence associated with each study.

## Reports, data, and artifact availability

### Analysis packages committed in this repository

| Package | Contents |
| --- | --- |
| [Original pilots](analysis/pilot-20261002/README.md) | Eight-run pilot analysis, storage/concurrency ratios, statistical tables, figures, input archive, and reproduction scripts. |
| [1,024-query comparison](analysis/read1024-20261002/README.md) | Follow-up SQL comparisons, read-duration diagnostics, runtime comparisons, original inputs, and reproducible analysis. |
| [Consolidated study](analysis/consolidated-20261004/README.md) | Nine-page report, **15 CSV tables**, seven figures, six input archives, validation records, and analysis/report scripts. |

Start with the [consolidated report](analysis/consolidated-20261004/ipdps-consolidated-report.pdf)
and its [table dictionary](analysis/consolidated-20261004/README.md#table-dictionary).
Its `all_trials.csv` contains **1,392 records across 96 runs**: 1,080 measured
performance trials, 216 warmups, and 96 calibration/diagnostic observations.
That count excludes the later SQLite studies and dedicated server profiling.

The 15 consolidated tables cover individual trials, condition summaries,
allocation means and contrasts, confidence intervals, SQLite environment ratios,
mixed-workload storage ratios, size-calibration context, short-read diagnostics,
worker resources, system measurements, node-memory samples, database settings,
run inventory, and study counts.

### Later follow-up exports

The following completed studies have code/configurations in this repository and
separately exported raw results and analysis packages. Their CSVs and reports are
**not included in the three committed analysis directories above**.

| Study documentation | Raw result archive | Separate findings report |
| --- | --- | --- |
| [SQLite size and partitioning](SQLITE_MECHANISM_STUDY.md) | `sqlite-mechanism-ke2co78m.zip` | `sqlite-mechanism-findings-report.pdf` |
| [Multi-node SQLite](SQLITE_MULTINODE_STUDY.md) | `sqlite-multinode-20261005T080702Z-adju6cmo.zip` (downloaded as `sqlite-multinode-results.zip`) | `sqlite-multinode-findings-report.pdf` |
| [Lock policy and copy-out](SQLITE_LOCK_REVISION.md) | `sqlite-lock-policy-3o50s582.zip`, `sqlite-copy-out-vlx155m8.zip` | `sqlite-revision-findings-report.pdf` |

A repository clone alone therefore does not reproduce those later numerical
analyses; the corresponding exported inputs and analysis scripts are also
required. Reports are internal findings summaries, separate from the manuscript.
Historical reports and planning documents describe their own dated scope and
may list experiments that were completed in a later campaign.

## Measurements and validation

| Measurement | Saved evidence and scope |
| --- | --- |
| Throughput and timing | Write-only, completion/checkpoint, warm-query, lifecycle, and copy-out durations where applicable; counts and timing endpoints retained. |
| Latency distributions | Mean, p50, p95, p99, and raw batch/query samples. A write-batch latency is not per-record latency. |
| Worker resources | User/system CPU, memory high-water marks, page faults, context switches, and available kernel I/O counters in CSV/JSON. |
| Server resources | Optional PostgreSQL/InfluxDB launch-lifecycle CPU/I/O accounting and sampled process-tree PSS; raw accounting and memory samples retained. |
| Node diagnostics | Full-lifecycle CPU/iowait, sampled node memory, GNU time accounting, and available iostat output. These are not database-phase measurements. |
| SQLite mechanisms | Data preparation, BEGIN IMMEDIATE acquisition elapsed, transaction body, COMMIT, checkpoint, queue wait where applicable, errors, and application retries. |
| Copy-out correctness | Source/destination hashes, row counts, reopened-copy checks, copied bytes, and fsync outcomes. |
| Provenance | Configurations, plans, seeds, software/image identities, source hashes or snapshots, job/node identity, checksums, and validation receipts. |

Unavailable values remain missing, not zero. The system study's attempted
node-wide perf events were unavailable under the recorded permissions, so it
provides no hardware cycles, instructions, cache-miss counts, or IPC. The later
optional Lustre probe found no `lctl` in PATH and supplies no Lustre-counter result.

The harness audit found a **60-second effective SQLite write-worker busy
timeout**, despite the initial connection default of five seconds. There is no
application-level transaction retry loop. Internal SQLite busy-handler callback
counts are unavailable. In the lock-policy study, every measured one-second and
five-second timeout attempt was incomplete; those BUSY outcomes are retained,
validated against committed partial work, and excluded from full-work throughput.

Offline validation checks retained evidence and recomputes summaries; it does
not reopen databases already removed after on-node correctness checks.

## Getting started

### Inspect plans and run local checks

Use Linux and Python 3.11 or newer. The current experimental implementation is
on `master`. For a new checkout:

```bash
git clone --branch master https://github.com/HPCAI-lab/ipdps-storage-benchmark.git
cd ipdps-storage-benchmark
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements-benchmark.txt

.venv/bin/python experiment.py --config configs/metadata-pilot.yaml --dry-run
.venv/bin/python scripts/sqlite_mechanism_study.py \
    --config configs/sqlite-mechanism/size-sweep.json --dry-run
.venv/bin/python scripts/sqlite_mechanism_study.py \
    --config configs/sqlite-mechanism/partitioned.json --dry-run
.venv/bin/python scripts/sqlite_multinode.py dry-run
.venv/bin/python scripts/sqlite_lock_revision.py dry-run

.venv/bin/python -m unittest discover -s tests -p 'test_sqlite*.py' -v
```

These dry-runs do not submit jobs. The SQLite tests use small local workloads;
local simulation does not establish Perlmutter filesystem or multi-node behavior.
The concurrent synthetic workloads do not require the legacy XYZ trajectory.

### Run on Perlmutter

Use a scratch checkout, the Python module recorded by the launchers, a valid
allocation account, and the required pinned Shifter images. Launchers expect
`.venv` for native SQLite and `.venv-postgres` for PostgreSQL/InfluxDB clients.
Create the latter with `requirements-postgres.txt`. The Shifter SQLite image must
contain the controller dependencies; image IDs are recorded in the configurations.

For example, after preparing the native environment, submit the small telemetry
check from the repository root:

```bash
sbatch --account=YOUR_ACCOUNT hpc/ipdps_smoke.sh configs/telemetry-smoke.yaml
```

| Workflow | Entry points and documentation |
| --- | --- |
| Broad database experiments | [experiment.py](experiment.py), [configs](configs/), and `hpc/ipdps_smoke.sh`, `hpc/ipdps_shifter.sh`, `hpc/ipdps_postgres.sh`, `hpc/ipdps_influx.sh` |
| Mixed workloads | [MIXED_WORKLOAD.md](MIXED_WORKLOAD.md) |
| InfluxDB lifecycle and profiling | [INFLUX_INTEGRATION.md](INFLUX_INTEGRATION.md), [INFLUX_RESOURCES.md](INFLUX_RESOURCES.md) |
| PostgreSQL resource measurements | [VALIDATION.md](VALIDATION.md), `pg_server.py`, `server_metrics.py`, and `configs/*-postgres-resources.yaml` / `*-postgres-overhead.yaml` |
| SQL allocation repeats | [hpc/ipdps_sql_allocations.sh](hpc/ipdps_sql_allocations.sh); requires a prepared source snapshot, workload, and allocation-block number |
| System diagnostics | [SYSTEM_MEASUREMENTS.md](SYSTEM_MEASUREMENTS.md) |
| SQLite size/partitioning | [SQLITE_MECHANISM_STUDY.md](SQLITE_MECHANISM_STUDY.md), `scripts/prepare_sqlite_mechanism.py` |
| Multi-node SQLite | [SQLITE_MULTINODE_STUDY.md](SQLITE_MULTINODE_STUDY.md), `scripts/prepare_sqlite_multinode.py` |
| Lock policies/copy-out | [SQLITE_LOCK_REVISION.md](SQLITE_LOCK_REVISION.md), `scripts/prepare_sqlite_lock_revision.py`; requires a checksum-verified runtime-audit directory |

Before freezing a snapshot, configure account/time limits and commit the study
files. Preparation helpers freeze committed source; linked virtual environments
must remain unchanged while jobs are queued or running. Bundled launchers contain
original site settings. Helpers may submit multiple jobs with `--submit`; follow
the corresponding study document for its plan, validation, and export procedure.

### Reproduce the committed analysis without new compute jobs

From the repository root, use a fresh checkout or a copy of the analysis package
because the scripts regenerate tables, figures, and reports in place:

```bash
python3 -m venv .venv-analysis
.venv-analysis/bin/python -m pip install \
    -r analysis/consolidated-20261004/requirements-analysis.txt

(
    cd analysis/consolidated-20261004
    sha256sum -c SHA256SUMS
    ../../.venv-analysis/bin/python scripts/analyze_final.py
    ../../.venv-analysis/bin/python scripts/plot_final.py
    ../../.venv-analysis/bin/python scripts/build_report.py
)
```

The report builder requires DejaVu Sans at the path documented in the package
README. Distributed checksums identify the saved artifact; regenerated PDF
timestamps and rendering metadata can differ even when numerical results agree.
Other analysis directories provide their own reproduction instructions.

## Interpretation and limitations

- **Timing and durability differ across engines.** SQL completion includes final
  checkpointing. InfluxDB ends its write measurement at synchronous HTTP
  acknowledgement; shutdown is separate. These are not equal durability policies.
- **tmpfs is volatile.** The copy-out study measures completed copy/fsync API
  operations and validated destination contents, not a power-failure experiment.
- **Native/Shifter versions differ.** The recorded native build uses Python
  3.11.7 / SQLite 3.44.2; the SQLite Shifter build uses Python 3.11.15 / SQLite
  3.46.1. Their comparison does not isolate container overhead.
- **Short warm reads are not steady state.** Original SQL pilots use 64,000
  queries; follow-up SQL and InfluxDB pilots use 1,024. Many follow-up SQL read
  phases are shorter than 50 ms.
- **Mechanism harnesses have a narrower workload.** They use explicit
  transactions and metadata inserts only. Their shared-file controls, rather
  than earlier pilots, are the matched baselines. Acquisition elapsed includes
  SQLite overhead, waiting, and scheduling; it is not pure kernel lock-wait time.
- **Partitioning changes more than locks.** Shard size, cache behavior, and
  checkpoint work also change. Cross-shard queries, merging, and atomic
  transactions are outside these experiments. Multi-node throughput uses one
  coordinator clock; no database file is accessed from multiple hosts.
- **Replication is bounded.** SQL allocation repeats cover three allocation
  blocks per workload. Later SQLite suites each use one allocation. Repeated
  trials and thousands of batch samples are not independent node allocations.
- **Coverage remains explicit.** Broad all-engine size sweeps, a matched
  native/container comparison, real workflow-trace replay, and NVMe experiments
  are not completed evidence. No validated predictive placement model is claimed.

## Repository map and provenance

`experiment.py` is the broad study controller; `concurrent_*.py`,
`backend_factory.py`, and [src](src/) implement workers and database adapters.
[configs](configs/) contains study plans, [hpc](hpc/) contains launchers,
[scripts](scripts/) contains focused harnesses and validators, [tests](tests/)
contains local correctness tests, and [analysis](analysis/) contains the three
committed analysis packages described above.

This work extends the CANOPIE-HPC benchmark derived from Sakthi Muthuswamy's
`file-storage-app`. Original provenance and outstanding licensing/data-rights
items are recorded in [PROVENANCE.md](PROVENANCE.md). The
[archived CANOPIE README](https://github.com/HPCAI-lab/ipdps-storage-benchmark/blob/498df0f5c827ad041e8d404f7792fc3a0e04d778/README.md)
preserves the earlier XYZ-ingestion and container setup instructions.
When reproducing or citing results, identify the study, source commit, runtime
versions, and associated input archive; repository HEAD is not a substitute for
the version recorded by a completed experiment.
