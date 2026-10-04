# Consolidated Perlmutter storage benchmark analysis

Internal findings package, 4 October 2026. This is an analysis artifact, not a publication manuscript. It closes the agreed experimental round using completed data only. No script in this package submits benchmark jobs.

## Read first

- `ipdps-consolidated-report.pdf`: nine-page report with findings, design, uncertainty and explicit scope limits.
- `tables/`: 15 CSV tables. `all_trials.csv` preserves individual trial rows; do not pool all rows as one experimental population.
- `figures/`: seven figures, each as PNG and vector PDF. Six are embedded in the report; `sqlite_environment_ratios` is supplemental.
- `validation.json`: input SHA-256 hashes, validation counts, source matches and diagnostic audit.
- `input/`: six original, unchanged input archives, including benchmark source snapshots and logs where supplied.
- `scripts/`: standalone analysis, plotting and report generation, with two readers retained from earlier analysis packages.
- `SHA256SUMS`: checksums for all delivered files except the checksum list itself.

The new package is an offline analysis of the exported data. It is different from the raw-result ZIPs downloaded from Perlmutter. It has not been committed or pushed to the lab repository by this analysis session.

## Evidence inventory

| Study | Runs | Measured performance trials | Warmups | Other observations |
|---|---:|---:|---:|---:|
| Original pilots, four deployments | 8 | 240 | 48 | 0 |
| SQL 1,024-query follow-up | 6 | 180 | 36 | 0 |
| Mixed data-type pilots | 4 | 120 | 24 | 0 |
| SQL allocation repeats | 18 | 540 | 108 | 0 |
| 10M-record calibration | 12 | 0 | 0 | 48 |
| System diagnostics | 48 | 0 | 0 | 48 |
| **Total** | **96** | **1,080** | **216** | **96** |

There are 1,392 trial records overall. The four newly exported archives account for 82 runs and 888 of these records. Calibration and diagnostic rows are labelled `phase=measured` by the original controller, but they are kept separate from repeated-performance inference. Older integration checks, PostgreSQL profiling-overhead trials and dedicated server-resource studies are not included in these six inputs or in the 1,392 count.

## Main findings

- Native SQLite metadata-write tmpfs/Lustre ratios across the three new allocation blocks are 1.87 at one client (95% interval 1.82–1.92) and 1.03 at 64 clients (0.93–1.13). The high-concurrency interval includes parity.
- PostgreSQL metadata-write gains are 1.42 at one client and 1.38 at 64 clients. These ratios use checkpoint-inclusive completion throughput.
- Mixed native SQLite retains a 1.50 ratio of mean write throughput at 64 clients in its separate, single-allocation pilot. The metadata-only pattern is not universal across workloads. These studies do not isolate workload composition as a causal intervention.
- 360/540 measured SQL allocation-repeat read phases take less than 50 ms. These are warm bursts of 1,024 total queries, not steady-state throughput measurements.
- Selected PostgreSQL settings **are present** in all 276 PostgreSQL raw trials from the four new archives. `database_configuration.csv` exposes those values. The saved source also confirms that workers report ready before the timed event release; client process startup is outside the performance timer.
- All 48 system diagnostics completed and yielded node CPU/memory summaries. The attempted node-wide perf counters were denied; missing event counts and IPC remain blank, not zero.

## Experimental scope

Deployments are SQLite native, SQLite under Shifter, native Python clients with PostgreSQL in Shifter, and native Python HTTP clients with InfluxDB in Shifter. Lustre and tmpfs were verified for the recorded trials. This is single-node client concurrency. Separate jobs or repeated allocations on different nodes are not distributed multi-node scaling.

Repeated studies use 1M records, batch size 1,000, query window 100, clients 1/16/64 and five repetitions plus one warmup per condition. Original SQL baseline reads use 64,000 queries; later SQL, InfluxDB and mixed pilots use 1,024. Mixed trials split the total records evenly between metadata and telemetry and split queries evenly between the two types. Each logical mixed write batch makes two backend calls, without a cross-call atomicity guarantee. Writes and reads are separate phases.

Native software: Python 3.11.7 / SQLite 3.44.2. Shifter SQLite: Python 3.11.15 / SQLite 3.46.1. Server versions: PostgreSQL 16.12 and InfluxDB 2.9.1. Image identities remain in original JSON/configuration records. Runtime differences are execution-environment comparisons; they are not isolated container overhead.

### Timing and measurement units

- `completion_throughput_records_s` = total records / completion duration. SQL completion includes the final checkpoint and intervening worker-finalization/controller time; InfluxDB completion ends at the last write acknowledgement.
- Workers initialize and report ready before the controller starts timing and releases the event. Row generation and scheduling after release are in write workload time. Batch latency wraps the backend insertion call and excludes row generation.
- `transaction_latency_*_ms` is a batch latency, not per-record latency. InfluxDB uses an HTTP batch; mixed uses a two-call logical batch.
- `read_queries_per_s` = completed queries / warm-read phase duration. Query latency is measured per query; reported throughput also reflects the synchronized burst and worker-side work.
- SQLite uses WAL/NORMAL, automatic checkpoints and final TRUNCATE. PostgreSQL records fsync/synchronous_commit/full_page_writes on and explicit final CHECKPOINT. InfluxDB records synchronous HTTP 204 acknowledgement, WAL fsync delay 0s and no explicit final checkpoint API. These boundaries do not establish equivalent durability across engines. tmpfs does not provide persistent storage after node failure.
- Warm reads follow writes and validation; these are not cold-cache results.

## Statistics

### SQL allocation study

Jobs 59304573/59304575/59304580 are metadata blocks 1/2/3; jobs 59304581/59304582/59304585 are telemetry blocks 1/2/3. All three SQL deployments share a node within each allocation. Deployment order rotates by block. Metadata uses three different nodes; telemetry uses two, with the last two allocations on nid006520. This is three allocation blocks per workload, not six independent nodes per condition.

First average the five repetitions for each deployment/workload/tier/client condition within an allocation. A storage contrast is that allocation's tmpfs mean divided by its Lustre mean. Compute the mean of the three log contrasts and a Student-t 95% interval on that mean, using 2 degrees of freedom; exponentiate both the estimate and endpoints. Concurrency and storage–concurrency interaction contrasts use the same allocation-level treatment. The descriptive throughput curves average the three allocation means arithmetically and also show the individual allocation curves.

The intervals assume independent, approximately normal log contrasts. With three blocks, those assumptions are weakly assessable. No multiple-comparison correction is applied; intervals are exploratory. Same-node reuse and time-varying Lustre load constrain generalization. Earlier studies are not pooled into these block intervals.

`sqlite_environment_ratios.csv` uses same-allocation Shifter/native mean throughput contrasts and the same three-block method. Different Python/SQLite versions preclude an isolated container-overhead estimate.

### Mixed and calibration

Mixed throughput figures show mean ± sample SD of five repetitions, not a 95% interval. `mixed_storage_ratios.csv` provides the ratio of arithmetic means and an exploratory log-t interval over five repetition-matched tier ratios (4 degrees of freedom). That interval describes within-allocation variation only. Reported mixed ratios use the ratio of means. Median trial p99 is not a pooled global p99.

10M calibration has one observation per deployment/workload/tier/client condition, with clients 1/64 and no warmups. The comparison table uses 1M SQL allocation means for metadata/telemetry SQL, the original 1M InfluxDB pilots for pure InfluxDB workloads, and the mixed pilots for mixed workloads. These are descriptive context ratios only: campaign/allocation/seed/source differences can confound them. No interval or causal size effect is estimated.

### Size sweeps omitted from this round

The planned 100K/1M/10M dimension varies **records written**, not total operations; reads stay at 1,024 in the prepared sweeps. The 1M repeated matrix is complete and the 10M calibration is complete. Repeated 100K and 10M sweeps did **not** run. Submission of the 24-task array was rejected before an array job ID: requested estimated cost 72 node-hours, displayed user balance 7.00 node-hours (repository balance 6.25). This is a requested reservation estimate, not hours consumed. The user then explicitly dropped those sweeps to conclude the round. The prepared configurations are not evidence of results.

## System diagnostics

Job 59329430 ran on nid006158, completed in 14:46, and contains 48 conditions: four deployments × three workloads × two tiers × clients 1/64. Each is one observation with no warmup. A node-wide /proc sampler ran during the full trial lifecycle, including setup, validation, read, shutdown and monitors; it does not attribute samples to SQL phases.

- Busy percent excludes idle and iowait from the first-eight-field aggregate CPU-tick delta. It averages over the whole node and lifecycle, not just busy worker cores.
- Iowait is a node accounting statistic, not database lock wait and not a direct Lustre bandwidth measurement. Low iowait cannot rule out either filesystem or lock-related delays.
- Sampled node used memory is `MemTotal - MemAvailable`. Peak-minus-start is provided but is still not an isolated database footprint. GNU time maximum RSS is not a process-tree peak. Earlier sampled server PSS studies are separate.
- GNU time waited-process CPU and context switches and raw iostat logs are retained. The iostat processes were intentionally terminated after collection (recorded exit -15). Device reports are not assumed to measure Lustre network/filesystem throughput.
- Probes for `{cycles,instructions}`, `cache-misses` and `context-switches` exited 255 under `perf_event_paranoid=2`. The attempted node-wide collection was unavailable. This does not prove that every alternative user-space profiling mode is impossible. No permission changes or new probes were attempted in this analysis.
- Missing perf events/IPC are blank. No diagnostic instrumentation-overhead control or statistical replication was performed.

## Verification scope

Input ZIP CRCs, supplied per-file SHA-256 manifests, plans, phase counts, trial numbering, image identities, job/node metadata, stored/committed counts, read counts/rows, mixed composition, worker totals, finite timings and derived throughput are checked. All 82 newly supplied run source manifests match bundled source snapshots. SQL allocation order and within-allocation node consistency are checked.

The 216 read1024 follow-up raw records and 888 newer records allow recomputation of write/read latency mean/p50/p95/p99 from stored arrays: **1,104 raw trials**. Percentiles use linear interpolation between sorted sample positions. The original 288 baseline records have CSV summaries and saved metadata but no original latency arrays in this input; those are checked at the available level, not falsely described as revalidated raw samples. Saved correctness evidence is checked offline; the databases are not reopened and query payloads are not rerun.

All 48 node CPU/memory summaries are recomputed from the raw node samples. CPU deltas, sampler errors, GNU time exit status and iostat availability are checked. `validation.json` records the exact audit.

## Table dictionary

| File | Contents / statistical unit |
|---|---|
| `all_trials.csv` | All 1,392 trial records, tagged by study; warmups retained |
| `run_inventory.csv` | 96 runs with deployment, workload, job, node, software and sizes |
| `study_counts.csv` | Counts by study and original phase label |
| `condition_summary.csv` | Within-study arithmetic summaries; read `n_trials` and `n_allocations` |
| `sql_allocation_means.csv` | Five-repetition means, one row per condition/allocation |
| `sql_allocation_contrasts.csv` | Individual storage, concurrency and interaction ratios by allocation |
| `sql_ratio_intervals.csv` | Geometric ratios and log-t intervals across three blocks |
| `sqlite_environment_ratios.csv` | Same-allocation Shifter/native ratios, versions differ |
| `mixed_storage_ratios.csv` | Ratios of means and repetition-level contrasts within one allocation |
| `size_calibration_context.csv` | Single-observation 10M versus designated 1M reference, descriptive only |
| `read_duration_diagnostics.csv` | Phase durations and counts below 50 ms |
| `system_measurements.csv` | Recomputed node metrics and parsed GNU time counters; absent perf fields blank |
| `node_memory_samples.csv` | Node memory time series, elapsed time within each diagnostic condition |
| `worker_resource_summary.csv` | Client/worker resource summaries; excludes separate server CPU accounting |
| `database_configuration.csv` | Selected settings recorded in every new raw trial; original units preserved |

`condition_summary.csv` has sample SD missing for n=1. Empty values are not zeros. In `database_configuration.csv`, PostgreSQL `listen_addresses` is an actual empty string (Unix-socket setup), not an unknown value. Use original raw JSON when typed null/empty distinctions matter. `all_trials.csv` is a union of schemas; not every field applies to every engine/study. It retains original file/path references as provenance, not as portable local paths.

## Reproduce locally

This is CPU-only offline analysis. A normal workstation is sufficient; no Slurm or database server is required. Python 3.12.14 was used; Python 3.11+ should support the scripts with compatible dependencies. Exact analysis package versions are in `requirements-analysis.txt`. The report renderer expects DejaVu Sans in `/usr/share/fonts/truetype/dejavu`; install the font package or adjust the `FONT` constant if needed.

From this directory in a fresh extraction:

```bash
sha256sum -c SHA256SUMS
python -m venv .venv-analysis
.venv-analysis/bin/python -m pip install -r requirements-analysis.txt
.venv-analysis/bin/python scripts/analyze_final.py
.venv-analysis/bin/python scripts/plot_final.py
.venv-analysis/bin/python scripts/build_report.py
```

The first command checks the delivered files. Subsequent commands rewrite generated tables, figures, validation JSON and PDF in place; keep a pristine copy if the original checksums are needed. PDF creation timestamps and analysis-runtime metadata can change on reproduction, so regenerated byte hashes need not match even when numerical tables do. No benchmark source within the inputs is executed by these commands.

## Explicit boundaries and remaining research

The finished artifact supports a bounded measurement study. It does not complete the professor's proposed shared-versus-partitioned SQLite harness, direct lock-wait decomposition, 50-microsecond retry intervention, 1/2/4/8/16/32/64 client grid, syscall-lock probe, or matched-software native/Shifter comparison. Those experiments are different causal follow-ups, not already completed by collecting system counters. Distributed multi-node scaling, replicated low/high-size sweeps and a placement-model evaluation are also absent. No claim of general production readiness or publication acceptance is made.

The publication manuscript is the researcher's work. This package supplies the validated evidence, figures, assumptions and limitations for that work.
