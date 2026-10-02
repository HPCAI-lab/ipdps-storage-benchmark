# Perlmutter 1,024-query SQL follow-up

Internal research progress analysis, 2 October 2026. This package summarizes
the saved experiments; it is not a publication manuscript. It complements
the original eight-run pilot report without replacing its measurements.

## Coverage and input provenance

The new export contains six SQL runs: native SQLite, Shifter SQLite and
PostgreSQL, each with metadata and telemetry workloads. Every run has 30
measured trials and six warmups: Lustre/tmpfs x 1/16/64 clients x five measured
repetitions. Each trial writes 1,000,000 records in batches of 1,000 and runs
1,024 range queries returning 100 rows each. Profiling is disabled.

Metadata job 59220317 used nid005765; telemetry job 59220318 used nid006712.
Within each job, deployment order was fixed: native SQLite, Shifter SQLite,
then PostgreSQL. Storage/client conditions were randomized within repetition
blocks separately for each deployment. The common seed and query-selection
routine give the same logical query IDs across these configurations.

The baseline export contains the original eight runs: 240 measured trials
and 48 warmups. SQL ran 64,000 queries per trial; InfluxDB ran 1,024. Its
InfluxDB observations are reused as the equal-count reference; there are no
new InfluxDB trials here. Across both inputs there are 420 distinct measured
trials and 84 warmups, not a single pooled sample of independent experiments.
There are 14 run directories from 10 allocations, with three new SQL runs
sharing each of the two new allocations. Separate lifecycle profiling and
PostgreSQL profiling-overhead experiments are outside this package.

New source commit recorded in the export:
`63c227bc25fc394d3a3752f3fe4f19ebc78fff65`.
All six source manifests agree, and every supplied source file was verified
against them. Commit metadata is retained, but no full Git object database
is included, so Git-object correspondence is not independently verified.

## Files

- `read1024-comparison-report.pdf`: six-page findings, figures and methods.
- `figures/`: five figures, each in PNG and nonempty vector PDF form.
- `tables/`: ten CSV tables with full-precision observations and estimates.
- `validation.json`: audit results, input digests, source differences and limits.
- `scripts/analyze_comparison.py`: validation and numerical analysis.
- `scripts/baseline_reader.py`: original baseline validation/statistical helpers.
- `scripts/build_comparison_report.py`: figures and report generation.
- `input/`: both original ZIP uploads, unchanged. The new ZIP includes full
  trial JSON, latency samples, benchmark source files and scheduler logs;
  the old ZIP includes CSV, selected trial metadata, plans and source hashes.
- `requirements-analysis.txt`: dependency versions used for this analysis.
- `SHA256SUMS`: hashes for every other package file.
- `.gitattributes`: LF CSV handling and binary PDF/PNG/ZIP handling for Git.

This package is ready to add as a new analysis directory in the project
repository. No remote commit or push was performed by this analysis.

## Reproduction

Use a separate analysis environment with `requirements-analysis.txt`.
The analysis reads archives offline; it never launches databases, runs
benchmarks or executes benchmark source contained in the input ZIP.

From this directory:

```bash
python scripts/build_comparison_report.py \
    input/read1024-results-20261002T200306Z-67f254d2.zip \
    input/ipdps-pilot-analysis.zip \
    --output regenerated
```

For validation and tables only:

```bash
python scripts/analyze_comparison.py \
    input/read1024-results-20261002T200306Z-67f254d2.zip \
    input/ipdps-pilot-analysis.zip \
    --output regenerated
```

Numerical tables are deterministic for these inputs and dependencies. PDF
metadata or fonts may differ on another platform. DejaVu Sans is used when
available, with a standard-font fallback for the report. Validators and
report prose intentionally target this fixed experiment snapshot. Extending
the study requires explicit changes to identities, assumptions and prose.

## Table definitions

| Table | Contents and interpretation |
| --- | --- |
| `all_trials.csv` | 504 rows, including 84 declared warmups. `dataset` is `read1024` or `baseline`. Blank means unavailable/not applicable, not zero. |
| `run_inventory.csv` | 14 run directories, jobs, nodes, versions, query counts and trial totals. |
| `condition_summary.csv` | Per-dataset/deployment/workload/tier/client metric summaries: n, mean, sample SD, median, range, CV and pointwise 95% t interval. |
| `storage_gains.csv` | tmpfs/Lustre throughput: ratio of arithmetic means plus geometric repetition-matched ratio and its log-t interval. |
| `concurrency_scaling.csv` | Throughput at 16 or 64 clients divided by throughput at one client, holding other conditions fixed. |
| `storage_concurrency_interactions.csv` | Geometric contrast `[T(tmpfs,C)/T(lustre,C)] / [T(tmpfs,1)/T(lustre,1)]`; below one means reduced relative tmpfs benefit at higher concurrency. |
| `repetition_contrasts.csv` | Individual storage-gain and interaction ratios underlying the geometric estimates. |
| `read_count_context_comparison.csv` | New/old arithmetic-mean ratios, read durations and write ratios. Query count, allocation and source snapshot differ; these are descriptive contrasts. |
| `sqlite_runtime_comparison.csv` | Shifter/native arithmetic-mean throughput ratios for each dataset. New runs share a node but have fixed order and different software versions. No causal-effect interval is assigned. |
| `read1024_duration_diagnostics.csv` | Per-trial read duration, queries/client, mean and p99 request latency, aggregate backend-call fraction and client CPU metrics. |

The diagnostic `aggregate_backend_call_fraction` is
`sum(request_latency_seconds) / (clients * read_wall_seconds)`.
It describes the aggregate fraction of the C-worker wall-time budget inside
timed backend methods. Its complement combines worker wakeup/start skew,
query preparation, validation, idle time after earlier workers finish, and
other untimed work. It is not an isolated overhead estimate or CPU utilization.
Request samples are pooled across workers only to recompute the original
per-trial summaries; they are not treated as independent experimental replicates.

## Figure definitions

- `write_throughput_1024`: new SQL deployments only; five trial points,
  arithmetic means and pointwise mean t intervals, in krecords/s, with
  panel-specific scales. SQL completion includes final checkpoint.
- `read_throughput_1024`: new SQL plus the earlier InfluxDB reference, all
  using 1,024 queries. Five trial points, arithmetic means and pointwise
  mean t intervals. Log y axes and panel-specific scales. Different physical
  schemas, adapters and protocols remain.
- `write_storage_gains_1024`: new SQL geometric paired tmpfs/Lustre ratios
  and their log-t intervals. The dashed line marks parity.
- `read_count_context`: new 1,024-query mean throughput divided by the
  earlier 64,000-query mean. Descriptive; no inferential interval. A line
  connecting client counts does not establish a smooth response between them.
- `sqlite_same_node_runtime`: new Shifter/native ratios of means. Same node,
  but different versions and fixed sequential order. Descriptive, not isolated
  container overhead.

## Statistical methods and limits

All 36 rows per run are retained. The six declared warmups are excluded from
estimates. No outliers are removed. Every measured condition has n=5 within
its allocation.

Arithmetic mean intervals:
`mean(x) +/- t(0.975,4) * sample_sd(x) / sqrt(5)`.
Geometric contrast intervals apply the same calculation to five
repetition-matched log contrasts and exponentiate the bounds. All intervals
are pointwise, exploratory and model-based. They assume the repetition
blocks adequately capture within-allocation variation. Dependence or drift
can invalidate nominal coverage. No multiple-comparison correction or
global hypothesis test is claimed. These intervals do not quantify variation
across independent allocations or nodes.

Means of per-trial rates are not total work divided by total time. Ratios of
arithmetic means and geometric paired ratios are distinct estimands and have
separate columns. Percentile metrics are already trial-level percentiles;
their across-trial median is not the p99 of pooled requests.

### What was validated

For the new input, ZIP CRC and all 280 listed member hashes passed. The 216
full JSON trials reconcile with configs/plans/CSV, including complete factor
coverage, job/node/image identity, counts, worker allocation, finite timings,
rate identities, CPU sums and profiler-off status. Mean/p50/p95/p99 values
were recomputed from all 437,184 saved samples, including warmups. Actual
bundled source bytes match all six manifests. Logs contain 108 PASS lines
and the completion marker for each job.

The earlier input has shallower evidence: no full raw request samples or
historical source bytes. Its configs/plans/CSV/selected metadata were checked
for consistency. Neither validation reopens the database or rechecks every
returned payload; the benchmark's implemented count and ordered-key checks
are reflected in its saved successful results.

### What remains uncontrolled

- Native SQLite is Python 3.11.7 / SQLite 3.44.2; Shifter is Python 3.11.15 /
  SQLite 3.46.1. Same-node placement removes node-identity differences in the
  new runtime comparison, but not software-version or fixed-order effects.
- SQL write completion includes the final checkpoint. InfluxDB completes at
  HTTP acknowledgement, without an explicit final checkpoint in that timing.
  Durability settings are not matched; tmpfs is volatile. Batch latency is
  not per-record latency or identical cross-engine transaction semantics.
- Read phases start at parent event release and end at the last worker finish.
  Connection/process setup is excluded, but release/wakeup, deterministic
  query preparation, backend calls and ordered-key validation are included.
  A backend-call latency covers only the timed query method.
- All reads are warm after writes and validation. Equal query counts do not
  ensure equal durations, engine work, cache residency or background-server
  state. With 64 clients, each new worker receives only 16 queries.
- New/old SQL configs differ only in query count, but allocations and some
  source hashes also changed. `validation.json` lists the exact file names.
  The observed read-rate differences cannot be assigned solely to duration.
- New SQL worker counters include client processes only for PostgreSQL.
  Per-worker lifetime peak RSS is not a server/process-tree peak. Process I/O
  counters are not storage-device bandwidth or system I/O wait.

## Remaining work

The query-count comparison and its offline analysis are complete. Remaining
research includes mixed workloads, systematic 100k/10M record coverage,
matched runtime builds and explicit durability/completion policies,
independent allocations with execution-order control, hardware counters and
aligned system measurements, and final synthesis/artifact preparation.

If sustained-read claims are required, a focused randomized comparison of
1,024 and 64,000 queries on the same allocation and source/build snapshot
would separate read-count effects more directly. Current results support
short-burst descriptions, and the earlier pilot remains available for the
longer read workload. The follow-up has not invalidated either dataset;
it has clarified their measurement scope.
