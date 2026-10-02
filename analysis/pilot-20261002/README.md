# Perlmutter pilot analysis, 2 October 2026

This package analyzes the eight completed baseline runs exported in
`ipdps-pilot-analysis.zip`. It does not submit benchmark jobs or modify the
benchmark repository. All 288 saved rows are retained, with the 48 declared
warmups excluded from the estimates. The analysis covers 240 measured trials
in 48 conditions, each with five repetitions within a single allocation.

## Files

- `pilot-analysis-report.pdf`: six-page findings, figures, scope and methods.
- `tables/`: flat, full-precision CSVs for all observations and derived results.
- `figures/`: six figures, each in PNG and vector PDF form.
- `validation.json`: checks, limits, input SHA-256 and analysis library versions.
- `scripts/analyze_pilots.py`: validation and numerical analysis.
- `scripts/build_report.py`: the same analysis plus figures and the report.
- `input/ipdps-pilot-analysis.zip`: the original upload, unchanged.
- `requirements-analysis.txt`: versions used to generate the results.
- `SHA256SUMS`: checksums for all other files in the package.

Figure captions: `write_throughput` and `read_throughput` show individual
trials, arithmetic means and pointwise 95% mean t intervals, with panel-specific
scales. Write completion is checkpoint-inclusive for SQL and HTTP-acknowledged
for InfluxDB. Reads are warm and use different query counts across SQL/InfluxDB.
`storage_gains` shows geometric paired ratios and log-t intervals; its y axes
are logarithmic. `sqlite_runtime_ratios` shows ratios of arithmetic means with
no inference interval because environments and allocations differ.
`write_tail_latency` shows median trial p99s with min-to-max whiskers, not
confidence intervals, on logarithmic axes. `influx_read_variability` shows all
five measured repetitions for the selected condition; connecting lines indicate
repetition order, not an estimated trend. The report supplies context for all
cross-engine panels; the figures alone do not support a matched engine ranking.

## Reproduce

Use a separate Python environment with the packages in
`requirements-analysis.txt`. This is an offline analysis of small saved data,
not a database experiment. From this directory:

```bash
python scripts/build_report.py input/ipdps-pilot-analysis.zip --output regenerated
```

For tables and checks without plots or the report:

```bash
python scripts/analyze_pilots.py input/ipdps-pilot-analysis.zip --output regenerated
```

Report text describes this fixed pilot snapshot. The validator intentionally
checks the eight expected runs, job IDs, record counts, query counts and image
IDs. Extending the study requires explicit changes to those expectations and
to the report narrative; this is not a generic analyzer for arbitrary runs.
PDF metadata can change on regeneration, but numerical CSV outputs are
deterministic for the same input and dependencies.

## Table definitions

`all_trials.csv` preserves the union of the original CSV columns, with the
exported run metadata added. A blank is missing or not applicable, not zero.
Columns originating in different schemas can therefore be blank in earlier
runs. `phase` distinguishes measured observations from warmups.

`condition_summary.csv` is long-form: one row per condition and metric. It
contains n, arithmetic mean, sample SD (n-1 divisor), median, range, sample CV
in percent, and a pointwise 95% t interval for the mean. A zero-mean metric has
blank CV. The p99 metric is already a per-trial percentile; its median is a
median of trial p99s, not a percentile of pooled requests. No individual
request samples were supplied, so pooled latency distributions are unavailable.

`storage_gains.csv` reports both the ratio of arithmetic mean throughput
and the geometric mean of five repetition-matched tmpfs/Lustre ratios.
The log-t interval belongs to the geometric estimate. `storage_sensitivity`
is the observed `(best mean - worst mean) / worst mean` across the two tiers,
reported as a fraction. It is descriptive and does not identify an optimal
tier with statistical certainty.

`concurrency_scaling.csv` reports throughput at 16 or 64 clients divided by
throughput at one client, holding deployment, workload and storage fixed.
Work is fixed at one million records and a fixed query count per engine,
so this measures scaling at fixed total work, not weak scaling. The geometric
estimate and interval use matching repetition blocks.

`storage_concurrency_interactions.csv` reports
`I(C) = [T(tmpfs,C)/T(lustre,C)] / [T(tmpfs,1)/T(lustre,1)]` for C=16 and 64.
I<1 means the relative tmpfs throughput benefit shrinks as concurrency rises.
The geometric estimate uses five paired log contrasts. This is a specific
exploratory interaction contrast, not an omnibus factorial test. Read and
write phases are kept separate.

`sqlite_runtime_comparison.csv` reports
`GR = mean(Tnative)/mean(Tshifter)` and
`RS = (mean(Tnative)-mean(Tshifter))/mean(Tnative)`.
RS is a fraction: a positive value means lower Shifter throughput. These
runtime comparisons are descriptive only; no intervals for isolated container
effects are asserted.

`repetition_ratios.csv` preserves every block ratio behind the storage,
concurrency and interaction calculations. `run_inventory.csv` links runs to
jobs, nodes, engine versions and query counts.

## Statistical method and limits

For five values x, the arithmetic-mean interval is
`mean(x) +/- t(0.975,4) * sample_sd(x) / sqrt(5)`.
For block log-ratios z, the geometric estimate is `exp(mean(z))` and the
interval is the exponentiation of that same t formula applied to z.
All intervals are pointwise and model-based, conditional on the observed
allocation and assuming the five blocks adequately represent its variation.
Serial correlation or drift can invalidate that assumption. No multiple-test
correction, no outlier removal and no global significance claim are made.
Replicates within an allocation do not estimate between-allocation variance.

Arithmetic mean throughput is a mean of per-trial rates, not total operations
divided by total duration. Ratio-of-means and geometric paired-ratio estimates
are intentionally separate fields, because they are different estimands.

SQLite uses native Python 3.11.7 / SQLite 3.44.2 and Shifter Python 3.11.15 /
SQLite 3.46.1 in different allocations. PostgreSQL 16.12 and InfluxDB 2.9.1 use
native clients with Shifter servers. These are four deployment configurations,
not a fully crossed database-by-runtime experiment. Stored source manifests
show matched core SQLite worker and backend hashes between native and Shifter,
but a changed controller. Actual source bytes are not in the export.

SQL write completion includes the final checkpoint. InfluxDB completes at
HTTP acknowledgement with no explicit final checkpoint in that time. Batch
latencies are not per-record latencies or equivalent transaction semantics.
SQLite's recorded WAL/NORMAL settings and the different server engines do not
establish matched durability. tmpfs is a memory-backed experimental tier;
these results do not establish persistent-storage durability.

SQL reads use 64,000 queries per trial, while InfluxDB uses 1,024. All queries
return 100 rows; reads follow writes and validation and are warm. Different
query counts, protocols, physical schemas and server behavior limit direct
cross-engine rankings. Metadata here is the concurrent pilot's synthetic
workload, not the earlier XYZ end-to-end ingestion benchmark.

Worker CPU and memory summaries remain available, but PostgreSQL/InfluxDB
worker metrics cover clients only. Max worker RSS is not total process-tree
peak memory. This archive does not contain the separate server profiling
runs; their lifecycle CPU/PSS results are not merged into these estimates.
Kernel/process I/O counters are not device bandwidth or I/O-wait measurements.

Validation checks consistency of saved counts, rates, phases and metadata.
It does not reopen databases, recheck original raw request samples or verify
current remote source bytes. Source manifests and their differences are
retained in `validation.json`.

## Next steps

1. Preserve this baseline analysis in the project repository.
2. Plan independent allocation repeats with explicit blocking and affinity.
3. Match query count and selection protocol, or study duration sensitivity,
   before formal cross-engine read comparisons.
4. Align and document write boundaries and durability; match Python/SQLite
   versions for a controlled native/Shifter comparison.
5. Add mixed workloads and workload sizes. Fit and evaluate placement rules
   using held-out allocations rather than splitting repetitions from one job
   across train and test sets.

The findings are preliminary; the remaining experiments address specific
inference gaps rather than repeating already completed integration checks.
