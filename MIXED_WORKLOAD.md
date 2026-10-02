# Mixed metadata/telemetry workload

This extension adds one explicit composition to the existing shared workers:
50% metadata records and 50% telemetry records, with writes followed by warm
reads. It is not a simultaneous read/write workload. The professor's outline
names MixedWorkload without specifying a ratio; 50/50 is this implementation's
chosen, documented definition.

## Data and measurement

- `records` is the **combined** record count. A 128,000-record check inserts
  64,000 metadata records and 64,000 telemetry records into the same database.
- Each logical batch of 1,000 records generates 500 metadata and 500 telemetry
  records. It writes metadata first, then telemetry, in **two separate backend
  calls**. Every writer handles both types. The pair is not atomic.
- Metadata IDs and telemetry timestamp indices each run from zero to
  `records / 2 - 1`. Seeded data is independent of worker assignment.
- `read_queries` is the combined query count. Even query IDs target metadata;
  odd IDs target telemetry. Query pairs are distributed round-robin across
  clients so each reader handles both types. Ranges cover `query_window` rows
  of one type. Counts and ordered returned keys are checked for every query.
- The write and read phases remain separate, using the existing ready/start
  synchronization, process metrics and latency sampling.
- SQL completion still includes the final checkpoint. Influx completion still
  ends at synchronous HTTP acknowledgements. These are different boundaries.
- Compared with a pure workload at batch size 1,000, each mixed backend call
  writes 500 records. Both composition and physical batch size therefore differ;
  a mixed/pure throughput ratio does not isolate a composition-only effect.

## Output fields

`logical_write_batches` counts paired batches. `backend_write_calls` and
`write_batches` count the two calls per pair. SQL `transactions` counts the two
SQL transactions; Influx `transactions` is null because HTTP writes are not SQL
transactions. Existing `transaction_latency_*` field names are retained for
compatibility, but for mixed runs their samples measure the **whole logical
pair**, explicitly identified by `write_latency_unit`. Do not pool these
latencies with single-call pilot latencies as if their units were identical.

`stored_metadata_records` and `stored_telemetry_records` come from per-table
validation. `metadata_read_queries`, `telemetry_read_queries` and raw
`read_worker_queries_by_workload` count successfully validated queries.
Successful trials require exact combined and per-table counts and unique keys.
A failed second write call fails the trial; no automatic retry hides partial
work. Failed database directories are retained under existing cleanup rules.

## Validation stage

Four configurations cover SQLite native, SQLite Shifter, PostgreSQL and InfluxDB.
Each checks Lustre/tmpfs at 1, 16 and 64 clients: **24 trials total**, without
warmups. These are integration checks, not repeated performance results.

```bash
.venv-postgres/bin/python -m unittest discover -s tests -p 'test_mixed_workload.py' -v
for config in configs/mixed-*-check.yaml; do
    .venv-postgres/bin/python experiment.py --config "$config" --dry-run
done
sbatch hpc/ipdps_mixed_check.sh
```

The job runs the existing launchers sequentially in one allocation. After each
six-trial run, `scripts/validate_mixed_results.py` validates CSV/raw consistency,
counts, query composition by worker, recomputed latency statistics, timings,
pinned image IDs, source hashes and Influx shutdown codes. Validation failures
stop the job. No performance pilot is submitted automatically.

Expect the final message:

```
MIXED_CHECK_COMPLETE: 24 mixed trials verified across four deployments
```

Also check the Slurm allocation and step exit codes. Logs are under
`results/mixed-check-JOBID/`; each result directory receives
`mixed-validation.json` only after validation succeeds. Do not edit benchmark
sources while the job runs: the validator intentionally rejects source drift.

## Local verification scope

The installer was checked against the exported source snapshot underlying
commit 63c227b; the later analysis-only commit 9dab2a5 did not change those
benchmark sources. Local tests exercise deterministic data, per-client mixed
query assignment, rejection of invalid dimensions and wrong per-table counts,
partial-write failure propagation, existing pure workloads, Influx line-protocol
routing, real multiprocess SQLite writes/reads/checkpointing, and a complete
controller CSV/raw/validator round trip with deliberate result corruption.

Local tests do not establish PostgreSQL, InfluxDB or Shifter execution on
Perlmutter. The one integration job above provides that next evidence.
