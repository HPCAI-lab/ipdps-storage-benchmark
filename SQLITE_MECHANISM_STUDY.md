# Fixed SQLite size and partitioning study

This is a NEW native SQLite metadata-write harness. It preserves the existing
benchmark files and outputs. No reads, PostgreSQL, InfluxDB, Shifter, retry/spin
variants, syscall tracing or multi-node database accesses are in this campaign.
It does not submit any job by itself or automatically rerun failed conditions.

## Registered questions, before results

1. Does the tmpfs/Lustre write-throughput ratio depend on total record count
   (100K, 1M, 10M), particularly at 64 clients? The suggested prediction is
   Lustre faster at 100K/64. This is a hypothesis, not a promised outcome.
2. At 1M total records, does a separate SQLite file per client reduce lock
   acquisition elapsed time and improve throughput relative to a shared file?
   Does this change the tmpfs/Lustre ratio at high concurrency?

No quantitative prediction/model coefficients were supplied. Preserve this
document and its source hash before submission. Do not describe a model fit
using these results as held-out predictive validation. Report estimates and
uncertainty even when the proposed direction is not observed.

## Fixed design and stopping point

| Suite | Conditions | Repetitions | Total |
|---|---|---|---|
| size-sweep | 3 sizes x 2 tiers x 1/64 clients x shared | 1 warmup + 5 measured | 72 |
| partitioned | 1M x 2 tiers x 1/2/4/8/16/32/64 clients x shared/partitioned | 1 warmup + 5 measured | 168 |

Total: **200 measured + 40 warmups = 240 trials**, in **two single-node jobs**.
Each block contains each condition once in a seeded random order. All warmups
precede measured blocks. Data varies by block and is identical across sizes
(prefix), tiers, layouts and client counts within the corresponding block.
The one-client layout comparison is a useful internal consistency control.
The suites each include their own shared 1M baseline because they may run in
different allocations. Within-suite comparisons are the primary comparisons.

Each trial is fresh: a private directory, new database(s), initialized schema,
fresh workers, synchronized start, explicit checkpoint, record validation,
then cleanup. Condition repetitions within one job are NOT independent-node
replicates. Concurrent submission is two separate single-node experiments,
not a distributed multi-node database experiment. No automatic repetitions
beyond the fixed plan; reconsider any failures explicitly.

## Matched work and schema

Scientific metadata rows and generation match the earlier MetadataBatchWorkload:
record_id, timestep=id//64, variable, location=node_(id%64), Gaussian value;
an index on (variable,timestep). Inserts only: no added lookups or updates.
The unused legacy coordinates/telemetry tables are omitted in this harness.
Each batch contains at most 1,000 rows. Batch b is assigned to worker b%clients.
Partitioning does not multiply the total record count: a worker writes exactly
its assigned records to its own file. This models data that can be sharded;
it does not preserve cross-shard transaction/query semantics or measure merging.

WAL, synchronous=NORMAL, default SQLite busy handler with busy_timeout=60000ms,
wal_autocheckpoint=1000 pages. No user retry loop and no silent batch retries.
PRAGMA settings, Python/SQLite versions and compile options are recorded.
Lustre/tmpfs must be verified with findmnt on the compute node. All accesses
to each WAL database originate on that one node. Record mount options in the
results; this study does not establish WAL support for arbitrary shared-FS
or multi-host configurations. tmpfs is volatile and is not durability-equivalent
to Lustre following node failure.

## Timers: accurate names and interpretation

The prior adapter used implicit transactions. This harness uses explicit
BEGIN IMMEDIATE on BOTH layouts, so interpret its shared mode as the internal
baseline; do not pool it with old results as if transaction paths were identical.

Per batch, monotonic nanosecond clocks measure:

* prepare_ms: Python row generation, including construction of the seeded RNG.
* lock_acquire_ms: elapsed BEGIN IMMEDIATE call. It includes lock acquisition,
  SQLite/SQL overhead, busy-handler waits and descheduling. It is NOT pure
  sleep time and is not a kernel-level lock trace.
* transaction_body_ms: after BEGIN returns through executemany returning.
* commit_ms: elapsed COMMIT call, including any automatic checkpoint work.
* acquired_to_commit_return_ms: transaction_body_ms + commit_ms. A lock-hold
  proxy/upper interval, NOT exact WAL write-lock ownership time: SQLite may
  release that lock before COMMIT returns, for example during auto-checkpointing.
* batch_ms: complete preparation-through-commit interval (includes small Python
  and timer overheads). It differs from the old adapter's transaction latency.

Every raw per-batch sample is retained. Mean/p50/p95/p99 and sums are recomputed
by the validator. These counters are always enabled in both modes; there is no
instrumentation-overhead estimate, and close performance differences require
caution. Summed worker-seconds can overlap and MUST NOT be stacked as trial
wall time or interpreted as a wall-time fraction.

write_wall_s runs from the parent's synchronized release to the last worker's
final COMMIT return. Startup, schema creation and record validation are outside
this interval. Preparation is inside it. CPU counters cover each worker's
write loop, so core equivalents do not measure parallel writers inside the DB.

Keepers remain open throughout. Parent checkpoints every file with TRUNCATE,
sequentially after workers commit. completion_wall_s runs from release through
the last checkpoint, including a separately reported coordination_gap_s.
This includes checkpoint costs on ALL shards; report write-only and completion
throughput together. Large worker-sample transfer is delayed until checkpoints
finish. Checkpoints finish before correctness scans (which otherwise warm data).

These observations address WRITE bursts only. There are no read-query timings.
Preparation and acquisition/body/commit separation add evidence, but partitioning
also changes indexes per file, cache organization and checkpoint behavior. Do
not attribute every speed difference exclusively to the write lock.

## Correctness and provenance

Each database is checked for full primary-key count, bounds and sum; shard key
ownership is checked for every row. Deterministic payload samples are checked
at the first/middle/last assigned batches, not every payload field of every row.
Global IDs and total stored/committed rows must reconcile. Failed trials stop
the job and keep diagnostics; data is preserved where storage survives the
allocation (failed tmpfs contents are not a durable backup).

The offline validator checks the full plan, CSV/raw agreement, per-worker batch
ownership, phase clocks, checkpoint results, per-batch timing recomputation,
stored correctness evidence and source hashes. Each run saves config, plan,
environment, exact new source files, SOURCE_COMMIT, source_hashes, raw JSON,
results.csv, completion.json, validation.json and SHA256SUMS. Local smoke data
is explicitly labeled local_test and rejected as production evidence.

## Install, validate, commit, freeze (login node)

The installer creates these files only; it refuses to overwrite existing files.
Use the existing native SQLite environment; no dependency installation is needed.

```bash
.venv/bin/python -m unittest discover -s tests -p 'test_sqlite_mechanism.py' -v
.venv/bin/python scripts/sqlite_mechanism_study.py --config configs/sqlite-mechanism/size-sweep.json --dry-run
.venv/bin/python scripts/sqlite_mechanism_study.py --config configs/sqlite-mechanism/partitioned.json --dry-run
```

The tests use tiny synthetic local data only. Commit/push the nine new files,
then, from a clean repository:

```bash
.venv/bin/python scripts/prepare_sqlite_mechanism.py
```

Save the printed SNAPSHOT path. Source is frozen using committed Git bytes.
The existing venv is linked, not a frozen interpreter binary; its resolved
packages are recorded and the compute-node interpreter/runtime is captured.

## Budget before submission

Two regular CPU allocations, 1 node each; defaults request 90 minutes EACH:
**3 requested node-hours total**. This is a conservative budget request, not a
measured runtime promise. Check the current project and user CPU balances in
NERSC Iris before submission. The old reported balance is not current evidence.
Queue waiting is separate. The prior all-engine 72-node-hour array is NOT used.
Default time limits can be changed at submission after an explicit decision;
do not silently shrink them merely to pass balance checks.

After the budget check, submit exactly once per suite from the main project
directory. Replace the example snapshot variable with the printed path.

```bash
snapshot='/absolute/path/printed/as/SNAPSHOT'
sbatch "$snapshot/hpc/run_size_sweep.slurm" "$snapshot"
sbatch "$snapshot/hpc/run_sqlite_partition.slurm" "$snapshot"
```

No job arrays or automatic reruns. Each job runs all its conditions sequentially
on its allocated node. The launcher invokes the offline validator before
printing SQLITE_MECHANISM_COMPLETE. Final acceptance requires Slurm COMPLETED
0:0 and the corresponding 72/168-trial validation receipt. Preserve partial
results on failure and decide explicitly whether any rerun is warranted.

## Analysis after successful completion

Use measured rows only. Report per-condition distributions; tmpfs/Lustre ratios
by size/concurrency; partitioned/shared ratios by tier/concurrency; acquisition,
body and commit latency distributions; throughput with and without final
checkpoints. Pair contrasts by data-seed/repetition block within each suite.
State uncertainty is across blocks within one allocation per suite. Keep the
old pilot as context. Archive results and publish reproducible analysis; do not
start another campaign automatically.

SQLite transaction and WAL semantics:
https://www.sqlite.org/lang_transaction.html
https://www.sqlite.org/wal.html
NERSC accounting: https://docs.nersc.gov/jobs/policy/
