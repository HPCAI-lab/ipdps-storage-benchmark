# SQLite lock policy and tmpfs copy-out revision

This is the bounded follow-up to the October 8 harness audit. It adds two native
SQLite experiments. The prior harness, saved results and configurations are not
modified. No changes to the SQLite binary, OS permissions or filesystem policies
are made. This study does not automatically retry failed transactions or jobs.

## Step 1 evidence

The audit on Perlmutter reported native Python 3.11.7 / SQLite 3.44.2 and Shifter
Python 3.11.15 / SQLite 3.46.1. Both showed a 5,000 ms initial adapter timeout and
a 60,000 ms effective write-worker timeout. The mechanism/multi-node helper also
reported 60,000 ms. The inspected source has no Python transaction retry loop.

Both builds omitted HAVE_USLEEP from PRAGMA compile_options, but their verified
sqlite3_sleep(1) calls returned 1 and took about 1.06 ms. This checks the default
VFS sleep API, not the busy-handler callback. It does not establish why the
performance plateau occurs. Internal SQLite busy-handler callback counts remain
unavailable; their fields are null, never fabricated zeros. Python retries are
zero by design and database errors are counted explicitly.

Preparation requires the saved audit directory and verifies its checksums. The
native runtime version/source ID is checked again on the compute node.

## Job A: lock policy

All conditions write the same 1,000,000 deterministic metadata records in batches
of 1,000, with 64 work producers, one shared database file, WAL/NORMAL and the
existing 1,000-page automatic checkpoint policy. There are no range reads,
lookups or updates. This matches the prior mechanism study's insert-only scope.

Seven conditions, each on Lustre and tmpfs:

1. 64 direct writers, busy timeout 1 second.
2. 64 direct writers, busy timeout 5 seconds.
3. 64 direct writers, busy timeout 30 seconds.
4. 64 direct writers, busy timeout 60 seconds (current-setting control).
5. 64 direct writers, busy timeout 120 seconds.
6. 64 producers, bounded queue of 128 batches, one database writer, 60 seconds.
7. 64 direct writers, independent seeded uniform 0–2-second start delays,
   60 seconds. The same worker delays are used on both storage tiers in a block.

One full randomized warmup block and five randomized measured blocks produce
**70 measured attempts and 14 warmups**. Data seeds match across conditions in
a block. Conditions use fresh databases. A new control in the same allocation
is required; ratios to older allocations are not the primary endpoint.

The queue adds one dedicated writer process. Producers own generation; they do
not open database connections. Generation, pickle/IPC, queue backpressure,
writer service and end-marker delivery occur inside the measured write window.
The queue can change commit ordering and cache locality as well as contention.
A gain alone is not proof of unfair waiting as the sole causal mechanism.

Every worker stops after its first surfaced SQLite error. SQLITE_BUSY is a
planned experimental outcome: the surviving workers finish their assigned work,
the database is checkpointed, partial committed keys are checked, and the trial
is retained with status `busy`, success false, and primary throughput null.
There are no hidden application retries or replacements for failed observations.
Other errors, failed checkpoints or correctness failures stop the study and are
preserved as errors. A successful Slurm job means the planned experiment was
collected and validated; it does not mean every short-timeout trial finished all
one million writes. `completion.json` reports both meanings separately.

## Job B: copy-out

Four shapes: 1M and 10M total records, each with:

- one database file written by one client;
- 64 independent database files written by 64 clients, fixed total record count.

Each shape runs on Lustre and tmpfs, one warmup and five measured blocks:
**40 measured trials plus 8 warmups**. Compare tiers within each shape. These
copy-out rows do not isolate partitioning as a single variable, since client
count also differs between the one-file and 64-file cases. The earlier dedicated
partition study provides matched-client partitioning comparisons.

Once all writers have completed, the parent checkpoints every file with
TRUNCATE, sequentially, and requires return values (0,0,0). Workers then remain
idle; their connections and the parent keeper connections prevent last-close
checkpoint work. No transaction is active and all WAL files must be empty.

For tmpfs, copy-out begins immediately after those checks, before validation
reads or checksums. The timer includes destination directory creation, sequential
1 MiB buffered file copies, flush and fsync on each destination file, then fsync
on the destination directory and its parent. Directory fsync EINVAL/ENOTSUP is
reported as unsupported, not silently treated as success; other sync failures
invalidate the trial. No global `sync`, cache dropping, sudo or permission
changes are used. Timings describe completed filesystem API calls, not a
power-failure test of the storage system.

After timing, source keys/payload samples are checked, each source/destination
SHA256 is compared, and each copy is reopened read-only and checked using
quick_check and its row count. Temporary source/copy databases are removed only
after validation. Raw measurements, receipts, hashes and source code remain.

Results expose write time, final checkpoint time, completion without copy,
copy time/bytes, and start-to-copy-completion time including intervening gaps.
Direct Lustre's persistent completion endpoint is its final checkpoint return.
Tmpfs without copy has no persistent-completion rate. Excluding setup, validation
and cleanup is documented consistently. No equal crash-durability guarantee is
implied by the two different completion boundaries.

## Optional counters, within Job B

For one-client Lustre conditions, attempt read-only lctl snapshots after workers
are ready and again after final checkpoint timing. Snapshots cover write plus
checkpoint and background node-client activity, not just a database process.
The commands request llite stats, LDLM namespace/pool stats and lock gauges.
Missing tools, denied access, partial output and truncation are explicit statuses.
If the first attempted snapshot is unavailable, later probes are skipped. No
extra job, permissions request or privileged operation is triggered.

Lock counts are gauges; a gauge difference must not be presented as a cumulative
enqueue count. Counter availability does not itself prove a lock/I/O mechanism.

## Measurement and validation

Process creation, connections and schema setup precede a shared start barrier.
The parent's monotonic start precedes releasing that barrier. Its write endpoint
is the latest worker completion timestamp on the same host. Deliberate stagger
sleep stays inside the window. A separate final checkpoint window and the
coordination gap are retained. Copy-out has separate start/end timestamps.

Raw per-batch evidence includes data preparation, BEGIN IMMEDIATE acquisition,
transaction body, COMMIT, queue wait where applicable, and errors/rollback.
BEGIN elapsed is not a pure kernel lock trace. COMMIT can include automatic
checkpoint work. Producer and database-worker CPU are separate; parent checkpoint
CPU is outside worker CPU accounting. CPU samples end when each worker builds its
completion receipt. Producers may still have queue-feeder work after that CPU
sample; their CPU totals therefore are not complete IPC CPU accounting. The
global wall-time endpoint waits for the writer to consume every end marker, so
transfer/serialization time is included in the primary completion measurement.

All stored integer keys are checked by per-batch count/bounds/sum with a primary
key, and deterministic payloads are spot-checked. Partial busy trials are checked
against exactly the batch IDs whose COMMIT returned successfully. The validator
checks ownership, timing ordering, policy delays, source hashes, CSV/raw agreement,
copy receipts and recomputed summaries. These checks cannot retroactively
re-query temporary databases removed after the on-node checks.

Both studies use one allocation each. Repetitions are within-allocation blocks;
they are not five independently allocated nodes. Warmups are excluded from
analysis. Report failure rates for all timeout conditions, and any performance
summary conditional on successful full-work trials must be labelled accordingly.

## Execution

Run the installer on the existing project checkout. It adds nine files and checks
that the existing mechanism worker has its previously validated hash. Then:

```
.venv/bin/python -m unittest discover -s tests -p 'test_sqlite_lock_revision.py' -v
.venv/bin/python scripts/sqlite_lock_revision.py dry-run
```

Commit the nine new files. Prepare from committed code, supplying the actual
audit output directory:

```
.venv/bin/python scripts/prepare_sqlite_lock_revision.py --audit /absolute/audit/directory --submit
```

The two scripts each request one exclusive CPU node for 60 minutes, account m5289,
regular QoS: two requested node-hours total. This is a walltime reservation, not
a prediction of actual runtime or scheduler wait. Submission records prevent
automatic duplicate submissions for the same commit. Partial submission or budget
rejection is preserved; inspect it before submitting anything else.

Prepared snapshots contain the committed study files, audited generator, audit
outputs, source hashes and resolved Python requirements. The existing venv is
linked, not copied; runtime identity is checked on the compute node. Successful
studies validate and export ZIPs with raw JSON, CSV, configurations, samples,
source, validation receipt, controller log and SHA256SUMS. Home backups are
byte-verified. No new experiment is automatically scheduled afterward.

Primary references:
- https://www.sqlite.org/c3ref/wal_checkpoint_v2.html
- https://www.sqlite.org/howtocorrupt.html
- https://www.sqlite.org/c3ref/busy_timeout.html
- https://www.sqlite.org/c3ref/sleep.html
- https://docs.python.org/3.11/library/multiprocessing.html
- https://docs.python.org/3.11/library/os.html#os.fsync
