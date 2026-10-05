# Fixed SQLite multi-node partitioned study

This extension uses the unchanged, validated `sqlite_mechanism_study.py`
worker from commit `65b103b`. It coordinates separate SQLite files across
nodes; no file is accessed from two hosts during a trial.

## Fixed scope and budget

- One four-node regular CPU allocation, account m5289, maximum 30 minutes.
- Requested budget: **2 CPU node-hours**. This is a request ceiling, not an
  execution-time prediction or a guarantee of the current allocation balance.
- 10,000,000 **total** records, 64 **total** writers and database files.
- 1, 2 or 4 active nodes: respectively 64, 32 or 16 writers per node.
- Lustre and node-local tmpfs; partitioned layout only; metadata inserts only.
- One warmup block plus five randomized measured blocks: **36 trials total,
  30 measured and 6 warmups**. No read queries, extra size matrix or shared-file
  cross-node experiment is added.
- The full allocation stays reserved during the 1-node and 2-node conditions.
  Inactive nodes do not run database workers. Their reserved time still counts
  toward allocation use. Keeping all conditions in one allocation controls
  the available machine pool; it does not provide independent allocation repeats.
- Physical host order is deterministically shuffled per block; each node-count
  condition uses the corresponding subset. Both storage tiers use the same
  subset within a block. Node membership is recorded and validated.

This is fixed-work, fixed-writer/fixed-shard **node-distribution scaling**.
It does not measure increasing total writers, a shared distributed database,
distributed SQL query execution, or automatic failover.

## Implementation and unchanged workload

The SQLite schema, row generator, worker ownership, batch size (1,000), explicit
BEGIN IMMEDIATE transactions and per-batch timing functions are reused without
editing the original worker. Global worker i always owns batch IDs
`i, i+64, i+128, ...` and file `worker-i.db`, regardless of node count.
Node controller rank r owns global workers `r, r+N, ...`, where N is active nodes.

The policy remains WAL, synchronous=NORMAL, 1,000-page automatic checkpoint,
and a 60-second busy timeout. Total data and the seed within each block are
identical across placements and tiers. The larger 10M case reduces the risk
that the partitioned workload consists mostly of a very short burst; actual
phase durations must still be inspected.

Slurm launches one Python controller per active node. Controllers spawn the
existing local workers. Coordination uses framed JSON over a job-specific TCP
connection, authenticated by a random token in a private mode-0600 control
file. The token file is removed after the trial and is not part of the export.
No MPI installation or new Python package is needed. This still requires
compute-node TCP reachability; that cannot be certified by local testing.

All clients of each WAL database stay on its owning host. Lustre paths are
unique to each node and trial; each node independently verifies its mount.
tmpfs is node-local and volatile. Both placements retain the established
correctness checks; observed benchmark success is not a general certification
of every network-filesystem/WAL deployment.

## What the timings mean

1. Every node initializes its own shards and waits for all local workers to
   report ready. The coordinator waits for every node's ready message.
2. The coordinator starts its monotonic clock and sends start messages. Each
   node releases its local worker event. The start-message fanout time is saved.
3. The write phase ends when the coordinator receives the final node's
   write-completed message.
4. Only after **all** nodes finish writing does the coordinator release final
   checkpoints. Each node checkpoints its shards sequentially; different nodes
   checkpoint concurrently. Completion ends at the final checkpoint message.
5. Batch samples are collected, databases are validated and cleaned, and node
   JSON evidence is written to shared results storage after these timed phases.

Global throughput is total records divided by one coordinator elapsed duration.
It is **not** the sum or mean of local throughputs. The global duration includes
TCP control latency, release skew and node-controller notification overhead.
Local worker timestamps are used only within their own host. They are never
subtracted from a different host's clock.

Worker setup, process launch, correctness validation, sample output and cleanup
are outside completion throughput and are represented in lifecycle evidence.
Data generation and final checkpoints are inside the relevant timed phases.
CPU/core-equivalent measures include Python generation and other worker work.
BEGIN elapsed remains acquisition overhead plus waiting/scheduling, not a pure
kernel lock-wait trace.

The coordinator is on the batch node, which may or may not be an active database
node for a subset condition. Its overhead is included in the global scope.
CPU affinity is recorded per node; each selected node controller receives its
full CPU-node allocation, and workers inherit that affinity without per-worker
pinning, consistent with the existing mechanism harness.

**Checkpoint policy is a material part of the experiment.** With 64 fixed
files, one active node checkpoints 64 sequentially; four active nodes each
checkpoint 16, concurrently across nodes. Report write-only and completion
throughput separately. Do not attribute the full completion speedup solely
to additional CPU or storage bandwidth.

This protocol has fresh 1-node controls in the same allocation. Do not pool
its global notification timings with the earlier local last-commit timings.
No final unified database is produced. If a later application needs a merge
or durable export of tmpfs contents, that cost is outside this study and would
need its own explicit timing boundary.

## Installation and execution

The supplied installer adds eight new files and refuses to overwrite different
existing content. It checks the exact SHA256 of the existing worker. It does
not modify previous configurations, source, results or Git settings.

From the repository, using the existing Python environment:

```bash
.venv/bin/python -m unittest discover -s tests -p 'test_sqlite_multinode.py' -v
.venv/bin/python scripts/sqlite_multinode.py dry-run
```

The tests run only tiny local simulations, clearly labeled `local_test`.
They do not submit jobs or claim network/node scaling evidence.

Commit and push the eight new files, then prepare and submit once:

```bash
.venv/bin/python scripts/prepare_sqlite_multinode.py --submit
```

Preparation requires a clean working tree and freezes every required source
file from the commit with hashes. The existing `.venv` is linked, not copied;
installed requirements and per-node runtime versions are retained. Keep that
environment unchanged while the job is queued/running.

A submission record keyed by source commit prevents accidental duplicate
submission. Rejected or uncertain submissions are recorded and never retried
automatically. A failed trial stops the study; earlier evidence is retained.

The Slurm output is `ipdps-sqlite-multinode-JOBID.out` in the submission directory.
It prints `RESULTS=`, per-trial progress, `EXPORT=`, `BACKUP=` and the final
`SQLITE_MULTINODE_COMPLETE` marker. Check Slurm exit status as well as that marker.
No additional calibration or follow-up job is automatically submitted.

## Results and validation

- `results.csv`: 36 global trial summaries, with node count, total work, phase,
  seed, throughput, CPU, batch latency summaries and raw-file references.
- `trial-NNNN.json`: global clocks, node host placement, checkpoint policy,
  metrics and cryptographic references to every node's evidence.
- `trial-NNNN-node-RR.json`: each node's environment/mount, worker samples,
  local phase boundaries, settings, checkpoint status and correctness evidence.
- Step logs, config, plan, environment, source hashes, source commit, frozen
  source files, completion status, validation receipt and SHA256SUMS.

The validator recomputes raw batch summaries and checks exact worker/batch
coverage, stored row count/bounds/sums, ownership and recorded payload samples,
node uniqueness, source hashes, clock domains, checkpoint completion and CSV
agreement. Local simulations are rejected by default.

After success, the job automatically verifies a results ZIP and copies it to
`$HOME/ipdps-backups`. It never includes live databases: validated data files
are removed. Failed tmpfs files can disappear when the allocation ends; raw
failure reports are written to the shared results directory where possible.

Interpretation should use paired within-block effects and retain a clear
one-allocation limitation. Multi-node speedup is an experimental question,
not a promised outcome.

Primary interface references checked for this implementation:
- https://slurm.schedmd.com/srun.html
- https://docs.nersc.gov/systems/perlmutter/running-jobs/
- https://www.sqlite.org/fileformat.html#wal_index_format
