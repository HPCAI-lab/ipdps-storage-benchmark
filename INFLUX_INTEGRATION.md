# InfluxDB concurrent pilot integration

Status: lifecycle validation passed in job 59187195; all 12 concurrent
integration trials passed in job 59187722. Repeated unprofiled pilots passed
in jobs 59188702 and 59188703. Optional server profiling has local accounting
and supervision tests; its real Shifter validation is a separate next step.

## Execution and scope

Native Python clients use the existing synchronized write/read workers. Each
trial starts a fresh Shifter InfluxDB 2.9.1 server, bound to loopback, using image
`db0bdab1e5ad5ee899c127b8c13d9c986a3ced78cd9200dd80a1064bf1533b6e`.
Each worker owns a persistent HTTP connection. Writes require HTTP 204; no
asynchronous buffer or automatic write retry is used. Partial-write HTTP errors
fail the trial. The existing workers generate the same deterministic batches.

Storage is verified on the host and inside Shifter. Database files use a private
trial directory on the chosen tier. Supervisor control files use a separate
private tmpfs directory. Tokens are generated per trial, kept out of saved
configuration and command-line arguments, and passed only to worker processes.
Server shutdown uses the validated supervised SIGINT path and requires launcher
and child exit codes zero. Failed data directories are preserved; successful
ones are removed only after shutdown.

## Data model and correctness

Metadata stores one point per record. `variable` and `location` are tags;
`record_id` and `timestep` are integer fields; `value` is a float. A synthetic UTC
timestamp is 2024-01-01 plus record_id * 100 ms, supporting the same contiguous
record-ID range as the SQL workloads. This is a database representation of the
synthetic concurrent metadata workload, not the original XYZ ingestion task.

Telemetry stores `metric` as a tag, `value` as a float field and the original
payload tags as a JSON string field. Original 100-ms UTC timestamps are retained.
This preserves the fields returned by the SQL adapters; it is not a benchmark
of an independently optimized InfluxDB schema.

The Flux adapter pivots fields into rows before combining series and sorting by
time. Range queries return the same tuple structure used by existing workers.
Full validation counts the `value` field once per point and separately checks
unique timestamps. Workers validate each query's row count and ordered keys.
These checks do not claim a full bitwise audit of every generated payload.

## Measurement boundaries

- `workload_wall_s` and `completion_wall_s` measure the same synchronized write
  phase through the final acknowledged batch. They exclude initialization,
  validation, reads, and shutdown.
- `acknowledged_records` and `write_batches` are the preferred Influx names.
  Legacy CSV fields `committed_records` and `transactions` alias these counts;
  they do not establish SQL transaction semantics or batch atomicity.
- Legacy `transaction_latency_*` fields measure synchronous HTTP batch calls,
  including line-protocol encoding, transport, and response handling.
- `final_checkpoint_s` is null in JSON and empty in CSV. No explicit checkpoint
  API is invoked, so Influx completion times must not be labeled
  "write + final checkpoint" in cross-database figures.
- WAL fsync delay is explicitly 0s. Flush-on-shutdown is enabled; shutdown/flush
  runs after reads and is reported separately as `server_shutdown_s`.
- Reads are warm after acknowledged writes and validation. They include HTTP,
  Flux processing, CSV parsing and Python row construction.
- Existing CPU/memory/I/O worker fields describe client processes only.
  Optional server lifecycle profiling is documented in INFLUX_RESOURCES.md.

These settings do not make InfluxDB, SQLite WAL/NORMAL, and PostgreSQL durability
policies equivalent. Treat initial results as deployment/workload comparisons;
resolve durability and timing boundaries before publication comparisons.

## Check configuration

Each workload checks 128,000 records in batches of 1,000 and 128 range queries
of 100 rows, across Lustre/tmpfs and 1/16/64 clients: six trials per workload.
These are integration checks, with one repetition and no warmup. Run:

```bash
.venv-postgres/bin/python -m unittest discover -s tests -p test_influx_adapter.py -v
.venv-postgres/bin/python experiment.py --config configs/metadata-influx-check.yaml --dry-run
.venv-postgres/bin/python experiment.py --config configs/telemetry-influx-check.yaml --dry-run
sbatch hpc/ipdps_influx.sh
```

No additional Python dependencies are needed. The job uses `.venv-postgres`
because it already contains the controller's dependencies. A successful job must
finish both six-trial configurations and print `INFLUX_RUN_COMPLETE`. Inspect
raw JSON/CSV counts, version, storage and shutdown codes before committing the
integration or expanding to repeated pilots.

## Primary references

- https://docs.influxdata.com/influxdb/v2/write-data/developer-tools/api/
- https://docs.influxdata.com/influxdb/v2/write-data/best-practices/duplicate-points/
- https://docs.influxdata.com/influxdb/v2/api/setup/
- https://docs.influxdata.com/flux/v0/stdlib/universe/pivot/
- https://github.com/influxdata/influxdb/blob/v2.9.1/kit/signals/context.go
