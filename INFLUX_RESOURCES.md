# Optional InfluxDB lifecycle profiling

The unprofiled InfluxDB pilots were verified in jobs 59188702 (metadata) and
59188703 (telemetry). This addition provides optional resource measurements.
It does not change their write/read timing definitions, database schema, or
supervised SIGINT shutdown script. Existing configurations default to profiling
OFF. Profiling measurements must remain separate from those performance pilots
until instrumentation overhead has been evaluated for InfluxDB.

## What is measured

`server_profiling: true` wraps the Shifter launch with the existing wait4
accounting supervisor and samples its process tree every 0.2 seconds.

- CPU seconds and kernel resource counters cover the waited-for server launch
  lifecycle, including Shifter, the shell supervisor and its reaped children,
  InfluxDB startup/setup, workload service and shutdown. Python benchmark
  clients and the Python accounting supervisor are excluded from CPU/I/O totals.
- Descendant accounting depends on parents reaping children. It does not imply
  that arbitrary detached or reparented processes are captured.
- PSS is the sampled sum over the launch tree, including the Python accounting
  supervisor. Scans are non-atomic. Incomplete scans are retained in the raw
  JSONL but excluded from the reported peak. Brief peaks may be missed.
- `server_lifecycle_max_process_rss_kib` is a per-process high-water statistic;
  it is not a simultaneous aggregate tree peak. Do not sum it with PSS.
- Kernel input/output block counts are neither logical database bytes nor
  measured device bandwidth, and they are not I/O wait measurements.
- These lifecycle totals are not isolated write-phase or read-phase costs.
  They cannot be compared directly with phase-only worker measurements.

`server_lifecycle_sampled_max_influxd_processes` counts observed processes, not
Go threads. The profiler only reports `lifecycle_collected` if a complete sample
observes `influxd`, accounting is valid, and no sampler error occurred. The
InfluxDB coordinator additionally requires accounting exit code zero. A missing
or incomplete resource collection fails the profiling trial and preserves data.
PostgreSQL remains the default target of ServerProfiler; its existing process
field and scope labels are retained.

Raw accounting and memory filenames are saved in each trial JSON. CSV includes
the shared lifecycle metrics and the InfluxDB process-count field. Scope text
and sampler errors remain in JSON. Performance trials retain status
`not_collected` when server profiling is disabled.

## Resource check

The two resource configurations use 1,000,000 records, batches of 1,000, 1,024
warm range queries of 100 rows, clients 1/64, and Lustre/tmpfs. There is one
measured trial per condition and no discarded warmup: four per workload, eight
in total. This is instrumentation validation and exploratory resource data,
not a repeated statistical comparison.

```bash
.venv-postgres/bin/python -m unittest discover -s tests -p 'test_influx*.py' -v
.venv-postgres/bin/python experiment.py --config configs/metadata-influx-resources.yaml --dry-run
.venv-postgres/bin/python experiment.py --config configs/telemetry-influx-resources.yaml --dry-run
sbatch hpc/ipdps_influx.sh configs/metadata-influx-resources.yaml configs/telemetry-influx-resources.yaml
```

Local tests use small Linux subprocesses named `postgres` and `influxd` and a
fake executable to exercise signal handling. These are accounting and
supervision tests; they do not substitute for the real Shifter resource check.
The unchanged adapter tests also run. Local compatibility checks verified that
previous pilot plans and PostgreSQL paired-overhead plans are unchanged.

## Analysis use

Storage and concurrency ratios can already be computed from the unprofiled
pilots. This profiling addition supplies supporting resource observations;
CPU totals alone cannot establish that a workload is CPU-bound, and the
measurements cannot establish an isolated storage-device effect. The next
analysis should distinguish throughput gains from tail-latency changes and
retain each database's completion, query-count and deployment boundaries.
