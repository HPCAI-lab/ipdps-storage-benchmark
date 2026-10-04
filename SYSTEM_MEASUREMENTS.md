# Bounded diagnostic system-measurement study

One exclusive CPU-node allocation runs 48 single-trial conditions: metadata,
telemetry and mixed workloads; native SQLite, Shifter SQLite, PostgreSQL and
InfluxDB; Lustre and tmpfs; 1 and 64 clients. Each trial uses one million total
records, 1,024 queries and a 100-row query window. One repetition, no warmup.
This is supplemental instrumentation, not a replacement for repeated pilots.

The job first probes node-wide perf event permissions and availability in the
same allocation. Cycles and instructions are requested as a group for IPC.
Cache misses and context switches are requested if available. Unsupported or
permission-denied events are recorded, not assigned zero or bypassed. A combined
probe checks that the selected event set can run together. Raw perf CSV files
retain event running percentages. Multiplexing and partial coverage must be
reported when interpreting counts. No automatic benchmark retries occur.

Measurements span the entire controller trial lifecycle, including startup,
write, validation, read, shutdown and cleanup. They are not per-phase counters.
Node-wide perf includes operating-system activity and monitoring overhead.
/proc/stat supplies node busy, idle and I/O-wait fractions; negative deltas are
marked invalid. Guest CPU time is not double-counted. /proc/meminfo supplies
sampled node used memory (MemTotal minus MemAvailable), not database PSS.
iostat is captured if installed; its block-device data does not directly measure
network Lustre traffic. I/O-wait is a node-level indicator, not a causal database
latency or filesystem bandwidth measurement. Sampling interval is 0.5 seconds;
short-lived peaks may be missed.

GNU time -v is used if present. Its maximum RSS is not a process-tree memory
peak; child CPU accounting depends on waiting/reaping and privilege boundaries.
Existing worker metrics remain in the benchmark CSV/JSON. Optional server
profiling is disabled for this supplemental study; previously collected server
PSS and lifecycle CPU studies remain separate evidence.

Throughput from these instrumented single observations must not be pooled into
unprofiled performance pilots. Runtime versions and write completion semantics
are unchanged, including the native/Shifter SQLite version mismatch and SQL
checkpoint versus Influx HTTP acknowledgment boundaries.

Execution requires the committed source snapshot and the existing virtual
environments. Preserve both environments while running. Results are beneath
snapshot/results/system-metrics-JOBID: capabilities.json, raw counter probes,
summary.csv, per-condition metrics and monitor logs, and validated benchmark
runs. Completion is signalled by SYSTEM_MEASUREMENTS_COMPLETE and completion.json.
Hardware-counter availability is explicit; collection success does not mean all
requested counters were permitted. This study does not complete the unexecuted
100K/10M repeated sweeps or establish isolated container overhead.
