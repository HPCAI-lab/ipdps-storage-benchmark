"""Linux worker metrics; RSS is a lifetime peak, not phase-allocated memory."""
import os
import resource
from pathlib import Path

COUNTERS = {
    "user_cpu_s": "ru_utime",
    "system_cpu_s": "ru_stime",
    "minor_faults": "ru_minflt",
    "major_faults": "ru_majflt",
    "voluntary_switches": "ru_nvcsw",
    "involuntary_switches": "ru_nivcsw",
}
IO_KEYS = (
    "rchar", "wchar", "syscr", "syscw",
    "read_bytes", "write_bytes", "cancelled_write_bytes",
)
SUMMARY_KEYS = [
    *COUNTERS, "cpu_s", "cpu_core_equivalents", "peak_rss_max_kib",
    "io_available_workers", *["io_" + k for k in IO_KEYS],
]
SUMMARY_FIELDS = [
    f"{phase}_worker_{key}"
    for phase in ("write", "read") for key in SUMMARY_KEYS
]


def process_snapshot():
    io_error = None
    try:
        io = {
            k: int(v.strip())
            for k, v in (
                line.split(":", 1)
                for line in Path("/proc/self/io").read_text().splitlines()
            )
        }
    except (OSError, ValueError) as exc:
        io, io_error = {}, repr(exc)

    usage = resource.getrusage(resource.RUSAGE_SELF)
    return {
        "counters": {k: getattr(usage, attr) for k, attr in COUNTERS.items()},
        "peak_rss_lifetime_kib": usage.ru_maxrss,
        "io": io,
        "io_error": io_error,
    }


def process_delta(before):
    after = process_snapshot()
    values = {
        key: after["counters"][key] - before["counters"][key]
        for key in COUNTERS
    }
    io = {
        key: (
            after["io"][key] - before["io"][key]
            if key in before["io"] and key in after["io"] else None
        )
        for key in IO_KEYS
    }
    return {
        **values,
        "pid": os.getpid(),
        "cpu_s": values["user_cpu_s"] + values["system_cpu_s"],
        "peak_rss_lifetime_kib": after["peak_rss_lifetime_kib"],
        "io": io,
        "io_error": before["io_error"] or after["io_error"],
    }


def summarize_resources(completed, phase, wall_s):
    workers = [
        {"worker": item["worker"], **item["resources"]}
        for item in sorted(completed, key=lambda x: x["worker"])
    ]
    totals = {
        key: sum(w[key] for w in workers)
        for key in [*COUNTERS, "cpu_s"]
    }
    totals["cpu_core_equivalents"] = totals["cpu_s"] / wall_s
    totals["peak_rss_max_kib"] = max(
        w["peak_rss_lifetime_kib"] for w in workers
    )
    totals["io_available_workers"] = sum(
        all(w["io"][k] is not None for k in IO_KEYS) for w in workers
    )
    for key in IO_KEYS:
        values = [w["io"][key] for w in workers]
        totals["io_" + key] = (
            sum(values) if all(v is not None for v in values) else None
        )
    return {
        **{f"{phase}_worker_{k}": v for k, v in totals.items()},
        f"{phase}_worker_resources": workers,
        f"{phase}_resource_scope": (
            "worker_phase_includes_generation_or_validation_and_sampling_overhead; "
            "excludes_setup_close_parent_checkpoint; RSS_is_lifetime_peak"
        ),
    }
