"""Optional server-launch lifecycle accounting, separate from client phases.

Linux wait4 accounts for the launched command and waited-for descendants. The
tree includes Shifter, initdb, PostgreSQL and its backends, through shutdown;
it excludes Python clients and the separately launched pg_ctl stop command.
Kernel block counters are not device bandwidth or logical SQL byte counts.
PSS is sampled, non-atomic, and can miss peaks and short-lived processes.
"""
import json
import math
import os
from pathlib import Path
import subprocess
import sys
import threading
import time

USAGE_FIELDS = {
    'user_cpu_s': 'ru_utime', 'system_cpu_s': 'ru_stime',
    'max_process_rss_kib': 'ru_maxrss', 'kernel_input_blocks': 'ru_inblock',
    'kernel_output_blocks': 'ru_oublock', 'major_faults': 'ru_majflt',
    'minor_faults': 'ru_minflt', 'voluntary_switches': 'ru_nvcsw',
    'involuntary_switches': 'ru_nivcsw',
}
TIME_KEYS = [*USAGE_FIELDS, 'wall_s', 'exit_code']
PREFIX = 'server_lifecycle_'
SERVER_FIELDS = ['server_profiling'] + [PREFIX + key for key in TIME_KEYS] + [
    PREFIX + key for key in ('cpu_s', 'sampled_peak_pss_kib',
                            'memory_samples', 'complete_memory_samples',
                            'sampler_wall_s', 'sampled_max_postgres_processes')]


def identity(pid):
    # The comm field in stat may contain spaces or parentheses.
    fields = Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()
    return (pid, int(fields[19])), fields[0]


def proc_visible_child(pid):
    """Resolve the direct child if /proc exposes an outer PID namespace."""
    visible_self = int(Path('/proc/self/stat').read_text().split(' ', 1)[0])
    if visible_self == os.getpid():
        return pid
    for candidate in children_of(visible_self, {}):
        try:
            status = Path(f'/proc/{candidate}/status').read_text().splitlines()
            fields = {line.split(':', 1)[0]: line.split(':', 1)[1].split()
                      for line in status}
            if int(fields['NSpid'][-1]) == pid and int(fields['PPid'][0]) == visible_self:
                return candidate
        except (OSError, ValueError, KeyError):
            continue
    raise RuntimeError('Cannot resolve profiler child in the /proc PID namespace')


def children_of(pid, cache):
    try:
        return sorted(map(int, Path(f'/proc/{pid}/task/{pid}/children').read_text().split()))
    except FileNotFoundError:
        identity(pid)  # Distinguish an exited process from an unsupported children file.
        if 'parents' not in cache:
            parents = {}
            for entry in Path('/proc').iterdir():
                if not entry.name.isdigit():
                    continue
                try:
                    fields = (entry / 'stat').read_text().rsplit(')', 1)[1].split()
                    parents.setdefault(int(fields[1]), []).append(int(entry.name))
                except (OSError, ValueError, IndexError):
                    continue
            cache['parents'] = parents
        return sorted(cache['parents'].get(pid, []))


def memory_sample(root_pid, root_identity):
    """Read only the launched tree; reject PID reuse and incomplete scans."""
    pending, visited, identities, cache = [root_pid], set(), [], {}
    pss, errors, live, postgres_processes = 0, [], 0, 0
    while pending:
        pid = pending.pop()
        if pid in visited:
            continue
        visited.add(pid)
        try:
            before, state = identity(pid)
            if pid == root_pid and before != root_identity:
                raise RuntimeError('Profiler root PID was reused')
            child_ids = children_of(pid, cache)
            if state != 'Z':
                if Path(f'/proc/{pid}/comm').read_text().strip() == 'postgres':
                    postgres_processes += 1
                fields = Path(f'/proc/{pid}/smaps_rollup').read_text().splitlines()
                value = next(int(line.split()[1]) for line in fields
                             if line.startswith('Pss:'))
                pss += value
                live += 1
            after, _ = identity(pid)
            if after != before:
                raise RuntimeError('Process changed during memory sampling')
            # A changing process tree is kept as an incomplete sample.
            if child_ids != children_of(pid, cache):
                raise RuntimeError('Children changed during memory sampling')
            identities.append(before)
            pending.extend(child_ids)
        except (OSError, ValueError, StopIteration, RuntimeError) as exc:
            errors.append(f'{pid}: {type(exc).__name__}: {exc}')
    for pid, started in identities:
        try:
            if identity(pid)[0] != (pid, started):
                raise RuntimeError('Process changed before end of scan')
        except (OSError, ValueError, RuntimeError) as exc:
            errors.append(f'{pid}: {type(exc).__name__}: {exc}')
    return dict(pss_kib=pss if not errors and live else None,
                live_processes=live, discovered_processes=len(visited),
                postgres_processes=postgres_processes,
                discovery='proc_stat_snapshot' if 'parents' in cache else 'children_files',
                complete=not errors and live > 0, errors=errors[:8])


class ServerProfiler:
    def __init__(self, log_path, interval_s=0.2):
        self.time_path = Path(str(log_path) + '.rusage.json')
        self.memory_path = Path(str(log_path) + '.memory.jsonl')
        self.interval_s = interval_s
        self.stop_event = threading.Event()
        self.thread = None
        self.samples = []
        self.error = None

    def wrap(self, command):
        return [sys.executable, str(Path(__file__).resolve()),
                '--account', str(self.time_path), '--', *command]

    def start(self, root_pid):
        try:
            root_pid = proc_visible_child(root_pid)
            root_identity, _ = identity(root_pid)
        except (OSError, ValueError, RuntimeError) as exc:
            self.error = repr(exc)
            return
        origin = time.monotonic()

        def sample_loop():
            try:
                with self.memory_path.open('w') as stream:
                    while not self.stop_event.is_set():
                        started = time.monotonic()
                        sample = memory_sample(root_pid, root_identity)
                        sample['elapsed_s'] = started - origin
                        sample['scan_wall_s'] = time.monotonic() - started
                        self.samples.append(sample)
                        stream.write(json.dumps(sample) + '\n')
                        stream.flush()
                        self.stop_event.wait(max(0, self.interval_s - sample['scan_wall_s']))
            except Exception as exc:
                self.error = repr(exc)
        self.thread = threading.Thread(target=sample_loop, daemon=True)
        self.thread.start()

    def stop(self):
        self.stop_event.set()
        if self.thread is not None:
            self.thread.join(timeout=10)
            if self.thread.is_alive():
                self.error = 'Memory sampler did not finish within 10 seconds'

    def result(self):
        result = dict(server_resource_metrics_status='unavailable',
            server_resource_scope='wait4_launch_lifecycle_including_shifter_initdb_postgres_shutdown; excludes_python_clients_profiler_supervisor_and_external_pg_ctl',
            server_memory_scope='sampled_sum_PSS_of_launch_tree_including_profiler_supervisor; non_atomic; peaks_may_be_missed; max_process_RSS_is_not_tree_peak',
            server_io_scope='kernel_rusage_block_counters; not_logical_SQL_bytes_or_device_bandwidth',
            server_accounting_scope='Linux_wait4_waited_descendants; descendant_accounting_requires_parents_to_reap_children',
            server_accounting_file=self.time_path.name,
            server_memory_file=self.memory_path.name,
            server_memory_sampling_interval_s=self.interval_s,
            server_sampler_error=self.error)
        complete = [s for s in self.samples if s['complete']]
        postgres_samples = [s for s in complete if s['postgres_processes'] > 0]
        result.update({PREFIX + 'sampled_peak_pss_kib':
                           max((s['pss_kib'] for s in complete), default=None),
                       PREFIX + 'memory_samples': len(self.samples),
                       PREFIX + 'complete_memory_samples': len(complete),
                       PREFIX + 'sampled_max_postgres_processes': max(
                           (s['postgres_processes'] for s in complete), default=0),
                       PREFIX + 'sampler_wall_s': sum(s['scan_wall_s'] for s in self.samples)})
        try:
            values = json.loads(self.time_path.read_text())
            if set(values) != set(TIME_KEYS) or not all(
                isinstance(v, (int, float)) and math.isfinite(v) and v >= 0
                for v in values.values()
            ):
                raise ValueError('Invalid wait4 accounting output')
            result.update({PREFIX + k: v for k,v in values.items()})
            result[PREFIX + 'cpu_s'] = values['user_cpu_s'] + values['system_cpu_s']
            result['server_resource_metrics_status'] = (
                'lifecycle_collected' if postgres_samples and not self.error
                else 'lifecycle_cpu_io_collected_memory_incomplete')
        except (OSError, ValueError, StopIteration) as exc:
            result['server_accounting_error'] = repr(exc)
        return result


def account_command(output, command):
    """Isolated supervisor: only this server command contributes to wait4."""
    started = time.monotonic()
    process = subprocess.Popen(command)
    _, status, usage = os.wait4(process.pid, 0)
    process.returncode = os.waitstatus_to_exitcode(status)
    code = process.returncode if process.returncode >= 0 else 128 - process.returncode
    values = {key: getattr(usage, attr) for key, attr in USAGE_FIELDS.items()}
    values.update(wall_s=time.monotonic()-started, exit_code=code)
    with Path(output).open('w') as stream:
        json.dump(values, stream, indent=2)
        stream.write('\n')
        stream.flush()
        os.fsync(stream.fileno())
    return code


if __name__ == '__main__':
    if len(sys.argv) < 5 or sys.argv[1] != '--account' or sys.argv[3] != '--':
        raise SystemExit('Usage: server_metrics.py --account OUTPUT -- COMMAND [ARGS...]')
    raise SystemExit(account_command(sys.argv[2], sys.argv[4:]))
