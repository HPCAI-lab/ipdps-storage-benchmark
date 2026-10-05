#!/usr/bin/env python3
"""Bounded native SQLite write studies. Standard library only; no job submission."""
import argparse
import csv
from datetime import datetime, timezone
import hashlib
import itertools
import json
import math
import multiprocessing as mp
from multiprocessing.connection import wait
import os
from pathlib import Path
import platform
import random
import resource
import shutil
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
import traceback

PROTOCOL = 'sqlite-mechanism-v1'
ROOT = Path(__file__).resolve().parents[1]
VARIABLES = ('temperature', 'pressure', 'density', 'energy')
INSERT = ('INSERT INTO scientific_metadata '
          '(record_id,timestep,variable,location,value) VALUES (?,?,?,?,?)')
SCHEMA = '''
CREATE TABLE scientific_metadata (
 record_id INTEGER PRIMARY KEY, timestep INTEGER NOT NULL,
 variable TEXT NOT NULL, location TEXT NOT NULL, value REAL NOT NULL);
CREATE INDEX idx_scientific_metadata_lookup
 ON scientific_metadata(variable,timestep);
'''
SAMPLE_KEYS = ('prepare_ms', 'lock_acquire_ms', 'transaction_body_ms',
               'commit_ms', 'acquired_to_commit_return_ms', 'batch_ms')
METRIC_COLUMNS = [f'{key}_{stat}' for key in SAMPLE_KEYS
                  for stat in ('mean', 'p50', 'p95', 'p99', 'sum')]
CSV_COLUMNS = [
    'protocol', 'suite', 'execution_mode', 'trial', 'phase', 'repetition',
    'storage', 'layout', 'clients', 'records', 'batch_size', 'seed',
    'success', 'hostname', 'slurm_job_id', 'python_version', 'sqlite_version',
    'database_files', 'stored_records', 'transactions', 'write_wall_s',
    'checkpoint_wall_s', 'completion_wall_s', 'coordination_gap_s',
    'validation_wall_s', 'trial_lifecycle_wall_s', 'write_records_s',
    'completion_records_s', 'worker_cpu_s', 'worker_cpu_core_equivalents',
    'lock_acquire_worker_seconds', 'acquired_to_commit_return_worker_seconds',
    'default_busy_timeout_ms', *METRIC_COLUMNS, 'raw_file', 'error',
]
SOURCE_FILES = [
    'scripts/sqlite_mechanism_study.py', 'scripts/validate_sqlite_mechanism.py',
    'scripts/prepare_sqlite_mechanism.py', 'tests/test_sqlite_mechanism.py',
    'configs/sqlite-mechanism/size-sweep.json',
    'configs/sqlite-mechanism/partitioned.json',
    'hpc/run_size_sweep.slurm', 'hpc/run_sqlite_partition.slurm',
    'SQLITE_MECHANISM_STUDY.md',
]


def require(ok, message):
    if not ok:
        raise ValueError(message)


def write_json(path, value):
    path = Path(path)
    temporary = path.with_name(path.name + '.tmp')
    temporary.write_text(json.dumps(value, indent=2, allow_nan=False) + '\n')
    temporary.replace(path)


def digest(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def build_plan(cfg):
    require(cfg['protocol'] == PROTOCOL, 'Unknown protocol')
    for key in ('batch_size', 'seed', 'repetitions', 'busy_timeout_ms',
                'trial_timeout_s', 'startup_timeout_s'):
        require(type(cfg[key]) is int and cfg[key] > 0, f'Invalid {key}')
    require(type(cfg['warmup_repetitions']) is int and
            cfg['warmup_repetitions'] >= 0, 'Invalid warmups')
    require(cfg['journal_mode'] == 'WAL' and cfg['synchronous'] == 'NORMAL',
            'Protocol requires WAL/NORMAL')
    require(cfg['wal_autocheckpoint_pages'] == 1000, 'Checkpoint setting changed')
    require(cfg['workload'] == 'metadata_inserts_only', 'Unexpected workload')
    require(cfg['read_queries'] == 0, 'These are write-only experiments')
    require(cfg['storage'] == ['lustre', 'tmpfs'], 'Unexpected storage matrix')
    for key in ('records', 'clients', 'layouts'):
        require(cfg[key] and len(set(cfg[key])) == len(cfg[key]), f'Invalid {key}')
    require(set(cfg['layouts']) <= {'shared', 'partitioned'}, 'Invalid layout')
    for n in cfg['records']:
        require(type(n) is int and 0 < n <= 10_000_000, 'Invalid records')
        require(math.ceil(n / cfg['batch_size']) >= max(cfg['clients']),
                'Every client must receive a batch')
    require(all(type(c) is int and 0 < c <= 64 for c in cfg['clients']),
            'Invalid clients')
    conditions = [dict(records=n, storage=t, clients=c, layout=l)
                  for n, t, c, l in itertools.product(
                      cfg['records'], cfg['storage'], cfg['clients'], cfg['layouts'])]
    plan = []
    for phase, count in [('warmup', cfg['warmup_repetitions']),
                         ('measured', cfg['repetitions'])]:
        for repetition in range(1, count + 1):
            ordered = [dict(item) for item in conditions]
            # Each block contains every condition once. Data seed matches
            # across layouts/tiers/client counts within that block.
            phase_offset = 1_000_000 if phase == 'warmup' else 0
            random.Random(cfg['seed'] + phase_offset + repetition).shuffle(ordered)
            for item in ordered:
                plan.append(dict(item, trial=len(plan) + 1, phase=phase,
                                 repetition=repetition,
                                 seed=cfg['seed'] + phase_offset + repetition * 100_000))
    return plan


def check_fixed_design(cfg):
    plan = build_plan(cfg)
    common = dict(batch_size=1000, repetitions=5, warmup_repetitions=1,
                  busy_timeout_ms=60000)
    for key, value in common.items():
        require(cfg[key] == value, f'Fixed design mismatch: {key}')
    if cfg['suite'] == 'size-sweep':
        require(cfg['records'] == [100_000, 1_000_000, 10_000_000] and
                cfg['clients'] == [1, 64] and cfg['layouts'] == ['shared'],
                'Size-sweep matrix changed')
        require(len(plan) == 72, 'Size-sweep count')
    elif cfg['suite'] == 'partitioned':
        require(cfg['records'] == [1_000_000] and
                cfg['clients'] == [1, 2, 4, 8, 16, 32, 64] and
                cfg['layouts'] == ['shared', 'partitioned'], 'Partition matrix changed')
        require(len(plan) == 168, 'Partition count')
    else:
        raise ValueError('Unknown study')
    return plan


def generate_batch(batch_id, records, batch_size, seed):
    rng = random.Random(seed + batch_id)
    first = batch_id * batch_size
    return [(i, i // 64, rng.choice(VARIABLES), f'node_{i % 64}', rng.gauss(0, 1))
            for i in range(first, min(first + batch_size, records))]


def connect(path, busy_timeout_ms):
    conn = sqlite3.connect(str(path), timeout=busy_timeout_ms / 1000,
                           isolation_level=None)
    conn.execute(f'PRAGMA busy_timeout={busy_timeout_ms}')
    conn.execute('PRAGMA synchronous=NORMAL')
    conn.execute('PRAGMA wal_autocheckpoint=1000')
    require(conn.execute('PRAGMA journal_mode').fetchone()[0].lower() == 'wal',
            'Database not in WAL mode')
    return conn


def initialize(path, busy_timeout_ms):
    conn = sqlite3.connect(str(path), isolation_level=None)
    try:
        require(conn.execute('PRAGMA journal_mode=WAL').fetchone()[0].lower() == 'wal',
                'Could not enable WAL')
        conn.execute('PRAGMA synchronous=NORMAL')
        conn.execute(f'PRAGMA busy_timeout={busy_timeout_ms}')
        conn.execute('PRAGMA wal_autocheckpoint=1000')
        conn.executescript(SCHEMA)
        require(conn.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchone() == (0, 0, 0),
                'Initial schema checkpoint failed')
        return conn  # keeper prevents last-close checkpoints between phases
    except BaseException:
        conn.close()
        raise


def settings(conn):
    keys = ('journal_mode', 'synchronous', 'page_size', 'cache_size',
            'wal_autocheckpoint', 'busy_timeout', 'locking_mode', 'mmap_size')
    return {key: conn.execute('PRAGMA ' + key).fetchone()[0] for key in keys}


def timed_insert(conn, rows):
    # SQL boundary timings, not a kernel lock trace. COMMIT can do work
    # after releasing the WAL write lock (e.g. automatic checkpointing).
    before = time.perf_counter_ns()
    conn.execute('BEGIN IMMEDIATE')
    acquired = time.perf_counter_ns()
    try:
        conn.executemany(INSERT, rows)
        body_done = time.perf_counter_ns()
        conn.execute('COMMIT')
        returned = time.perf_counter_ns()
    except BaseException:
        if conn.in_transaction:
            conn.execute('ROLLBACK')
        raise
    return dict(lock_acquire_ms=(acquired - before) / 1e6,
                transaction_body_ms=(body_done - acquired) / 1e6,
                commit_ms=(returned - body_done) / 1e6,
                acquired_to_commit_return_ms=(returned - acquired) / 1e6,
                _returned_ns=returned)


def worker(worker_id, item, cfg, db_path, start_event, publish_event, channel):
    conn = None
    phase = 'connect'
    try:
        conn = connect(db_path, cfg['busy_timeout_ms'])
        channel.send(dict(kind='ready', worker=worker_id, settings=settings(conn)))
        phase = 'barrier'
        require(start_event.wait(cfg['startup_timeout_s']), 'Start barrier timeout')
        start_ns = time.perf_counter_ns()
        usage_before = resource.getrusage(resource.RUSAGE_SELF)
        observations = []
        committed = 0
        for batch_id in range(worker_id, math.ceil(item['records'] / cfg['batch_size']),
                              item['clients']):
            phase = 'prepare'
            prepared_start = time.perf_counter_ns()
            rows = generate_batch(batch_id, item['records'], cfg['batch_size'], item['seed'])
            prepared_end = time.perf_counter_ns()
            phase = 'transaction'
            sample = timed_insert(conn, rows)
            finished_ns = sample.pop('_returned_ns')
            sample.update(batch_id=batch_id, records=len(rows),
                          prepare_ms=(prepared_end - prepared_start) / 1e6,
                          batch_ms=(finished_ns - prepared_start) / 1e6)
            observations.append(sample)
            committed += len(rows)
        usage_after = resource.getrusage(resource.RUSAGE_SELF)
        result = dict(worker=worker_id, db_file=Path(db_path).name,
                      committed_records=committed, transactions=len(observations),
                      start_ns=start_ns, finished_ns=finished_ns,
                      user_cpu_s=usage_after.ru_utime - usage_before.ru_utime,
                      system_cpu_s=usage_after.ru_stime - usage_before.ru_stime,
                      minor_faults=usage_after.ru_minflt - usage_before.ru_minflt,
                      major_faults=usage_after.ru_majflt - usage_before.ru_majflt,
                      voluntary_switches=usage_after.ru_nvcsw - usage_before.ru_nvcsw,
                      involuntary_switches=usage_after.ru_nivcsw - usage_before.ru_nivcsw)
        # Do not transfer large sample arrays or close a database until after
        # the parent has completed its explicit final checkpoint(s).
        channel.send(dict(kind='done', **result))
        phase = 'await_checkpoint'
        require(publish_event.wait(cfg['trial_timeout_s']), 'Checkpoint barrier timeout')
        channel.send(dict(kind='samples', worker=worker_id, batches=observations))
    except BaseException as exc:
        try:
            channel.send(dict(kind='error', worker=worker_id, phase=phase,
                              error=repr(exc), traceback=traceback.format_exc()))
        except (OSError, EOFError):
            pass
    finally:
        if conn is not None:
            conn.close()
        channel.close()


def collect(channels, kind, timeout_s):
    pending = dict(enumerate(channels))
    results = {}
    deadline = time.monotonic() + timeout_s
    while pending:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError(f'Timed out awaiting {kind}; workers {sorted(pending)}')
        ready = wait(list(pending.values()), timeout=min(remaining, 1.0))
        for channel in ready:
            worker_id = next(i for i, value in pending.items() if value is channel)
            try:
                value = channel.recv()
            except EOFError as exc:
                raise RuntimeError(f'Worker {worker_id} exited before {kind}') from exc
            require(value.get('worker') == worker_id, 'Worker message identity mismatch')
            if value.get('kind') == 'error':
                raise RuntimeError(json.dumps(value))
            require(value.get('kind') == kind, f'Expected {kind}, got {value}')
            results[worker_id] = value
            del pending[worker_id]
    return [results[i] for i in range(len(channels))]


def expected_ids(item, batch_size, worker_id=None):
    batches = range(math.ceil(item['records'] / batch_size)) if worker_id is None else range(
        worker_id, math.ceil(item['records'] / batch_size), item['clients'])
    count = total = 0
    first = last = None
    for b in batches:
        lo, hi = b * batch_size, min((b + 1) * batch_size, item['records'])
        count += hi - lo
        total += (lo + hi - 1) * (hi - lo) // 2
        first = lo if first is None else first
        last = hi - 1
    return count, first, last, total


def validate_database(conn, item, cfg, worker_id=None):
    observed = tuple(conn.execute(
        'SELECT COUNT(*), MIN(record_id), MAX(record_id), SUM(record_id) '
        'FROM scientific_metadata').fetchone())
    expected = expected_ids(item, cfg['batch_size'], worker_id)
    require(observed == expected, f'ID count/bounds/sum mismatch: {observed} != {expected}')
    if worker_id is not None:
        invalid = conn.execute(
            'SELECT COUNT(*) FROM scientific_metadata '
            'WHERE (record_id / ?) % ? != ?',
            (cfg['batch_size'], item['clients'], worker_id)).fetchone()[0]
        require(invalid == 0, 'Rows assigned to wrong shard')
    # Full key/assignment checks above; deterministic payload spot checks below.
    batch_ids = list(range(math.ceil(item['records'] / cfg['batch_size']))) if worker_id is None else list(
        range(worker_id, math.ceil(item['records'] / cfg['batch_size']), item['clients']))
    chosen = sorted({batch_ids[0], batch_ids[len(batch_ids) // 2], batch_ids[-1]})
    checked = 0
    for batch in chosen:
        rows = generate_batch(batch, item['records'], cfg['batch_size'], item['seed'])
        for offset in sorted({0, len(rows) // 2, len(rows) - 1}):
            row = rows[offset]
            actual = conn.execute(
                'SELECT record_id,timestep,variable,location,value FROM scientific_metadata '
                'WHERE record_id=?', (row[0],)).fetchone()
            require(actual == row, f'Payload sample mismatch at {row[0]}')
            checked += 1
    return dict(records=observed[0], min_id=observed[1], max_id=observed[2],
                sum_ids=observed[3], payload_samples_checked=checked,
                assignment_verified=True, full_payload_verified=False)


def summarize(values):
    values = sorted(values)
    require(values and all(math.isfinite(v) and v >= 0 for v in values), 'Invalid timing samples')
    def percentile(p):
        position = (len(values) - 1) * p
        low = int(position)
        high = math.ceil(position)
        return values[low] + (values[high] - values[low]) * (position - low)
    return dict(mean=math.fsum(values) / len(values), p50=percentile(.5),
                p95=percentile(.95), p99=percentile(.99), sum=math.fsum(values))


def describe_storage(root, label, local_test=False):
    root = Path(root).resolve()
    require(root.is_dir(), f'Storage root missing: {root}')
    result = subprocess.run(['findmnt', '-n', '-r', '-o', 'FSTYPE', '-T', str(root)],
                            check=True, capture_output=True, text=True)
    types = sorted(set(result.stdout.split()))
    require(local_test or types == [label], f'Expected {label} at {root}; got {types}')
    mount = subprocess.run(['findmnt', '-J', '-T', str(root)],
                           check=True, capture_output=True, text=True)
    usage = shutil.disk_usage(root)
    return dict(root=str(root), requested_tier=label, filesystems=types,
                mount=json.loads(mount.stdout), free_bytes=usage.free,
                filesystem_verified=not local_test)


def run_trial(item, cfg, storage_root, output, local_test=False):
    lifecycle_start = time.perf_counter()
    output = Path(output)
    report = dict(protocol=PROTOCOL, suite=cfg['suite'], **item,
                  execution_mode='local_test' if local_test else 'perlmutter',
                  batch_size=cfg['batch_size'], success=False,
                  hostname=socket.gethostname(), slurm_job_id=os.getenv('SLURM_JOB_ID', ''),
                  python_version=platform.python_version(), sqlite_version=sqlite3.sqlite_version,
                  default_busy_timeout_ms=cfg['busy_timeout_ms'],
                  workload='metadata_inserts_only', reads=0,
                  timing_scope='synchronized_write_plus_explicit_final_checkpoints',
                  lock_timing_scope='BEGIN_IMMEDIATE_elapsed_not_pure_sleep_or_kernel_trace',
                  commit_scope='includes_automatic_checkpoint_if_triggered',
                  checkpoint_policy='sequential_TRUNCATE_all_files_after_workers_commit')
    directory = None
    keepers, processes, channels = [], [], []
    completed = []
    try:
        report['storage_info'] = describe_storage(storage_root, item['storage'], local_test)
        directory = Path(tempfile.mkdtemp(prefix='ipdps-mechanism-', dir=storage_root))
        report['data_directory'] = str(directory)
        count = 1 if item['layout'] == 'shared' else item['clients']
        paths = [directory / ('benchmark.db' if item['layout'] == 'shared' else f'worker-{i:03d}.db')
                 for i in range(count)]
        report['database_files'] = count
        for path in paths:
            keepers.append(initialize(path, cfg['busy_timeout_ms']))
        report['database_settings'] = [dict(file=p.name, **settings(c)) for p, c in zip(paths, keepers)]
        context = mp.get_context('spawn')
        start_event, publish_event = context.Event(), context.Event()
        for worker_id in range(item['clients']):
            parent, child = context.Pipe(duplex=False)
            path = paths[0] if item['layout'] == 'shared' else paths[worker_id]
            process = context.Process(target=worker, args=(worker_id, item, cfg, str(path),
                                                           start_event, publish_event, child))
            process.start()
            child.close()
            processes.append(process)
            channels.append(parent)
        report['worker_ready'] = collect(channels, 'ready', cfg['startup_timeout_s'])
        start_ns = time.perf_counter_ns()
        start_event.set()
        completed = collect(channels, 'done', cfg['trial_timeout_s'])
        finish_ns = max(row['finished_ns'] for row in completed)
        checkpoint_start_ns = time.perf_counter_ns()
        checkpoints = []
        for path, conn in zip(paths, keepers):
            t = time.perf_counter_ns()
            status = tuple(conn.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchone())
            checkpoints.append(dict(file=path.name, status=status,
                                    wall_s=(time.perf_counter_ns() - t) / 1e9))
            require(status == (0, 0, 0), f'Incomplete checkpoint: {status}')
        checkpoint_end_ns = time.perf_counter_ns()
        report['checkpoints'] = checkpoints
        report['timing_boundaries_ns'] = dict(start=start_ns, last_commit_return=finish_ns,
                                             checkpoint_start=checkpoint_start_ns,
                                             checkpoint_end=checkpoint_end_ns)
        report.update(write_wall_s=(finish_ns - start_ns) / 1e9,
                      coordination_gap_s=(checkpoint_start_ns - finish_ns) / 1e9,
                      checkpoint_wall_s=(checkpoint_end_ns - checkpoint_start_ns) / 1e9,
                      completion_wall_s=(checkpoint_end_ns - start_ns) / 1e9)
        publish_event.set()
        samples = collect(channels, 'samples', cfg['trial_timeout_s'])
        report['workers'] = [dict(done, batches=sample['batches'])
                             for done, sample in zip(completed, samples)]
        for process in processes:
            process.join(timeout=10)
            require(process.exitcode == 0, f'Worker exit status {process.exitcode}')
        begin_validation = time.perf_counter()
        report['validation'] = [dict(file=path.name, **validate_database(
            conn, item, cfg, None if item['layout'] == 'shared' else i))
            for i, (path, conn) in enumerate(zip(paths, keepers))]
        report['validation_wall_s'] = time.perf_counter() - begin_validation
        stored = sum(row['records'] for row in report['validation'])
        require(stored == item['records'] == sum(row['committed_records'] for row in completed),
                'Total stored/committed count mismatch')
        flattened = [batch for row in report['workers'] for batch in row['batches']]
        require(sorted(b['batch_id'] for b in flattened) == list(
            range(math.ceil(item['records'] / cfg['batch_size']))), 'Batch ownership mismatch')
        report['stored_records'] = stored
        report['transactions'] = len(flattened)
        report['latency_summary'] = {key: summarize([b[key] for b in flattened]) for key in SAMPLE_KEYS}
        for key, summary in report['latency_summary'].items():
            report.update({f'{key}_{stat}': value for stat, value in summary.items()})
        report['worker_cpu_s'] = sum(row['user_cpu_s'] + row['system_cpu_s'] for row in completed)
        report['worker_cpu_core_equivalents'] = report['worker_cpu_s'] / report['write_wall_s']
        report['write_records_s'] = stored / report['write_wall_s']
        report['completion_records_s'] = stored / report['completion_wall_s']
        report['lock_acquire_worker_seconds'] = report['lock_acquire_ms_sum'] / 1000
        report['acquired_to_commit_return_worker_seconds'] = report['acquired_to_commit_return_ms_sum'] / 1000
        report['success'] = True
    except BaseException as exc:
        report['error'] = repr(exc)
        report['traceback'] = traceback.format_exc()
        if completed and 'workers' not in report:
            report['workers_before_failure'] = completed
    finally:
        # Stop workers before releasing keepers or removing this trial's data.
        for process in processes:
            if process.is_alive():
                process.terminate()
                process.join(timeout=5)
            if process.is_alive():
                process.kill()
                process.join(timeout=5)
        for channel in channels:
            channel.close()
        for conn in keepers:
            conn.close()
        if report['success'] and directory is not None:
            try:
                shutil.rmtree(directory)
                report['data_cleanup'] = 'removed_after_validation'
            except OSError as exc:
                report['success'] = False
                report['error'] = f'Cleanup failed: {exc!r}'
                report['data_cleanup'] = 'incomplete'
        elif directory is not None:
            report['data_cleanup'] = 'preserved_failure_data_if_allocation_storage_survives'
        report['trial_lifecycle_wall_s'] = time.perf_counter() - lifecycle_start
        write_json(output, report)
    return report


def environment():
    result = dict(hostname=socket.gethostname(), job_id=os.getenv('SLURM_JOB_ID'),
                  python_executable=sys.executable, python_version=sys.version,
                  sqlite_version=sqlite3.sqlite_version, platform=platform.platform(),
                  cpu_affinity=sorted(os.sched_getaffinity(0)),
                  sqlite_compile_options=[r[0] for r in sqlite3.connect(':memory:').execute('PRAGMA compile_options')])
    for name, command in [('lscpu', ['lscpu', '-J']),
                          ('block_devices', ['lsblk', '-J', '-o', 'NAME,TYPE,SIZE,FSTYPE,MOUNTPOINTS'])]:
        try:
            capture = subprocess.run(command, capture_output=True, text=True, timeout=15)
            result[name] = dict(exit_code=capture.returncode, stdout=capture.stdout, stderr=capture.stderr)
        except (OSError, subprocess.TimeoutExpired) as exc:
            result[name] = dict(error=repr(exc))
    return result


def save_sources(destination):
    hashes = {}
    for relative in SOURCE_FILES:
        source = ROOT / relative
        require(source.is_file(), f'Source file missing: {relative}')
        target = destination / 'source' / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
        hashes[relative] = digest(target)
    # A prepared snapshot is mandatory for Perlmutter runs.
    if (ROOT / 'SOURCE_SHA256.json').exists():
        require(hashes == json.loads((ROOT / 'SOURCE_SHA256.json').read_text()),
                'Frozen source hash mismatch')
        shutil.copy2(ROOT / 'SOURCE_COMMIT', destination / 'SOURCE_COMMIT')
    write_json(destination / 'source_hashes.json', hashes)


def run_study(cfg, output_root, storage_roots, local_test=False, fixed=True):
    plan = check_fixed_design(cfg) if fixed else build_plan(cfg)
    if not local_test:
        require(os.getenv('SLURM_JOB_ID') and socket.gethostname().startswith('nid'),
                'Production studies must run on an allocated compute node')
        require(os.getenv('SLURM_JOB_NUM_NODES') == '1', 'This is a single-node study')
        require((ROOT / 'SOURCE_SHA256.json').is_file(), 'Run from a prepared frozen source snapshot')
    stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    Path(output_root).mkdir(parents=True, exist_ok=True)
    run = Path(tempfile.mkdtemp(prefix=f'{cfg["suite"]}-{stamp}-', dir=output_root))
    print('RESULTS=' + str(run.resolve()), flush=True)
    write_json(run / 'config.json', cfg)
    write_json(run / 'plan.json', plan)
    write_json(run / 'environment.json', environment())
    save_sources(run)
    with (run / 'results.csv').open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=CSV_COLUMNS, lineterminator='\n')
        writer.writeheader()
        for item in plan:
            raw_file = f'trial-{item["trial"]:04d}.json'
            report = run_trial(item, cfg, storage_roots[item['storage']], run / raw_file, local_test)
            report['raw_file'] = raw_file
            writer.writerow({key: report.get(key, '') for key in CSV_COLUMNS})
            stream.flush()
            os.fsync(stream.fileno())
            print(f'[{item["trial"]}/{len(plan)}] {"PASS" if report["success"] else "FAIL"} '
                  f'{item["phase"]} {item["storage"]} {item["layout"]} '
                  f'records={item["records"]} clients={item["clients"]}', flush=True)
            if not report['success']:
                write_json(run / 'completion.json', dict(success=False, trials=item['trial'],
                           expected_trials=len(plan), failed_trial=raw_file, automatic_retries=0))
                raise RuntimeError(report['error'])
    write_json(run / 'completion.json', dict(success=True, trials=len(plan),
               measured=sum(x['phase'] == 'measured' for x in plan),
               warmups=sum(x['phase'] == 'warmup' for x in plan), automatic_retries=0,
               execution_mode='local_test' if local_test else 'perlmutter'))
    return run


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', required=True)
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--output-root', default=str(ROOT / 'results'))
    parser.add_argument('--lustre-root', default=os.getenv('SCRATCH'))
    parser.add_argument('--tmpfs-root', default='/tmp')
    parser.add_argument('--local-smoke', action='store_true',
                        help='Small LOCAL TEST ONLY plan; never production evidence')
    args = parser.parse_args()
    cfg = json.loads(Path(args.config).read_text())
    plan = check_fixed_design(cfg)
    if args.dry_run:
        measured = sum(p['phase'] == 'measured' for p in plan)
        print(f'READY: {cfg["suite"]}: {measured} measured + {len(plan)-measured} warmups')
        print(json.dumps(dict(records=cfg['records'], clients=cfg['clients'], layouts=cfg['layouts'],
                              storage=cfg['storage'], batch_size=cfg['batch_size'], reads=0)))
        return
    if args.local_smoke:
        require(not os.getenv('SLURM_JOB_ID'), 'Local smoke is not a production job')
        cfg.update(records=[128], clients=[1, 4], layouts=['shared', 'partitioned'],
                   batch_size=16, repetitions=1, warmup_repetitions=0,
                   busy_timeout_ms=3000, trial_timeout_s=30, startup_timeout_s=30)
        with tempfile.TemporaryDirectory(prefix='sqlite-mechanism-local-') as temporary:
            run = run_study(cfg, args.output_root, {'lustre': temporary, 'tmpfs': temporary},
                            local_test=True, fixed=False)
    else:
        require(args.lustre_root, 'SCRATCH or --lustre-root is required')
        run = run_study(cfg, args.output_root,
                        {'lustre': args.lustre_root, 'tmpfs': args.tmpfs_root})
    from validate_sqlite_mechanism import validate
    validate(run, allow_local=args.local_smoke)
    print(f'SQLITE_MECHANISM_COMPLETE suite={cfg["suite"]}', flush=True)


if __name__ == '__main__':
    main()
