#!/usr/bin/env python3
"""Bounded SQLite lock-policy and copy-out experiments. No job submission."""
import argparse
from collections import Counter
import csv
import errno
import hashlib
import json
import math
import multiprocessing as mp
import os
from pathlib import Path
import platform
import queue
import random
import resource
import shutil
import socket
import sqlite3
import subprocess
import tempfile
import time
import traceback

import sqlite_mechanism_study as core

ROOT = Path(__file__).resolve().parents[1]
PROTOCOL = 'sqlite-lock-revision-v1'
CORE_SHA = '3f274160f5d208e2445841a53402a39faddd51a123a5b8c8baf40d10d41ec89f'
NEW_FILES = [
    'scripts/sqlite_lock_revision.py', 'scripts/validate_sqlite_lock_revision.py',
    'scripts/prepare_sqlite_lock_revision.py', 'tests/test_sqlite_lock_revision.py',
    'configs/sqlite-lock-revision/lock-policy.json',
    'configs/sqlite-lock-revision/copy-out.json',
    'hpc/run_sqlite_lock_policy.slurm', 'hpc/run_sqlite_copy_out.slurm',
    'SQLITE_LOCK_REVISION.md',
]
SOURCE_FILES = NEW_FILES + ['scripts/sqlite_mechanism_study.py']
require = core.require
write_json = core.write_json
digest = core.digest
CSV_FIELDS = [
    'protocol', 'suite', 'trial', 'phase', 'repetition', 'seed', 'storage',
    'policy', 'layout', 'clients', 'records', 'batch_size', 'busy_timeout_ms',
    'status', 'valid_observation', 'success', 'hostname', 'job_id',
    'python_version', 'sqlite_version', 'database_files', 'committed_records',
    'committed_batches', 'failed_batches', 'unattempted_batches',
    'sqlite_busy_errors', 'application_retries', 'internal_busy_callbacks',
    'write_wall_s', 'checkpoint_wall_s', 'coordination_gap_s', 'completion_wall_s',
    'copy_out_s', 'copy_out_bytes', 'copy_gap_s', 'persistent_completion_wall_s',
    'write_records_s', 'completion_records_s', 'persistent_records_s',
    'prepare_ms_mean', 'lock_acquire_ms_mean', 'lock_acquire_ms_p99',
    'transaction_body_ms_mean', 'commit_ms_mean', 'batch_ms_p99',
    'queue_wait_ms_mean', 'worker_cpu_s', 'producer_cpu_s', 'database_worker_cpu_s',
    'max_requested_start_delay_s', 'directory_fsync_supported', 'raw_file', 'error',
]


def build_plan(cfg, fixed=True):
    require(cfg['protocol'] == PROTOCOL, 'Wrong protocol')
    require(cfg['suite'] in ('lock-policy', 'copy-out'), 'Wrong suite')
    for k in ('batch_size', 'clients', 'repetitions', 'seed', 'trial_timeout_s',
              'startup_timeout_s', 'queue_batches'):
        require(type(cfg[k]) is int and cfg[k] > 0, 'Invalid ' + k)
    require(type(cfg['warmup_repetitions']) is int and cfg['warmup_repetitions'] >= 0, 'Invalid warmups')
    require(cfg['storage'] == ['lustre', 'tmpfs'], 'Wrong tiers')
    require(cfg['journal_mode'] == 'WAL' and cfg['synchronous'] == 'NORMAL' and
            cfg['wal_autocheckpoint_pages'] == 1000, 'Changed database settings')
    require(cfg['workload'] == 'metadata_inserts_only' and cfg['read_queries'] == 0,
            'Changed workload')
    require(cfg['application_retry_policy'] == 'none_stop_worker_on_first_sqlite_error',
            'Unexpected application retry policy')
    conditions = []
    if cfg['suite'] == 'lock-policy':
        require(len(set(cfg['timeouts_ms'])) == len(cfg['timeouts_ms']) and
                all(type(v) is int and v > 0 for v in cfg['timeouts_ms']), 'Invalid timeouts')
        require(60000 in cfg['timeouts_ms'], 'Missing current-setting reference')
        for ms in cfg['timeouts_ms']:
            conditions.append(dict(policy='direct', layout='shared', clients=cfg['clients'],
                                   records=cfg['records'][0], busy_timeout_ms=ms))
        for p in ('queue', 'staggered'):
            conditions.append(dict(policy=p, layout='shared', clients=cfg['clients'],
                                   records=cfg['records'][0], busy_timeout_ms=60000))
    else:
        for n in cfg['records']:
            for layout, clients in [('shared', 1), ('partitioned', cfg['clients'])]:
                conditions.append(dict(policy='direct', layout=layout, clients=clients,
                                       records=n, busy_timeout_ms=60000))
    for x in conditions:
        require(type(x['records']) is int and x['records'] > 0 and
                math.ceil(x['records']/cfg['batch_size']) >= x['clients'], 'Insufficient work')
    if fixed:
        require(cfg['clients'] == 64 and cfg['batch_size'] == 1000 and
                cfg['repetitions'] == 5 and cfg['warmup_repetitions'] == 1, 'Fixed design changed')
        require(cfg['stagger_max_s'] == 2.0 and cfg['queue_batches'] == 128, 'Policy setting changed')
        require(cfg['records'] == ([1_000_000] if cfg['suite'] == 'lock-policy'
                                  else [1_000_000, 10_000_000]), 'Size design changed')
        if cfg['suite'] == 'lock-policy':
            require(cfg['timeouts_ms'] == [1000, 5000, 30000, 60000, 120000], 'Timeout sweep changed')
    plan = []
    for phase, count in [('warmup', cfg['warmup_repetitions']), ('measured', cfg['repetitions'])]:
        for repetition in range(1, count+1):
            seed = cfg['seed'] + (1000000 if phase == 'warmup' else 0) + repetition*100000
            block = [dict(x, storage=t) for x in conditions for t in cfg['storage']]
            random.Random(seed).shuffle(block)
            for x in block:
                plan.append(dict(x, trial=len(plan)+1, phase=phase, repetition=repetition, seed=seed))
    return plan


def delay_for(item, cfg, worker):
    return (random.Random(item['seed']+17000000+worker).uniform(0, cfg['stagger_max_s'])
            if item['policy'] == 'staggered' else 0.0)


def ids_for(worker, item, cfg):
    return list(range(worker, math.ceil(item['records']/cfg['batch_size']), item['clients']))


def cpu():
    r = resource.getrusage(resource.RUSAGE_SELF)
    return r.ru_utime + r.ru_stime


def attempt(conn, rows, batch_id, owner, prepared_start, prepared_end, dequeued=None):
    s = dict(batch_id=batch_id, owner=owner, records=len(rows),
             prepare_start_ns=prepared_start, prepare_end_ns=prepared_end,
             dequeued_ns=dequeued, begin_ns=time.perf_counter_ns(), status='error')
    phase = 'begin'
    try:
        conn.execute('BEGIN IMMEDIATE')
        s['acquired_ns'] = time.perf_counter_ns()
        phase = 'body'
        conn.executemany(core.INSERT, rows)
        s['body_end_ns'] = time.perf_counter_ns()
        phase = 'commit'
        conn.execute('COMMIT')
        s['commit_end_ns'] = time.perf_counter_ns()
        s['status'] = 'committed'
    except sqlite3.Error as exc:
        s.update(error_ns=time.perf_counter_ns(), error_phase=phase, error=str(exc),
                 sqlite_errorcode=getattr(exc, 'sqlite_errorcode', None),
                 sqlite_errorname=getattr(exc, 'sqlite_errorname', None))
        if conn.in_transaction:
            conn.execute('ROLLBACK')
        s['rollback_complete_ns'] = time.perf_counter_ns()
        require(not conn.in_transaction, 'Rollback did not finish')
    return s


def worker_report(channel, worker, role, status, samples, cpu_before, started,
                  settings=None, extra=None):
    finished = time.perf_counter_ns()
    used = cpu()-cpu_before
    committed = [s['batch_id'] for s in samples if s.get('status') == 'committed']
    errors = [{k:v for k,v in s.items() if k not in ('prepare_start_ns', 'prepare_end_ns')}
              for s in samples if s.get('status') == 'error']
    done = dict(kind='done', worker=worker, role=role, status=status,
                finished_ns=finished, started_ns=started,
                cpu_s=used, committed_batch_ids=committed,
                errors=errors, application_retries=0)
    if extra:
        done.update(extra)
    channel.send(done)


def failure(channel, worker, exc):
    try:
        channel.send(dict(kind='error', worker=worker, error=repr(exc), traceback=traceback.format_exc()))
    except (BrokenPipeError, OSError):
        pass


def direct_worker(worker, item, cfg, path, start, publish, channel):
    conn = None
    try:
        conn = core.connect(path, item['busy_timeout_ms'])
        channel.send(dict(kind='ready', worker=worker, role='database_writer', settings=core.settings(conn)))
        require(start.wait(cfg['startup_timeout_s']), 'Start barrier timed out')
        started = time.perf_counter_ns()
        usage = cpu()
        delay = delay_for(item, cfg, worker)
        if delay:
            time.sleep(delay)
        samples = []
        for b in ids_for(worker, item, cfg):
            t = time.perf_counter_ns()
            rows = core.generate_batch(b, item['records'], cfg['batch_size'], item['seed'])
            sample = attempt(conn, rows, b, worker, t, time.perf_counter_ns())
            samples.append(sample)
            if sample['status'] != 'committed':
                break  # No application retry, and do not silently skip to another batch.
        status = 'ok' if all(s['status'] == 'committed' for s in samples) else 'sqlite_error'
        worker_report(channel, worker, 'database_writer', status, samples, usage, started,
                      extra=dict(requested_delay_s=delay))
        require(publish.wait(cfg['trial_timeout_s']), 'Publish barrier timed out')
        channel.send(dict(kind='samples', worker=worker, batches=samples))
    except BaseException as exc:
        failure(channel, worker, exc)
    finally:
        if conn is not None:
            conn.close()
        channel.close()


def put_bounded(q, value, abort, deadline):
    while not abort.is_set():
        if time.monotonic() >= deadline:
            raise TimeoutError('Producer exceeded trial deadline')
        try:
            q.put(value, timeout=0.2)
            return True
        except queue.Full:
            pass
    return False


def producer(worker, item, cfg, q, start, publish, abort, channel):
    try:
        channel.send(dict(kind='ready', worker=worker, role='producer'))
        require(start.wait(cfg['startup_timeout_s']), 'Producer barrier timeout')
        started, usage = time.perf_counter_ns(), cpu()
        deadline = time.monotonic()+cfg['trial_timeout_s']
        samples = []
        for b in ids_for(worker, item, cfg):
            if abort.is_set():
                break
            t = time.perf_counter_ns()
            rows = core.generate_batch(b, item['records'], cfg['batch_size'], item['seed'])
            generated = time.perf_counter_ns()
            accepted = put_bounded(q, ('batch', worker, b, rows, t, generated), abort, deadline)
            samples.append(dict(batch_id=b, owner=worker, records=len(rows),
                                prepare_start_ns=t, prepare_end_ns=generated,
                                put_return_ns=time.perf_counter_ns(), accepted_by_queue=accepted))
            if not accepted:
                break
        ended = put_bounded(q, ('producer_done', worker), abort, deadline)
        status = 'ok' if ended else 'cancelled_by_writer'
        worker_report(channel, worker, 'producer', status, [], usage, started,
                      extra=dict(produced_batches=len(samples), producer_end_sent=ended,
                                 requested_delay_s=0.0))
        require(publish.wait(cfg['trial_timeout_s']), 'Producer publish timeout')
        channel.send(dict(kind='samples', worker=worker, batches=samples))
    except BaseException as exc:
        abort.set()
        failure(channel, worker, exc)
    finally:
        if abort.is_set():
            q.cancel_join_thread()
        q.close()
        channel.close()


def queue_writer(worker, item, cfg, path, q, start, publish, abort, channel):
    conn = None
    try:
        conn = core.connect(path, item['busy_timeout_ms'])
        channel.send(dict(kind='ready', worker=worker, role='queue_writer', settings=core.settings(conn)))
        require(start.wait(cfg['startup_timeout_s']), 'Queue writer barrier timeout')
        started, usage = time.perf_counter_ns(), cpu()
        deadline = time.monotonic()+cfg['trial_timeout_s']
        ended, seen, samples = set(), set(), []
        while len(ended) < item['clients']:
            require(not abort.is_set(), 'Producer aborted')
            if time.monotonic() >= deadline:
                raise TimeoutError('Writer exceeded trial deadline')
            try:
                message = q.get(timeout=.2)
            except queue.Empty:
                continue
            dequeued = time.perf_counter_ns()
            if message[0] == 'producer_done':
                require(message[1] not in ended, 'Duplicate producer end')
                ended.add(message[1])
                continue
            kind, owner, b, rows, t, generated = message
            require(kind == 'batch' and owner not in ended and b not in seen and
                    0 <= owner < item['clients'] and b % item['clients'] == owner,
                    'Invalid queue ownership/ordering')
            seen.add(b)
            sample = attempt(conn, rows, b, owner, t, generated, dequeued)
            samples.append(sample)
            if sample['status'] != 'committed':
                abort.set()
                break
        status = 'ok' if not abort.is_set() else 'sqlite_error'
        worker_report(channel, worker, 'queue_writer', status, samples, usage, started,
                      extra=dict(producer_ends=sorted(ended), requested_delay_s=0.0))
        require(publish.wait(cfg['trial_timeout_s']), 'Writer publish timeout')
        channel.send(dict(kind='samples', worker=worker, batches=samples))
    except BaseException as exc:
        abort.set()
        failure(channel, worker, exc)
    finally:
        if conn is not None:
            conn.close()
        q.close()
        channel.close()


def verify_rows(conn, batches, item, cfg):
    """Every integer key in each committed batch is accounted for; payloads sampled."""
    observed = [tuple(r) for r in conn.execute(
        'SELECT record_id / ?, COUNT(*), MIN(record_id), MAX(record_id), SUM(record_id) '
        'FROM scientific_metadata GROUP BY record_id / ? ORDER BY record_id / ?',
        (cfg['batch_size'],)*3)]
    expected = []
    for b in sorted(batches):
        lo, hi = b*cfg['batch_size'], min((b+1)*cfg['batch_size'], item['records'])
        expected.append((b, hi-lo, lo, hi-1, (lo+hi-1)*(hi-lo)//2))
    require(observed == expected, 'Stored keys disagree with committed batch IDs')
    chosen = sorted(batches)
    checked = 0
    if chosen:
        for b in sorted({chosen[0], chosen[len(chosen)//2], chosen[-1]}):
            rows = core.generate_batch(b, item['records'], cfg['batch_size'], item['seed'])
            for offset in sorted({0, len(rows)//2, len(rows)-1}):
                row = rows[offset]
                actual = conn.execute('SELECT record_id,timestep,variable,location,value '
                                      'FROM scientific_metadata WHERE record_id=?', (row[0],)).fetchone()
                require(tuple(actual) == row, 'Stored payload mismatch')
                checked += 1
    return dict(records=sum(r[1] for r in observed), committed_batch_ids=sorted(batches),
                all_keys_verified=True, sampled_payloads=checked, full_payload_verified=False)


def sync_directory(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
        return dict(supported=True)
    except OSError as exc:
        if exc.errno not in (errno.EINVAL, errno.ENOTSUP):
            raise
        return dict(supported=False, errno=exc.errno, error=str(exc))
    finally:
        os.close(fd)


def copy_out(paths, destination, receipt, fsync=os.fsync):
    """Quiescent checkpointed files only; source/destination validation is untimed."""
    receipt.update(start_ns=time.perf_counter_ns(), destination=str(destination), files=[])
    destination.mkdir()  # Unique new path; directory creation is part of copy-out time.
    for path in paths:
        target = destination / path.name
        entry = dict(file=path.name, bytes=path.stat().st_size, file_fsync=False)
        receipt['files'].append(entry)
        with path.open('rb') as src, target.open('xb') as dst:
            shutil.copyfileobj(src, dst, length=1024*1024)
            dst.flush()
            fsync(dst.fileno())
            entry['file_fsync'] = True
    receipt['directory_fsync'] = sync_directory(destination)
    receipt['parent_directory_fsync'] = sync_directory(destination.parent)
    receipt.update(end_ns=time.perf_counter_ns(), success=True,
                   method='sequential_buffered_copy_1MiB_then_each_file_fsync_then_directory_fsync')


def lustre_counters(enabled):
    if not enabled:
        return dict(status='not_requested')
    executable = shutil.which('lctl')
    if not executable:
        return dict(status='unavailable', reason='lctl_not_found')
    args = [executable, 'get_param', 'llite.*.stats', 'ldlm.namespaces.*.stats',
            'ldlm.namespaces.*.pool.stats', 'ldlm.namespaces.*.lock_count',
            'ldlm.namespaces.*.lock_unused_count']
    try:
        p = subprocess.run(args, capture_output=True, text=True, timeout=5)
        truncated = len(p.stdout) > 2_000_000
        return dict(status='truncated' if truncated else ('collected' if p.returncode == 0 else
                    'partial' if p.stdout.strip() else 'unavailable'), command=args,
                    returncode=p.returncode, stdout=p.stdout[:2_000_000], stderr=p.stderr[:10000],
                    scope='node_Lustre_client_counters_write_plus_final_checkpoint_not_DB_specific',
                    note='lock_count and lock_unused_count are gauges, not cumulative enqueue counts')
    except (OSError, subprocess.TimeoutExpired) as exc:
        return dict(status='unavailable', error=repr(exc), command=args)


def derive(report):
    times = report['timing_ns']
    done = report['workers']
    db = [w for w in done if w['role'] != 'producer']
    attempts = [s for w in db for s in w['batches']]
    good = [s for s in attempts if s['status'] == 'committed']
    bad = [s for s in attempts if s['status'] == 'error']
    count = sum(s['records'] for s in good)
    t = (times['write_end']-times['start'])/1e9
    completion = (times['checkpoint_end']-times['start'])/1e9
    values = dict(committed_records=count, committed_batches=len(good), failed_batches=len(bad),
                  unattempted_batches=math.ceil(report['records']/report['batch_size'])-len(attempts),
                  sqlite_busy_errors=sum((s.get('sqlite_errorcode') or 0)&255 == 5 for s in bad),
                  application_retries=0, internal_busy_callbacks=None, write_wall_s=t,
                  checkpoint_wall_s=(times['checkpoint_end']-times['checkpoint_start'])/1e9,
                  coordination_gap_s=(times['checkpoint_start']-times['write_end'])/1e9,
                  completion_wall_s=completion, worker_cpu_s=math.fsum(w['cpu_s'] for w in done),
                  producer_cpu_s=math.fsum(w['cpu_s'] for w in done if w['role'] == 'producer'),
                  database_worker_cpu_s=math.fsum(w['cpu_s'] for w in db),
                  max_requested_start_delay_s=max(w['requested_delay_s'] for w in done),
                  copy_out_s=None, copy_out_bytes=None, copy_gap_s=None,
                  persistent_completion_wall_s=None, directory_fsync_supported=None)
    copy = report.get('copy_out')
    if copy and copy.get('success'):
        values.update(copy_out_s=(copy['end_ns']-copy['start_ns'])/1e9,
                      copy_out_bytes=sum(x['bytes'] for x in copy['files']),
                      copy_gap_s=(copy['start_ns']-times['checkpoint_end'])/1e9,
                      persistent_completion_wall_s=(copy['end_ns']-times['start'])/1e9,
                      directory_fsync_supported=all(copy[k]['supported'] for k in
                          ('directory_fsync','parent_directory_fsync')))
    elif report['storage'] == 'lustre':
        values['persistent_completion_wall_s'] = completion
    for name, get in {
        'prepare_ms': lambda s:(s['prepare_end_ns']-s['prepare_start_ns'])/1e6,
        'lock_acquire_ms': lambda s:(s['acquired_ns']-s['begin_ns'])/1e6,
        'transaction_body_ms': lambda s:(s['body_end_ns']-s['acquired_ns'])/1e6,
        'commit_ms': lambda s:(s['commit_end_ns']-s['body_end_ns'])/1e6,
        'batch_ms': lambda s:(s['commit_end_ns']-s['prepare_start_ns'])/1e6,
    }.items():
        if good:
            summary = core.summarize([get(s) for s in good])
            values.update({name+'_'+key: val for key,val in summary.items()})
    if report['policy'] == 'queue' and good:
        values['queue_wait_ms_mean'] = math.fsum((s['dequeued_ns']-s['prepare_end_ns'])/1e6
                                                for s in good)/len(good)
    else:
        values['queue_wait_ms_mean'] = None
    for key, duration in [('write_records_s', t), ('completion_records_s', completion),
                          ('persistent_records_s', values['persistent_completion_wall_s'])]:
        values[key] = report['records']/duration if report['success'] and duration else None
    return values


def run_trial(item, cfg, storage_roots, output, local_test=False, counter_enabled=False):
    report = dict(protocol=PROTOCOL, suite=cfg['suite'], **item, batch_size=cfg['batch_size'],
                  valid_observation=False, success=False, status='error',
                  hostname=socket.gethostname(), job_id=os.getenv('SLURM_JOB_ID',''),
                  python_version=platform.python_version(), sqlite_version=sqlite3.sqlite_version,
                  execution_mode='local_test' if local_test else 'perlmutter',
                  raw_file=Path(output).name, application_retries=0, internal_busy_callbacks=None,
                  timing_scope='after_all_ready_includes_generation_queue_delay_and_stagger_sleep',
                  lock_timing_scope='BEGIN_IMMEDIATE_elapsed_not_pure_kernel_lock_wait',
                  cpu_scope='worker_start_to_done_receipt; producer_CPU_may_exclude_late_queue_feeder_work',
                  commit_scope='includes_automatic_checkpoints_when_triggered',
                  trial_lifecycle_start_ns=time.perf_counter_ns())
    processes, channels, keepers, paths = [], [], [], []
    directory = destination = None
    q = None
    try:
        report['storage_info'] = core.describe_storage(storage_roots[item['storage']], item['storage'], local_test)
        if cfg['suite'] == 'copy-out':
            report['destination_storage_info'] = core.describe_storage(storage_roots['lustre'], 'lustre', local_test)
        directory = Path(tempfile.mkdtemp(prefix='ipdps-lock-revision-', dir=storage_roots[item['storage']]))
        report['data_directory'] = str(directory)
        count = 1 if item['layout'] == 'shared' else item['clients']
        paths = [directory/f'worker-{i:03d}.db' for i in range(count)]
        report['database_files'] = count
        for p in paths:
            keepers.append(core.initialize(p, item['busy_timeout_ms']))
        report['database_settings'] = [dict(file=p.name, **core.settings(c)) for p,c in zip(paths,keepers)]
        ctx = mp.get_context('spawn')
        start, publish, abort = ctx.Event(), ctx.Event(), ctx.Event()
        q = ctx.Queue(maxsize=cfg['queue_batches']) if item['policy'] == 'queue' else None
        nproc = item['clients']+(item['policy'] == 'queue')
        for i in range(nproc):
            parent, child = ctx.Pipe(duplex=False)
            if item['policy'] != 'queue':
                path = paths[0] if count == 1 else paths[i]
                target, args = direct_worker, (i,item,cfg,str(path),start,publish,child)
            elif i < item['clients']:
                target, args = producer, (i,item,cfg,q,start,publish,abort,child)
            else:
                target, args = queue_writer, (i,item,cfg,str(paths[0]),q,start,publish,abort,child)
            process = ctx.Process(target=target, args=args)
            process.start()
            child.close()
            processes.append(process)
            channels.append(parent)
        report['ready'] = core.collect(channels, 'ready', cfg['startup_timeout_s'])
        report['lustre_before'] = lustre_counters(counter_enabled)
        report['timing_ns'] = times = dict(start=time.perf_counter_ns())
        start.set()
        done = core.collect(channels, 'done', cfg['trial_timeout_s'])
        report['workers_before_samples'] = done
        times['write_end'] = max(w['finished_ns'] for w in done)
        errors = [s for w in done for s in w['errors']]
        require(all((s.get('sqlite_errorcode') or 0)&255 == 5 for s in errors),
                'Unexpected SQLite error: '+repr(errors))
        report['status'] = 'busy' if errors else 'ok'
        require(not errors or cfg['suite'] == 'lock-policy', 'Copy-out source workload failed')
        times['checkpoint_start'] = time.perf_counter_ns()
        report['checkpoints'] = []
        for p,c in zip(paths,keepers):
            t = time.perf_counter_ns()
            status = list(c.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchone())
            report['checkpoints'].append(dict(file=p.name, status=status,
                                              wall_s=(time.perf_counter_ns()-t)/1e9))
            require(status == [0,0,0], 'Checkpoint did not complete')
        times['checkpoint_end'] = time.perf_counter_ns()
        report['lustre_after'] = lustre_counters(counter_enabled)
        if cfg['suite'] == 'copy-out' and item['storage'] == 'tmpfs':
            # Every worker is idle at publish_event; keepers prevent last-close work.
            require(all(not c.in_transaction for c in keepers), 'Active keeper transaction')
            require(all(not Path(str(p)+'-wal').exists() or Path(str(p)+'-wal').stat().st_size == 0
                        for p in paths), 'Nonempty WAL cannot be omitted from a copy')
            # Allocate only a unique name here; actual mkdir is inside timed copy_out.
            destination = Path(storage_roots['lustre']) / (directory.name+'-copy')
            report['copy_out'] = dict(success=False)
            copy_out(paths, destination, report['copy_out'])
        publish.set()
        samples = core.collect(channels, 'samples', cfg['trial_timeout_s'])
        report['workers'] = [dict(w, batches=s['batches']) for w,s in zip(done,samples)]
        for process in processes:
            process.join(timeout=10)
            require(process.exitcode == 0, f'Worker exit status {process.exitcode}')
        report['validation'] = []
        all_batches = [b for w in done for b in w['committed_batch_ids']]
        require(len(set(all_batches)) == len(all_batches), 'Duplicate committed batch')
        for i,(p,c) in enumerate(zip(paths,keepers)):
            batches = all_batches if count == 1 else [b for b in all_batches if b % item['clients'] == i]
            v = dict(file=p.name, **verify_rows(c, batches, item, cfg))
            if destination is not None:
                target = destination/p.name
                src_hash, dst_hash = digest(p), digest(target)
                require(src_hash == dst_hash, 'Copied file differs from source')
                copied = sqlite3.connect(target.resolve().as_uri()+'?mode=ro&immutable=1', uri=True)
                try:
                    require(copied.execute('PRAGMA quick_check').fetchall() == [('ok',)], 'Copied DB failed quick_check')
                    require(copied.execute('SELECT COUNT(*) FROM scientific_metadata').fetchone()[0] == v['records'],
                            'Copied DB count mismatch')
                finally:
                    copied.close()
                v['copy_verified'] = dict(source_sha256=src_hash, destination_sha256=dst_hash,
                                         records=v['records'], quick_check='ok')
            report['validation'].append(v)
        report['success'] = report['status'] == 'ok'
        if report['success']:
            require(sorted(all_batches) == list(range(math.ceil(item['records']/cfg['batch_size']))),
                    'Successful trial missing work')
        report.update(derive(report))
        require(report['committed_records'] == sum(x['records'] for x in report['validation']), 'Count mismatch')
        report['valid_observation'] = True
    except BaseException as exc:
        report.update(status='error', success=False, valid_observation=False,
                      error=repr(exc), traceback=traceback.format_exc())
    finally:
        for process in processes:
            if process.is_alive():
                process.terminate()
                process.join(timeout=5)
            if process.is_alive():
                process.kill()
                process.join(timeout=5)
        for channel in channels:
            channel.close()
        if q is not None:
            q.close()
        for c in keepers:
            c.close()
        report['data_cleanup'] = 'preserved_on_error_if_allocation_storage_survives'
        if report['valid_observation']:
            try:
                if destination is not None:
                    shutil.rmtree(destination)
                if directory is not None:
                    shutil.rmtree(directory)
                report['data_cleanup'] = 'removed_after_correctness_and_copy_validation'
            except OSError as exc:
                report.update(status='error', valid_observation=False, success=False, error=repr(exc))
        report['trial_lifecycle_end_ns'] = time.perf_counter_ns()
        write_json(output, report)
    return report


def run_study(cfg, output_root, storage_roots, local_test=False, fixed=True, pointer=None):
    plan = build_plan(cfg, fixed)
    require(digest(ROOT/'scripts/sqlite_mechanism_study.py') == CORE_SHA, 'Audited generator changed')
    if not local_test:
        require(os.getenv('SLURM_JOB_ID') and socket.gethostname().startswith('nid') and
                os.getenv('SLURM_JOB_NUM_NODES') == '1', 'Run production on one allocated compute node')
        frozen = json.loads((ROOT/'SOURCE_SHA256.json').read_text())
        require(set(frozen) == set(SOURCE_FILES), 'Frozen source inventory mismatch')
        require(all(digest(ROOT/p)==h for p,h in frozen.items()), 'Frozen sources changed')
        audit = json.loads((ROOT/'audit/native.json').read_text())
        c = sqlite3.connect(':memory:')
        try:
            source_id = c.execute('SELECT sqlite_source_id()').fetchone()[0]
        finally:
            c.close()
        require(audit['python_version'] == platform.python_version() and
                audit['sqlite_source_id'] == source_id, 'Runtime differs from Step 1 audit')
    Path(output_root).mkdir(parents=True, exist_ok=True)
    run = Path(tempfile.mkdtemp(prefix='sqlite-'+cfg['suite']+'-', dir=output_root))
    print('RESULTS='+str(run.resolve()), flush=True)
    if pointer:
        with Path(pointer).open('x') as f:
            f.write(str(run.resolve())+'\n')
    write_json(run/'config.json', cfg)
    write_json(run/'plan.json', plan)
    write_json(run/'environment.json', core.environment())
    hashes = {}
    for p in SOURCE_FILES:
        dest = run/'source'/p
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT/p,dest)
        hashes[p] = digest(dest)
    write_json(run/'source_hashes.json', hashes)
    if not local_test:
        shutil.copyfile(ROOT/'SOURCE_COMMIT', run/'SOURCE_COMMIT')
        shutil.copytree(ROOT/'audit', run/'audit')
        shutil.copyfile(ROOT/'requirements-native-resolved.txt', run/'requirements-native-resolved.txt')
    counts = Counter()
    counters_available = True
    with (run/'results.csv').open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=CSV_FIELDS, lineterminator='\n')
        writer.writeheader()
        for item in plan:
            raw = run/f'trial-{item["trial"]:04d}.json'
            counters = bool(cfg['suite'] == 'copy-out' and cfg['lustre_counters'] and
                            item['storage'] == 'lustre' and item['clients'] == 1 and counters_available)
            r = run_trial(item,cfg,storage_roots,raw,local_test,counters)
            if counters and r.get('lustre_before',{}).get('status') == 'unavailable':
                counters_available = False
            writer.writerow({k:r.get(k) for k in CSV_FIELDS})
            stream.flush()
            os.fsync(stream.fileno())
            counts[r['status']] += 1
            print(f'[{item["trial"]}/{len(plan)}] {r["status"].upper()} {item["phase"]} '
                  f'{item["storage"]} {item["policy"]} timeout_ms={item["busy_timeout_ms"]} '
                  f'clients={item["clients"]} records={item["records"]}', flush=True)
            if not r['valid_observation']:
                write_json(run/'completion.json', dict(study_complete=False, outcomes=dict(counts),
                            failed_trial=raw.name, automatic_reruns=0))
                raise RuntimeError(r.get('error','Invalid observation'))
    write_json(run/'completion.json',dict(study_complete=True, trials=len(plan),
        measured=sum(x['phase']=='measured' for x in plan), outcomes=dict(counts),
        warmups=sum(x['phase']=='warmup' for x in plan), automatic_reruns=0,
        all_trials_completed_work=counts['busy']==0,
        execution_mode='local_test' if local_test else 'perlmutter'))
    from validate_sqlite_lock_revision import validate
    validate(run, allow_local=local_test)
    return run


def export(run, log=None):
    import zipfile
    from validate_sqlite_lock_revision import validate
    validate(run)
    if log:
        shutil.copyfile(log,run/'controller.log')
    checksum = run/'SHA256SUMS'
    checksum.write_text(''.join(f'{digest(p)}  {p.relative_to(run).as_posix()}\n'
        for p in sorted(run.rglob('*')) if p.is_file() and p != checksum))
    archive = run.with_suffix('.zip')
    require(not archive.exists(), 'Archive already exists; nothing overwritten')
    with zipfile.ZipFile(archive,'x',zipfile.ZIP_DEFLATED) as z:
        for p in sorted(run.rglob('*')):
            require(not p.is_symlink(),'Unexpected archive symlink')
            if p.is_file():
                z.write(p,'sqlite-lock-revision-v1/'+p.relative_to(run).as_posix())
    with zipfile.ZipFile(archive) as z:
        require(z.testzip() is None,'Archive CRC failed')
        for p in run.rglob('*'):
            if p.is_file():
                require(hashlib.sha256(z.read('sqlite-lock-revision-v1/'+p.relative_to(run).as_posix())).hexdigest()
                        == digest(p), 'Archive content mismatch')
    parent = Path.home()/'ipdps-backups'
    parent.mkdir(exist_ok=True)
    backup = Path(tempfile.mkdtemp(prefix='sqlite-lock-revision-',dir=parent))
    shutil.copy2(archive,backup/archive.name)
    require(digest(archive)==digest(backup/archive.name),'Backup mismatch')
    (backup/'SHA256SUMS').write_text(f'{digest(archive)}  {archive.name}\n')
    print('EXPORT='+str(archive),flush=True)
    print('BACKUP='+str(backup),flush=True)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('action',choices=['dry-run','run','export'])
    p.add_argument('--suite',choices=['lock-policy','copy-out'])
    p.add_argument('--run',type=Path)
    p.add_argument('--log',type=Path)
    p.add_argument('--output-root',type=Path,default=ROOT/'results')
    p.add_argument('--result-pointer',type=Path)
    p.add_argument('--lustre-root',default=os.getenv('SCRATCH'))
    p.add_argument('--tmpfs-root',default='/tmp')
    a=p.parse_args()
    if a.action=='export':
        require(a.run is not None,'--run required')
        export(a.run,a.log)
        return
    suites=[a.suite] if a.suite else ['lock-policy','copy-out']
    if a.action=='run':
        require(a.suite and a.lustre_root,'Specify --suite and SCRATCH/--lustre-root')
    for suite in suites:
        cfg=json.loads((ROOT/f'configs/sqlite-lock-revision/{suite}.json').read_text())
        plan=build_plan(cfg)
        measured=sum(x['phase']=='measured' for x in plan)
        if a.action=='dry-run':
            print(f'READY: {suite} — {measured} measured attempts + {len(plan)-measured} warmups')
        else:
            run_study(cfg,a.output_root,{'lustre':a.lustre_root,'tmpfs':a.tmpfs_root},pointer=a.result_pointer)


if __name__=='__main__':
    main()
