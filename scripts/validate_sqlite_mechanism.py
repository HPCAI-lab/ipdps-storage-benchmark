#!/usr/bin/env python3
"""Recompute study summaries and check plans, ownership, provenance, CSV and raw JSON."""
import argparse
from collections import Counter
import csv
import hashlib
import json
import math
from pathlib import Path
import statistics

from sqlite_mechanism_study import (PROTOCOL, SAMPLE_KEYS, CSV_COLUMNS, SOURCE_FILES,
                                   build_plan, check_fixed_design, expected_ids)


def require(ok, message):
    if not ok:
        raise ValueError(message)


def close(a, b, label):
    require(math.isfinite(float(a)) and math.isfinite(float(b)) and
            math.isclose(float(a), float(b), rel_tol=1e-9, abs_tol=1e-9), label)


def independent_summary(samples):
    require(samples and all(math.isfinite(v) and v >= 0 for v in samples), 'Bad samples')
    ordered = sorted(samples)
    q = statistics.quantiles(ordered, n=100, method='inclusive') if len(ordered) > 1 else [ordered[0]] * 99
    return dict(mean=statistics.fmean(ordered), p50=statistics.median(ordered),
                p95=q[94], p99=q[98], sum=math.fsum(ordered))


def validate_trial(raw, item, cfg, mode):
    require(raw['success'] is True and raw['protocol'] == PROTOCOL, 'Trial failed/protocol mismatch')
    require(raw['suite'] == cfg['suite'] and raw['execution_mode'] == mode, 'Study/mode mismatch')
    require(all(raw[key] == value for key, value in item.items()), 'Trial/plan mismatch')
    require(raw['batch_size'] == cfg['batch_size'] and raw['reads'] == 0, 'Workload mismatch')
    require(raw['default_busy_timeout_ms'] == cfg['busy_timeout_ms'], 'Busy timeout changed')
    count = 1 if item['layout'] == 'shared' else item['clients']
    require(raw['database_files'] == count, 'File count mismatch')
    require(raw['stored_records'] == item['records'], 'Record count mismatch')
    require(raw['data_cleanup'] == 'removed_after_validation', 'Data cleanup incomplete')
    if mode == 'perlmutter':
        require(raw['storage_info']['filesystem_verified'] is True, 'Filesystem not verified')
        require(raw['storage_info']['filesystems'] == [item['storage']], 'Filesystem mismatch')
    require(raw['storage_info']['requested_tier'] == item['storage'], 'Storage label mismatch')
    settings = raw['database_settings']
    ready = raw['worker_ready']
    require(len(settings) == count and len(ready) == item['clients'], 'Missing connections/settings')
    for setting in settings + [row['settings'] for row in ready]:
        require(setting['journal_mode'].lower() == 'wal' and setting['synchronous'] == 1
                and setting['wal_autocheckpoint'] == 1000
                and setting['busy_timeout'] == cfg['busy_timeout_ms'], 'Settings mismatch')
    require([r['worker'] for r in ready] == list(range(item['clients'])), 'Missing ready workers')
    timing = raw['timing_boundaries_ns']
    start, finish, cp_start, cp_end = (timing[k] for k in (
        'start', 'last_commit_return', 'checkpoint_start', 'checkpoint_end'))
    require(start < finish <= cp_start <= cp_end, 'Invalid phase boundaries')
    for name, expected in [('write_wall_s', (finish - start) / 1e9),
                           ('checkpoint_wall_s', (cp_end - cp_start) / 1e9),
                           ('coordination_gap_s', (cp_start - finish) / 1e9),
                           ('completion_wall_s', (cp_end - start) / 1e9)]:
        close(raw[name], expected, name)
    require(len(raw['checkpoints']) == count and
            all(c['status'] == [0, 0, 0] for c in raw['checkpoints']), 'Checkpoint incomplete')
    require(len(raw['validation']) == count, 'Missing DB validation')
    for i, record in enumerate(raw['validation']):
        expected = expected_ids(item, cfg['batch_size'], None if item['layout'] == 'shared' else i)
        require(tuple(record[k] for k in ('records', 'min_id', 'max_id', 'sum_ids')) == expected,
                'Stored ID validation mismatch')
        require(record['assignment_verified'] and record['payload_samples_checked'] > 0,
                'Missing assignment/payload checks')
    workers = raw['workers']
    require([w['worker'] for w in workers] == list(range(item['clients'])), 'Worker count/order')
    require(max(w['finished_ns'] for w in workers) == finish, 'Write finish mismatch')
    batches = math.ceil(item['records'] / cfg['batch_size'])
    all_samples = []
    for w in workers:
        i = w['worker']
        expected_batches = list(range(i, batches, item['clients']))
        require([b['batch_id'] for b in w['batches']] == expected_batches, 'Batch ownership mismatch')
        require(start <= w['start_ns'] <= w['finished_ns'] <= finish, 'Worker barrier/timing mismatch')
        name = 'benchmark.db' if item['layout'] == 'shared' else f'worker-{i:03d}.db'
        require(w['db_file'] == name, 'Worker database mapping mismatch')
        expected_records = expected_ids(item, cfg['batch_size'], i)[0]
        require(w['committed_records'] == expected_records and w['transactions'] == len(expected_batches),
                'Worker accounting mismatch')
        for b in w['batches']:
            require(b['records'] == min(cfg['batch_size'], item['records'] - b['batch_id'] * cfg['batch_size']),
                    'Partial-batch count mismatch')
            require(all(math.isfinite(b[k]) and b[k] >= 0 for k in SAMPLE_KEYS), 'Invalid batch timing')
            close(b['acquired_to_commit_return_ms'], b['transaction_body_ms'] + b['commit_ms'],
                  'Acquired-to-return decomposition')
            require(b['batch_ms'] + 1e-8 >= b['prepare_ms'] + b['lock_acquire_ms'] +
                    b['acquired_to_commit_return_ms'], 'Batch decomposition exceeds total')
        for key in ('user_cpu_s', 'system_cpu_s'):
            require(math.isfinite(w[key]) and w[key] >= 0, 'Bad CPU measurement')
        all_samples.extend(w['batches'])
    require(raw['transactions'] == len(all_samples) == batches, 'Transaction count mismatch')
    for key in SAMPLE_KEYS:
        summary = independent_summary([b[key] for b in all_samples])
        for stat, value in summary.items():
            close(raw[f'{key}_{stat}'], value, f'{key}_{stat}')
            close(raw['latency_summary'][key][stat], value, f'Raw {key}_{stat}')
    close(raw['worker_cpu_s'], sum(w['user_cpu_s'] + w['system_cpu_s'] for w in workers), 'CPU sum')
    close(raw['worker_cpu_core_equivalents'], raw['worker_cpu_s'] / raw['write_wall_s'], 'CPU cores')
    close(raw['write_records_s'], item['records'] / raw['write_wall_s'], 'Write throughput')
    close(raw['completion_records_s'], item['records'] / raw['completion_wall_s'], 'Completion throughput')
    close(raw['lock_acquire_worker_seconds'], raw['lock_acquire_ms_sum'] / 1000, 'Lock wait sum')
    close(raw['acquired_to_commit_return_worker_seconds'], raw['acquired_to_commit_return_ms_sum'] / 1000,
          'Acquire-to-return sum')
    require(raw['trial_lifecycle_wall_s'] >= raw['completion_wall_s'] and
            math.isfinite(raw['validation_wall_s']) and raw['validation_wall_s'] >= 0, 'Lifecycle timing')


def validate(run, allow_local=False):
    run = Path(run).resolve()
    cfg = json.loads((run / 'config.json').read_text())
    plan = json.loads((run / 'plan.json').read_text())
    completion = json.loads((run / 'completion.json').read_text())
    mode = completion['execution_mode']
    require(mode == 'perlmutter' or (allow_local and mode == 'local_test'),
            'Local test results cannot be validated as production evidence')
    require(plan == (check_fixed_design(cfg) if mode == 'perlmutter' else build_plan(cfg)), 'Plan mismatch')
    phases = Counter(row['phase'] for row in plan)
    require(completion['success'] is True and completion['trials'] == len(plan) and
            completion['measured'] == phases['measured'] and
            completion['warmups'] == phases['warmup'], 'Completion mismatch')
    with (run / 'results.csv').open(newline='') as stream:
        reader = csv.DictReader(stream)
        require(reader.fieldnames == CSV_COLUMNS, 'CSV schema mismatch')
        rows = list(reader)
    require(len(rows) == len(plan), 'CSV trial count mismatch')
    require(len(list(run.glob('trial-[0-9][0-9][0-9][0-9].json'))) == len(plan), 'Raw trial count mismatch')
    hashes = json.loads((run / 'source_hashes.json').read_text())
    require(set(hashes) == set(SOURCE_FILES), 'Missing source hashes')
    for relative, value in hashes.items():
        require(hashlib.sha256((run / 'source' / relative).read_bytes()).hexdigest() == value,
                'Source mismatch: ' + relative)
    if mode == 'perlmutter':
        recorded_config = json.loads((run / 'source' / 'configs/sqlite-mechanism' /
                                     f'{cfg["suite"]}.json').read_text())
        require(recorded_config == cfg, 'Run config differs from frozen source')
    environment = json.loads((run / 'environment.json').read_text())
    for row, item in zip(rows, plan):
        name = f'trial-{item["trial"]:04d}.json'
        require(row['raw_file'] == name, 'CSV raw path mismatch')
        raw = json.loads((run / name).read_text())
        validate_trial(raw, item, cfg, mode)
        require(raw['hostname'] == environment['hostname'] and
                raw['slurm_job_id'] == (environment['job_id'] or '') and
                raw['sqlite_version'] == environment['sqlite_version'], 'Environment mismatch')
        for key in CSV_COLUMNS:
            if key == 'raw_file':
                continue
            value = raw.get(key, '')
            if isinstance(value, (float, int)) and not isinstance(value, bool):
                close(row[key], value, f'CSV mismatch {name} {key}')
            else:
                require(row[key] == str(value), f'CSV mismatch {name} {key}')
    receipt = dict(success=True, protocol=PROTOCOL, suite=cfg['suite'], execution_mode=mode,
                   trials=len(plan), measured=phases['measured'], warmups=phases['warmup'],
                   checks=['plan', 'CSV/raw', 'per-worker batch ownership', 'record counts',
                           'checkpoint completion', 'phase timing', 'latency recomputation',
                           'recorded payload spot checks', 'source hashes', 'environment'],
                   cautions=['Lock acquisition elapsed includes SQL overhead and scheduling.',
                             'Acquired-to-COMMIT-return is not exact kernel lock-hold duration.',
                             'Worker-seconds overlap across workers; do not sum them as trial wall time.'])
    (run / 'validation.json').write_text(json.dumps(receipt, indent=2) + '\n')
    with (run / 'SHA256SUMS').open('w') as stream:
        for path in sorted(run.rglob('*')):
            if path.is_file() and path.name != 'SHA256SUMS':
                stream.write(hashlib.sha256(path.read_bytes()).hexdigest() + '  ' +
                             path.relative_to(run).as_posix() + '\n')
    print(f'PASS: {cfg["suite"]} — {phases["measured"]} measured + {phases["warmup"]} warmups; '
          f'counts, lock timing, checkpoints, CSV, samples and sources verified ({mode})', flush=True)
    return receipt


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('run')
    parser.add_argument('--allow-local', action='store_true')
    args = parser.parse_args()
    validate(args.run, allow_local=args.allow_local)
