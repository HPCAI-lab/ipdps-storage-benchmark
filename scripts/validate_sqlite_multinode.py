#!/usr/bin/env python3
"""Offline audit of global and node-local evidence; never opens remote databases."""
import argparse
from collections import Counter
import csv
import json
import math
from pathlib import Path
import statistics

import sqlite_mechanism_study as core
from sqlite_multinode import (PROTOCOL, CONFIG, SOURCE_FILES, COLUMNS, BASE_SHA256,
                              build_plan, selected_hosts, digest, write_json, require)


def close(actual, expected, label):
    require(math.isfinite(float(actual)) and math.isfinite(float(expected)) and
            math.isclose(float(actual), float(expected), rel_tol=1e-9, abs_tol=1e-9), label)


def summary(values):
    require(values and all(math.isfinite(x) and x >= 0 for x in values), 'Invalid timing sample')
    ordered = sorted(values)
    q = statistics.quantiles(ordered, n=100, method='inclusive') if len(ordered) > 1 else [ordered[0]] * 99
    return dict(mean=statistics.fmean(ordered), p50=statistics.median(ordered),
                p95=q[94], p99=q[98], sum=math.fsum(ordered))


def audit_node(node, item, cfg, rank, mode, job):
    require(node['success'] is True and node['protocol'] == PROTOCOL, 'Failed node')
    require(node['execution_mode'] == mode and node['rank'] == rank, 'Node identity/mode')
    require(all(node[k] == v for k, v in item.items()), 'Node item mismatch')
    require(node['slurm_job_id'] == job, 'Node job mismatch')
    require(node['worker_source_sha256'] == BASE_SHA256, 'Worker source identity')
    require(node['data_cleanup'] == 'removed_after_validation', 'Incomplete node cleanup')
    env = node['environment']
    require(env['hostname'] == node['hostname'] and (env['job_id'] or '') == job, 'Node environment')
    require(node['storage_info']['requested_tier'] == item['storage'], 'Node tier label')
    if mode == 'perlmutter':
        require(node['storage_info']['filesystem_verified'] is True and
                node['storage_info']['filesystems'] == [item['storage']], 'Wrong node filesystem')
    ids = list(range(rank, item['clients'], item['nodes']))
    require(node['worker_ids'] == ids, 'Node worker ownership')
    for key in ('workers', 'worker_ready', 'database_settings', 'validation', 'checkpoints'):
        require([w['worker'] for w in node[key]] == ids, f'Missing/duplicate {key}')
    for setting in node['database_settings'] + [w['settings'] for w in node['worker_ready']]:
        require(setting['journal_mode'].lower() == 'wal' and setting['synchronous'] == 1 and
                setting['wal_autocheckpoint'] == 1000 and setting['busy_timeout'] == cfg['busy_timeout_ms'],
                'SQLite policy mismatch')
    t = node['local_timing_ns']
    start, finish, cp_start, cp_end = [t[k] for k in
                                    ('start', 'last_commit_return', 'checkpoint_start', 'checkpoint_end')]
    require(start < finish <= cp_start <= cp_end, 'Node-local phase timing')
    require(max(w['finished_ns'] for w in node['workers']) == finish, 'Node write finish')
    require(all(c['status'] == [0, 0, 0] and math.isfinite(c['wall_s']) and c['wall_s'] >= 0
                for c in node['checkpoints']), 'Incomplete checkpoint')
    require(sum(c['wall_s'] for c in node['checkpoints']) <= (cp_end - cp_start) / 1e9 + 1e-8,
            'Checkpoint duration accounting')
    require(math.isfinite(node['validation_wall_s']) and node['validation_wall_s'] >= 0 and
            node['node_lifecycle_wall_s'] >= (cp_end - start) / 1e9, 'Node lifecycle')
    batches = math.ceil(item['records'] / cfg['batch_size'])
    samples = []
    for w, db, checkpoint, settings in zip(node['workers'], node['validation'],
                                         node['checkpoints'], node['database_settings']):
        i = w['worker']
        name = f'worker-{i:03d}.db'
        require(w['db_file'] == db['file'] == checkpoint['file'] == settings['file'] == name,
                'Shard file mismatch')
        expected = core.expected_ids(item, cfg['batch_size'], i)
        require(tuple(db[k] for k in ('records', 'min_id', 'max_id', 'sum_ids')) == expected,
                'Shard count/bounds/sum mismatch')
        require(db['assignment_verified'] is True and db['payload_samples_checked'] > 0,
                'Missing shard correctness evidence')
        require(start <= w['start_ns'] <= w['finished_ns'] <= finish, 'Worker local timing')
        expected_batches = list(range(i, batches, item['clients']))
        require([b['batch_id'] for b in w['batches']] == expected_batches, 'Batch ownership mismatch')
        require(w['committed_records'] == expected[0] and w['transactions'] == len(expected_batches),
                'Worker accounting')
        for key in ('user_cpu_s', 'system_cpu_s'):
            require(math.isfinite(w[key]) and w[key] >= 0, 'CPU accounting')
        for b in w['batches']:
            require(b['records'] == min(cfg['batch_size'], item['records'] - b['batch_id'] * cfg['batch_size']),
                    'Partial batch mismatch')
            require(all(math.isfinite(b[k]) and b[k] >= 0 for k in core.SAMPLE_KEYS), 'Batch timing')
            close(b['acquired_to_commit_return_ms'], b['transaction_body_ms'] + b['commit_ms'],
                  'Transaction timing decomposition')
            require(b['batch_ms'] + 1e-8 >= b['prepare_ms'] + b['lock_acquire_ms'] +
                    b['acquired_to_commit_return_ms'], 'Batch timing decomposition')
        samples.extend(w['batches'])
    require(node['stored_records'] == sum(v['records'] for v in node['validation']), 'Node total')
    return samples


def audit_trial(run, raw, item, cfg, mode, hosts, job):
    require(raw['success'] is True and raw['protocol'] == PROTOCOL, 'Failed global trial')
    require(raw['execution_mode'] == mode and raw['slurm_job_id'] == job, 'Global mode/job')
    require(all(raw[k] == v for k, v in item.items()), 'Global plan mismatch')
    require(raw['batch_size'] == cfg['batch_size'] and raw['database_files'] == item['clients'], 'Global design')
    selected = selected_hosts(hosts, item)
    require(raw['selected_hosts'] == selected and raw['hosts'] == ','.join(selected), 'Placement plan')
    require(raw['checkpoint_policy'] == 'global_write_barrier_then_sequential_per_node_parallel_across_nodes',
            'Checkpoint policy changed')
    if mode == 'perlmutter':
        require(len(set(raw['node_hosts'])) == item['nodes'] and set(raw['node_hosts']) == set(selected),
                'Physical nodes not distinct')
    require([r['rank'] for r in raw['node_results']] == list(range(item['nodes'])), 'Node result set')
    samples, workers, nodes = [], [], []
    versions, stored = set(), 0
    for ref in raw['node_results']:
        rank = ref['rank']
        name = f'trial-{item["trial"]:04d}-node-{rank:02d}.json'
        require(ref['raw_file'] == name and digest(run / name) == ref['sha256'], 'Node evidence checksum')
        node = json.loads((run / name).read_text())
        require(node['hostname'] == raw['node_hosts'][rank], 'Node host mismatch')
        samples.extend(audit_node(node, item, cfg, rank, mode, job))
        workers.extend(node['workers'])
        nodes.append(node)
        stored += node['stored_records']
        versions.add((node['environment']['python_version'], node['environment']['sqlite_version']))
    require(len(versions) == 1, 'Node runtime versions differ')
    require(sorted(w['worker'] for w in workers) == list(range(item['clients'])), 'Global worker coverage')
    require(stored == raw['stored_records'] == item['records'], 'Global record count')
    count = math.ceil(item['records'] / cfg['batch_size'])
    require(raw['transactions'] == len(samples) == count and
            sorted(b['batch_id'] for b in samples) == list(range(count)), 'Global batch coverage')
    t = raw['coordinator_timing_ns']
    start, end, cpstart, cpend = [t[k] for k in ('start', 'last_write_receipt', 'checkpoint_start',
                                               'last_checkpoint_receipt')]
    require(start <= t['start_fanout_end'] < end <= cpstart < cpend, 'Coordinator timeline')
    for key, final in [('write_done_receipts', end), ('checkpoint_done_receipts', cpend)]:
        require(len(t[key]) == item['nodes'] and max(t[key]) == final, 'Missing coordinator receipts')
    require(min(t['write_done_receipts']) > t['start_fanout_end'] and
            min(t['checkpoint_done_receipts']) >= cpstart, 'Message phase order')
    for key, value in dict(write_wall_s=(end - start) / 1e9, coordination_gap_s=(cpstart - end) / 1e9,
                           checkpoint_wall_s=(cpend - cpstart) / 1e9,
                           completion_wall_s=(cpend - start) / 1e9,
                           start_fanout_s=(t['start_fanout_end'] - start) / 1e9).items():
        close(raw[key], value, f'Global {key}')
    require(raw['trial_lifecycle_wall_s'] >= raw['completion_wall_s'], 'Global lifecycle')
    for key in core.SAMPLE_KEYS:
        for stat, value in summary([b[key] for b in samples]).items():
            close(raw[f'{key}_{stat}'], value, f'Latency recomputation {key} {stat}')
    cpu = sum(w['user_cpu_s'] + w['system_cpu_s'] for w in workers)
    close(raw['worker_cpu_s'], cpu, 'Global CPU')
    close(raw['worker_cpu_core_equivalents'], cpu / raw['write_wall_s'], 'Global CPU equivalents')
    close(raw['write_records_s'], item['records'] / raw['write_wall_s'], 'Global write rate')
    close(raw['completion_records_s'], item['records'] / raw['completion_wall_s'], 'Global completion rate')
    return len(samples), versions


def validate(run, allow_local=False):
    run = Path(run).resolve()
    completion = json.loads((run / 'completion.json').read_text())
    mode = completion['execution_mode']
    require(mode == 'perlmutter' or (mode == 'local_test' and allow_local), 'Local evidence not permitted')
    require(completion['success'] is True and completion['automatic_retries'] == 0, 'Study incomplete')
    cfg = json.loads((run / 'config.json').read_text())
    plan = build_plan(cfg, fixed=mode == 'perlmutter')
    require(plan == json.loads((run / 'plan.json').read_text()), 'Saved plan mismatch')
    require(completion['trials'] == completion['expected_trials'] == len(plan), 'Completion count')
    phases = Counter(p['phase'] for p in plan)
    require(completion['measured'] == phases['measured'] and completion['warmups'] == phases['warmup'],
            'Phase counts')
    hashes = json.loads((run / 'source_hashes.json').read_text())
    require(set(hashes) == set(SOURCE_FILES), 'Missing source provenance')
    for rel, value in hashes.items():
        require(digest(run / 'source' / rel) == value, 'Source checksum mismatch: ' + rel)
    require(hashes['scripts/sqlite_mechanism_study.py'] == BASE_SHA256, 'Original worker mismatch')
    if mode == 'perlmutter':
        require(cfg == json.loads((run / 'source' / CONFIG).read_text()), 'Frozen configuration mismatch')
        require(len((run / 'SOURCE_COMMIT').read_text().strip()) == 40, 'Missing source commit')
    env = json.loads((run / 'environment.json').read_text())
    require(env['execution_mode'] == mode, 'Environment mode')
    hosts, job = env['allocated_hosts'], env['coordinator']['job_id'] or ''
    if mode == 'perlmutter':
        require(len(hosts) == len(set(hosts)) == 4 and bool(job), 'Allocation scope')
    with (run / 'results.csv').open(newline='') as stream:
        reader = csv.DictReader(stream)
        require(reader.fieldnames == COLUMNS, 'CSV schema')
        rows = list(reader)
    require(len(rows) == len(plan) == len(list(run.glob('trial-[0-9][0-9][0-9][0-9].json'))),
            'Trial files/count')
    require(len(list(run.glob('trial-*-node-*.json'))) == sum(p['nodes'] for p in plan), 'Node file count')
    count, versions = 0, set()
    for row, item in zip(rows, plan):
        name = f'trial-{item["trial"]:04d}.json'
        require(row['raw_file'] == name, 'Raw reference')
        raw = json.loads((run / name).read_text())
        batches, runtime = audit_trial(run, raw, item, cfg, mode, hosts, job)
        count += batches
        versions |= runtime
        for key in COLUMNS:
            if key == 'raw_file':
                continue
            value = raw.get(key, '')
            if isinstance(value, (float, int)) and not isinstance(value, bool):
                close(row[key], value, 'CSV mismatch: ' + key)
            else:
                require(row[key] == str(value), 'CSV mismatch: ' + key)
    require(len(versions) == 1, 'Runtime changed within study')
    receipt = dict(success=True, protocol=PROTOCOL, execution_mode=mode, trials=len(plan),
                   measured=phases['measured'], warmups=phases['warmup'], batches_recomputed=count,
                   checks=['source/config/plan', 'physical node placement', 'shard ownership',
                           'record counts and payload spot checks', 'global/local clock separation',
                           'checkpoint completion', 'batch latency recomputation', 'CSV/raw agreement'],
                   limitations=['One allocation; within-allocation repetitions only.',
                                'Coordinator timing includes TCP control overhead.',
                                'Sequential checkpoint per node; concurrent checkpoint across nodes.',
                                'No cross-shard read, merge, transaction or persistent tmpfs export.',
                                'Local simulation is never multi-node evidence.'])
    write_json(run / 'validation.json', receipt)
    with (run / 'SHA256SUMS').open('w') as stream:
        for p in sorted(run.rglob('*')):
            if p.is_file() and p.name != 'SHA256SUMS':
                stream.write(f'{digest(p)}  {p.relative_to(run).as_posix()}\n')
    print(f'PASS: {len(plan)} coordinated trials; {count} batch timings; nodes, counts, '
          f'checkpoints, CSV and sources verified ({mode})', flush=True)
    return receipt


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('run')
    parser.add_argument('--allow-local', action='store_true')
    args = parser.parse_args()
    validate(args.run, args.allow_local)
