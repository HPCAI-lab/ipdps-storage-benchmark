#!/usr/bin/env python3
"""Coordinated per-worker SQLite shards across Slurm nodes; stdlib only.

All benchmark SQL, row generation and worker batch timings reuse the existing
sqlite_mechanism_study.py unchanged. TCP carries coordination messages, not SQL.
"""
import argparse
import csv
from datetime import datetime, timezone
import hashlib
import json
import math
import multiprocessing as mp
from multiprocessing.connection import wait
import os
from pathlib import Path
import random
import secrets
import select
import shutil
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import time
import traceback

import sqlite_mechanism_study as core

ROOT = Path(__file__).resolve().parents[1]
PROTOCOL = 'sqlite-multinode-v1'
CONFIG = 'configs/sqlite-multinode/partitioned.json'
BASE_SHA256 = '3f274160f5d208e2445841a53402a39faddd51a123a5b8c8baf40d10d41ec89f'
SOURCE_FILES = ['scripts/sqlite_mechanism_study.py', 'scripts/sqlite_multinode.py',
                'scripts/validate_sqlite_multinode.py', 'scripts/prepare_sqlite_multinode.py',
                'scripts/export_sqlite_multinode.py', 'tests/test_sqlite_multinode.py',
                CONFIG, 'hpc/run_sqlite_multinode.slurm', 'SQLITE_MULTINODE_STUDY.md']
COLUMNS = ['protocol', 'execution_mode', 'trial', 'phase', 'repetition', 'seed',
           'storage', 'layout', 'nodes', 'clients', 'records', 'batch_size',
           'success', 'slurm_job_id', 'hosts', 'database_files', 'stored_records',
           'transactions', 'write_wall_s', 'coordination_gap_s', 'checkpoint_wall_s',
           'completion_wall_s', 'start_fanout_s', 'write_records_s',
           'completion_records_s', 'worker_cpu_s', 'worker_cpu_core_equivalents',
           'trial_lifecycle_wall_s', *core.METRIC_COLUMNS, 'raw_file', 'error']
require = core.require
write_json = core.write_json
digest = core.digest


def build_plan(cfg, fixed=True):
    require(cfg['protocol'] == PROTOCOL, 'Unknown multi-node protocol')
    for key in ('records', 'clients', 'batch_size', 'seed', 'repetitions',
                'startup_timeout_s', 'trial_timeout_s', 'busy_timeout_ms'):
        require(type(cfg[key]) is int and cfg[key] > 0, f'Invalid {key}')
    require(type(cfg['warmup_repetitions']) is int and cfg['warmup_repetitions'] >= 0,
            'Invalid warmups')
    require(cfg['nodes'] and len(set(cfg['nodes'])) == len(cfg['nodes']), 'Invalid node list')
    require(all(type(n) is int and n > 0 and cfg['clients'] % n == 0 for n in cfg['nodes']),
            'Every node count must divide total clients')
    require(0 < cfg['clients'] <= 64 and cfg['records'] <= 10_000_000, 'Work limit')
    require(math.ceil(cfg['records'] / cfg['batch_size']) >= cfg['clients'], 'Empty worker')
    require(cfg['storage'] == ['lustre', 'tmpfs'], 'Storage matrix mismatch')
    for key, value in dict(workload='metadata_inserts_only', read_queries=0,
                           journal_mode='WAL', synchronous='NORMAL',
                           wal_autocheckpoint_pages=1000).items():
        require(cfg[key] == value, f'Unexpected {key}')
    if fixed:
        for key, value in dict(records=10_000_000, clients=64, nodes=[1, 2, 4],
                               batch_size=1000, repetitions=5, warmup_repetitions=1,
                               busy_timeout_ms=60000).items():
            require(cfg[key] == value, f'Fixed design mismatch: {key}')
    plan = []
    for phase, count in [('warmup', cfg['warmup_repetitions']), ('measured', cfg['repetitions'])]:
        for rep in range(1, count + 1):
            offset = 1_000_000 if phase == 'warmup' else 0
            conditions = [(n, t) for n in cfg['nodes'] for t in cfg['storage']]
            random.Random(cfg['seed'] + offset + rep).shuffle(conditions)
            for nodes, tier in conditions:
                plan.append(dict(trial=len(plan) + 1, phase=phase, repetition=rep,
                                 seed=cfg['seed'] + offset + rep * 100_000,
                                 storage=tier, layout='partitioned', nodes=nodes,
                                 clients=cfg['clients'], records=cfg['records']))
    return plan


def selected_hosts(hosts, item):
    ordered = list(hosts)
    random.Random(item['seed'] + 719).shuffle(ordered)
    return ordered[:item['nodes']]


def send(sock, obj):
    data = json.dumps(obj, allow_nan=False, separators=(',', ':')).encode()
    require(len(data) < 8 * 1024 * 1024, 'Control message too large')
    sock.sendall(struct.pack('!I', len(data)) + data)


def receive(sock):
    def exact(n):
        chunks = bytearray()
        while len(chunks) < n:
            data = sock.recv(n - len(chunks))
            if not data:
                raise EOFError('Node connection closed')
            chunks.extend(data)
        return bytes(chunks)
    n = struct.unpack('!I', exact(4))[0]
    require(0 < n < 8 * 1024 * 1024, 'Invalid control frame')
    value = json.loads(exact(n))
    require(isinstance(value, dict), 'Invalid control object')
    return value


def receive_all(peers, kind, timeout):
    pending = dict(peers)
    records, stamps = {}, {}
    deadline = time.monotonic() + timeout
    while pending:
        left = deadline - time.monotonic()
        if left <= 0:
            raise TimeoutError(f'Nodes {sorted(pending)} did not send {kind}')
        readable, _, _ = select.select(list(pending.values()), [], [], min(left, 1))
        for sock in readable:
            rank = next(k for k, v in pending.items() if v is sock)
            sock.settimeout(max(.001, deadline - time.monotonic()))
            value = receive(sock)
            stamp = time.perf_counter_ns()
            require(value.get('rank') == rank, 'Node rank mismatch')
            if value.get('kind') == 'error':
                raise RuntimeError(f'Node {rank}: {value.get("error")}')
            require(value.get('kind') == kind, f'Expected node {kind}, got {value.get("kind")}')
            records[rank], stamps[rank] = value, stamp
            del pending[rank]
    return [records[i] for i in range(len(peers))], [stamps[i] for i in range(len(peers))]


def collect_workers(channels, kind, timeout):
    pending, result = dict(channels), {}
    deadline = time.monotonic() + timeout
    while pending:
        left = deadline - time.monotonic()
        if left <= 0:
            raise TimeoutError(f'Workers {sorted(pending)} did not send {kind}')
        for pipe in wait(list(pending.values()), min(left, 1)):
            worker_id = next(k for k, v in pending.items() if v is pipe)
            value = pipe.recv()
            require(value.get('worker') == worker_id, 'Worker identity mismatch')
            if value.get('kind') == 'error':
                raise RuntimeError(json.dumps(value))
            require(value.get('kind') == kind, f'Expected worker {kind}')
            result[worker_id] = value
            del pending[worker_id]
    return [result[k] for k in sorted(result)]


def stop_processes(processes):
    for process in processes:
        if isinstance(process, subprocess.Popen):
            if process.poll() is None:
                try:
                    os.killpg(process.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait(timeout=10)
        else:
            if process.is_alive():
                process.terminate()
                process.join(5)
            if process.is_alive():
                process.kill()
                process.join(5)


def node_main(control_path, local_rank=None):
    control = json.loads(Path(control_path).read_text())
    cfg, item, local = control['config'], control['item'], control['local_test']
    require(local or socket.gethostname().startswith('nid'), 'Worker must run on compute node')
    rank = local_rank if local else int(os.environ['SLURM_PROCID'])
    require(type(rank) is int and 0 <= rank < item['nodes'], 'Bad node rank')
    mode = 'local_test' if local else 'perlmutter'
    ids = list(range(rank, item['clients'], item['nodes']))
    path = Path(control['run']) / f'trial-{item["trial"]:04d}-node-{rank:02d}.json'
    report = dict(protocol=PROTOCOL, execution_mode=mode, **item, rank=rank,
                  success=False, worker_ids=ids, batch_size=cfg['batch_size'],
                  hostname=socket.gethostname(), slurm_job_id=os.getenv('SLURM_JOB_ID', ''),
                  worker_source_sha256=digest(ROOT / 'scripts/sqlite_mechanism_study.py'))
    peersock, directory = None, None
    processes, keepers, channels = [], {}, {}
    lifecycle = time.perf_counter()
    phase = 'connect_to_coordinator'
    try:
        require(report['worker_source_sha256'] == BASE_SHA256, 'Original worker source changed')
        peersock = socket.create_connection((control['host'], control['port']), cfg['startup_timeout_s'])
        peersock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        peersock.settimeout(cfg['trial_timeout_s'] + cfg['startup_timeout_s'])
        send(peersock, dict(kind='hello', rank=rank, token=control['token'], hostname=report['hostname']))
        phase = 'initialize_shards'
        report['environment'] = core.environment()
        root = control['storage_roots'][item['storage']]
        report['storage_info'] = core.describe_storage(root, item['storage'], local)
        # This hook exists only for the explicit local failure-path test.
        if local and control.get('test_fail_rank') == rank:
            raise RuntimeError('Injected node failure for local test')
        directory = Path(tempfile.mkdtemp(prefix=f'ipdps-multinode-r{rank}-', dir=root))
        report['data_directory'] = str(directory)
        paths = {i: directory / f'worker-{i:03d}.db' for i in ids}
        for i in ids:
            keepers[i] = core.initialize(paths[i], cfg['busy_timeout_ms'])
        report['database_settings'] = [dict(worker=i, file=paths[i].name, **core.settings(keepers[i]))
                                       for i in ids]
        context = mp.get_context('spawn')
        start_event, publish_event = context.Event(), context.Event()
        for i in ids:
            parent, child = context.Pipe(duplex=False)
            process = context.Process(target=core.worker, args=(i, item, cfg, str(paths[i]),
                                                               start_event, publish_event, child))
            process.start()
            child.close()
            processes.append(process)
            channels[i] = parent
        report['worker_ready'] = collect_workers(channels, 'ready', cfg['startup_timeout_s'])
        phase = 'global_start_barrier'
        send(peersock, dict(kind='ready', rank=rank, workers=ids, hostname=report['hostname']))
        require(receive(peersock)['kind'] == 'start', 'Expected global start')
        start = time.perf_counter_ns()
        start_event.set()
        phase = 'write'
        completed = collect_workers(channels, 'done', cfg['trial_timeout_s'])
        report['workers_before_failure'] = completed
        finish = max(w['finished_ns'] for w in completed)
        phase = 'global_write_barrier'
        send(peersock, dict(kind='done', rank=rank, records=sum(w['committed_records'] for w in completed)))
        require(receive(peersock)['kind'] == 'checkpoint', 'Expected global checkpoint release')
        phase = 'checkpoint'
        cp_start = time.perf_counter_ns()
        checkpoints = []
        for i in ids:
            t = time.perf_counter_ns()
            status = list(keepers[i].execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchone())
            checkpoints.append(dict(worker=i, file=paths[i].name, status=status,
                                    wall_s=(time.perf_counter_ns() - t) / 1e9))
            require(status == [0, 0, 0], 'Final checkpoint incomplete')
        cp_end = time.perf_counter_ns()
        report.update(checkpoints=checkpoints, local_timing_ns=dict(
            start=start, last_commit_return=finish, checkpoint_start=cp_start, checkpoint_end=cp_end))
        send(peersock, dict(kind='checkpointed', rank=rank, files=len(ids)))
        phase = 'global_checkpoint_barrier'
        require(receive(peersock)['kind'] == 'collect', 'Expected collection release')
        publish_event.set()
        phase = 'samples_and_validation'
        samples = collect_workers(channels, 'samples', cfg['trial_timeout_s'])
        report['workers'] = [dict(done, batches=sample['batches']) for done, sample in zip(completed, samples)]
        report.pop('workers_before_failure')
        for process in processes:
            process.join(10)
            require(process.exitcode == 0, f'Worker exit {process.exitcode}')
        validation_start = time.perf_counter()
        report['validation'] = [dict(worker=i, file=paths[i].name,
                                     **core.validate_database(keepers[i], item, cfg, i)) for i in ids]
        report['validation_wall_s'] = time.perf_counter() - validation_start
        report['stored_records'] = sum(v['records'] for v in report['validation'])
        require(report['stored_records'] == sum(w['committed_records'] for w in completed),
                'Node stored count mismatch')
        for conn in keepers.values():
            conn.close()
        keepers.clear()
        shutil.rmtree(directory)
        report['data_cleanup'] = 'removed_after_validation'
        report['success'] = True
    except BaseException as exc:
        report.update(error=repr(exc), failure_phase=phase, traceback=traceback.format_exc())
        report['data_cleanup'] = 'preserved_failure_data_if_storage_survives'
    finally:
        stop_processes(processes)
        for channel in channels.values():
            channel.close()
        for conn in keepers.values():
            conn.close()
        report['node_lifecycle_wall_s'] = time.perf_counter() - lifecycle
        write_json(path, report)
        if peersock:
            try:
                send(peersock, dict(kind='result' if report['success'] else 'error', rank=rank,
                                    raw_file=path.name, sha256=digest(path), error=report.get('error', '')))
            except (OSError, EOFError):
                pass
            peersock.close()
    return 0 if report['success'] else 1


def run_trial(item, cfg, run, hosts, roots, local=False, test_fail_rank=None):
    lifecycle = time.perf_counter()
    selected = selected_hosts(hosts, item)
    report = dict(protocol=PROTOCOL, execution_mode='local_test' if local else 'perlmutter',
                  **item, batch_size=cfg['batch_size'], success=False,
                  slurm_job_id=os.getenv('SLURM_JOB_ID', ''), selected_hosts=selected,
                  hosts=','.join(selected), coordinator_hostname=socket.gethostname(),
                  timing_scope='coordinator_monotonic_release_to_all_completion_messages',
                  checkpoint_policy='global_write_barrier_then_sequential_per_node_parallel_across_nodes',
                  clock_policy='coordinator_global_clock_and_separate_node_local_clocks')
    raw_path = run / f'trial-{item["trial"]:04d}.json'
    control_path = run / f'.control-{item["trial"]:04d}.json'
    processes, peers, logfile = [], {}, None
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    phase = 'launch_and_connect'
    try:
        listener.bind(('127.0.0.1' if local else '0.0.0.0', 0))
        listener.listen(8)
        listener.settimeout(cfg['startup_timeout_s'])
        control = dict(config=cfg, item=item, local_test=local, run=str(run),
                       storage_roots=roots, host='127.0.0.1' if local else socket.gethostname(),
                       port=listener.getsockname()[1], token=secrets.token_hex(32))
        if local and test_fail_rank is not None:
            control['test_fail_rank'] = test_fail_rank
        fd = os.open(control_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, 'w') as stream:
            json.dump(control, stream)
        logfile = (run / f'trial-{item["trial"]:04d}-step.log').open('w')
        env = dict(os.environ, PYTHONDONTWRITEBYTECODE='1', PYTHONUNBUFFERED='1')
        command = [sys.executable, str(Path(__file__).resolve()), 'node', '--control', str(control_path)]
        if local:
            for rank in range(item['nodes']):
                processes.append(subprocess.Popen(command + ['--local-rank', str(rank)],
                                                  stdout=logfile, stderr=subprocess.STDOUT,
                                                  env=env, start_new_session=True))
        else:
            launcher = ['srun', '--exact', f'--nodes={item["nodes"]}', f'--ntasks={item["nodes"]}',
                        '--ntasks-per-node=1', '--cpus-per-task=256', '--cpu-bind=none', '--mpi=none',
                        '--kill-on-bad-exit=1', '--nodelist=' + ','.join(selected)]
            report['launch_command'] = launcher + command
            processes.append(subprocess.Popen(launcher + command, stdout=logfile,
                                              stderr=subprocess.STDOUT, env=env, start_new_session=True))
        hello_hosts = {}
        deadline = time.monotonic() + cfg['startup_timeout_s']
        for _ in range(item['nodes']):
            listener.settimeout(max(.001, deadline - time.monotonic()))
            sock, _ = listener.accept()
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            sock.settimeout(max(.001, deadline - time.monotonic()))
            try:
                hello = receive(sock)
                rank = hello.get('rank')
                require(hello.get('kind') == 'hello' and
                        secrets.compare_digest(str(hello.get('token', '')), control['token']),
                        'Unauthenticated node connection')
                require(type(rank) is int and 0 <= rank < item['nodes'] and rank not in peers,
                        'Unexpected or duplicate node rank')
                peers[rank], hello_hosts[rank] = sock, hello['hostname']
            except BaseException:
                sock.close()
                raise
        report['node_hosts'] = [hello_hosts[i] for i in range(item['nodes'])]
        if not local:
            require(len(set(hello_hosts.values())) == item['nodes'] and
                    set(hello_hosts.values()) == set(selected), 'Physical-node placement mismatch')
        phase = 'worker_readiness'
        ready, _ = receive_all(peers, 'ready', cfg['startup_timeout_s'])
        for r in ready:
            require(r['workers'] == list(range(r['rank'], item['clients'], item['nodes'])),
                    'Node ownership mismatch')
        start = time.perf_counter_ns()
        for rank in sorted(peers):
            send(peers[rank], dict(kind='start'))
        fanout_end = time.perf_counter_ns()
        phase = 'write'
        done, done_receipts = receive_all(peers, 'done', cfg['trial_timeout_s'])
        finish = max(done_receipts)
        require(sum(v['records'] for v in done) == item['records'], 'Global committed count mismatch')
        cp_start = time.perf_counter_ns()
        phase = 'checkpoint'
        for rank in sorted(peers):
            send(peers[rank], dict(kind='checkpoint'))
        checkpointed, cp_receipts = receive_all(peers, 'checkpointed', cfg['trial_timeout_s'])
        cp_end = max(cp_receipts)
        require(sum(v['files'] for v in checkpointed) == item['clients'], 'Missing checkpoint files')
        report['coordinator_timing_ns'] = dict(start=start, start_fanout_end=fanout_end,
                                              write_done_receipts=done_receipts,
                                              last_write_receipt=finish, checkpoint_start=cp_start,
                                              checkpoint_done_receipts=cp_receipts,
                                              last_checkpoint_receipt=cp_end)
        report.update(write_wall_s=(finish - start) / 1e9,
                      coordination_gap_s=(cp_start - finish) / 1e9,
                      checkpoint_wall_s=(cp_end - cp_start) / 1e9,
                      completion_wall_s=(cp_end - start) / 1e9,
                      start_fanout_s=(fanout_end - start) / 1e9)
        for rank in sorted(peers):
            send(peers[rank], dict(kind='collect'))
        phase = 'samples_and_validation'
        results, _ = receive_all(peers, 'result', cfg['trial_timeout_s'])
        report['node_results'] = results
        for process in processes:
            require(process.wait(timeout=30) == 0, 'Node or Slurm step exited nonzero')
        node_data = []
        for r in results:
            expected_name = f'trial-{item["trial"]:04d}-node-{r["rank"]:02d}.json'
            require(r['raw_file'] == expected_name and digest(run / expected_name) == r['sha256'],
                    'Node evidence hash mismatch')
            data = json.loads((run / expected_name).read_text())
            require(data['success'] is True, 'Failed node')
            node_data.append(data)
        samples = [b for n in node_data for w in n['workers'] for b in w['batches']]
        workers = [w for n in node_data for w in n['workers']]
        report.update(database_files=len(workers), stored_records=sum(n['stored_records'] for n in node_data),
                      transactions=len(samples), worker_cpu_s=sum(w['user_cpu_s'] + w['system_cpu_s'] for w in workers))
        require(report['stored_records'] == item['records'], 'Global stored count mismatch')
        report['worker_cpu_core_equivalents'] = report['worker_cpu_s'] / report['write_wall_s']
        report['write_records_s'] = item['records'] / report['write_wall_s']
        report['completion_records_s'] = item['records'] / report['completion_wall_s']
        for key in core.SAMPLE_KEYS:
            for stat, value in core.summarize([b[key] for b in samples]).items():
                report[f'{key}_{stat}'] = value
        report['success'] = True
    except BaseException as exc:
        report.update(error=repr(exc), failure_phase=phase, traceback=traceback.format_exc())
    finally:
        for peer in peers.values():
            peer.close()
        listener.close()
        stop_processes(processes)
        if logfile:
            logfile.close()
        control_path.unlink(missing_ok=True)
        report['trial_lifecycle_wall_s'] = time.perf_counter() - lifecycle
        write_json(raw_path, report)
    return report


def save_sources(run, local):
    hashes = {rel: digest(ROOT / rel) for rel in SOURCE_FILES}
    require(hashes['scripts/sqlite_mechanism_study.py'] == BASE_SHA256, 'Worker source changed')
    if not local:
        require(hashes == json.loads((ROOT / 'SOURCE_SHA256.json').read_text()), 'Frozen source mismatch')
        shutil.copy2(ROOT / 'SOURCE_COMMIT', run / 'SOURCE_COMMIT')
    for rel in SOURCE_FILES:
        target = run / 'source' / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(ROOT / rel, target)
    write_json(run / 'source_hashes.json', hashes)


def run_study(cfg, output_root, roots, local=False):
    plan = build_plan(cfg, fixed=not local)
    if local:
        hosts = [f'LOCAL_TEST_SLOT_{i}' for i in range(max(cfg['nodes']))]
    else:
        require(os.getenv('SLURM_JOB_ID') and socket.gethostname().startswith('nid'),
                'Run only on allocated compute nodes')
        hosts = subprocess.check_output(['scontrol', 'show', 'hostnames', os.environ['SLURM_JOB_NODELIST']],
                                        text=True).split()
        require(len(hosts) == len(set(hosts)) == 4, 'Production design requires four allocated nodes')
        require((ROOT / 'SOURCE_SHA256.json').is_file(), 'Prepare committed snapshot first')
    output_root = Path(output_root).resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    run = Path(tempfile.mkdtemp(prefix=f'sqlite-multinode-{stamp}-', dir=output_root))
    print('RESULTS=' + str(run), flush=True)
    write_json(run / 'config.json', cfg)
    write_json(run / 'plan.json', plan)
    write_json(run / 'environment.json', dict(coordinator=core.environment(), allocated_hosts=hosts,
                                              execution_mode='local_test' if local else 'perlmutter'))
    save_sources(run, local)
    completion = dict(success=False, expected_trials=len(plan), automatic_retries=0,
                      execution_mode='local_test' if local else 'perlmutter')
    try:
        with (run / 'results.csv').open('w', newline='') as stream:
            writer = csv.DictWriter(stream, fieldnames=COLUMNS, lineterminator='\n')
            writer.writeheader()
            for item in plan:
                report = run_trial(item, cfg, run, hosts, roots, local)
                report['raw_file'] = f'trial-{item["trial"]:04d}.json'
                writer.writerow({k: report.get(k, '') for k in COLUMNS})
                stream.flush()
                os.fsync(stream.fileno())
                completion['trials'] = item['trial']
                print(f'[{item["trial"]}/{len(plan)}] {"PASS" if report["success"] else "FAIL"} '
                      f'{item["phase"]} nodes={item["nodes"]} {item["storage"]}', flush=True)
                require(report['success'], report.get('error', 'Trial failed'))
        completion.update(success=True, measured=sum(p['phase'] == 'measured' for p in plan),
                          warmups=sum(p['phase'] == 'warmup' for p in plan))
    except BaseException as exc:
        completion['error'] = repr(exc)
        raise
    finally:
        write_json(run / 'completion.json', completion)
    from validate_sqlite_multinode import validate
    validate(run, allow_local=local)
    return run


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=['dry-run', 'run', 'local-smoke', 'node'])
    parser.add_argument('--config', default=str(ROOT / CONFIG))
    parser.add_argument('--output-root', default=str(ROOT / 'results'))
    parser.add_argument('--control')
    parser.add_argument('--local-rank', type=int)
    args = parser.parse_args()
    if args.command == 'node':
        return node_main(args.control, args.local_rank)
    cfg = json.loads(Path(args.config).read_text())
    plan = build_plan(cfg)
    if args.command == 'dry-run':
        print('READY: 30 measured + 6 warmups; 10M total records; 64 total writers/files; nodes=1/2/4')
        print('REQUEST: one four-node allocation, 30-minute limit = 2 requested CPU node-hours')
        return 0
    if args.command == 'local-smoke':
        require(not os.getenv('SLURM_JOB_ID'), 'Local smoke is not Perlmutter evidence')
        cfg.update(records=103, clients=4, nodes=[1, 2], batch_size=16, repetitions=1,
                   warmup_repetitions=0, startup_timeout_s=30, trial_timeout_s=30)
        with tempfile.TemporaryDirectory(prefix='sqlite-multinode-local-') as root:
            run_study(cfg, args.output_root, dict(lustre=root, tmpfs=root), local=True)
    else:
        require(os.getenv('SCRATCH'), 'SCRATCH is required')
        run = run_study(cfg, args.output_root, dict(lustre=os.environ['SCRATCH'], tmpfs='/tmp'))
        from export_sqlite_multinode import export
        export(run)
        print('SQLITE_MULTINODE_COMPLETE: 30 measured + 6 warmups validated and archived', flush=True)
    return 0


if __name__ == '__main__':
    sys.exit(main())
