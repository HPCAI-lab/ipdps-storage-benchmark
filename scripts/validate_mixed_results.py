#!/usr/bin/env python3
"""Validate a saved mixed run; reads results and writes a verification receipt."""
import argparse
import csv
import hashlib
import json
import math
import sys
from pathlib import Path
import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from experiment import build_plan
from mixed_workloads import query_ids, query_workload


def require(condition, context):
    if not condition:
        raise ValueError(context)


def verify(root, expected_job=None):
    root = Path(root).resolve()
    cfg = yaml.safe_load((root / 'config.yaml').read_text())
    require(cfg['workload'] == 'mixed', 'Expected mixed workload')
    plan = json.loads((root / 'plan.json').read_text())
    require(plan == build_plan(cfg), 'Saved plan mismatch')
    with (root / 'results.csv').open() as stream:
        rows = list(csv.DictReader(stream))
    require(len(rows) == len(plan), 'Incomplete CSV')
    hashes = json.loads((root / 'source_hashes.json').read_text())
    require('mixed_workloads.py' in hashes, 'Mixed source missing from provenance')
    for name, digest in hashes.items():
        path = (ROOT / name).resolve()
        require(path.is_relative_to(ROOT), 'Invalid source path')
        require(hashlib.sha256(path.read_bytes()).hexdigest() == digest,
                f'Source changed since run: {name}')
    hosts, jobs = set(), set()
    batches = (cfg['records'] + cfg['batch_size'] - 1) // cfg['batch_size']
    for trial, (row, item) in enumerate(zip(rows, plan), 1):
        path = (root / row['raw_file']).resolve()
        require(path.is_relative_to(root), 'Invalid raw path')
        raw = json.loads(path.read_text())
        tc = raw['config']
        label = f'trial={trial}'
        require(raw['success'] is True and row['success'] == 'True', label + ': failed')
        require(raw['trial'] == trial == int(row['trial']), label + ': index')
        require(raw['workload'] == row['workload'] == 'mixed', label + ': workload')
        require(raw['database'] == cfg['database'] and raw['runtime'] == cfg['runtime'], label + ': deployment')
        for key in ('phase', 'repetition'):
            require(raw[key] == item[key] and str(raw[key]) == row[key], label + ': ' + key)
        for key in ('storage', 'clients', 'seed'):
            require(tc[key] == item[key] and str(tc[key]) == row[key], label + ': ' + key)
        for key in ('records', 'batch_size', 'read_queries', 'query_window', 'timeout'):
            require(tc[key] == cfg[key], label + ': configuration ' + key)
        require(raw['storage_info']['filesystem'] == tc['storage'], label + ': filesystem')
        expected = {
            'committed_records': cfg['records'], 'stored_records': cfg['records'],
            'stored_metadata_records': cfg['records'] // 2,
            'stored_telemetry_records': cfg['records'] // 2,
            'logical_write_batches': batches, 'backend_write_calls': batches * 2,
            'write_batches': batches * 2,
            'read_queries_completed': cfg['read_queries'],
            'metadata_read_queries': cfg['read_queries'] // 2,
            'telemetry_read_queries': cfg['read_queries'] // 2,
            'read_rows_returned': cfg['read_queries'] * cfg['query_window'],
        }
        for key, value in expected.items():
            require(raw[key] == value == int(row[key]), label + ': count ' + key)
        if cfg['database'] == 'influxdb':
            require(raw['transactions'] is None and row['transactions'] == '', label + ': HTTP is not SQL transaction')
            require(raw['server_launcher_exit_code'] == raw['server_exit_code'] == 0, label + ': shutdown')
            require(raw['final_checkpoint_s'] is None, label + ': Influx checkpoint boundary')
        else:
            require(raw['transactions'] == batches * 2 == int(row['transactions']), label + ': SQL transactions')
            require(raw['final_checkpoint_s'] >= 0, label + ': checkpoint')
        require(raw['write_latency_unit'] == row['write_latency_unit'] ==
                'logical_mixed_batch_two_nonatomic_backend_calls', label + ': latency scope')
        require(raw['mixed_policy'] == row['mixed_policy'], label + ': mixed policy')
        expected_workers = [sum(min(cfg['batch_size'], cfg['records'] - b * cfg['batch_size'])
                                for b in range(w, batches, tc['clients'])) for w in range(tc['clients'])]
        require(raw['worker_committed_records'] == expected_workers, label + ': write distribution')
        expected_queries = []
        for worker in range(tc['clients']):
            ids = list(query_ids(tc, worker))
            expected_queries.append({'worker': worker,
                'metadata': sum(query_workload(tc, q) == 'metadata' for q in ids),
                'telemetry': sum(query_workload(tc, q) == 'telemetry' for q in ids)})
        require(raw['read_worker_queries_by_workload'] == expected_queries, label + ': read composition')
        require(raw['worker_read_queries'] == [q['metadata'] + q['telemetry'] for q in expected_queries], label + ': read distribution')
        for samples_key, prefix, count in (
            ('transaction_latency_samples_ms', 'transaction_latency', batches),
            ('read_latency_samples_ms', 'read_latency', cfg['read_queries'])):
            values = sorted(raw[samples_key])
            require(len(values) == count and all(math.isfinite(v) and v >= 0 for v in values), label + ': latency samples')
            summaries = {'mean': sum(values) / len(values)}
            for pct in (50, 95, 99):
                pos = (len(values)-1) * pct / 100
                lo, hi = math.floor(pos), math.ceil(pos)
                summaries['p'+str(pct)] = values[lo] + (values[hi]-values[lo]) * (pos-lo)
            for suffix, value in summaries.items():
                key = prefix + '_' + suffix + '_ms'
                require(math.isclose(raw[key], value, rel_tol=1e-9, abs_tol=1e-9), label + ': raw ' + key)
                require(math.isclose(float(row[key]), value, rel_tol=1e-9, abs_tol=1e-9), label + ': CSV ' + key)
        for seconds, rate, numerator in (
            ('workload_wall_s', 'throughput_records_s', cfg['records']),
            ('completion_wall_s', 'completion_throughput_records_s', cfg['records']),
            ('read_wall_s', 'read_queries_per_s', cfg['read_queries'])):
            require(math.isfinite(raw[seconds]) and raw[seconds] > 0, label + ': ' + seconds)
            require(math.isclose(raw[seconds], float(row[seconds]), rel_tol=1e-9), label + ': CSV timing')
            require(math.isclose(raw[rate], numerator/raw[seconds], rel_tol=1e-9), label + ': throughput')
        if cfg['runtime'] == 'shifter':
            require(raw['shifter_image_id'] == cfg['shifter_image_id'], label + ': client image')
        if cfg['database'] in ('postgresql', 'influxdb'):
            require(raw['server_image_id'] == cfg['server_image_id'], label + ': server image')
        if expected_job:
            require(raw['slurm_job_id'] == expected_job, label + ': job')
        hosts.add(raw['hostname']); jobs.add(raw['slurm_job_id'])
    require(len(hosts) == len(jobs) == 1, 'Inconsistent host/job')
    receipt = {'success': True, 'database': cfg['database'], 'runtime': cfg['runtime'],
               'trials': len(rows), 'records_per_trial': cfg['records'],
               'metadata_records_per_trial': cfg['records']//2,
               'telemetry_records_per_trial': cfg['records']//2,
               'host': next(iter(hosts)), 'job': next(iter(jobs)),
               'validator_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
    (root / 'mixed-validation.json').write_text(json.dumps(receipt, indent=2)+'\n')
    print(f"PASS: {cfg['database']} {cfg['runtime']} — {len(rows)} mixed trials; composition, counts, timings and sources verified", flush=True)
    return receipt


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--log', type=Path)
    parser.add_argument('--results', type=Path)
    parser.add_argument('--job')
    args = parser.parse_args()
    if bool(args.log) == bool(args.results):
        parser.error('Provide exactly one of --log or --results')
    if args.log:
        roots = [line.split('=',1)[1] for line in args.log.read_text().splitlines() if line.startswith('RESULTS=')]
        require(len(roots) == 1, 'Expected one RESULTS directory in log')
        root = roots[0]
    else:
        root = args.results
    verify(root, args.job)

if __name__ == '__main__': main()
