#!/usr/bin/env python3
"""Validate one prepared size-sweep run against its configuration and raw data."""
import argparse
from collections import Counter
import csv
import hashlib
import json
import math
from pathlib import Path
import sys
import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from experiment import build_plan


def require(ok, message):
    if not ok:
        raise ValueError(message)


def verify(log, config, job):
    paths = [line.split('=', 1)[1].strip() for line in Path(log).read_text().splitlines()
             if line.startswith('RESULTS=')]
    require(len(paths) == 1, 'Expected exactly one RESULTS directory')
    run = Path(paths[0]).resolve()
    require(run.is_relative_to(ROOT / 'results'), 'Result outside frozen study')
    cfg = yaml.safe_load(Path(config).read_text())
    require(yaml.safe_load((run / 'config.yaml').read_text()) == cfg, 'Saved config mismatch')
    plan = json.loads((run / 'plan.json').read_text())
    require(plan == build_plan(cfg) and len(plan) == 36, 'Plan mismatch')
    with (run / 'results.csv').open() as stream:
        rows = list(csv.DictReader(stream))
    require(len(rows) == 36, 'Incomplete CSV')
    require(Counter(r['phase'] for r in rows) == {'measured': 30, 'warmup': 6}, 'Phase count')
    require(len({r['raw_file'] for r in rows}) == 36, 'Duplicate raw file')
    hashes = json.loads((run / 'source_hashes.json').read_text())
    require(bool(hashes) and 'experiment.py' in hashes, 'Missing source provenance')
    for name, digest in hashes.items():
        path = (ROOT / name).resolve()
        require(path.is_relative_to(ROOT), 'Invalid source path')
        require(hashlib.sha256(path.read_bytes()).hexdigest() == digest, f'Source mismatch: {name}')
    hosts = set()
    for number, (row, item) in enumerate(zip(rows, plan), 1):
        path = (run / row['raw_file']).resolve()
        require(path.is_relative_to(run), 'Invalid raw path')
        raw = json.loads(path.read_text())
        label = f'trial {number}'
        require(raw['success'] is True and row['success'] == 'True', label + ': failure')
        require(raw['trial'] == int(row['trial']) == number, label + ': index')
        require(str(raw['slurm_job_id']) == str(job), label + ': job')
        hosts.add(raw['hostname'])
        for key in ('database', 'runtime', 'workload'):
            require(raw[key] == row[key] == cfg[key], label + ': ' + key)
        for key in ('phase', 'repetition'):
            require(raw[key] == item[key] and row[key] == str(item[key]), label + ': ' + key)
        for key in ('storage', 'clients', 'seed'):
            require(raw['config'][key] == item[key] and row[key] == str(item[key]), label + ': ' + key)
        for key in ('records', 'batch_size', 'read_queries', 'query_window', 'timeout'):
            require(raw['config'][key] == cfg[key], label + ': config ' + key)
        require(raw['storage_info']['filesystem'] == item['storage'], label + ': filesystem')
        expected = {'committed_records': cfg['records'], 'stored_records': cfg['records'],
                    'read_queries_completed': cfg['read_queries'],
                    'read_rows_returned': cfg['read_queries'] * cfg['query_window']}
        for key, value in expected.items():
            require(raw[key] == int(row[key]) == value, label + ': ' + key)
        for seconds, rate, amount in (
            ('completion_wall_s', 'completion_throughput_records_s', cfg['records']),
            ('read_wall_s', 'read_queries_per_s', cfg['read_queries'])):
            value = raw[seconds]
            require(math.isfinite(value) and value > 0, label + ': timing')
            require(math.isclose(value, float(row[seconds]), rel_tol=1e-8, abs_tol=1e-8), label + ': CSV timing')
            require(math.isclose(raw[rate], amount/value, rel_tol=1e-8), label + ': rate')
            require(math.isclose(raw[rate], float(row[rate]), rel_tol=1e-8), label + ': CSV rate')
        batches = (cfg['records'] + cfg['batch_size'] - 1) // cfg['batch_size']
        for key, count in (('transaction_latency_samples_ms', batches),
                           ('read_latency_samples_ms', cfg['read_queries'])):
            values = raw[key]
            require(len(values) == count and all(math.isfinite(v) and v >= 0 for v in values), label + ': samples')
        if cfg['runtime'] == 'shifter':
            require(raw['shifter_image_id'] == cfg['shifter_image_id'], label + ': image')
        if cfg['database'] in ('postgresql', 'influxdb'):
            require(raw['server_image_id'] == cfg['server_image_id'], label + ': server image')
            require(not raw['config']['server_profiling'], label + ': profiling enabled')
        if cfg['database'] == 'influxdb':
            require(raw['server_launcher_exit_code'] == raw['server_exit_code'] == 0, label + ': shutdown')
            require(raw['influx_validation_timeout_s'] == cfg.get('influx_validation_timeout_s', 30), label + ': validation timeout')
    require(len(hosts) == 1, 'Multiple hosts within task')
    if cfg['workload'] == 'mixed':
        from scripts.validate_mixed_results import verify as verify_mixed
        verify_mixed(run, str(job))
    receipt = dict(success=True, job=str(job), hostname=next(iter(hosts)),
                   trials=36, measured=30, warmups=6, results=str(run),
                   source_commit=(ROOT / 'SOURCE_COMMIT').read_text().strip(),
                   config_sha256=hashlib.sha256(Path(config).read_bytes()).hexdigest(),
                   validator_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest())
    (run / 'size-validation.json').write_text(json.dumps(receipt, indent=2) + '\n')
    print(f'PASS: {Path(config).name} — 30 measured + 6 warmups; counts, timing, samples and sources verified', flush=True)
    return receipt


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--log', required=True)
    parser.add_argument('--config', required=True)
    parser.add_argument('--job', required=True)
    args = parser.parse_args()
    verify(args.log, args.config, args.job)
