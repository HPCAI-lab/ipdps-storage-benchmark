#!/usr/bin/env python3
"""Analyze the eight saved Perlmutter pilots without running a benchmark.

Input is the original ipdps-pilot-analysis.zip. No source observations are
changed or dropped, except the declared warmups are excluded from estimates.
"""
import argparse
import csv
import hashlib
import io
import json
import math
from collections import Counter, defaultdict
from pathlib import Path
import platform
import zipfile

import numpy as np
import scipy
from scipy.stats import t
import yaml

DEPLOYMENTS = ('sqlite-native', 'sqlite-shifter', 'postgres', 'influx')
WORKLOADS = ('metadata', 'telemetry')
CLIENTS = (1, 16, 64)
PHASE_METRICS = {'write': 'completion_throughput_records_s', 'read': 'read_queries_per_s'}
IMAGE_IDS = {
    'sqlite-shifter': '366e975effc41801a615a88b44c609e98932ed1247531b4d1a777c4b70f3e6ac',
    'postgres': 'e4842c8a99ca99339e1693e6fe5fe62c7becb31991f066f989047dfb2fbf47af',
    'influx': 'db0bdab1e5ad5ee899c127b8c13d9c986a3ced78cd9200dd80a1064bf1533b6e',
}
EXPECTED_JOBS = {
    'sqlite-native-metadata': '59115833', 'sqlite-native-telemetry': '59115834',
    'sqlite-shifter-metadata': '59132736', 'sqlite-shifter-telemetry': '59132737',
    'postgres-metadata': '59136228', 'postgres-telemetry': '59136230',
    'influx-metadata': '59188702', 'influx-telemetry': '59188703',
}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def near(a, b):
    return math.isclose(float(a), float(b), rel_tol=1e-8, abs_tol=1e-8)


def descriptive(values):
    values = np.asarray(values, dtype=float)
    n = len(values)
    mean, sd = float(values.mean()), float(values.std(ddof=1))
    half = float(t.ppf(.975, n - 1) * sd / np.sqrt(n))
    return dict(n=n, mean=mean, sd=sd, median=float(np.median(values)),
                minimum=float(values.min()), maximum=float(values.max()),
                cv_percent=100 * sd / mean if mean else None,
                mean_t95_low=mean-half, mean_t95_high=mean+half)


def log_interval(log_values):
    """Exponentiated t interval on five repetition-block log contrasts."""
    s = descriptive(log_values)
    return dict(geometric_ratio=math.exp(s['mean']),
                log_t95_low=math.exp(s['mean_t95_low']),
                log_t95_high=math.exp(s['mean_t95_high']), n_blocks=s['n'])


def write_csv(path, records):
    require(bool(records), str(path) + ': empty table')
    fields = list(dict.fromkeys(k for row in records for k in row))
    with path.open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(records)


def load_data(source):
    all_rows, inventory, configs, source_hashes = [], [], {}, {}
    with zipfile.ZipFile(source) as archive:
        require(len(archive.namelist()) == len(set(archive.namelist())), 'Duplicate ZIP members')
        runs = json.loads(archive.read('runs.json'))
        require(set(runs) == set(EXPECTED_JOBS), 'Unexpected run set')
        for label, run_directory in runs.items():
            deployment, workload = label.rsplit('-', 1)
            cfg = yaml.safe_load(archive.read(label + '/config.yaml'))
            plan = json.loads(archive.read(label + '/plan.json'))
            metadata = json.loads(archive.read(label + '/trial-metadata.json'))
            rows = list(csv.DictReader(io.StringIO(archive.read(label + '/results.csv').decode())))
            hashes = json.loads(archive.read(label + '/source_hashes.json'))
            configs[label], source_hashes[label] = cfg, hashes
            require(len(rows) == len(plan) == len(metadata) == 36, label + ': trial count')
            require(cfg['workload'] == workload, label + ': workload')
            require(cfg['records'] == 1_000_000 and cfg['batch_size'] == 1000, label + ': workload size')
            queries = 1024 if deployment == 'influx' else 64000
            require(cfg['read_queries'] == queries and cfg['query_window'] == 100, label + ': query size')
            require(cfg['repetitions'] == 5 and cfg['warmup_repetitions'] == 1, label + ': repetitions')
            require(set(cfg['clients']) == set(CLIENTS) and set(cfg['storage']) == {'lustre', 'tmpfs'}, label + ': conditions')
            require(all(len(v) == 64 and all(c in '0123456789abcdef' for c in v) for v in hashes.values()), label + ': hashes')
            require({'experiment.py', 'concurrent_sqlite.py', 'pilot_workloads.py', 'read_phase.py', 'process_metrics.py'} <= set(hashes), label + ': missing source manifest entries')
            metabyfile = {m['raw_file']: m for m in metadata}
            require(len(metabyfile) == len(metadata), label + ': repeated raw file')
            seen, hosts, jobs, runtimes, pyversions, versions = Counter(), set(), set(), set(), set(), set()
            for number, (row, item) in enumerate(zip(rows, plan), 1):
                meta = metabyfile[row['raw_file']]
                require(int(row['trial']) == meta['trial'] == number, label + ': trial index')
                require(row['success'] == 'True' and meta['success'] is True and not row['error'], label + ': failed trial')
                require(row['workload'] == meta['workload'] == workload, label + ': workload mismatch')
                require(meta['slurm_job_id'] == EXPECTED_JOBS[label], label + ': job mismatch')
                require(row['raw_file'] == f'trial-{number:04d}.json', label + ': raw file name')
                for key in ('phase', 'repetition'):
                    require(str(meta[key]) == row[key] == str(item[key]), label + ': ' + key)
                for key in ('storage', 'clients', 'seed'):
                    require(row[key] == str(meta['config'][key]) == str(item[key]), label + ': ' + key)
                for key in ('records', 'batch_size', 'read_queries', 'query_window'):
                    require(int(row[key]) == meta['config'][key] == cfg[key], label + ': config ' + key)
                require(meta['storage_info']['label'] == meta['storage_info']['filesystem'] == row['storage'], label + ': filesystem')
                for key, expected in {'committed_records':1_000_000, 'stored_records':1_000_000, 'transactions':1000,
                                      'read_queries_completed':queries, 'read_rows_returned':100*queries}.items():
                    require(int(row[key]) == expected, label + ': ' + key)
                for key in ('workload_wall_s', 'completion_wall_s', 'read_wall_s', 'throughput_records_s',
                            'completion_throughput_records_s', 'read_queries_per_s'):
                    require(math.isfinite(float(row[key])) and float(row[key]) > 0, label + ': ' + key)
                for phase in ('transaction', 'read'):
                    latencies = [float(row[f'{phase}_latency_p{q}_ms']) for q in (50,95,99)]
                    require(all(math.isfinite(x) and x >= 0 for x in latencies) and latencies == sorted(latencies), label + ': latency quantiles')
                require(near(float(row['throughput_records_s'])*float(row['workload_wall_s']), 1_000_000), label + ': write rate')
                require(near(float(row['completion_throughput_records_s'])*float(row['completion_wall_s']), 1_000_000), label + ': completion rate')
                require(near(float(row['read_queries_per_s'])*float(row['read_wall_s']), queries), label + ': read rate')
                if deployment == 'influx':
                    require(row['final_checkpoint_s'] == '' and near(row['completion_wall_s'], row['workload_wall_s']), label + ': acknowledgement boundary')
                    require(row['server_exit_code'] == row['server_launcher_exit_code'] == '0', label + ': shutdown')
                    require(meta['write_completion_policy'] == 'synchronous_HTTP_204_acknowledgements', label + ': completion policy')
                else:
                    require(float(row['final_checkpoint_s']) >= 0 and float(row['completion_wall_s']) >= float(row['workload_wall_s']), label + ': checkpoint timing')
                if deployment in IMAGE_IDS:
                    field = 'shifter_image_id' if deployment == 'sqlite-shifter' else 'server_image_id'
                    require(cfg[field] == row[field] == IMAGE_IDS[deployment], label + ': image')
                if deployment.startswith('sqlite'):
                    require(meta['journal_mode'] == 'WAL' and meta['synchronous'] == 'NORMAL', label + ': SQLite settings')
                for phase in ('write','read'):
                    require(near(row[phase+'_worker_cpu_s'], float(row[phase+'_worker_user_cpu_s']) + float(row[phase+'_worker_system_cpu_s'])), label + ': worker CPU')
                seen[(row['phase'],int(row['repetition']),row['storage'],int(row['clients']))] += 1
                hosts.add(meta['hostname']); jobs.add(meta['slurm_job_id']); runtimes.add(meta['runtime']); pyversions.add(meta['python_version'])
                version = row.get('postgresql_version') or row.get('influxdb_version') or row.get('sqlite_version') or meta.get('sqlite_version')
                versions.add(version)
                unified = {**row, 'run_label':label, 'run_directory':run_directory, 'deployment':deployment,
                           'hostname':meta['hostname'], 'slurm_job_id':meta['slurm_job_id'], 'python_version':meta['python_version'],
                           'engine_version':version, 'timestamp_utc':meta['timestamp_utc'],
                           'checkpoint_policy':meta['checkpoint_policy'], 'read_cache_state':meta['read_cache_state'],
                           'runtime':meta['runtime']}
                all_rows.append(unified)
            expected = Counter({(phase,r,s,c):1 for phase,reps in (('warmup',[1]),('measured',range(1,6))) for r in reps for s in ('lustre','tmpfs') for c in CLIENTS})
            require(seen == expected and len(hosts) == len(jobs) == len(pyversions) == len(versions) == 1, label + ': coverage/environment')
            inventory.append(dict(run_label=label, run_directory=run_directory, deployment=deployment, workload=workload,
                                  job_id=next(iter(jobs)), hostname=next(iter(hosts)), python_version=next(iter(pyversions)),
                                  engine_version=next(iter(versions)), read_queries=queries, measured_trials=30, warmup_trials=6,
                                  source_file_hashes=len(hashes)))
    return all_rows, inventory, configs, source_hashes


def analyze(source, output):
    output.mkdir(parents=True, exist_ok=True)
    tables = output / 'tables'; tables.mkdir(exist_ok=True)
    rows, inventory, configs, hashes = load_data(source)
    measured = [r for r in rows if r['phase'] == 'measured']
    groups = defaultdict(list)
    for r in measured:
        groups[(r['deployment'],r['workload'],r['storage'],int(r['clients']))].append(r)
    for group in groups.values():
        group.sort(key=lambda r:int(r['repetition']))
        require([int(r['repetition']) for r in group] == list(range(1,6)), 'Unmatched repetitions')

    def values(d,w,s,c,metric):
        return np.array([float(r[metric]) for r in groups[(d,w,s,c)]])

    summaries=[]
    measures = list(PHASE_METRICS.values()) + ['completion_wall_s','workload_wall_s','read_wall_s']
    measures += [f'{phase}_latency_{name}_ms' for phase in ('transaction','read') for name in ('mean','p50','p95','p99')]
    measures += [f'{phase}_worker_{name}' for phase in ('write','read') for name in ('cpu_s','cpu_core_equivalents','peak_rss_max_kib','major_faults','voluntary_switches','involuntary_switches')]
    for (d,w,s,c), group in groups.items():
        for metric in measures:
            summaries.append(dict(deployment=d,workload=w,storage=s,clients=c,metric=metric,**descriptive(values(d,w,s,c,metric))))

    storage_gains=[]; scaling=[]; interactions=[]; paired=[]; runtime=[]
    for d in DEPLOYMENTS:
        for w in WORKLOADS:
            for phase,metric in PHASE_METRICS.items():
                for c in CLIENTS:
                    a,b = values(d,w,'tmpfs',c,metric), values(d,w,'lustre',c,metric)
                    logs=np.log(a/b)
                    ratio=float(a.mean()/b.mean())
                    storage_gains.append(dict(deployment=d,workload=w,phase=phase,clients=c,
                        ratio_of_arithmetic_means=ratio,storage_sensitivity=max(ratio,1/ratio)-1,
                        best_observed_mean_storage='tmpfs' if ratio>1 else 'lustre', **log_interval(logs)))
                    for rep, val in enumerate(a/b,1):
                        paired.append(dict(kind='storage_gain',deployment=d,workload=w,phase=phase,storage='tmpfs/lustre',clients=c,repetition=rep,ratio=float(val)))
                for s in ('lustre','tmpfs'):
                    for c in (16,64):
                        a,b=values(d,w,s,c,metric), values(d,w,s,1,metric)
                        scaling.append(dict(deployment=d,workload=w,phase=phase,storage=s,clients=c,
                            ratio_of_arithmetic_means=float(a.mean()/b.mean()), **log_interval(np.log(a/b))))
                        for rep,val in enumerate(a/b,1):
                            paired.append(dict(kind='concurrency_scaling',deployment=d,workload=w,phase=phase,storage=s,clients=c,repetition=rep,ratio=float(val)))
                for c in (16,64):
                    a,b=values(d,w,'tmpfs',c,metric),values(d,w,'lustre',c,metric)
                    x,y=values(d,w,'tmpfs',1,metric),values(d,w,'lustre',1,metric)
                    logs=np.log(a/b)-np.log(x/y)
                    interactions.append(dict(deployment=d,workload=w,phase=phase,clients=c,baseline_clients=1,
                        ratio_of_mean_ratios=float((a.mean()/b.mean())/(x.mean()/y.mean())), **log_interval(logs)))
                    for rep,val in enumerate(np.exp(logs),1):
                        paired.append(dict(kind='storage_concurrency_interaction',deployment=d,workload=w,phase=phase,storage='tmpfs/lustre',clients=c,repetition=rep,ratio=float(val)))

    for w in WORKLOADS:
        nc,sc=configs['sqlite-native-'+w],configs['sqlite-shifter-'+w]
        require({k:v for k,v in nc.items() if k!='runtime'} == {k:v for k,v in sc.items() if k not in ('runtime','shifter_image_id')}, 'SQLite runtime configs differ')
        for s in ('lustre','tmpfs'):
            for c in CLIENTS:
                for phase,metric in PHASE_METRICS.items():
                    a,b=values('sqlite-native',w,s,c,metric), values('sqlite-shifter',w,s,c,metric)
                    ratio=float(a.mean()/b.mean())
                    runtime.append(dict(workload=w,storage=s,clients=c,phase=phase,native_mean=float(a.mean()),shifter_mean=float(b.mean()),
                        native_over_shifter=ratio,shifter_over_native=1/ratio,
                        runtime_sensitivity=(float(a.mean())-float(b.mean()))/float(a.mean()),
                        inference_scope='descriptive_only_different_allocations_Python_and_SQLite_versions'))
    comparisons=[]
    for d in ('sqlite-shifter','postgres','influx'):
        a,b=hashes['sqlite-native-metadata'],hashes[d+'-metadata']
        comparisons.append(dict(comparison='sqlite-native vs '+d,
            equal_common_files=sorted(k for k in a.keys() & b.keys() if a[k]==b[k]),
            different_common_files=sorted(k for k in a.keys() & b.keys() if a[k]!=b[k]),
            only_in_later=sorted(b.keys()-a.keys())))
    for d in DEPLOYMENTS:
        require(hashes[d+'-metadata']==hashes[d+'-telemetry'], d+': within-deployment workload source manifests differ')
    audit=dict(status='PASS',input_sha256=hashlib.sha256(source.read_bytes()).hexdigest(),runs=8,
        recorded_allocations=len({r['job_id'] for r in inventory}),rows=len(rows),measured=len(measured),warmups=len(rows)-len(measured),conditions=len(groups),
        checks=['CSV/config/plan/metadata consistency','complete factorial coverage and unique repetition IDs','record and query counts reported in CSV',
                'positive finite timings and rate identities','ordered latency percentiles','worker CPU sums','pinned image IDs','one node and job per run',
                'equal metadata/telemetry source-hash manifests within each deployment'],
        limits=['Full raw trial JSONs and per-request latency samples were not included; counts are saved reported counts, not a fresh database reread.',
                'Source-hash manifests were compared; underlying source file bytes are not in the input ZIP.',
                'Profiling runs and their server-accounting files are not in this archive.',
                'Confidence intervals describe within-allocation repetitions under t-model assumptions; not between-node or between-allocation uncertainty.',
                'No outlier removal, no multiplicity-adjusted hypothesis tests, no global cross-engine ranking.'],
        source_manifest_comparisons=comparisons,
        dependencies=dict(python=platform.python_version(),numpy=np.__version__,scipy=scipy.__version__,PyYAML=yaml.__version__))
    outputs={'all_trials.csv':rows,'run_inventory.csv':inventory,'condition_summary.csv':summaries,
        'storage_gains.csv':storage_gains,'concurrency_scaling.csv':scaling,'storage_concurrency_interactions.csv':interactions,
        'sqlite_runtime_comparison.csv':runtime,'repetition_ratios.csv':paired}
    for name,data in outputs.items():write_csv(tables/name,data)
    (output/'validation.json').write_text(json.dumps(audit,indent=2)+'\n')
    print(json.dumps({k:v for k,v in audit.items() if k in ('status','runs','rows','measured','warmups','conditions')},indent=2))
    return dict(groups=groups,summary=summaries,storage_gains=storage_gains,scaling=scaling,interactions=interactions,runtime=runtime,inventory=inventory,audit=audit)


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('input',type=Path)
    parser.add_argument('--output',type=Path,default=Path('analysis-output'))
    args=parser.parse_args()
    analyze(args.input,args.output)
