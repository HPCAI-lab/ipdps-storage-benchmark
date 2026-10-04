#!/usr/bin/env python3
"""Validate six new raw SQL runs and compare with the eight-run baseline export.

Reads archives as data; never executes any bundled benchmark source.
"""
import argparse
import csv
import hashlib
import io
import itertools
import json
import math
import platform
import random
import zipfile
from collections import Counter, defaultdict
from pathlib import Path

import numpy as np
import scipy
import yaml
from baseline_reader import load_data, descriptive, log_interval, write_csv, IMAGE_IDS

SQL = ('sqlite-native', 'sqlite-shifter', 'postgres')
WORKLOADS = ('metadata', 'telemetry')
CLIENTS = (1, 16, 64)
STORAGE = ('lustre', 'tmpfs')
METRICS = {'write': 'completion_throughput_records_s', 'read': 'read_queries_per_s'}
RUNS = {
    ('sqlite-native','metadata'): ('59220317','runs/20261002T191358Z-c71f8af9'),
    ('sqlite-shifter','metadata'): ('59220317','runs/20261002T191719Z-a4afb9d5'),
    ('postgres','metadata'): ('59220317','runs/20261002T192035Z-99c36a78'),
    ('sqlite-native','telemetry'): ('59220318','runs/20261002T191358Z-38b4d399'),
    ('sqlite-shifter','telemetry'): ('59220318','runs/20261002T191848Z-8721f09e'),
    ('postgres','telemetry'): ('59220318','runs/20261002T192352Z-db4495ae'),
}

def check(ok, message):
    if not ok:
        raise ValueError(message)

def near(a, b):
    return math.isclose(float(a), float(b), rel_tol=1e-8, abs_tol=1e-8)

def plan_for(cfg):
    rng = random.Random(cfg['seed'])
    plan = []
    for phase, count in (('warmup', cfg['warmup_repetitions']), ('measured', cfg['repetitions'])):
        for rep in range(1, count + 1):
            conditions = list(itertools.product(cfg['storage'], cfg['clients']))
            rng.shuffle(conditions)
            seed = cfg['seed'] + (rep if phase == 'measured' else -rep)
            plan += [dict(phase=phase, repetition=rep, storage=s, clients=c, seed=seed) for s,c in conditions]
    return plan

def read_new(path):
    all_rows, inventory, configs, raw_trials, source_sets = [], [], {}, {}, {}
    sample_count = 0
    with zipfile.ZipFile(path) as z:
        names = z.namelist()
        check(len(names) == len(set(names)), 'Duplicate new ZIP members')
        check(z.testzip() is None, 'ZIP CRC failure')
        sums = {}
        for line in z.read('SHA256SUMS').decode().splitlines():
            digest, name = line.split('  ', 1)
            check(name not in sums, 'Repeated checksum member')
            sums[name] = digest
            check(hashlib.sha256(z.read(name)).hexdigest() == digest, 'Checksum mismatch: '+name)
        check(set(sums) == set(names)-{'SHA256SUMS'}, 'Incomplete SHA256SUMS coverage')
        commit = z.read('source-commit.txt').decode().strip()
        check(commit == '63c227bc25fc394d3a3752f3fe4f19ebc78fff65', 'Unexpected recorded source commit')
        manifest = json.loads(z.read('manifest.json'))
        check(len(manifest) == 6 and {(r['deployment'],r['workload']) for r in manifest} == set(RUNS), 'Unexpected run manifest')
        for run in manifest:
            d,w = run['deployment'],run['workload']
            job,directory = RUNS[(d,w)]
            check((run['job_id'],run['directory']) == (job,directory), 'Unexpected run identity')
            label = d+'-'+w
            cfg = yaml.safe_load(z.read(directory+'/config.yaml'))
            plan = json.loads(z.read(directory+'/plan.json'))
            rows = list(csv.DictReader(io.StringIO(z.read(directory+'/results.csv').decode())))
            hashes = json.loads(z.read(directory+'/source_hashes.json'))
            configs[label],source_sets[label] = cfg,hashes
            for key,val in dict(records=1000000,batch_size=1000,read_queries=1024,query_window=100,repetitions=5,warmup_repetitions=1,seed=20260814,workload=w).items():
                check(cfg[key] == val, label+': '+key)
            check(cfg['clients'] == list(CLIENTS) and cfg['storage'] == list(STORAGE), label+': conditions')
            check(not cfg.get('server_profiling',False) and not cfg.get('profiling_comparison',False), label+': profiling')
            check(cfg['database'] == ('postgresql' if d=='postgres' else 'sqlite'), label+': database')
            check(cfg['runtime'] == ('shifter' if d=='sqlite-shifter' else 'native'), label+': runtime')
            check(len(rows)==len(plan)==36 and plan==plan_for(cfg), label+': count/plan')
            check(hashes, label+': empty source manifest')
            for name,digest in hashes.items():
                check(hashlib.sha256(z.read('source/'+name)).hexdigest()==digest, label+': source '+name)
            coverage,hosts,versions,pyversions = Counter(),set(),set(),set()
            for number,(row,item) in enumerate(zip(rows,plan),1):
                trial_label = label+f': trial {number}'
                check(row['raw_file']==f'trial-{number:04d}.json', trial_label+': raw name')
                raw = json.loads(z.read(directory+'/'+row['raw_file']))
                tc = raw['config']
                check(raw['success'] is True and row['success']=='True' and not row['error'], trial_label+': success')
                check(raw['trial']==number and raw['slurm_job_id']==job, trial_label+': identity')
                check(raw['workload']==w and raw['database']==cfg['database'] and raw['runtime']==cfg['runtime'], trial_label+': deployment')
                check(not raw['profiling_comparison'] and not tc.get('server_profiling',False), trial_label+': profiling off')
                for k in ('phase','repetition'):
                    check(raw[k]==item[k], trial_label+': '+k)
                for k in ('storage','clients','seed'):
                    check(tc[k]==item[k], trial_label+': '+k)
                for k in ('records','batch_size','timeout','workload','read_queries','query_window'):
                    check(tc[k]==cfg[k], trial_label+': '+k)
                merged = {**raw,**tc}
                for key, value in row.items():
                    if key=='raw_file': continue
                    actual = merged.get(key)
                    if actual is None:
                        check(value=='', trial_label+': missing/blank '+key)
                    elif isinstance(actual,float):
                        check(math.isfinite(actual) and near(actual,value), trial_label+': CSV '+key)
                    else:
                        check(str(actual)==value, trial_label+': CSV '+key)
                c,s = tc['clients'],tc['storage']
                check(raw['storage_info']['label']==raw['storage_info']['filesystem']==s, trial_label+': storage')
                check(len(set(raw['available_cpus']))>=c, trial_label+': affinity')
                counts=dict(committed_records=1000000,stored_records=1000000,transactions=1000,read_queries_completed=1024,read_rows_returned=102400)
                for k,val in counts.items(): check(raw[k]==val, trial_label+': '+k)
                check(raw['worker_read_queries']==[1024//c]*c, trial_label+': read distribution')
                check(raw['worker_committed_records']==[len(range(i,1000,c))*1000 for i in range(c)], trial_label+': write distribution')
                for prefix,count in (('transaction',1000),('read',1024)):
                    samples = np.asarray(raw[prefix+'_latency_samples_ms'],dtype=float)
                    check(len(samples)==count and np.isfinite(samples).all() and (samples>=0).all(), trial_label+': samples')
                    check(near(samples.mean(),raw[prefix+'_latency_mean_ms']), trial_label+': mean latency')
                    for q in (50,95,99):
                        check(near(np.quantile(samples,q/100,method='linear'),raw[f'{prefix}_latency_p{q}_ms']), trial_label+': quantile')
                    sample_count += len(samples)
                for key in ('workload_wall_s','completion_wall_s','read_wall_s'):
                    check(math.isfinite(raw[key]) and raw[key]>0, trial_label+': timing')
                check(0<=raw['final_checkpoint_s']<=raw['completion_wall_s'], trial_label+': checkpoint')
                check(raw['completion_wall_s']>=raw['workload_wall_s'], trial_label+': completion')
                for rate,wall,n in (('throughput_records_s','workload_wall_s',1000000),('completion_throughput_records_s','completion_wall_s',1000000),('read_queries_per_s','read_wall_s',1024)):
                    check(near(raw[rate]*raw[wall],n), trial_label+': rate identity')
                for phase,wall in (('write','workload_wall_s'),('read','read_wall_s')):
                    workers=raw[phase+'_worker_resources']
                    check([r['worker'] for r in workers]==list(range(c)), trial_label+': resource workers')
                    check(near(sum(r['cpu_s'] for r in workers),raw[phase+'_worker_cpu_s']), trial_label+': CPU sum')
                    check(near(raw[phase+'_worker_cpu_core_equivalents'],raw[phase+'_worker_cpu_s']/raw[wall]), trial_label+': CPU/wall')
                if d.startswith('sqlite'):
                    check(raw['journal_mode']=='WAL' and raw['synchronous']=='NORMAL', trial_label+': SQLite settings')
                    version=raw['sqlite_version']
                    if d=='sqlite-shifter': check(raw['shifter_image_id']==cfg['shifter_image_id']==IMAGE_IDS[d], trial_label+': image')
                else:
                    version=raw['postgresql_version']
                    check(raw['server_image_id']==cfg['server_image_id']==IMAGE_IDS[d], trial_label+': image')
                    check(raw['server_resource_metrics_status']=='not_collected', trial_label+': profiler')
                    for k in ('fsync','full_page_writes','synchronous_commit'):
                        check(raw['postgresql_settings'][k]['value']=='on', trial_label+': '+k)
                check(raw['read_cache_state']=='warm_after_write_checkpoint_and_validation', trial_label+': read scope')
                unified={**row,'dataset':'read1024','deployment':d,'run_label':label,'hostname':raw['hostname'],
                         'job_id':job,'engine_version':version,'python_version':raw['python_version'],
                         'timestamp_utc':raw['timestamp_utc'],'checkpoint_policy':raw['checkpoint_policy']}
                all_rows.append(unified)
                raw_trials[(d,w,s,c,int(row['repetition']),row['phase'])]=raw
                coverage[(row['phase'],int(row['repetition']),s,c)]+=1
                hosts.add(raw['hostname']);versions.add(version);pyversions.add(raw['python_version'])
            expected=Counter({(phase,rep,s,c):1 for phase,reps in (('warmup',[1]),('measured',range(1,6))) for rep in reps for s in STORAGE for c in CLIENTS})
            check(coverage==expected and len(hosts)==len(versions)==len(pyversions)==1,label+': coverage/environment')
            inventory.append(dict(dataset='read1024',deployment=d,workload=w,job_id=job,hostname=next(iter(hosts)),
                                  engine_version=next(iter(versions)),python_version=next(iter(pyversions)),
                                  read_queries=1024,measured=30,warmups=6,run_directory=directory))
        check(all(h==next(iter(source_sets.values())) for h in source_sets.values()),'Source manifests differ across new runs')
        for w in WORKLOADS:
            check(len({r['hostname'] for r in inventory if r['workload']==w})==1, 'Different nodes within workload')
        for job in ('59220317','59220318'):
            log=z.read(f'logs/ipdps-read1024-{job}.out').decode()
            check(log.count('READ1024_COMPLETE')==1 and len([x for x in log.splitlines() if '] PASS ' in x])==108, 'Job log completion')
            check([x.split('=',1)[1] for x in log.splitlines() if x.startswith('DEPLOYMENT=')]==list(SQL), 'Execution order')
    return all_rows,inventory,configs,source_sets,raw_trials,dict(archive_members=len(names),checksummed_members=len(sums),raw_trials=216,latency_samples_recomputed=sample_count,source_commit=commit)

def analyze(new_path,baseline_path,out):
    out.mkdir(parents=True,exist_ok=True)
    tables=out/'tables';tables.mkdir(exist_ok=True)
    new,inventory,cfg,hashes,raw,audit = read_new(new_path)
    old,old_inventory,old_cfg,old_hashes = load_data(baseline_path)
    for w in WORKLOADS:
        reference = old_cfg['influx-'+w]
        current = cfg['sqlite-native-'+w]
        for k in ('records','batch_size','read_queries','query_window','seed','repetitions','warmup_repetitions','storage','clients'):
            check(current[k]==reference[k], w+': InfluxDB reference settings differ')
        for k in ('read_phase.py','pilot_workloads.py'):
            check(hashes['sqlite-native-'+w][k]==old_hashes['influx-'+w][k], w+': reference selection/workload source differs')
    for row in old:
        row['dataset']='baseline'
        row['job_id']=row['slurm_job_id']
    for row in old_inventory:
        inventory.append({**row,'dataset':'baseline','measured':30,'warmups':6})
    all_rows=new+old
    groups=defaultdict(list)
    for row in all_rows:
        if row['phase']=='measured':
            groups[(row['dataset'],row['deployment'],row['workload'],row['storage'],int(row['clients']))].append(row)
    for rows in groups.values():
        rows.sort(key=lambda r:int(r['repetition']))
        check([int(r['repetition']) for r in rows]==[1,2,3,4,5],'Missing repetition')
    def vals(ds,d,w,s,c,metric):
        return np.array([float(r[metric]) for r in groups[(ds,d,w,s,c)]])
    summaries=[]
    metrics=list(METRICS.values())+['workload_wall_s','completion_wall_s','read_wall_s']
    metrics += [f'{p}_latency_{q}_ms' for p in ('transaction','read') for q in ('mean','p50','p95','p99')]
    metrics += [f'{p}_worker_{k}' for p in ('write','read') for k in ('cpu_s','cpu_core_equivalents','peak_rss_max_kib','involuntary_switches','voluntary_switches')]
    for ds,d,w,s,c in groups:
        for metric in metrics:
            summaries.append(dict(dataset=ds,deployment=d,workload=w,storage=s,clients=c,metric=metric,**descriptive(vals(ds,d,w,s,c,metric))))
    gains,interactions,scaling,contrasts=[],[],[],[]
    for ds,deployments in (('read1024',SQL),('baseline',SQL+('influx',))):
        for d in deployments:
            for w in WORKLOADS:
                for phase,metric in METRICS.items():
                    for c in CLIENTS:
                        a,b=vals(ds,d,w,'tmpfs',c,metric),vals(ds,d,w,'lustre',c,metric)
                        ratio=a/b
                        gains.append(dict(dataset=ds,deployment=d,workload=w,phase=phase,clients=c,ratio_of_means=float(a.mean()/b.mean()),**log_interval(np.log(ratio))))
                        for rep,value in enumerate(ratio,1):contrasts.append(dict(dataset=ds,deployment=d,workload=w,phase=phase,kind='storage_gain',clients=c,repetition=rep,ratio=float(value)))
                    for c in (16,64):
                        ratio=vals(ds,d,w,'tmpfs',c,metric)/vals(ds,d,w,'lustre',c,metric)
                        ratio/=vals(ds,d,w,'tmpfs',1,metric)/vals(ds,d,w,'lustre',1,metric)
                        interactions.append(dict(dataset=ds,deployment=d,workload=w,phase=phase,clients=c,**log_interval(np.log(ratio))))
                        for rep,value in enumerate(ratio,1):contrasts.append(dict(dataset=ds,deployment=d,workload=w,phase=phase,kind='storage_concurrency_interaction',clients=c,repetition=rep,ratio=float(value)))
                        for s in STORAGE:
                            a,b=vals(ds,d,w,s,c,metric),vals(ds,d,w,s,1,metric)
                            scaling.append(dict(dataset=ds,deployment=d,workload=w,phase=phase,storage=s,clients=c,ratio_of_means=float(a.mean()/b.mean()),**log_interval(np.log(a/b))))
    context_comparison=[]
    source_comparison=[]
    for d in SQL:
        for w in WORKLOADS:
            key=d+'-'+w
            check({k:v for k,v in cfg[key].items() if k!='read_queries'}=={k:v for k,v in old_cfg[key].items() if k!='read_queries'}, key+': extra config differences')
            shared=set(hashes[key])&set(old_hashes[key])
            source_comparison.append(dict(deployment=d,workload=w,changed_common_files=sorted(k for k in shared if hashes[key][k]!=old_hashes[key][k]),
                                          identical_common_files=sorted(k for k in shared if hashes[key][k]==old_hashes[key][k]),new_manifest_files=sorted(set(hashes[key])-set(old_hashes[key]))))
            for s in STORAGE:
                for c in CLIENTS:
                    new_read=vals('read1024',d,w,s,c,'read_queries_per_s')
                    old_read=vals('baseline',d,w,s,c,'read_queries_per_s')
                    new_write=vals('read1024',d,w,s,c,'completion_throughput_records_s')
                    old_write=vals('baseline',d,w,s,c,'completion_throughput_records_s')
                    context_comparison.append(dict(deployment=d,workload=w,storage=s,clients=c,
                        read1024_mean_qps=float(new_read.mean()),baseline64000_mean_qps=float(old_read.mean()),
                        read1024_over_baseline64000=float(new_read.mean()/old_read.mean()),
                        baseline64000_over_read1024=float(old_read.mean()/new_read.mean()),
                        read1024_mean_wall_s=float(vals('read1024',d,w,s,c,'read_wall_s').mean()),
                        baseline64000_mean_wall_s=float(vals('baseline',d,w,s,c,'read_wall_s').mean()),
                        write_new_over_baseline=float(new_write.mean()/old_write.mean()),
                        inference_scope='descriptive; query_count_and_allocation_and_source_snapshot_differ'))
    runtime=[]
    for ds in ('baseline','read1024'):
        for w in WORKLOADS:
            for s in STORAGE:
                for c in CLIENTS:
                    for phase,metric in METRICS.items():
                        a,b=vals(ds,'sqlite-shifter',w,s,c,metric),vals(ds,'sqlite-native',w,s,c,metric)
                        runtime.append(dict(dataset=ds,workload=w,storage=s,clients=c,phase=phase,
                                            shifter_over_native=float(a.mean()/b.mean()),native_mean=float(b.mean()),shifter_mean=float(a.mean()),
                                            scope='same_node_sequential_fixed_order_different_versions' if ds=='read1024' else 'different_nodes_and_versions'))
    duration=[]
    for (d,w,s,c,rep,phase),r in raw.items():
        if phase!='measured': continue
        call_time=sum(r['read_latency_samples_ms'])/1000
        fraction=call_time/(c*r['read_wall_s'])
        check(0<=fraction<=1+1e-5,'Invalid aggregate backend-call time fraction')
        duration.append(dict(deployment=d,workload=w,storage=s,clients=c,repetition=rep,read_queries=1024,queries_per_client=1024//c,
                             read_wall_ms=1000*r['read_wall_s'],read_queries_per_s=r['read_queries_per_s'],
                             mean_query_latency_ms=r['read_latency_mean_ms'],p99_query_latency_ms=r['read_latency_p99_ms'],
                             sum_query_latency_s=call_time,aggregate_backend_call_fraction=fraction,
                             read_client_cpu_s=r['read_worker_cpu_s'],read_client_core_equivalents=r['read_worker_cpu_core_equivalents']))
    audit.update(status='PASS',new_runs=6,new_allocations=2,new_measured=180,new_warmups=36,
                 baseline_reference_runs=8,baseline_measured=240,baseline_warmups=48,
                 distinct_measured_trials=420,distinct_warmups=84,independent_replicates_per_condition_not_established=True,
                 input_sha256={new_path.name:hashlib.sha256(new_path.read_bytes()).hexdigest(),baseline_path.name:hashlib.sha256(baseline_path.read_bytes()).hexdigest()},
                 checks=['SHA256SUMS and ZIP CRC','bundled source bytes match all six run manifests','config/plan/CSV/full JSON agreement for new runs',
                         'record/query counts and worker allocation','recomputed 50/95/99 percentiles and means from raw samples','rate and CPU accounting identities',
                         'coverage, pinned images, engine settings, node and log identity','baseline CSV/config/plan/selected metadata agreement'],
                 scope_limits=['Saved results validated; databases not reopened. No per-request returned payloads in archives.',
                               'Baseline export lacks raw samples and historical source bytes. New source snapshots are byte-verified, not independently Git-object verified.',
                               'No new InfluxDB runs. Its original 60 measured trials are reused once as a reference.',
                               'Same-node SQL deployments run in fixed native/Shifter/PostgreSQL order. Versions differ and runtime is not randomized.',
                               '64k vs 1024 comparisons also differ in allocation and some source hashes; not causal query-count effects.',
                               'Five repetitions per condition are within an allocation; pointwise intervals do not measure between-allocation uncertainty.',
                               'CPU counters are client-worker metrics; no server profiling in these six runs. RSS is a lifetime per-worker maximum.',
                               'SQL writes include checkpoint; Influx writes end at HTTP acknowledgement. Durability differs, and tmpfs is volatile.'],
                 source_comparisons=source_comparison,
                 dependencies=dict(python=platform.python_version(),numpy=np.__version__,scipy=scipy.__version__,PyYAML=yaml.__version__))
    products={'all_trials.csv':all_rows,'run_inventory.csv':inventory,'condition_summary.csv':summaries,'storage_gains.csv':gains,
              'storage_concurrency_interactions.csv':interactions,'concurrency_scaling.csv':scaling,'repetition_contrasts.csv':contrasts,
              'read_count_context_comparison.csv':context_comparison,'sqlite_runtime_comparison.csv':runtime,'read1024_duration_diagnostics.csv':duration}
    for filename,values in products.items(): write_csv(tables/filename,values)
    (out/'validation.json').write_text(json.dumps(audit,indent=2)+'\n')
    print(json.dumps({k:audit[k] for k in ('status','new_measured','new_warmups','distinct_measured_trials','latency_samples_recomputed')},indent=2))
    return dict(rows=all_rows,groups=groups,summary=summaries,gains=gains,interactions=interactions,scaling=scaling,
                context=context_comparison,runtime=runtime,duration=duration,inventory=inventory,audit=audit)

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('new_archive',type=Path);p.add_argument('baseline_archive',type=Path)
    p.add_argument('--output',type=Path,default=Path('analysis-output'))
    a=p.parse_args();analyze(a.new_archive,a.baseline_archive,a.output)
