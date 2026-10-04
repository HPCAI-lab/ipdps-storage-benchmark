#!/usr/bin/env python3
"""Reproduce consolidated analysis from six immutable inputs; never runs a benchmark."""
import argparse,csv,hashlib,io,itertools,json,math,platform,tarfile,zipfile
from collections import Counter
from pathlib import Path
import numpy as np
import pandas as pd
import scipy
from scipy.stats import t
import yaml
from baseline_reader import load_data
from analyze_comparison import read_new,plan_for

DEP=('sqlite-native','sqlite-shifter','postgres','influx')
MET={'write':'completion_throughput_records_s','read':'read_queries_per_s'}
JOBS={'59304573':('metadata',1),'59304575':('metadata',2),'59304580':('metadata',3),
      '59304581':('telemetry',1),'59304582':('telemetry',2),'59304585':('telemetry',3)}

def need(ok,why):
    if not ok: raise ValueError(why)

def near(a,b):return math.isclose(float(a),float(b),rel_tol=1e-8,abs_tol=1e-8)
def sha(b):return hashlib.sha256(b).hexdigest()
def csvread(z,name):return list(csv.DictReader(io.StringIO(z.read(name).decode())))
def dump(path,obj):path.write_text(json.dumps(obj,indent=2,allow_nan=False)+'\n')
def csvout(path,df):df.to_csv(path,index=False,lineterminator='\n',float_format='%.12g')

def read_group(path):
    group=path.stem; rows=[];inventory=[];raws={};raw_count=0;sample_count=0;matched_sources={}
    with zipfile.ZipFile(path) as z:
        need(z.testzip() is None,'ZIP integrity '+group)
        names=z.namelist();need(len(names)==len(set(names)),'Duplicate ZIP member')
        m=json.loads(z.read('manifest.json'))
        for name,digest in m['files_sha256'].items():need(sha(z.read(name))==digest,group+': hash '+name)
        sources={}
        for name in names:
            if name.startswith('sources/') and name.endswith('.tar'):
                with tarfile.open(fileobj=io.BytesIO(z.read(name))) as tf:
                    sources[Path(name).stem]={p.name:sha(tf.extractfile(p).read()) for p in tf.getmembers() if p.isfile()}
        for info in m['runs']:
            run=info['run'];prefix=('system/runs/' if group=='system-measurements' else 'runs/')+run
            cfg=yaml.safe_load(z.read(prefix+'/config.yaml'))
            plan=json.loads(z.read(prefix+'/plan.json'));saved=csvread(z,prefix+'/results.csv')
            need(plan==plan_for(cfg),run+': plan')
            need(len(plan)==len(saved)==info['trials'],run+': trials')
            need(Counter(x['phase'] for x in saved)==Counter(x['phase'] for x in plan),run+': phase')
            hashes=json.loads(z.read(prefix+'/source_hashes.json'))
            matches=[commit for commit,src in sources.items() if all(src.get(n)==d for n,d in hashes.items())]
            need(bool(matches),run+': source snapshot does not match manifest')
            matched_sources[run]=matches
            dep=('sqlite-'+cfg['runtime'] if cfg['database']=='sqlite' else 'postgres' if cfg['database']=='postgresql' else 'influx')
            block=JOBS.get(info['job'],('',0))[1]
            inv=dict(study=group,run_id=run,job_id=info['job'],deployment=dep,workload=cfg['workload'],
                hostname=info['hostname'],records=cfg['records'],read_queries=cfg['read_queries'],
                trials=len(saved),measured=info['measured'],warmups=info['warmups'],allocation_block=block,
                source_commit=','.join(matches))
            conditions=Counter()
            for number,(row,item) in enumerate(zip(saved,plan),1):
                raw=json.loads(z.read(prefix+'/'+row['raw_file']));tc=raw['config'];raw_count+=1
                need(raw['success'] is True and row['success']=='True',run+': failure')
                need(raw['trial']==int(row['trial'])==number,run+': index')
                need(str(raw['slurm_job_id'])==info['job'] and raw['hostname']==info['hostname'],run+': provenance')
                for k in ('database','runtime','workload'):need(raw[k]==cfg[k]==row[k],run+': '+k)
                for k in ('phase','repetition'):need(raw[k]==item[k] and row[k]==str(item[k]),run+': '+k)
                for k in ('clients','storage','seed'):need(tc[k]==item[k] and row[k]==str(item[k]),run+': '+k)
                for k in ('records','batch_size','read_queries','query_window','timeout'):need(tc[k]==cfg[k],run+': config '+k)
                conditions[(item['phase'],item['repetition'],item['storage'],item['clients'])]+=1
                need(raw['storage_info']['filesystem']==tc['storage'],run+': filesystem')
                counts={'committed_records':cfg['records'],'stored_records':cfg['records'],
                        'read_queries_completed':cfg['read_queries'],'read_rows_returned':cfg['read_queries']*cfg['query_window']}
                batches=math.ceil(cfg['records']/cfg['batch_size'])
                if cfg['workload']=='mixed':
                    counts.update(stored_metadata_records=cfg['records']//2,stored_telemetry_records=cfg['records']//2,
                        logical_write_batches=batches,backend_write_calls=2*batches,
                        metadata_read_queries=cfg['read_queries']//2,telemetry_read_queries=cfg['read_queries']//2)
                    need(raw['write_latency_unit']=='logical_mixed_batch_two_nonatomic_backend_calls',run+': mixed unit')
                for k,v in counts.items():need(raw[k]==int(row[k])==v,run+': '+k)
                need(sum(raw['worker_committed_records'])==cfg['records'] and len(raw['worker_committed_records'])==tc['clients'],run+': write workers')
                need(sum(raw['worker_read_queries'])==cfg['read_queries'] and len(raw['worker_read_queries'])==tc['clients'],run+': read workers')
                for seconds,rate,total in [('workload_wall_s','throughput_records_s',cfg['records']),('completion_wall_s',MET['write'],cfg['records']),('read_wall_s',MET['read'],cfg['read_queries'])]:
                    need(math.isfinite(raw[seconds]) and raw[seconds]>0 and near(raw[seconds],row[seconds]),run+': time')
                    need(near(raw[rate],total/raw[seconds]) and near(raw[rate],row[rate]),run+': rate')
                for samples,prefix2,count in [('transaction_latency_samples_ms','transaction_latency',batches),('read_latency_samples_ms','read_latency',cfg['read_queries'])]:
                    a=np.asarray(raw[samples]);need(len(a)==count and np.isfinite(a).all() and (a>=0).all(),run+': samples')
                    vals={'mean':float(a.mean()),**{f'p{p}':float(np.percentile(a,p,method='linear')) for p in (50,95,99)}}
                    for stat,val in vals.items():
                        k=prefix2+'_'+stat+'_ms';need(near(val,raw[k]) and near(val,row[k]),run+': latency '+k)
                    sample_count+=len(a)
                if cfg['database']=='influxdb':
                    need(raw['server_exit_code']==raw['server_launcher_exit_code']==0,run+': shutdown')
                    need(raw['final_checkpoint_s'] is None and near(raw['completion_wall_s'],raw['workload_wall_s']),run+': ack boundary')
                else:
                    need(raw['final_checkpoint_s']>=0 and raw['completion_wall_s']>=raw['workload_wall_s'],run+': checkpoint')
                if cfg['runtime']=='shifter':need(raw['shifter_image_id']==cfg['shifter_image_id'],run+': container')
                if cfg['database']!='sqlite':need(raw['server_image_id']==cfg['server_image_id'],run+': server image')
                engine=raw.get('sqlite_version') or raw.get('postgresql_version') or raw.get('influxdb_version')
                inv.update(python_version=raw['python_version'],engine_version=engine)
                rows.append({**row,**{k:raw[k] for k in ('hostname','python_version','timestamp_utc')},'study':group,
                    'deployment':dep,'run_id':run,'job_id':info['job'],'allocation_block':block,'engine_version':engine})
                raws[(run,number)]=raw
            need(all(v==1 for v in conditions.values()),run+': repeated condition')
            inventory.append(inv)
        if group=='sql-repeats':
            order={1:['sqlite-native','sqlite-shifter','postgres'],2:['sqlite-shifter','postgres','sqlite-native'],3:['postgres','sqlite-native','sqlite-shifter']}
            for job,(work,block) in JOBS.items():
                text=z.read(f'logs/ipdps-sql-repeat-{job}.out').decode()
                observed=[s.split()[0].split('=',1)[1] for s in text.splitlines() if s.startswith('DEPLOYMENT=')]
                need(observed==order[block],job+': order')
                subset=[r for r in inventory if r['job_id']==job]
                need(len(subset)==3 and len({r['hostname'] for r in subset})==1,job+': within allocation')
        audit=dict(archive_members=len(names),checksummed_files=len(m['files_sha256']),raw_trials=raw_count,
                   latency_samples_recomputed=sample_count,matched_source_snapshots=matched_sources)
    return rows,inventory,raws,audit


def system_data(path):
    output=[];series=[];audit=[]
    with zipfile.ZipFile(path) as z:
        caps=json.loads(z.read('system/capabilities.json'))
        for row in csvread(z,'system/summary.csv'):
            label=f"{row['workload']}-{row['deployment']}-{row['storage']}-{row['clients']}"
            prefix='system/'+label+'/'
            m=json.loads(z.read(prefix+'metrics.json'))
            samples=[json.loads(x) for x in z.read(prefix+'node-samples.jsonl').decode().splitlines() if x]
            ticks=np.array(samples[-1]['cpu_ticks'])-np.array(samples[0]['cpu_ticks']);total=ticks.sum()
            need(len(ticks)==8 and (ticks>=0).all() and total>0,label+': CPU samples')
            busy=100*(total-ticks[3]-ticks[4])/total;wait=100*ticks[4]/total
            mem=[(s['memory_kib']['MemTotal']-s['memory_kib']['MemAvailable'])/1024 for s in samples]
            for k,val in [('node_busy_percent',busy),('node_iowait_percent',wait),('node_sampled_peak_used_MiB',max(mem))]:
                need(near(m[k],val) and near(row[k],val),label+': recomputed node metric')
            need(m['node_cpu_status']=='collected' and not m['sampler_errors'],label+': sampler')
            need(not m['perf_events'] and row['perf_status']=='unavailable',label+': unexpected perf')
            for k in ('cycles','instructions','cache_misses','context_switches','node_ipc'):need(row[k]=='',label+': missing perf must be blank')
            timing=z.read(prefix+'time.txt').decode();time_fields={}
            for line in timing.splitlines():
                if ': ' in line:
                    k,v=line.strip().rsplit(': ',1);time_fields[k]=v
            iostat=z.read(prefix+'iostat.txt').decode()
            need(m['iostat_available'] and 'Device' in iostat and m['iostat_exit_code'] in (0,-15),label+': iostat')
            need(time_fields.get('Exit status')=='0',label+': GNU time exit')
            row.update(run_id=Path(row['results']).name,node_start_used_MiB=mem[0],node_end_used_MiB=mem[-1],
                node_peak_minus_start_MiB=max(mem)-mem[0],samples=len(samples),job_id=caps['job_id'],hostname=caps['hostname'],
                time_user_cpu_s=float(time_fields['User time (seconds)']),time_system_cpu_s=float(time_fields['System time (seconds)']),
                time_max_process_rss_kib=int(time_fields['Maximum resident set size (kbytes)']),
                time_voluntary_switches=int(time_fields['Voluntary context switches']),time_involuntary_switches=int(time_fields['Involuntary context switches']),
                time_fs_inputs=int(time_fields['File system inputs']),time_fs_outputs=int(time_fields['File system outputs']))
            output.append(row)
            for sample,mi in zip(samples,mem):
                series.append(dict(condition=label,elapsed_s=sample['monotonic_s']-samples[0]['monotonic_s'],node_used_MiB=mi))
            audit.append(dict(condition=label,samples=len(samples),iostat_exit_code=m['iostat_exit_code']))
    return pd.DataFrame(output),pd.DataFrame(series),dict(capabilities=caps,conditions=audit)


def interval(values):
    a=np.asarray(values,dtype=float);need((a>0).all(),'Nonpositive ratio')
    logs=np.log(a);n=len(a);mid=logs.mean();half=t.ppf(.975,n-1)*logs.std(ddof=1)/np.sqrt(n)
    return dict(n_units=n,geometric_ratio=float(np.exp(mid)),ci95_low=float(np.exp(mid-half)),ci95_high=float(np.exp(mid+half)))


def main(root):
    tables=root/'tables';tables.mkdir(exist_ok=True);inputs=root/'input'
    baseline,inv,_,_=load_data(inputs/'ipdps-pilot-analysis.zip')
    for r in baseline:r.update(study='baseline',run_id=Path(r['run_directory']).name,job_id=r['slurm_job_id'],allocation_block=0)
    inventory=[dict(study='baseline',run_id=Path(r['run_directory']).name,trials=36,measured=30,warmups=6,records=1000000,allocation_block=0,**{k:r[k] for k in ('job_id','deployment','workload','hostname','python_version','engine_version','read_queries')}) for r in inv]
    follow,inv,_,_,_,audit0=read_new(inputs/'read1024-results-20261002T200306Z-67f254d2.zip')
    for r in follow:r.update(study='read1024',run_id=Path(next(i['run_directory'] for i in inv if i['deployment']==r['deployment'] and i['workload']==r['workload'])).name,allocation_block=0)
    inventory.extend(dict(study='read1024',run_id=Path(r['run_directory']).name,trials=36,records=1000000,allocation_block=0,**{k:r[k] for k in ('job_id','deployment','workload','hostname','python_version','engine_version','read_queries','measured','warmups')}) for r in inv)
    rows=baseline+follow;configurations=[];audits={'read1024':audit0,'baseline':{'trials':288,'validation':'CSV + saved trial metadata; raw latency samples not in baseline input'}}
    for name in ('sql-repeats','mixed-pilots','size-calibration','system-measurements'):
        r,i,raws,a=read_group(inputs/(name+'.zip'));rows+=r;inventory+=i;audits[name]=a
        for (run,trial),raw in raws.items():
            settings={}
            if raw['database']=='postgresql':
                settings=raw['postgresql_settings']
                for key in ('fsync','synchronous_commit','full_page_writes'):
                    need(settings[key]['value']=='on',run+': PostgreSQL durability '+key)
            elif raw['database']=='sqlite':
                settings={k:{'value':raw[k],'unit':None} for k in ('journal_mode','synchronous','wal_autocheckpoint_pages')}
            else:
                settings={k:{'value':raw[k],'unit':None} for k in ('write_completion_policy','server_wal_fsync_delay','server_wal_flush_on_shutdown')}
            for key,value in settings.items():
                configurations.append(dict(study=name,run_id=run,trial=trial,database=raw['database'],setting=key,value=value['value'],unit=value['unit']))
    df=pd.DataFrame(rows)
    ids=['study','deployment','run_id','job_id','workload','storage','phase','hostname','python_version','engine_version','timestamp_utc']
    for c in df:
        if c not in ids and c not in ('raw_file','run_label','run_directory','dataset'):
            try:df[c]=pd.to_numeric(df[c])
            except (ValueError,TypeError):pass
    for c in ('clients','records','read_queries','batch_size','query_window','repetition','trial','allocation_block'):df[c]=pd.to_numeric(df[c]).astype(int)
    need(len(df)==1392 and not df.duplicated(['run_id','trial']).any(),'Global trial reconciliation')
    repeated=df[~df.study.isin(['size-calibration','system-measurements'])]
    need(Counter(repeated.phase)=={'measured':1080,'warmup':216},'Repeated counts')
    csvout(tables/'all_trials.csv',df);csvout(tables/'run_inventory.csv',pd.DataFrame(inventory))
    counts=df.groupby(['study','phase'],sort=False).size().reset_index(name='trials');csvout(tables/'study_counts.csv',counts)
    meas=df[df.phase=='measured'];condition=[]
    for keys,g in meas.groupby(['study','deployment','workload','storage','clients','records','read_queries'],sort=False):
        d=dict(zip(['study','deployment','workload','storage','clients','records','read_queries'],keys));d['n_trials']=len(g);d['n_allocations']=g.job_id.nunique()
        for m in [*MET.values(),'completion_wall_s','read_wall_s','transaction_latency_p99_ms','read_latency_p99_ms']:
            a=pd.to_numeric(g[m]);d[m+'_mean']=a.mean();d[m+'_sd']=a.std(ddof=1);d[m+'_median']=a.median()
        condition.append(d)
    csvout(tables/'condition_summary.csv',pd.DataFrame(condition))
    sql=meas[meas.study=='sql-repeats']
    block=sql.groupby(['workload','deployment','allocation_block','job_id','hostname','storage','clients'])[list(MET.values())].mean().reset_index()
    csvout(tables/'sql_allocation_means.csv',block)
    contrasts=[]
    for (w,d,b,j,h),g in block.groupby(['workload','deployment','allocation_block','job_id','hostname']):
        for phase,metric in MET.items():
            pivot=g.pivot(index='clients',columns='storage',values=metric)
            for c in (1,16,64):contrasts.append(dict(workload=w,deployment=d,allocation_block=b,job_id=j,hostname=h,phase=phase,kind='storage',condition=str(c),ratio=pivot.loc[c,'tmpfs']/pivot.loc[c,'lustre']))
            for c in (16,64):
                contrasts.append(dict(workload=w,deployment=d,allocation_block=b,job_id=j,hostname=h,phase=phase,kind='storage_interaction',condition=f'{c}/1',ratio=(pivot.loc[c,'tmpfs']/pivot.loc[c,'lustre'])/(pivot.loc[1,'tmpfs']/pivot.loc[1,'lustre'])))
            for s in ('lustre','tmpfs'):
                for c in (16,64):contrasts.append(dict(workload=w,deployment=d,allocation_block=b,job_id=j,hostname=h,phase=phase,kind='concurrency',condition=f'{s}:{c}/1',ratio=pivot.loc[c,s]/pivot.loc[1,s]))
    con=pd.DataFrame(contrasts);csvout(tables/'sql_allocation_contrasts.csv',con)
    ratios=[]
    for keys,g in con.groupby(['workload','deployment','phase','kind','condition']):
        need(len(g)==3,'SQL ratio units');ratios.append(dict(zip(['workload','deployment','phase','kind','condition'],keys),**interval(g.ratio),unit='allocation block'))
    csvout(tables/'sql_ratio_intervals.csv',pd.DataFrame(ratios))
    runtime=[]
    for (w,s,c),g in block[block.deployment.str.startswith('sqlite')].groupby(['workload','storage','clients']):
        for phase,metric in MET.items():
            p=g.pivot(index='allocation_block',columns='deployment',values=metric)
            runtime.append(dict(workload=w,storage=s,clients=c,phase=phase,**interval(p['sqlite-shifter']/p['sqlite-native']),interpretation='execution environments; software versions differ'))
    csvout(tables/'sqlite_environment_ratios.csv',pd.DataFrame(runtime))
    mixed=meas[meas.study=='mixed-pilots'];mixrat=[]
    for (d,c),g in mixed.groupby(['deployment','clients']):
        for phase,metric in MET.items():
            p=g.pivot(index='repetition',columns='storage',values=metric)
            mixrat.append(dict(deployment=d,clients=c,phase=phase,ratio_of_means=p.tmpfs.mean()/p.lustre.mean(),**interval(p.tmpfs/p.lustre),unit='repetition within one allocation'))
    csvout(tables/'mixed_storage_ratios.csv',pd.DataFrame(mixrat))
    cal=meas[meas.study=='size-calibration'];calcomp=[]
    for _,r in cal.iterrows():
        ref=mixed if r.workload=='mixed' else meas[meas.study==('baseline' if r.deployment=='influx' else 'sql-repeats')]
        ref=ref[(ref.deployment==r.deployment)&(ref.workload==r.workload)&(ref.storage==r.storage)&(ref.clients==r.clients)]
        for phase,metric in MET.items():
            calcomp.append(dict(deployment=r.deployment,workload=r.workload,storage=r.storage,clients=r.clients,phase=phase,
                calibration_run=r.run_id,ten_million_value=r[metric],one_million_mean=ref[metric].mean(),one_million_n=len(ref),
                descriptive_10M_over_1M=r[metric]/ref[metric].mean(),interpretation='one 10M observation; allocation/source/seed may differ; no causal size estimate'))
    csvout(tables/'size_calibration_context.csv',pd.DataFrame(calcomp))
    durations=meas.groupby(['study','deployment','workload']).read_wall_s.agg(['count','min','median','max']).reset_index()
    small=meas.assign(under_50ms=meas.read_wall_s<.05).groupby(['study','deployment','workload']).under_50ms.sum().reset_index()
    csvout(tables/'read_duration_diagnostics.csv',durations.merge(small))
    system,series,sysaudit=system_data(inputs/'system-measurements.zip')
    csvout(tables/'system_measurements.csv',system);csvout(tables/'node_memory_samples.csv',series)
    resources=[]
    for keys,g in meas[meas.study.isin(['sql-repeats','mixed-pilots'])].groupby(['study','deployment','workload','storage','clients']):
        r=dict(zip(['study','deployment','workload','storage','clients'],keys))
        for phase in ('write','read'):
            for name in ('cpu_s','cpu_core_equivalents','voluntary_switches','involuntary_switches','peak_rss_max_kib','io_read_bytes','io_write_bytes'):
                k=phase+'_worker_'+name
                if k in g:r[k+'_mean']=pd.to_numeric(g[k],errors='coerce').mean()
        resources.append(r)
    csvout(tables/'worker_resource_summary.csv',pd.DataFrame(resources))
    csvout(tables/'database_configuration.csv',pd.DataFrame(configurations))
    audit={'total_records':len(df),'repeated_measured':1080,'repeated_warmups':216,'calibration_trials':48,'diagnostic_trials':48,
           'run_count':len(inventory),'input_sha256':{p.name:sha(p.read_bytes()) for p in inputs.glob('*.zip')},
           'validation':audits,'system':sysaudit,'python':platform.python_version(),'numpy':np.__version__,'pandas':pd.__version__,'scipy':scipy.__version__,
           'limitations':['Databases were not reopened; saved correctness records checked.','Baseline archive has summaries/metadata, not original per-request latency arrays.',
           'Node-wide perf unavailable; counts absent rather than zero.','Three allocation blocks per SQL workload, not six distinct nodes.','No independent Influx or mixed allocation repeats.',
           'Single 10M observation per condition; 100K/10M repeated sweeps unexecuted.','Workers report ready before timed event release; short read bursts still do not establish steady state.',
           'Exploratory t intervals assume independent normal log contrasts; no multiple-comparison adjustment; only three SQL blocks.']}
    dump(root/'validation.json',audit)
    print('PASS: 1392 trial records; 1104 raw trials with latency recomputation; 288 baseline summary records')
    print('PASS: all 82 new run source manifests match bundled snapshots; 48 node summaries recomputed')
    print('SQL storage intervals:');print(pd.DataFrame(ratios).query("kind=='storage' and phase=='write'")[['workload','deployment','condition','geometric_ratio','ci95_low','ci95_high']].to_string(index=False))

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--root',type=Path,default=Path(__file__).resolve().parents[1]);main(p.parse_args().root)
