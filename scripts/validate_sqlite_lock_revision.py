#!/usr/bin/env python3
"""Validate the fixed study, including deliberately retained SQLITE_BUSY outcomes."""
import argparse
from collections import Counter
import csv
import json
import math
from pathlib import Path
import re

import sqlite_lock_revision as study

require = study.require


def equal(actual, expected, label):
    if isinstance(expected, float):
        require(isinstance(actual,(int,float)) and math.isfinite(actual) and
                math.isclose(actual,expected,rel_tol=1e-10,abs_tol=1e-9), label)
    else:
        require(actual == expected,label)


def check_trial(r,item,cfg,local):
    for k,v in item.items():
        equal(r[k],v,'Plan field mismatch: '+k)
    require(r['protocol']==study.PROTOCOL and r['suite']==cfg['suite'],'Protocol/suite mismatch')
    require(r['execution_mode']==('local_test' if local else 'perlmutter'),'Execution label mismatch')
    require(r['valid_observation'] and r['status'] in ('ok','busy'),'Invalid observation')
    require(r['success'] == (r['status']=='ok'),'Success/outcome mismatch')
    require(cfg['suite']=='lock-policy' or r['status']=='ok','Copy study has failed source writes')
    require(r['application_retries']==0 and r['internal_busy_callbacks'] is None,'Retry semantics changed')
    require(r['batch_size']==cfg['batch_size'],'Batch size changed')
    t=r['timing_ns']
    require(all(type(v) is int and v>0 for v in t.values()),'Invalid timing boundaries')
    require(r['trial_lifecycle_start_ns']<=t['start']<t['write_end']<=t['checkpoint_start']
            <=t['checkpoint_end']<=r['trial_lifecycle_end_ns'],'Invalid timing ordering')
    total=math.ceil(item['records']/cfg['batch_size'])
    nproc=item['clients']+(item['policy']=='queue')
    require([w['worker'] for w in r['workers']]==list(range(nproc)),'Missing/duplicate workers')
    require([w['worker'] for w in r['ready']]==list(range(nproc)),'Missing/duplicate ready messages')
    for ready in r['ready']:
        if ready['role']!='producer':
            require(ready['settings']['busy_timeout']==item['busy_timeout_ms'],'Worker timeout mismatch')
            require(ready['settings']['journal_mode']=='wal' and ready['settings']['synchronous']==1,
                    'Worker database settings mismatch')
    require(t['write_end']==max(w['finished_ns'] for w in r['workers']),'Write endpoint mismatch')
    committed,errors,producer_batches=[],[],{}
    for w in r['workers']:
        expected_role=('database_writer' if item['policy']!='queue' else
                       'producer' if w['worker']<item['clients'] else 'queue_writer')
        require(w['role']==expected_role,'Worker role mismatch')
        require(t['start']<=w['started_ns']<=w['finished_ns']<=t['write_end'],'Worker clock mismatch')
        require(math.isfinite(w['cpu_s']) and w['cpu_s']>=0,'Invalid worker CPU')
        expected_delay=study.delay_for(item,cfg,w['worker']) if expected_role=='database_writer' else 0.0
        equal(w['requested_delay_s'],expected_delay,'Stagger seed/delay changed')
        require(w['application_retries']==0,'Unexpected retry')
        batch_ids=[s['batch_id'] for s in w['batches']]
        require(len(batch_ids)==len(set(batch_ids)),'Duplicate worker batch')
        if expected_role!='queue_writer':
            assigned=study.ids_for(w['worker'],item,cfg)
            require(batch_ids==assigned[:len(batch_ids)],'Worker assignment/order mismatch')
        if expected_role=='producer':
            require(not w['committed_batch_ids'] and not w['errors'],'Producer claimed a DB write')
            for s in w['batches']:
                require(w['started_ns']<=s['prepare_start_ns']<=s['prepare_end_ns']<=s['put_return_ns']
                        <=w['finished_ns'],'Producer timestamps invalid')
                require(s['owner']==w['worker'],'Producer owner invalid')
                require(s['batch_id'] not in producer_batches,'Producer duplicate batch')
                producer_batches[s['batch_id']]=s
            if r['success']:
                require(batch_ids==assigned and w['producer_end_sent'] and
                        all(s['accepted_by_queue'] for s in w['batches']),'Queue dropped input')
            continue
        require(w['batches'],'Database worker made no attempt')
        if expected_role=='database_writer':
            require(w['batches'][0]['prepare_start_ns']-w['started_ns']>=expected_delay*1e9-100000,
                    'Stagger delay excluded or not applied')
        good=[]
        bad=[]
        for i,s in enumerate(w['batches']):
            b=s['batch_id']
            require(type(b) is int and 0<=b<total,'Invalid batch ID')
            require(s['owner']==b%item['clients'],'Wrong batch owner')
            require(s['records']==min(cfg['batch_size'],item['records']-b*cfg['batch_size']),
                    'Wrong batch size')
            require(s['prepare_start_ns']<=s['prepare_end_ns']<=s['begin_ns'],'Invalid preparation times')
            if expected_role=='queue_writer':
                require(s['prepare_end_ns']<=s['dequeued_ns']<=s['begin_ns'],'Invalid queue time')
                origin=producer_batches.get(b)
                require(origin is not None and origin['accepted_by_queue'],'Queue write has no producer input')
                for k in ('prepare_start_ns','prepare_end_ns','records','owner'):
                    require(s[k]==origin[k],'Producer/writer disagreement: '+k)
            else:
                require(s['dequeued_ns'] is None,'Direct write has queue evidence')
            if s['status']=='committed':
                require(s['begin_ns']<=s['acquired_ns']<=s['body_end_ns']<=s['commit_end_ns']<=w['finished_ns'],
                        'Invalid transaction timings')
                good.append(b)
            else:
                require(s['status']=='error' and i==len(w['batches'])-1,'Error was retried or skipped')
                require((s['sqlite_errorcode'] or 0)&255==5,'Not an expected SQLITE_BUSY outcome')
                require(s['begin_ns']<=s['error_ns']<=s['rollback_complete_ns']<=w['finished_ns'],
                        'Invalid error/rollback timing')
                require(s['error_phase'] in ('begin','body','commit'),'Missing error phase')
                bad.append(s)
        require(w['committed_batch_ids']==good,'Committed receipt mismatch')
        require(len(w['errors'])==len(bad),'Error count mismatch')
        require(w['errors']==[{k:v for k,v in s.items() if k not in
                ('prepare_start_ns','prepare_end_ns')} for s in bad],'Error receipt mismatch')
        if r['success'] and expected_role=='database_writer':
            require(batch_ids==study.ids_for(w['worker'],item,cfg),'Successful worker stopped early')
        if r['success'] and expected_role=='queue_writer':
            require(w['producer_ends']==list(range(item['clients'])),'Missing producer end')
        committed.extend(good)
        errors.extend(bad)
    require(len(committed)==len(set(committed)),'Duplicate global committed batch')
    require(bool(errors)==(r['status']=='busy'),'Outcome/error mismatch')
    if r['success']:
        require(sorted(committed)==list(range(total)),'Missing work in successful trial')
    else:
        require(len(committed)<total,'Busy trial completed full work unexpectedly')
    files=1 if item['layout']=='shared' else item['clients']
    require(r['database_files']==files and len(r['validation'])==files and
            len(r['checkpoints'])==files and len(r['database_settings'])==files,'Wrong file count')
    for i,(validation,checkpoint,settings) in enumerate(zip(r['validation'],r['checkpoints'],r['database_settings'])):
        name=f'worker-{i:03d}.db'
        require(validation['file']==checkpoint['file']==settings['file']==name,'File mapping mismatch')
        assigned=sorted(committed) if files==1 else sorted(b for b in committed if b%item['clients']==i)
        require(validation['committed_batch_ids']==assigned and validation['all_keys_verified'],
                'Database key validation mismatch')
        expected=sum(min(cfg['batch_size'],item['records']-b*cfg['batch_size']) for b in assigned)
        require(validation['records']==expected,'Database row count mismatch')
        require(checkpoint['status']==[0,0,0] and checkpoint['wall_s']>=0,'Checkpoint failed')
        require(settings['journal_mode']=='wal' and settings['synchronous']==1 and
                settings['wal_autocheckpoint']==1000 and settings['busy_timeout']==item['busy_timeout_ms'],
                'Keeper settings changed')
    require(sum(c['wall_s'] for c in r['checkpoints'])<=
            (t['checkpoint_end']-t['checkpoint_start'])/1e9+1e-8,'Checkpoint sum exceeds interval')
    if cfg['suite']=='copy-out' and item['storage']=='tmpfs':
        copy=r['copy_out']
        require(copy['success'] and len(copy['files'])==files,'Missing copy receipt')
        require(t['checkpoint_end']<=copy['start_ns']<copy['end_ns']<=r['trial_lifecycle_end_ns'],
                'Copy not measured after checkpoint')
        for v,f in zip(r['validation'],copy['files']):
            require(f['file']==v['file'] and f['file_fsync'] and f['bytes']>0,'Copied file not synced')
            proof=v['copy_verified']
            require(re.fullmatch('[0-9a-f]{64}',proof['source_sha256']) is not None and
                    proof['source_sha256']==proof['destination_sha256'] and
                    proof['records']==v['records'] and proof['quick_check']=='ok','Copy validation failed')
    for k,v in study.derive(r).items():
        equal(r[k],v,'Derived metric mismatch: '+k)
    require(sum(v['records'] for v in r['validation'])==r['committed_records'],'Stored count mismatch')
    if not local:
        require(r['storage_info']['filesystem_verified'] and
                r['storage_info']['filesystems']==[item['storage']],'Unverified production tier')
        require(r['hostname'].startswith('nid') and r['job_id'].isdecimal(),'Missing allocation identity')
        if cfg['suite']=='copy-out':
            require(r['destination_storage_info']['filesystem_verified'] and
                    r['destination_storage_info']['filesystems']==['lustre'],'Unverified copy destination')


def validate(run,allow_local=False):
    run=Path(run)
    cfg=json.loads((run/'config.json').read_text())
    end=json.loads((run/'completion.json').read_text())
    local=end['execution_mode']=='local_test'
    require(not local or allow_local,'Local test cannot be labelled Perlmutter evidence')
    require(end['study_complete'],'Study incomplete')
    plan=study.build_plan(cfg,fixed=not local)
    require(json.loads((run/'plan.json').read_text())==plan,'Saved plan changed')
    hashes=json.loads((run/'source_hashes.json').read_text())
    require(set(hashes)==set(study.SOURCE_FILES),'Source inventory incomplete')
    require(hashes['scripts/sqlite_mechanism_study.py']==study.CORE_SHA,'Audited core changed')
    for p,h in hashes.items():
        require(study.digest(run/'source'/p)==h,'Archived source changed: '+p)
    raw_files=sorted(run.glob('trial-[0-9][0-9][0-9][0-9].json'))
    require(len(raw_files)==len(plan),'Missing/extra trials')
    with (run/'results.csv').open(newline='') as f:
        reader=csv.DictReader(f)
        require(reader.fieldnames==study.CSV_FIELDS,'CSV schema changed')
        rows=list(reader)
    require(len(rows)==len(plan),'CSV trial count mismatch')
    counts=Counter()
    identities=set()
    for item,row,path in zip(plan,rows,raw_files):
        require(path.name==f'trial-{item["trial"]:04d}.json','Raw filename mismatch')
        r=json.loads(path.read_text())
        check_trial(r,item,cfg,local)
        for k in study.CSV_FIELDS:
            expected=r.get(k)
            equal(row[k],'' if expected is None else str(expected),'CSV/raw disagreement: '+k)
        counts[r['status']]+=1
        identities.add((r['hostname'],r['job_id'],r['sqlite_version'],r['python_version']))
    require(len(identities)==1,'Conditions did not share one allocation/runtime')
    require(end['outcomes']==dict(counts) and end['trials']==len(plan) and
            end['all_trials_completed_work']==(counts['busy']==0),'Completion counts inconsistent')
    require(end['automatic_reruns']==0,'Rerun semantics changed')
    require(end['measured']==sum(x['phase']=='measured' for x in plan) and
            end['warmups']==sum(x['phase']=='warmup' for x in plan),'Phase count mismatch')
    receipt=dict(valid=True,suite=cfg['suite'],trials=len(plan),
                 measured=sum(x['phase']=='measured' for x in plan),
                 warmups=sum(x['phase']=='warmup' for x in plan),outcomes=dict(counts),
                 execution_mode='local_test' if local else 'perlmutter',
                 raw_sha256={p.name:study.digest(p) for p in raw_files},
                 scope='Saved key checks, raw timing recomputation, error outcomes, copy receipts, CSV and sources')
    study.write_json(run/'validation.json',receipt)
    print(f'PASS: {cfg["suite"]} — {len(plan)} attempts verified; outcomes={dict(counts)} '
          f'({receipt["execution_mode"]})',flush=True)
    return receipt


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('run',type=Path)
    p.add_argument('--allow-local',action='store_true')
    a=p.parse_args()
    validate(a.run,a.allow_local)
