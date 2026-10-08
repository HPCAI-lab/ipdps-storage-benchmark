#!/usr/bin/env python3
"""Freeze committed sources and Step 1 audit; optionally submit two jobs once."""
import argparse
import json
from pathlib import Path
import re
import shutil
import subprocess
import tempfile

import sqlite_lock_revision as study


def prepare(audit, submit):
    root=study.ROOT
    require=study.require
    audit=audit.resolve()
    receipt=json.loads((audit/'completion.json').read_text())
    require(receipt['both_runtime_audits_collected'] and receipt['original_sources_unchanged'],
            'Step 1 audit incomplete')
    for name in ('native','shifter'):
        require(json.loads((audit/(name+'.json')).read_text())['success'],'Runtime audit incomplete')
    native=json.loads((audit/'native.json').read_text())
    require(native['sources']['scripts/sqlite_mechanism_study.py']['sha256']==study.CORE_SHA,
            'Audit used a different mechanism source')
    # Check all audit files against its saved SHA256SUMS before freezing.
    for line in (audit/'SHA256SUMS').read_text().splitlines():
        sha,relative=line.split('  ',1)
        p=audit/relative
        require(not Path(relative).is_absolute() and '..' not in Path(relative).parts,
                'Unsafe audit path')
        require(study.digest(p)==sha,'Audit file changed: '+relative)
    interpreter=root/'.venv/bin/python'
    require(interpreter.is_file(),'Missing existing native virtualenv')
    commit=subprocess.check_output(['git','rev-parse','HEAD'],cwd=root,text=True).strip()
    frozen={}
    for relative in study.SOURCE_FILES:
        data=subprocess.check_output(['git','show',f'HEAD:{relative}'],cwd=root)
        require(data==(root/relative).read_bytes(),'Commit study file first: '+relative)
        frozen[relative]=data
    require(study.digest(root/'scripts/sqlite_mechanism_study.py')==study.CORE_SHA,'Audited core changed')
    for suite in ('lock-policy','copy-out'):
        study.build_plan(json.loads(frozen[f'configs/sqlite-lock-revision/{suite}.json']))
    parent=root/'results'
    parent.mkdir(exist_ok=True)
    marker=parent/f'sqlite-lock-revision-submission-{commit}.json'
    require(not submit or not marker.exists(),f'STOP: prior submission record exists: {marker}')
    work=Path(tempfile.mkdtemp(prefix='sqlite-lock-revision-study-',dir=parent))
    snapshot=work/'source'
    snapshot.mkdir()
    for relative,data in frozen.items():
        target=snapshot/relative
        target.parent.mkdir(parents=True,exist_ok=True)
        target.write_bytes(data)
    study.write_json(snapshot/'SOURCE_SHA256.json',
                     {p:study.digest(snapshot/p) for p in study.SOURCE_FILES})
    (snapshot/'SOURCE_COMMIT').write_text(commit+'\n')
    (snapshot/'.venv').symlink_to(root/'.venv',target_is_directory=True)
    (snapshot/'results').mkdir()
    require(not any(p.is_symlink() for p in audit.rglob('*')),'Unexpected audit symlink')
    shutil.copytree(audit,snapshot/'audit')
    requirements=subprocess.check_output([str(interpreter),'-m','pip','freeze'],cwd=root,text=True)
    (snapshot/'requirements-native-resolved.txt').write_text(requirements)
    info=dict(source_commit=commit,snapshot=str(snapshot),audit=str(audit),jobs={},
              requested_node_hours=2.0,walltime_minutes_per_job=60,nodes_per_job=1,
              lock_policy=dict(measured_attempts=70,warmups=14),
              copy_out=dict(measured_trials=40,warmups=8),automatic_reruns=0,
              environment_note='Existing venv linked; compute-node Python/SQLite identity checked against audit.')
    study.write_json(work/'study.json',info)
    print('STUDY='+str(work),flush=True)
    print('SNAPSHOT='+str(snapshot),flush=True)
    print('REQUEST: two one-node regular CPU jobs; 60 minutes each; 2 requested node-hours total',flush=True)
    if not submit:
        print('No job submitted. Use --submit to freeze and submit once.')
        return work
    with marker.open('x') as stream:
        json.dump(dict(study=str(work),state='submission_started',jobs={}),stream)
    with (work/'submissions.tsv').open('x') as record:
        record.write('suite\tjob_id\n')
        for suite,launcher in [('lock-policy','run_sqlite_lock_policy.slurm'),
                               ('copy-out','run_sqlite_copy_out.slurm')]:
            p=subprocess.run(['sbatch','--parsable',str(snapshot/'hpc'/launcher),str(snapshot)],
                             cwd=root,capture_output=True,text=True)
            job=p.stdout.strip().split(';')[0]
            if p.returncode or not re.fullmatch('[0-9]+',job):
                info['submission_error']=dict(suite=suite,returncode=p.returncode,
                                              stdout=p.stdout,stderr=p.stderr)
                study.write_json(work/'study.json',info)
                study.write_json(marker,dict(study=str(work),state='stopped_no_automatic_retry',
                                             jobs=info['jobs'],diagnostic=info['submission_error']))
                raise RuntimeError('Submission stopped; inspect saved record before any retry. '+p.stderr)
            info['jobs'][suite]=job
            record.write(f'{suite}\t{job}\n')
            record.flush()
            study.write_json(work/'study.json',info)
            study.write_json(marker,dict(study=str(work),state='submitted_so_far',jobs=info['jobs']))
            print(f'JOB {suite}={job}',flush=True)
    study.write_json(marker,dict(study=str(work),state='both_submitted',jobs=info['jobs']))
    print('PASS: exactly two jobs submitted; IDs recorded; no automatic resubmission',flush=True)
    return work


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--audit',type=Path,required=True)
    p.add_argument('--submit',action='store_true')
    a=p.parse_args()
    prepare(a.audit,a.submit)
