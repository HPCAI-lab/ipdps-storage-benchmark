#!/usr/bin/env python3
"""Freeze committed sources; optionally submit exactly one bounded four-node job."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import tempfile

from sqlite_multinode import ROOT, SOURCE_FILES, CONFIG, BASE_SHA256, build_plan, write_json, require


def prepare(submit=False):
    require(not subprocess.check_output(['git', 'status', '--porcelain'], cwd=ROOT, text=True).strip(),
            'STOP: commit or resolve working-tree changes first')
    interpreter = ROOT / '.venv/bin/python'
    require(interpreter.is_file(), 'Missing existing .venv/bin/python')
    commit = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip()
    parent = ROOT / 'results'
    parent.mkdir(exist_ok=True)
    marker = parent / f'sqlite-multinode-submission-{commit}.json'
    require(not submit or not marker.exists(), f'STOP: prior submission record exists: {marker}')
    frozen = {}
    for rel in SOURCE_FILES:
        data = subprocess.check_output(['git', 'show', f'HEAD:{rel}'], cwd=ROOT)
        require(data == (ROOT / rel).read_bytes(), 'Source differs from commit: ' + rel)
        frozen[rel] = data
    require(hashlib.sha256(frozen['scripts/sqlite_mechanism_study.py']).hexdigest() == BASE_SHA256,
            'Validated worker changed')
    plan = build_plan(json.loads(frozen[CONFIG]))
    study = Path(tempfile.mkdtemp(prefix='sqlite-multinode-study-', dir=parent))
    snapshot = study / 'source'
    snapshot.mkdir()
    hashes = {}
    for rel, data in frozen.items():
        target = snapshot / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
        hashes[rel] = hashlib.sha256(data).hexdigest()
    write_json(snapshot / 'SOURCE_SHA256.json', hashes)
    (snapshot / 'SOURCE_COMMIT').write_text(commit + '\n')
    (snapshot / '.venv').symlink_to(ROOT / '.venv', target_is_directory=True)
    (snapshot / 'results').mkdir()
    requirements = subprocess.check_output([str(interpreter), '-m', 'pip', 'freeze'], cwd=ROOT, text=True)
    (study / 'requirements-native-resolved.txt').write_text(requirements)
    info = dict(source_commit=commit, snapshot=str(snapshot), submitted=False,
                trials=len(plan), measured=30, warmups=6, allocated_nodes=4,
                walltime_limit_minutes=30, requested_node_hours=2,
                environment_note='Linked existing venv; binary environment is not frozen. Runtime versions saved on each node.')
    write_json(study / 'study.json', info)
    print('PASS: committed multi-node source snapshot prepared', flush=True)
    print('STUDY=' + str(study), flush=True)
    print('SNAPSHOT=' + str(snapshot), flush=True)
    print('REQUEST: one 4-node regular CPU job; 30-minute limit; 2 requested node-hours', flush=True)
    if submit:
        # Exclusive record prevents accidental double submission for this commit.
        with marker.open('x') as stream:
            json.dump(dict(study=str(study), state='submission_requested'), stream)
        result = subprocess.run(['sbatch', '--parsable', str(snapshot / 'hpc/run_sqlite_multinode.slurm'),
                                 str(snapshot)], cwd=ROOT, capture_output=True, text=True)
        if result.returncode != 0:
            info.update(submitted=False, submission_error=result.stderr.strip())
            write_json(study / 'study.json', info)
            write_json(marker, dict(study=str(study), state='rejected', stderr=result.stderr.strip()))
            raise RuntimeError('Submission rejected; no automatic retry. ' + result.stderr.strip() +
                               '\nSaved submission record: ' + str(marker))
        job = result.stdout.strip().split(';')[0]
        require(job.isdecimal(), 'Unknown submission response; inspect scheduler, do not resubmit')
        info.update(submitted=True, job_id=job)
        write_json(study / 'study.json', info)
        write_json(marker, dict(study=str(study), state='submitted', job_id=job))
        (study / 'job-id.txt').write_text(job + '\n')
        print('JOB=' + job, flush=True)
    else:
        print('No job submitted. Use --submit to prepare and submit once.', flush=True)
    return study


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--submit', action='store_true')
    args = parser.parse_args()
    prepare(args.submit)
