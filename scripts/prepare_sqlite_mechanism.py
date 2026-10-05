#!/usr/bin/env python3
"""Freeze committed study files for later submission; does not submit a job."""
import hashlib
import json
from pathlib import Path
import subprocess
import tempfile

from sqlite_mechanism_study import ROOT, SOURCE_FILES, check_fixed_design


def main():
    status = subprocess.check_output(['git', 'status', '--porcelain'], cwd=ROOT, text=True)
    if status.strip():
        raise SystemExit('STOP: commit or resolve working-tree changes before freezing')
    if not (ROOT / '.venv/bin/python').is_file():
        raise SystemExit('STOP: existing native SQLite .venv/bin/python is missing')
    commit = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip()
    frozen = {}
    for relative in SOURCE_FILES:
        data = subprocess.check_output(['git', 'show', f'HEAD:{relative}'], cwd=ROOT)
        if data != (ROOT / relative).read_bytes():
            raise SystemExit('STOP: file differs from committed source: ' + relative)
        frozen[relative] = data
    for suite in ('size-sweep', 'partitioned'):
        check_fixed_design(json.loads(frozen[f'configs/sqlite-mechanism/{suite}.json']))
    parent = ROOT / 'results'
    parent.mkdir(exist_ok=True)
    study = Path(tempfile.mkdtemp(prefix='sqlite-mechanism-study-', dir=parent))
    snapshot = study / 'source'
    snapshot.mkdir()
    hashes = {}
    for relative, data in frozen.items():
        destination = snapshot / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(data)
        hashes[relative] = hashlib.sha256(data).hexdigest()
    (snapshot / 'SOURCE_SHA256.json').write_text(json.dumps(hashes, indent=2) + '\n')
    (snapshot / 'SOURCE_COMMIT').write_text(commit + '\n')
    (snapshot / '.venv').symlink_to(ROOT / '.venv', target_is_directory=True)
    (snapshot / 'results').mkdir()
    requirements = subprocess.check_output([str(ROOT / '.venv/bin/python'), '-m', 'pip', 'freeze'],
                                           cwd=ROOT, text=True)
    (study / 'requirements-native-resolved.txt').write_text(requirements)
    (study / 'study.json').write_text(json.dumps(dict(
        source_commit=commit, snapshot=str(snapshot), jobs_submitted=False,
        suites={'size-sweep': {'measured': 60, 'warmups': 12},
                'partitioned': {'measured': 140, 'warmups': 28}},
        total_trials=240, default_requested_node_hours=3.0,
        interpreter_note='Existing venv is linked, not a frozen binary environment; runtime versions are recorded.'
    ), indent=2) + '\n')
    print('PASS: committed source snapshot prepared; no jobs submitted')
    print('STUDY=' + str(study))
    print('SNAPSHOT=' + str(snapshot))
    print('REQUEST: two regular CPU jobs, each one node and 90 minutes; three node-hours total')


if __name__ == '__main__':
    main()
