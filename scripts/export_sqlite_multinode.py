#!/usr/bin/env python3
"""Verify and archive a completed multi-node run, including every node's evidence."""
import argparse
from pathlib import Path
import shutil
import tempfile
import zipfile

from sqlite_multinode import digest, require
from validate_sqlite_multinode import validate


def export(run):
    run = Path(run).resolve()
    validate(run)  # local simulations are deliberately rejected
    archive = run.parent / (run.name + '.zip')
    files = sorted(p for p in run.rglob('*') if p.is_file())
    require(all(not p.is_symlink() for p in run.rglob('*')), 'Unexpected run symlink')
    temporary = archive.with_suffix('.zip.tmp')
    with zipfile.ZipFile(temporary, 'w', zipfile.ZIP_DEFLATED) as z:
        for p in files:
            z.write(p, f'sqlite-multinode-results-v1/{p.relative_to(run).as_posix()}')
    with zipfile.ZipFile(temporary) as z:
        require(z.testzip() is None and len(z.namelist()) == len(files), 'Archive integrity check')
        import hashlib
        for p in files:
            name = f'sqlite-multinode-results-v1/{p.relative_to(run).as_posix()}'
            require(hashlib.sha256(z.read(name)).hexdigest() == digest(p), 'Archive content mismatch')
    temporary.replace(archive)
    parent = Path.home() / 'ipdps-backups'
    parent.mkdir(exist_ok=True)
    backup = Path(tempfile.mkdtemp(prefix='sqlite-multinode-', dir=parent))
    destination = backup / archive.name
    shutil.copy2(archive, destination)
    require(digest(archive) == digest(destination), 'Home backup checksum mismatch')
    (backup / 'SHA256SUMS').write_text(f'{digest(destination)}  {destination.name}\n')
    print('PASS: multi-node raw results, samples, sources and checksums archived; home backup verified', flush=True)
    print('EXPORT=' + str(archive), flush=True)
    print('BACKUP=' + str(backup), flush=True)
    return archive


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('run')
    export(parser.parse_args().run)
