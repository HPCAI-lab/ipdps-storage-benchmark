"""One fresh, owned PostgreSQL cluster per trial; native client, Shifter server."""
import getpass
import os
from pathlib import Path
import shutil
import signal
import subprocess
import tempfile
import time
import psycopg

SERVER_SCRIPT = '''set -eu
test "$SHIFTER_IMAGE" = "$2"
test "$(findmnt -n -r -o FSTYPE -T /pgwork | sort -u)" = "$1"
initdb -D /pgwork/data --username="$3" --auth-local=peer --auth-host=reject --locale=C --encoding=UTF8
exec postgres -D /pgwork/data -k /pgsocket -p 5432 \
 -c listen_addresses= -c unix_socket_permissions=0700 \
 -c fsync=on -c synchronous_commit=on -c full_page_writes=on \
 -c max_connections=100 -c shared_buffers=128MB
'''


class PostgresServer:
    def __init__(self, storage, cfg, log_path):
        self.storage, self.cfg = storage, cfg
        self.socket_dir = tempfile.mkdtemp(prefix='ipdps-pgsock-', dir='/tmp')
        self.log_path = Path(log_path)
        self.log = None
        self.process = None
        self.command = ['shifter', '--image=id:' + cfg['server_image_id'],
                        '--volume=' + storage['path'] + ':/pgwork',
                        '--volume=' + self.socket_dir + ':/pgsocket']
        self.connection = dict(host=self.socket_dir, port=5432,
            user=getpass.getuser(), dbname='postgres', connect_timeout=2,
            application_name='ipdps-pilot',
            options=f"-c timezone=UTC -c statement_timeout={cfg['timeout'] * 1000}")

    def start(self):
        self.log = self.log_path.open('w')
        self.process = subprocess.Popen(
            self.command + ['/bin/bash', '-c', SERVER_SCRIPT, 'pg-server',
                            self.storage['label'], self.cfg['server_image_id'], getpass.getuser()],
            stdout=self.log, stderr=subprocess.STDOUT, start_new_session=True)
        deadline = time.monotonic() + min(120, self.cfg['timeout'])
        last_error = None
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError(f'PostgreSQL startup failed; see {self.log_path}')
            try:
                conn = psycopg.connect(**self.connection, autocommit=True)
            except psycopg.OperationalError as exc:
                last_error = exc
                time.sleep(0.1)
                continue
            with conn:
                if conn.info.server_version != 160012:
                    raise RuntimeError(f'Expected PostgreSQL 16.12, got {conn.info.server_version}')
                return
        raise TimeoutError(f'PostgreSQL startup timed out: {last_error}; log={self.log_path}')

    def stop(self):
        try:
            if self.process is not None:
                if self.process.poll() is None:
                    command = self.command + ['pg_ctl', '-D', '/pgwork/data',
                                              '-m', 'fast', '-w', '-t', '30', 'stop']
                    subprocess.run(command, stdout=self.log, stderr=subprocess.STDOUT,
                                   check=True, timeout=45)
                    self.process.wait(timeout=10)
                if self.process.returncode != 0:
                    raise RuntimeError(f'PostgreSQL exited {self.process.returncode}; log={self.log_path}')
        finally:
            if self.process is not None and self.process.poll() is None:
                os.killpg(self.process.pid, signal.SIGTERM)
                try:
                    self.process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    os.killpg(self.process.pid, signal.SIGKILL)
                    self.process.wait(timeout=10)
            if self.log is not None:
                self.log.close()

    def remove_successful_trial(self):
        if self.process is None or self.process.poll() is None:
            raise RuntimeError('Refusing cleanup while server may be running')
        shutil.rmtree(self.storage['path'])
        shutil.rmtree(self.socket_dir)
