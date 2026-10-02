"""Private, pinned Shifter InfluxDB server with supervised SIGINT shutdown."""
import json
import os
import secrets
import shutil
import signal
import socket
import subprocess
import tempfile
import time
from pathlib import Path

from db_backends.influx_backend import InfluxBackend
from server_metrics import ServerProfiler

IMAGE = 'db0bdab1e5ad5ee899c127b8c13d9c986a3ced78cd9200dd80a1064bf1533b6e'
SUPERVISOR = r'''
set -eu
test "$SHIFTER_IMAGE" = "$1"
types=$(findmnt -n -o FSTYPE -T /influxwork)
test -n "$types"
for fs in $types; do test "$fs" = "$3"; done
printf 'VERIFIED_FILESYSTEM=%s\n' "$3"
influxd --bolt-path /influxwork/influxd.bolt \
    --sqlite-path /influxwork/influxd.sqlite \
    --engine-path /influxwork/engine \
    --http-bind-address "$2" --reporting-disabled \
    --storage-wal-fsync-delay=0s --storage-wal-flush-on-shutdown=true &
child=$!
trap 'kill -INT "$child" 2>/dev/null || true' TERM INT
while kill -0 "$child" 2>/dev/null; do
    if [ -f /influxcontrol/stop ]; then
        kill -INT "$child" 2>/dev/null || true
        break
    fi
    sleep 0.2
done
set +e
wait "$child"
rc=$?
set -e
printf '%s\n' "$rc" > /influxcontrol/exit.tmp
mv /influxcontrol/exit.tmp /influxcontrol/exit
exit "$rc"
'''


class InfluxServer:
    def __init__(self, storage, cfg, log_path):
        if cfg['server_image_id'] != IMAGE:
            raise ValueError('InfluxDB pilot requires its validated image ID')
        self.storage, self.cfg = storage, cfg
        self.data = Path(storage['path'])
        self.control = Path(tempfile.mkdtemp(prefix='ipdps-influx-control-', dir='/tmp'))
        self.log_path = Path(log_path)
        self.process = self.log = None
        self.shutdown = {}
        self.profiler = (ServerProfiler(log_path, server_name="influxd")
                         if cfg.get("server_profiling", False) else None)
        with socket.socket() as probe:
            probe.bind(('127.0.0.1', 0))
            port = probe.getsockname()[1]
        self.connection = dict(host='127.0.0.1', port=port, org='ipdps', bucket='benchmark',
                               token=secrets.token_hex(32), http_timeout=30)

    def start(self):
        if not os.environ.get('SLURM_JOB_ID') or not socket.gethostname().startswith('nid'):
            raise RuntimeError('InfluxDB must run on an allocated compute node')
        self.log = self.log_path.open('w')
        command = ['shifter', '--image=id:' + IMAGE,
                   '--volume=' + str(self.data) + ':/influxwork',
                   '--volume=' + str(self.control) + ':/influxcontrol',
                   '/bin/sh', '-c', SUPERVISOR, 'influx-supervisor', IMAGE,
                   f'127.0.0.1:{self.connection["port"]}', self.storage['filesystem']]
        if self.profiler is not None:
            command = self.profiler.wrap(command)
        self.process = subprocess.Popen(command, stdout=self.log,
                                        stderr=subprocess.STDOUT, start_new_session=True)
        if self.profiler is not None:
            self.profiler.start(self.process.pid)
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError('InfluxDB exited during startup; inspect server log')
            backend = InfluxBackend(self.connection)
            try:
                backend.connect()
            except (OSError, RuntimeError):
                backend.close()
                time.sleep(0.2)
                continue
            try:
                setup = dict(username='benchmark', password=secrets.token_urlsafe(24),
                             org=self.connection['org'], bucket=self.connection['bucket'],
                             token=self.connection['token'], retentionPeriodSeconds=0)
                backend.request('/api/v2/setup', json.dumps(setup).encode(),
                                auth=False, expected=201)
                return backend.health()
            finally:
                backend.close()
        raise TimeoutError('InfluxDB readiness timeout')

    def stop(self):
        if self.process is None:
            if self.log is not None:
                self.log.close()
                self.log = None
            return
        began = time.perf_counter()
        process = self.process
        try:
            if process.poll() is not None:
                raise RuntimeError(f'InfluxDB exited before stop request: {process.returncode}')
            (self.control / 'stop').touch()
            try:
                code = process.wait(timeout=60)
            except subprocess.TimeoutExpired:
                # A forced shutdown is always a failed trial, never a valid result.
                try:
                    os.killpg(process.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    process.wait(timeout=10)
                raise RuntimeError('InfluxDB graceful shutdown timed out')
            child_code = int((self.control / 'exit').read_text().strip())
            self.shutdown = dict(server_launcher_exit_code=code,
                                 server_exit_code=child_code,
                                 server_shutdown_s=time.perf_counter() - began)
            if code != 0 or child_code != 0:
                raise RuntimeError(f'InfluxDB shutdown failed: launcher={code}, child={child_code}')
        finally:
            self.process = None
            if self.log is not None:
                self.log.close()
                self.log = None

    def resource_metrics(self):
        if self.profiler is None:
            return {}
        self.profiler.stop()
        return self.profiler.result()

    def remove_successful_trial(self):
        if self.shutdown.get('server_exit_code') != 0:
            raise RuntimeError('Refusing cleanup without successful shutdown')
        shutil.rmtree(self.data)
        shutil.rmtree(self.control)
