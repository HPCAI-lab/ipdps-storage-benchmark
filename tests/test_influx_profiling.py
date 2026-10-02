"""Local profiler accounting/regression checks; no database or Slurm job required."""
import ctypes
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import Mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / 'src'))
from server_metrics import ServerProfiler, memory_sample, identity
from influx_server import InfluxServer, IMAGE, SUPERVISOR


class InfluxProfilingTests(unittest.TestCase):
    def test_default_preserves_postgres_target(self):
        with tempfile.TemporaryDirectory() as tmp:
            profiler = ServerProfiler(Path(tmp) / 'server.log')
            self.assertEqual(profiler.server_name, 'postgres')
            self.assertIn('initdb_postgres', profiler.result()['server_resource_scope'])

    def test_live_waited_process_accounting_both_targets(self):
        for name in ('postgres', 'influxd'):
            with self.subTest(name=name), tempfile.TemporaryDirectory() as tmp:
                profiler = ServerProfiler(Path(tmp) / 'server.log', 0.02, name)
                code = ('import ctypes,time; '
                        f'ctypes.CDLL(None).prctl(15, {name.encode()!r}, 0, 0, 0); '
                        'data=bytearray(8*1024*1024); '
                        'deadline=time.monotonic()+0.4; '
                        '\nwhile time.monotonic()<deadline: sum(range(10000))\n')
                process = subprocess.Popen(profiler.wrap([sys.executable, '-c', code]))
                try:
                    profiler.start(process.pid)
                    self.assertEqual(process.wait(timeout=10), 0)
                finally:
                    if process.poll() is None:
                        process.kill(); process.wait()
                    profiler.stop()
                result = profiler.result()
                self.assertEqual(result['server_resource_metrics_status'], 'lifecycle_collected')
                self.assertEqual(result['server_lifecycle_exit_code'], 0)
                self.assertGreater(result['server_lifecycle_cpu_s'], 0)
                self.assertGreater(result['server_lifecycle_sampled_peak_pss_kib'], 0)
                self.assertGreater(result['server_lifecycle_sampled_max_' + name + '_processes'], 0)
                accounting = json.loads(profiler.time_path.read_text())
                self.assertAlmostEqual(result['server_lifecycle_cpu_s'],
                                       accounting['user_cpu_s'] + accounting['system_cpu_s'])
                if name == 'influxd':
                    self.assertNotIn('initdb', result['server_resource_scope'])
                    self.assertIn('shell_supervisor', result['server_resource_scope'])

    def test_memory_must_observe_selected_server(self):
        with tempfile.TemporaryDirectory() as tmp:
            profiler = ServerProfiler(Path(tmp) / 'server.log', server_name='influxd')
            process = subprocess.Popen(profiler.wrap([sys.executable, '-c', 'pass']))
            self.assertEqual(process.wait(timeout=10), 0)
            profiler.samples = [dict(complete=True, pss_kib=100, postgres_processes=1,
                                     influxd_processes=0, scan_wall_s=0.01)]
            result = profiler.result()
            self.assertEqual(result['server_resource_metrics_status'],
                             'lifecycle_cpu_io_collected_memory_incomplete')

    def test_sigint_supervisor_with_and_without_profiling(self):
        # Fake executable only tests process supervision, not InfluxDB behavior.
        for enabled in (False, True):
            with self.subTest(enabled=enabled), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                fake = root / 'influxd'
                fake.write_text('#!' + sys.executable + '\n' +
                    'import ctypes,signal,time\n'
                    'ctypes.CDLL(None).prctl(15,b"influxd",0,0,0)\n'
                    'signal.signal(signal.SIGINT,lambda *a:exit(0))\n'
                    f'open({str(root / "ready")!r},"w").close()\n'
                    'while True: time.sleep(0.02)\n')
                fake.chmod(0o700)
                findmnt = root / 'findmnt'
                findmnt.write_text('#!/bin/sh\necho tmpfs\n');findmnt.chmod(0o700)
                script = SUPERVISOR.replace('/influxcontrol', str(root))
                env = dict(os.environ, PATH=str(root)+os.pathsep+os.environ['PATH'], SHIFTER_IMAGE=IMAGE)
                launch = ['/bin/sh', '-c', script, 'local-supervisor', IMAGE, '127.0.0.1:12345', 'tmpfs']
                profiler = ServerProfiler(root/'server.log', 0.02, 'influxd') if enabled else None
                if profiler: launch = profiler.wrap(launch)
                process = subprocess.Popen(launch, env=env, start_new_session=True,
                                           stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
                try:
                    if profiler: profiler.start(process.pid)
                    deadline = time.monotonic()+5
                    while not (root/'ready').exists() and time.monotonic()<deadline:
                        time.sleep(0.02)
                    self.assertTrue((root/'ready').exists())
                    if profiler:
                        while not any(s.get('influxd_processes',0)>0 and s['complete'] for s in profiler.samples) and time.monotonic()<deadline:
                            time.sleep(0.02)
                    (root/'stop').touch()
                    _, errors = process.communicate(timeout=10)
                    self.assertEqual(process.returncode,0,errors.decode())
                    self.assertEqual((root/'exit').read_text().strip(),'0')
                finally:
                    if process.poll() is None:
                        os.killpg(process.pid, signal.SIGKILL);process.communicate()
                    if profiler: profiler.stop()
                if profiler:
                    self.assertEqual(profiler.result()['server_resource_metrics_status'],'lifecycle_collected')

    def test_shutdown_nonzero_remains_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            server=InfluxServer.__new__(InfluxServer)
            server.control=Path(tmp);server.log=None;server.shutdown={}
            (server.control/'exit').write_text('143')
            server.process=Mock()
            server.process.poll.return_value=None
            server.process.wait.return_value=143
            with self.assertRaisesRegex(RuntimeError,'shutdown failed'):
                server.stop()
            self.assertEqual(server.shutdown['server_exit_code'],143)


if __name__=='__main__':
    unittest.main()
