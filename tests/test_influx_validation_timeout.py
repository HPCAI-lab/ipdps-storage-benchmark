"""Check scoped validation deadlines and retained evidence after failures."""
import sys
import csv
import json
import os
import tempfile
import yaml
import unittest
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import concurrent_influx as coordinator
from experiment import build_plan, FIELDS

IMAGE = 'db0bdab1e5ad5ee899c127b8c13d9c986a3ced78cd9200dd80a1064bf1533b6e'
BASE = dict(database='influxdb', runtime='native', server_runtime='shifter',
            server_image_id=IMAGE, workload='metadata', records=1000,
            batch_size=1000, read_queries=1, query_window=100, clients=[1],
            storage=['tmpfs'], repetitions=1, warmup_repetitions=0, seed=42,
            timeout=600)

class ValidationTimeoutTests(unittest.TestCase):
    def test_config_validation(self):
        self.assertEqual(len(build_plan(BASE)), 1)
        self.assertEqual(len(build_plan(dict(BASE, influx_validation_timeout_s=300))), 1)
        self.assertIn('influx_validation_timeout_s', FIELDS)
        for value in (True, 0, -1, 1801, 30.5, '300'):
            with self.subTest(value=value), self.assertRaises(ValueError):
                build_plan(dict(BASE, influx_validation_timeout_s=value))

    def test_controller_saves_explicit_limit_and_failure(self):
        import experiment
        with tempfile.TemporaryDirectory() as parent:
            parent = Path(parent)
            config = parent / 'config.yaml'
            config.write_text(yaml.safe_dump(dict(BASE, influx_validation_timeout_s=300)))
            output = parent / 'output'
            def fail(cfg, result, log):
                self.assertEqual(cfg['influx_validation_timeout_s'], 300)
                result.update(influx_failure_phase='validation_metadata', acknowledged_records=1000)
                raise TimeoutError('injected')
            env = dict(os.environ, SLURM_JOB_ID='local-test', IPDPS_RUNTIME='native')
            env.pop('SHIFTER_IMAGE', None)
            with patch.dict(os.environ, env, clear=True), \
                 patch('socket.gethostname', return_value='nid-test'), \
                 patch('os.sched_getaffinity', return_value={0}), \
                 patch.object(coordinator, 'run', side_effect=fail), \
                 patch.object(sys, 'argv', ['experiment.py', '--config', str(config), '--output-root', str(output)]):
                self.assertEqual(experiment.main(), 1)
            run = next(output.iterdir())
            raw = json.loads((run / 'trial-0001.json').read_text())
            with (run / 'results.csv').open() as stream:
                row = next(csv.DictReader(stream))
            self.assertEqual(raw['config']['influx_validation_timeout_s'], 300)
            self.assertEqual(row['influx_validation_timeout_s'], '300')
            self.assertEqual(row['influx_failure_phase'], 'validation_metadata')
            self.assertEqual(row['success'], 'False')

    def execute(self, failure=None, explicit=True):
        cfg = dict(BASE, clients=1, storage='tmpfs')
        if explicit:
            cfg['influx_validation_timeout_s'] = 300
        server = MagicMock()
        server.connection = {'host': '127.0.0.1', 'http_timeout': 30}
        server.start.return_value = {'version': 'v2.9.1'}
        server.shutdown = {'server_exit_code': 0, 'server_launcher_exit_code': 0}
        server.resource_metrics.return_value = {}
        backend = MagicMock()
        backend.validate_records.return_value = 1000
        if failure == 'validation':
            backend.validate_records.side_effect = TimeoutError('injected validation timeout')
        result = {'success': False}
        with ExitStack() as stack:
            stack.enter_context(patch.dict('os.environ', {'SCRATCH': '/tmp'}))
            stack.enter_context(patch.object(coordinator, 'StorageManager'))
            stack.enter_context(patch.object(coordinator, 'InfluxServer', return_value=server))
            connect = stack.enter_context(patch.object(coordinator, 'connect_backend', return_value=backend))
            write = stack.enter_context(patch.object(coordinator, 'write_phase', return_value=(10.0, [
                dict(worker=0, finished=11.0, committed=1000, latencies_ms=[1.0])
            ])))
            stack.enter_context(patch.object(coordinator, 'summarize_resources', return_value={}))
            read = stack.enter_context(patch.object(coordinator, 'run_read_phase', return_value={'read_wall_s': 0.1}))
            if failure == 'read':
                read.side_effect = TimeoutError('injected read timeout')
            if failure:
                with self.assertRaises(TimeoutError):
                    coordinator.run(cfg, result, 'test-influx.log')
            else:
                coordinator.run(cfg, result, 'test-influx.log')
        self.assertEqual(server.connection['http_timeout'], 30)
        self.assertEqual(write.call_args.args[1]['http_timeout'], 30)
        self.assertEqual(connect.call_args.args[1]['http_timeout'], 300 if explicit else 30)
        server.stop.assert_called_once()
        self.assertEqual(result['acknowledged_records'], 1000)
        self.assertEqual(result['completion_wall_s'], 1.0)
        self.assertEqual(result['transaction_latency_samples_ms'], [1.0])
        return result, server, read

    def test_success_preserves_worker_deadlines(self):
        result, server, read = self.execute()
        self.assertTrue(result['success'])
        self.assertEqual(result['stored_records'], 1000)
        self.assertEqual(read.call_args.args[1]['http_timeout'], 30)
        server.remove_successful_trial.assert_called_once()

    def test_default_preserves_existing_deadline(self):
        result, _, _ = self.execute(explicit=False)
        self.assertEqual(result['influx_validation_timeout_s'], 30)

    def test_validation_failure_retains_write_evidence(self):
        result, server, read = self.execute('validation')
        self.assertFalse(result['success'])
        self.assertEqual(result['influx_failure_phase'], 'validation_metadata')
        self.assertNotIn('stored_records', result)
        read.assert_not_called()
        server.remove_successful_trial.assert_not_called()

    def test_read_timeout_still_fails(self):
        result, server, _ = self.execute('read')
        self.assertEqual(result['influx_failure_phase'], 'read')
        self.assertFalse(result['success'])
        server.remove_successful_trial.assert_not_called()

if __name__ == '__main__':
    unittest.main()
