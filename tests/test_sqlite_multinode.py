import copy
import csv
import json
import math
from pathlib import Path
import socket
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts'))
import sqlite_multinode as multi
import validate_sqlite_multinode as validator


def config():
    return json.loads((ROOT / multi.CONFIG).read_text())


class MultinodeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temporary = tempfile.TemporaryDirectory(prefix='sqlite-multinode-test-')
        root = Path(cls.temporary.name)
        cls.cfg = config()
        cls.cfg.update(records=103, batch_size=16, clients=4, nodes=[1, 2, 4],
                       repetitions=1, warmup_repetitions=0, startup_timeout_s=30, trial_timeout_s=30)
        cls.run_dir = multi.run_study(cls.cfg, root / 'output',
                                  dict(lustre=str(root), tmpfs=str(root)), local=True)

    @classmethod
    def tearDownClass(cls):
        cls.temporary.cleanup()

    def test_fixed_work_and_randomized_complete_blocks(self):
        cfg = config()
        plan = multi.build_plan(cfg)
        self.assertEqual(len(plan), 36)
        self.assertEqual(sum(p['phase'] == 'measured' for p in plan), 30)
        self.assertTrue(all(p['clients'] == 64 and p['records'] == 10_000_000 for p in plan))
        hosts = ['nid000001', 'nid000002', 'nid000003', 'nid000004']
        for block in range(0, len(plan), 6):
            group = plan[block:block + 6]
            self.assertEqual(len({p['seed'] for p in group}), 1)
            self.assertEqual({(p['nodes'], p['storage']) for p in group},
                             {(n, t) for n in (1, 2, 4) for t in ('lustre', 'tmpfs')})
            for n in (1, 2, 4):
                pair = [p for p in group if p['nodes'] == n]
                self.assertEqual(multi.selected_hosts(hosts, pair[0]), multi.selected_hosts(hosts, pair[1]))
                ids = [i for r in range(n) for i in range(r, 64, n)]
                self.assertEqual(sorted(ids), list(range(64)))
                batch_ids = [b for i in ids for b in range(i, 10000, 64)]
                self.assertEqual(sorted(batch_ids), list(range(10000)))

    def test_invalid_configuration_rejected(self):
        for key, value in [('nodes', [1, 3, 4]), ('clients', 128), ('read_queries', 1),
                           ('records', 10), ('repetitions', 0)]:
            cfg = config()
            cfg[key] = value
            with self.assertRaises(ValueError):
                multi.build_plan(cfg)

    def test_real_sqlite_roundtrip_one_two_four_leaders_partial_batch(self):
        receipt = validator.validate(self.run_dir, allow_local=True)
        self.assertTrue(receipt['success'])
        self.assertEqual(receipt['trials'], 6)
        self.assertEqual(receipt['batches_recomputed'], 6 * math.ceil(103 / 16))
        with self.assertRaises(ValueError):
            validator.validate(self.run_dir)

    def test_node_evidence_corruption_is_rejected(self):
        target = next(self.run_dir.glob('trial-*-node-*.json'))
        original = target.read_bytes()
        try:
            obj = json.loads(original)
            obj['workers'][0]['committed_records'] += 1
            multi.write_json(target, obj)
            with self.assertRaisesRegex(ValueError, 'checksum'):
                validator.validate(self.run_dir, allow_local=True)
        finally:
            target.write_bytes(original)

    def test_duplicate_shard_ownership_rejected_independently_of_hash(self):
        target = next(self.run_dir.glob('trial-*-node-*.json'))
        node = json.loads(target.read_text())
        item = {key: node[key] for key in ('trial', 'phase', 'repetition', 'seed', 'storage',
                                          'layout', 'nodes', 'clients', 'records')}
        node['workers'][0]['batches'][0]['batch_id'] = 999
        with self.assertRaisesRegex(ValueError, 'ownership'):
            validator.audit_node(node, item, self.cfg, node['rank'], 'local_test', node['slurm_job_id'])

    def test_independent_node_clock_epochs_do_not_change_global_measurement(self):
        target = next(self.run_dir.glob('trial-*-node-*.json'))
        node = json.loads(target.read_text())
        item = {key: node[key] for key in ('trial', 'phase', 'repetition', 'seed', 'storage',
                                          'layout', 'nodes', 'clients', 'records')}
        shifted = copy.deepcopy(node)
        shift = 10**18
        for key in shifted['local_timing_ns']:
            shifted['local_timing_ns'][key] += shift
        for worker in shifted['workers']:
            worker['start_ns'] += shift
            worker['finished_ns'] += shift
        before = validator.audit_node(node, item, self.cfg, node['rank'], 'local_test', node['slurm_job_id'])
        after = validator.audit_node(shifted, item, self.cfg, node['rank'], 'local_test', node['slurm_job_id'])
        self.assertEqual(before, after)

    def test_global_throughput_tampering_rejected(self):
        name = 'trial-0001.json'
        raw = json.loads((self.run_dir / name).read_text())
        item = multi.build_plan(self.cfg, fixed=False)[0]
        env = json.loads((self.run_dir / 'environment.json').read_text())
        raw['completion_records_s'] *= 2
        with self.assertRaisesRegex(ValueError, 'completion rate'):
            validator.audit_trial(self.run_dir, raw, item, self.cfg, 'local_test',
                                  env['allocated_hosts'], raw['slurm_job_id'])

    def test_csv_corruption_rejected(self):
        target = self.run_dir / 'results.csv'
        original = target.read_bytes()
        try:
            with target.open(newline='') as f:
                rows = list(csv.DictReader(f))
            rows[0]['completion_records_s'] = '0'
            with target.open('w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=multi.COLUMNS, lineterminator='\n')
                writer.writeheader()
                writer.writerows(rows)
            with self.assertRaisesRegex(ValueError, 'CSV mismatch'):
                validator.validate(self.run_dir, allow_local=True)
        finally:
            target.write_bytes(original)

    def test_failed_node_is_preserved_and_never_reports_global_success(self):
        root = Path(self.temporary.name)
        output = root / 'failure'
        output.mkdir()
        item = next(p for p in multi.build_plan(self.cfg, fixed=False) if p['nodes'] == 2)
        report = multi.run_trial(item, self.cfg, output, ['slot0', 'slot1'],
                                 dict(lustre=str(root), tmpfs=str(root)), local=True, test_fail_rank=1)
        self.assertFalse(report['success'])
        self.assertIn('Injected node failure', report['error'])
        self.assertTrue((output / f'trial-{item["trial"]:04d}.json').is_file())
        self.assertFalse(list(output.glob('.control-*')))

    def test_missing_peer_is_bounded_by_timeout(self):
        a, b = socket.socketpair()
        try:
            with self.assertRaises(TimeoutError):
                multi.receive_all({0: a}, 'done', .02)
        finally:
            a.close()
            b.close()


if __name__ == '__main__':
    unittest.main()
