"""Correctness, real contention, failure handling and export validation tests."""
import copy
import csv
import json
import multiprocessing as mp
from pathlib import Path
import sqlite3
import sys
import tempfile
import threading
import time
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts'))
import sqlite_mechanism_study as study
import validate_sqlite_mechanism as validator


def release_when_asked(path, channel):
    conn = study.connect(path, 1000)
    conn.execute('BEGIN IMMEDIATE')
    channel.send('locked')
    channel.recv()
    conn.execute('ROLLBACK')
    conn.close()
    channel.close()


class SQLiteMechanismTests(unittest.TestCase):
    def config(self, suite='size-sweep'):
        return json.loads((ROOT / 'configs/sqlite-mechanism' / (suite + '.json')).read_text())

    def small_config(self):
        cfg = self.config()
        cfg.update(records=[103], batch_size=16, clients=[1, 4],
                   layouts=['shared', 'partitioned'], repetitions=1,
                   warmup_repetitions=0, busy_timeout_ms=2000,
                   startup_timeout_s=30, trial_timeout_s=30)
        return cfg

    def test_fixed_counts_randomized_complete_blocks_and_data_seeds(self):
        for suite, trials, conditions in [('size-sweep', 72, 12), ('partitioned', 168, 28)]:
            cfg = self.config(suite)
            plan = study.check_fixed_design(cfg)
            self.assertEqual(len(plan), trials)
            for start in range(0, trials, conditions):
                block = plan[start:start + conditions]
                self.assertEqual(len({(p['records'], p['storage'], p['clients'], p['layout'])
                                      for p in block}), conditions)
                self.assertEqual(len({p['seed'] for p in block}), 1)
            self.assertNotEqual(plan[0]['seed'], plan[conditions]['seed'])
        cfg = self.config()
        cfg['records'] = [1000000]
        with self.assertRaisesRegex(ValueError, 'matrix'):
            study.check_fixed_design(cfg)

    def test_partition_preserves_exact_work_and_data_with_partial_batch(self):
        baseline = [r for b in range(7) for r in study.generate_batch(b, 103, 16, 27)]
        combined = []
        for worker_id in range(4):
            rows = [r for b in range(worker_id, 7, 4)
                    for r in study.generate_batch(b, 103, 16, 27)]
            combined.extend(rows)
            ids = [r[0] for r in rows]
            self.assertEqual((len(ids), min(ids), max(ids), sum(ids)),
                             study.expected_ids(dict(records=103, clients=4), 16, worker_id))
        self.assertEqual(sorted(combined), baseline)
        self.assertEqual(len(combined), 103)

    def test_real_shared_lock_blocks_but_independent_file_does_not(self):
        with tempfile.TemporaryDirectory() as temporary:
            shared, separate = Path(temporary) / 'shared.db', Path(temporary) / 'separate.db'
            keeper = study.initialize(shared, 1000)
            other_keeper = study.initialize(separate, 1000)
            context = mp.get_context('spawn')
            parent, child = context.Pipe()
            process = context.Process(target=release_when_asked, args=(str(shared), child))
            process.start()
            child.close()
            self.assertTrue(parent.poll(10))
            self.assertEqual(parent.recv(), 'locked')
            blocked = study.connect(shared, 80)
            independent = study.connect(separate, 80)
            row = study.generate_batch(0, 1, 1, 3)
            try:
                began = time.perf_counter()
                with self.assertRaisesRegex(sqlite3.OperationalError, 'locked'):
                    study.timed_insert(blocked, row)
                self.assertGreaterEqual(time.perf_counter() - began, .05)
                study.timed_insert(independent, row)
                self.assertEqual(independent.execute('SELECT COUNT(*) FROM scientific_metadata').fetchone()[0], 1)
                blocked.execute('PRAGMA busy_timeout=2000')
                timer = threading.Timer(.12, parent.send, args=('release',))
                timer.start()
                result = study.timed_insert(blocked, row)
                timer.join()
                process.join(10)
                self.assertEqual(process.exitcode, 0)
                self.assertGreaterEqual(result['lock_acquire_ms'], 60)
                self.assertAlmostEqual(result['acquired_to_commit_return_ms'],
                                       result['transaction_body_ms'] + result['commit_ms'])
            finally:
                if process.is_alive():
                    process.terminate()
                    process.join(10)
                parent.close()
                blocked.close()
                independent.close()
                keeper.close()
                other_keeper.close()

    def test_failed_transaction_rolls_back(self):
        with tempfile.TemporaryDirectory() as temporary:
            conn = study.initialize(Path(temporary) / 'db', 1000)
            try:
                rows = study.generate_batch(0, 2, 2, 6)
                with self.assertRaises(sqlite3.IntegrityError):
                    study.timed_insert(conn, rows + [rows[0]])
                self.assertFalse(conn.in_transaction)
                self.assertEqual(conn.execute('SELECT COUNT(*) FROM scientific_metadata').fetchone()[0], 0)
            finally:
                conn.close()

    def test_shard_validation_rejects_wrong_ownership(self):
        with tempfile.TemporaryDirectory() as temporary:
            cfg = self.small_config()
            item = dict(records=103, clients=4, seed=17)
            conn = study.initialize(Path(temporary) / 'db', 1000)
            try:
                # Worker zero should own batches 0 and 4, not batches 0 and 1.
                for b in [0, 1]:
                    study.timed_insert(conn, study.generate_batch(b, 103, 16, 17))
                with self.assertRaises(ValueError):
                    study.validate_database(conn, item, cfg, worker_id=0)
            finally:
                conn.close()

    def test_production_storage_label_cannot_hide_local_filesystem(self):
        with tempfile.TemporaryDirectory() as temporary:
            with self.assertRaises(ValueError):
                study.describe_storage(temporary, 'lustre')
            info = study.describe_storage(temporary, 'lustre', local_test=True)
            self.assertFalse(info['filesystem_verified'])

    def test_concurrent_roundtrip_and_corrupt_results_are_rejected(self):
        cfg = self.small_config()
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            data = root / 'data'
            data.mkdir()
            run = study.run_study(cfg, root / 'output', {'lustre': data, 'tmpfs': data},
                                  local_test=True, fixed=False)
            receipt = validator.validate(run, allow_local=True)
            self.assertEqual(receipt['trials'], 8)
            self.assertFalse(list(data.iterdir()))
            with self.assertRaisesRegex(ValueError, 'Local test'):
                validator.validate(run)
            raw_path = run / 'trial-0001.json'
            original = json.loads(raw_path.read_text())
            for key, bad in [('stored_records', 999), ('lock_acquire_ms_p99', -1),
                             ('completion_records_s', 1.0)]:
                changed = copy.deepcopy(original)
                changed[key] = bad
                raw_path.write_text(json.dumps(changed))
                with self.assertRaises(ValueError):
                    validator.validate(run, allow_local=True)
            raw_path.write_text(json.dumps(original))
            path = run / 'results.csv'
            with path.open(newline='') as stream:
                rows = list(csv.DictReader(stream))
            rows[0]['worker_cpu_s'] = '-1'
            with path.open('w', newline='') as stream:
                writer = csv.DictWriter(stream, fieldnames=study.CSV_COLUMNS)
                writer.writeheader()
                writer.writerows(rows)
            with self.assertRaisesRegex(ValueError, 'CSV mismatch'):
                validator.validate(run, allow_local=True)

    def test_failed_trial_is_preserved_and_not_reported_successful(self):
        cfg = self.small_config()
        item = study.build_plan(cfg)[0]
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / 'failed.json'
            result = study.run_trial(item, cfg, Path(temporary) / 'missing-root', output, local_test=True)
            self.assertFalse(result['success'])
            self.assertIn('Storage root missing', result['error'])
            self.assertFalse(json.loads(output.read_text())['success'])


if __name__ == '__main__':
    unittest.main()
