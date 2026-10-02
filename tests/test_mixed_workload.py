"""Deterministic mixed data and real concurrent SQLite integration checks."""
import random
import csv
import json
import os
import sqlite3
import sys
import tempfile
import unittest
import yaml
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / 'src'))
from mixed_workloads import (MixedBatchWorkload, query_ids, query_workload,
                             validate_mixed_config, validate_sql_counts)
from experiment import build_plan

BASE = dict(database='sqlite', runtime='native', workload='mixed', records=12800,
            batch_size=100, read_queries=128, query_window=10, clients=[1,16,64],
            storage=['tmpfs'], repetitions=1, warmup_repetitions=0,
            seed=20261002, timeout=90)

class MixedTests(unittest.TestCase):
    def test_plan_rejects_invalid_mixed_dimensions(self):
        self.assertEqual(len(build_plan(BASE)), 3)
        for key, value in [('records',12799),('batch_size',99),
                           ('read_queries',127),('query_window',6401)]:
            cfg={**BASE,key:value}
            with self.assertRaises(ValueError): build_plan(cfg)

    def test_every_client_receives_both_query_types_exactly_once(self):
        for clients in (1,16,64):
            cfg={**BASE,'clients':clients}
            assigned=[list(query_ids(cfg,w)) for w in range(clients)]
            self.assertEqual(sorted(q for qs in assigned for q in qs),list(range(128)))
            for qs in assigned:
                self.assertEqual({query_workload(cfg,q) for q in qs},{'metadata','telemetry'})

    def test_data_independent_of_worker_assignment(self):
        generator=MixedBatchWorkload(None,100,42)
        base=datetime(2024,1,1,tzinfo=timezone.utc)
        results=[]
        for clients in (1,16,64):
            batches={}
            for worker in range(clients):
                for batch in range(worker,128,clients):
                    rows=generator.generate_batch(batch*100,(batch+1)*100,base,random.Random(42+batch))
                    self.assertEqual(len(rows),100)
                    self.assertEqual([r[0] for r in rows[:50]],list(range(batch*50,(batch+1)*50)))
                    batches[batch]=rows
            results.append(batches)
        self.assertEqual(results[0],results[1]); self.assertEqual(results[0],results[2])

    def test_second_backend_call_failure_propagates(self):
        class Broken:
            def insert_scientific_metadata_batch(self, rows): pass
            def insert_telemetry_batch(self, rows): raise RuntimeError('injected failure')
        with self.assertRaisesRegex(RuntimeError,'injected failure'):
            MixedBatchWorkload(Broken(),2,42).insert_batch([(0,0,'x','y',1),{}])

    def test_counts_reject_missing_rows_even_when_total_matches(self):
        conn=sqlite3.connect(':memory:')
        conn.execute('CREATE TABLE scientific_metadata(record_id INTEGER)')
        conn.execute('CREATE TABLE telemetry(ts TEXT)')
        conn.executemany('INSERT INTO scientific_metadata VALUES (?)',[(0,),(1,),(2,),(3,)])
        with self.assertRaises(RuntimeError):
            validate_sql_counts(conn,{'workload':'mixed','records':4})
        conn.close()

    def test_real_concurrent_sqlite_write_read_checkpoint(self):
        from concurrent_sqlite import run
        # Use a real temporary directory; mock only site-specific mount discovery.
        for clients in (1,4):
            with self.subTest(clients=clients), tempfile.TemporaryDirectory() as parent:
                trial=Path(parent)/'trial'; trial.mkdir()
                cfg={**BASE,'clients':clients,'storage':'tmpfs'}
                result={}
                with patch('concurrent_sqlite.StorageManager.prepare',return_value={
                    'path':str(trial),'filesystem':'local_test_only','label':'tmpfs'}):
                    run(cfg,result)
                self.assertTrue(result['success'])
                self.assertEqual(result['stored_records'],12800)
                self.assertEqual(result['stored_metadata_records'],6400)
                self.assertEqual(result['stored_telemetry_records'],6400)
                self.assertEqual(result['backend_write_calls'],256)
                self.assertEqual(result['transactions'],256)
                self.assertEqual(result['metadata_read_queries'],64)
                self.assertEqual(result['telemetry_read_queries'],64)
                self.assertEqual(result['logical_write_batches'],128)
                self.assertEqual(result['read_rows_returned'],1280)
                self.assertEqual(len(result['transaction_latency_samples_ms']),128)
                self.assertEqual(result['worker_read_queries'],[128//clients]*clients)
                self.assertGreater(result['final_checkpoint_s'],0)
                self.assertFalse(trial.exists())


    def test_controller_csv_raw_and_validator_round_trip(self):
        import experiment
        sys.path.insert(0, str(ROOT / 'scripts'))
        from validate_mixed_results import verify
        with tempfile.TemporaryDirectory() as parent:
            parent=Path(parent)
            cfg={**BASE, 'clients':[1,4]}
            config=parent/'config.yaml'; config.write_text(yaml.safe_dump(cfg))
            output=parent/'output'
            def prepare(manager,label):
                trial=Path(tempfile.mkdtemp(dir=parent))
                return dict(path=str(trial),filesystem=label,label=label)
            env={**os.environ,'SLURM_JOB_ID':'local-test','IPDPS_RUNTIME':'native'}
            env.pop('SHIFTER_IMAGE',None)
            with patch.dict(os.environ,env,clear=True), \
                 patch('socket.gethostname',return_value='nid-local-test'), \
                 patch('os.sched_getaffinity',return_value=set(range(64))), \
                 patch('concurrent_sqlite.StorageManager.prepare',prepare), \
                 patch.object(sys,'argv',['experiment.py','--config',str(config),'--output-root',str(output)]):
                self.assertEqual(experiment.main(),0)
            run=next(output.iterdir())
            receipt=verify(run,'local-test')
            self.assertTrue(receipt['success'])
            raw_path=run/'trial-0001.json'
            raw=json.loads(raw_path.read_text())
            raw['metadata_read_queries']-=1
            raw_path.write_text(json.dumps(raw))
            with self.assertRaisesRegex(ValueError,'metadata_read_queries'):
                verify(run,'local-test')

    def test_existing_single_workloads_still_run(self):
        from concurrent_sqlite import run
        for workload in ('metadata','telemetry'):
            with self.subTest(workload=workload),tempfile.TemporaryDirectory() as parent:
                trial=Path(parent)/'trial';trial.mkdir()
                cfg={**BASE,'workload':workload,'clients':2,'storage':'tmpfs','records':800,'read_queries':8}
                result={}
                with patch('concurrent_sqlite.StorageManager.prepare',return_value={
                    'path':str(trial),'filesystem':'local_test_only','label':'tmpfs'}):
                    run(cfg,result)
                self.assertTrue(result['success'])
                self.assertEqual(result['stored_records'],800)
                self.assertEqual(result['transactions'],8)
                self.assertNotIn('mixed_policy',result)
                self.assertEqual(result[workload+'_read_queries'],8)

    def test_influx_mixed_uses_two_preserving_adapter_calls(self):
        from db_backends.influx_backend import InfluxBackend
        class Capture(InfluxBackend):
            def __init__(self): self.lines=[]
            def write(self,lines):self.lines.append(lines)
        backend=Capture()
        generator=MixedBatchWorkload(backend,4,42)
        records=generator.generate_batch(0,4,datetime(2024,1,1,tzinfo=timezone.utc),random.Random(42))
        generator.insert_batch(records)
        self.assertEqual([len(lines) for lines in backend.lines],[2,2])
        self.assertTrue(all(line.startswith('scientific_metadata,') for line in backend.lines[0]))
        self.assertTrue(all(line.startswith('telemetry,') for line in backend.lines[1]))
        self.assertIn('record_id=0i',backend.lines[0][0])
        self.assertIn('record_id=1i',backend.lines[0][1])
        self.assertEqual([line.rsplit(' ',1)[1] for line in backend.lines[0]],
                         [line.rsplit(' ',1)[1] for line in backend.lines[1]])

if __name__=='__main__': unittest.main()
