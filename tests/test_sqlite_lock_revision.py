"""Real small SQLite processes; local filesystems are explicitly labelled tests."""
import copy
import csv
import json
import os
from pathlib import Path
import shutil
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'scripts'))
import sqlite_lock_revision as s
from validate_sqlite_lock_revision import validate,check_trial


def config(suite):
    c=json.loads((ROOT/f'configs/sqlite-lock-revision/{suite}.json').read_text())
    c.update(clients=4,batch_size=16,records=[103] if suite=='lock-policy' else [103,201],
             repetitions=1,warmup_repetitions=0,queue_batches=2,stagger_max_s=.01,
             trial_timeout_s=25,startup_timeout_s=25,lustre_counters=False)
    return c


class RevisionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp=tempfile.TemporaryDirectory(prefix='sqlite-revision-tests-')
        cls.root=Path(cls.temp.name)
        cls.runs={}
        for suite in ('lock-policy','copy-out'):
            cls.runs[suite]=s.run_study(config(suite),cls.root/'results',
                {'lustre':cls.root,'tmpfs':cls.root},local_test=True,fixed=False)

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    def test_all_policies_queue_partial_batch_and_copy_roundtrip(self):
        for suite,run in self.runs.items():
            receipt=validate(run,allow_local=True)
            self.assertEqual(receipt['outcomes'],{'ok':14 if suite=='lock-policy' else 8})

    def test_fixed_plan_counts_and_matched_block_seeds(self):
        for suite,count in [('lock-policy',84),('copy-out',48)]:
            cfg=json.loads((ROOT/f'configs/sqlite-lock-revision/{suite}.json').read_text())
            plan=s.build_plan(cfg)
            self.assertEqual(len(plan),count)
            for rep in range(1,6):
                block=[p for p in plan if p['phase']=='measured' and p['repetition']==rep]
                self.assertEqual(len({p['seed'] for p in block}),1)
        self.assertEqual(sum(p['policy']=='queue' for p in s.build_plan(
            json.loads((ROOT/'configs/sqlite-lock-revision/lock-policy.json').read_text()))),12)

    def test_real_busy_errors_preserved_without_primary_throughput(self):
        cfg=config('lock-policy')
        item=dict(s.build_plan(cfg,fixed=False)[0],policy='direct',busy_timeout_ms=20,
                  storage='tmpfs')
        held=[]
        initialize,collect=s.core.initialize,s.core.collect
        def init(*a):
            c=initialize(*a)
            c.execute('BEGIN IMMEDIATE')
            held.append(c)
            return c
        def receive(channels,kind,timeout):
            out=collect(channels,kind,timeout)
            if kind=='done':
                for c in held:
                    c.execute('ROLLBACK')
            return out
        with patch.object(s.core,'initialize',side_effect=init),patch.object(s.core,'collect',side_effect=receive):
            r=s.run_trial(item,cfg,{'tmpfs':self.root,'lustre':self.root},
                          self.root/'expected-busy.json',local_test=True)
        self.assertTrue(r['valid_observation'],r.get('error'))
        self.assertEqual(r['status'],'busy')
        self.assertEqual(r['sqlite_busy_errors'],4)
        self.assertEqual(r['committed_records'],0)
        self.assertEqual(r['application_retries'],0)
        self.assertIsNone(r['write_records_s'])
        check_trial(r,item,cfg,True)

    def test_failed_insert_rolls_back_whole_batch(self):
        with tempfile.TemporaryDirectory(dir=self.root) as d:
            c=s.core.initialize(Path(d)/'db',60000)
            try:
                rows=s.core.generate_batch(0,4,4,1)
                rows.append(rows[0])
                a=s.attempt(c,rows,0,0,1,2)
                self.assertEqual(a['status'],'error')
                self.assertFalse(c.in_transaction)
                self.assertEqual(c.execute('SELECT COUNT(*) FROM scientific_metadata').fetchone()[0],0)
            finally:
                c.close()

    def test_fsync_failure_is_not_successful_copy(self):
        with tempfile.TemporaryDirectory(dir=self.root) as d:
            src=Path(d)/'source'
            src.write_bytes(b'content')
            receipt={}
            with self.assertRaises(OSError):
                s.copy_out([src],Path(d)/'copy',receipt,fsync=lambda fd:(_ for _ in ()).throw(OSError('injected sync error')))
            self.assertFalse(receipt.get('success',False))
            self.assertFalse(receipt['files'][0]['file_fsync'])

    def test_csv_tampering_rejected(self):
        run=self.runs['lock-policy']
        path=run/'results.csv'
        original=path.read_bytes()
        try:
            rows=list(csv.DictReader(path.read_text().splitlines()))
            rows[0]['completion_records_s']='1'
            with path.open('w',newline='') as f:
                w=csv.DictWriter(f,fieldnames=s.CSV_FIELDS,lineterminator='\n')
                w.writeheader();w.writerows(rows)
            with self.assertRaises(ValueError):
                validate(run,allow_local=True)
        finally:
            path.write_bytes(original)

    def test_queue_origin_and_stagger_timing_corruption_rejected(self):
        cfg=config('lock-policy')
        run=self.runs['lock-policy']
        for policy in ('queue','staggered'):
            r=next(json.loads(p.read_text()) for p in run.glob('trial-*.json')
                   if json.loads(p.read_text())['policy']==policy)
            item={k:r[k] for k in s.build_plan(cfg,fixed=False)[0]}
            altered=copy.deepcopy(r)
            if policy=='queue':
                altered['workers'][-1]['batches'][0]['owner']=99
            else:
                w=altered['workers'][0]
                w['batches'][0]['prepare_start_ns']=w['started_ns']-1
            with self.assertRaises(ValueError):
                check_trial(altered,item,cfg,True)

    def test_copy_checksum_corruption_rejected(self):
        cfg=config('copy-out')
        r=next(json.loads(p.read_text()) for p in self.runs['copy-out'].glob('trial-*.json')
               if json.loads(p.read_text())['storage']=='tmpfs')
        item={k:r[k] for k in s.build_plan(cfg,fixed=False)[0]}
        r['validation'][0]['copy_verified']['destination_sha256']='0'*64
        with self.assertRaises(ValueError):
            check_trial(r,item,cfg,True)

    def test_unknown_sql_error_preserved_and_never_successful(self):
        cfg=config('lock-policy')
        item=dict(s.build_plan(cfg,fixed=False)[0],policy='direct',storage='tmpfs')
        initialize=s.core.initialize
        def init(*a):
            c=initialize(*a)
            c.execute('DROP TABLE scientific_metadata')
            return c
        with patch.object(s.core,'initialize',side_effect=init):
            r=s.run_trial(item,cfg,{'tmpfs':self.root,'lustre':self.root},
                          self.root/'unexpected-error.json',local_test=True)
        self.assertFalse(r['valid_observation'])
        self.assertFalse(r['success'])
        self.assertEqual(r['status'],'error')
        self.assertTrue(r['workers_before_samples'][0]['errors'])

    def test_unsupported_counter_access_is_documented(self):
        with patch.object(s.shutil,'which',return_value=None):
            self.assertEqual(s.lustre_counters(True)['status'],'unavailable')


if __name__=='__main__':
    unittest.main()
