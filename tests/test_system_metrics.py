import json
from pathlib import Path
import sys
import tempfile
import unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'))
from collect_system_metrics import summarize_node, perf_values, collect

class SystemMetricsTests(unittest.TestCase):
    def test_cpu_and_memory_deltas(self):
        samples=[dict(cpu_ticks=[100]*8,memory_kib={'MemTotal':2048,'MemAvailable':1024}),
                 dict(cpu_ticks=[120,100,110,150,110,100,100,110],memory_kib={'MemTotal':2048,'MemAvailable':512})]
        result=summarize_node(samples)
        self.assertEqual(result['node_busy_percent'],40)
        self.assertEqual(result['node_iowait_percent'],10)
        self.assertEqual(result['node_idle_percent'],50)
        self.assertEqual(result['node_sampled_peak_used_MiB'],1.5)

    def test_negative_iowait_is_not_silently_clamped(self):
        samples=[dict(cpu_ticks=[100]*8,memory_kib={}),dict(cpu_ticks=[120,100,110,150,99,100,100,110],memory_kib={})]
        result=summarize_node(samples)
        self.assertEqual(result['node_cpu_status'],'invalid_counter_delta')
        self.assertNotIn('node_iowait_percent',result)

    def test_perf_unavailable_and_coverage(self):
        with tempfile.TemporaryDirectory() as temp:
            path=Path(temp)/'perf.csv'
            path.write_text('100;;cycles;1000000;99.5;;\n200;;instructions;1000000;99.5;;\n<not supported>;;cache-misses;0;0;;\n')
            result=perf_values(path)
            self.assertEqual(result['cycles']['count'],100)
            self.assertEqual(result['instructions']['running_percent'],99.5)
            self.assertNotIn('cache-misses',result)

    def test_collection_with_real_subprocess_without_perf(self):
        with tempfile.TemporaryDirectory() as temp:
            directory=Path(temp)
            result=collect([sys.executable,'-c','sum(i*i for i in range(1000000))'],directory,None,[],None)
            self.assertEqual(result['command_exit_code'],0)
            self.assertEqual(result['perf_events'],{})
            self.assertGreaterEqual(result['node_samples'],2)
            samples=[json.loads(line) for line in (directory/'node-samples.jsonl').read_text().splitlines()]
            self.assertEqual(len(samples),result['node_samples'])
            self.assertFalse(result['sampler_errors'])

if __name__=='__main__':unittest.main()
