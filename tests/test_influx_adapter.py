"""Local contract tests. Real Flux/Shifter execution requires the compute check."""
import csv
import io
import json
import sys
import unittest
from datetime import timedelta
from pathlib import Path
from unittest.mock import Mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
sys.path.insert(0, str(ROOT))
from db_backends.influx_backend import InfluxBackend, EPOCH, EPOCH_NS, STEP_NS, parse_csv


class InfluxAdapterTests(unittest.TestCase):
    def backend(self):
        return InfluxBackend(dict(host='127.0.0.1', port=12345,
                                  org='ipdps', bucket='benchmark', token='secret-test-token'))

    def test_metadata_identity_and_type_encoding(self):
        b = self.backend()
        b.write = Mock()
        b.insert_scientific_metadata_batch([
            (0, 0, 'temperature', 'node_0', 1.5),
            (1, 0, 'pressure', 'node_1', -2.0)])
        self.assertEqual(b.write.call_args.args[0], [
            f'scientific_metadata,variable=temperature,location=node_0 record_id=0i,timestep=0i,value=1.5 {EPOCH_NS}',
            f'scientific_metadata,variable=pressure,location=node_1 record_id=1i,timestep=0i,value=-2.0 {EPOCH_NS + STEP_NS}'])

    def test_telemetry_preserves_json_and_exact_timestamp(self):
        b = self.backend()
        b.write = Mock()
        b.insert_telemetry_batch([dict(ts=EPOCH + timedelta(milliseconds=100),
            metric='temperature', value=2.5, tags={'sensor_id': 'sensor_1'})])
        self.assertEqual(b.write.call_args.args[0], [
            'telemetry,metric=temperature value=2.5,tags_json="{\\"sensor_id\\": \\"sensor_1\\"}" '
            + str(EPOCH_NS + STEP_NS)])

    def test_csv_tables_and_query_error(self):
        text = ',result,table,_value\n,_result,0,3\n\n,result,table,_value\n,_result,1,4\n'
        self.assertEqual([r['_value'] for r in parse_csv(text)], ['3', '4'])
        with self.assertRaises(RuntimeError):
            parse_csv(',error,reference\n,query failed,123\n')
        with self.assertRaises(RuntimeError):
            parse_csv('nonsense\n')

    def test_range_rows_keep_payload_and_key_types(self):
        b = self.backend()
        b.query = Mock(return_value=[dict(record_id='2', timestep='0',
                                          variable='density', location='node_2', value='3.5')])
        self.assertEqual(b.query_scientific_metadata_range(2, 3), [(2, 0, 'density', 'node_2', 3.5)])
        query = b.query.call_args.args[0]
        self.assertLess(query.index('pivot('), query.index('group('))
        self.assertIn(f'start: time(v: {EPOCH_NS + 2 * STEP_NS})', query)
        b.query.return_value = [{'_time': '2024-01-01T00:00:00.2Z', 'metric': 'density',
                                 'value': '3.5', 'tags_json': '{"node": "node_2"}'}]
        rows = b.query_telemetry_range(EPOCH + timedelta(milliseconds=200), EPOCH + timedelta(milliseconds=200))
        self.assertEqual(rows[0], (EPOCH + timedelta(milliseconds=200), 'density', 3.5, '{"node": "node_2"}'))
        self.assertIn(f'stop: time(v: {EPOCH_NS + 2 * STEP_NS + 1})', b.query.call_args.args[0])

    def test_duplicate_timestamps_rejected(self):
        b = self.backend()
        b.query = Mock(side_effect=[[{'_value': '3'}], [{'_value': '2'}]])
        with self.assertRaises(RuntimeError):
            b.validate_records('metadata', 3)

    def test_http_rejects_partial_writes_without_retry(self):
        b = self.backend()
        b.conn = Mock()
        response = b.conn.getresponse.return_value
        response.status = 400
        response.read.return_value = b'partial write secret-test-token'
        with self.assertRaises(RuntimeError) as error:
            b.write(['telemetry value=1.0 1'])
        self.assertNotIn('secret-test-token', str(error.exception))
        self.assertEqual(b.conn.request.call_count, 1)
        b.read_only = True
        with self.assertRaises(RuntimeError):
            b.write(['telemetry value=1.0 1'])
        self.assertEqual(b.conn.request.call_count, 1)


if __name__ == '__main__':
    unittest.main()
