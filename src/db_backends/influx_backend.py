"""InfluxDB OSS v2 pilot adapter: synchronous HTTP batches and Flux ranges.

Each worker owns one persistent HTTP connection. No asynchronous buffering or
automatic write retries. This implements the concurrent pilot interface only.
"""
import csv
import http.client
import io
import json
import math
from datetime import datetime, timezone
from urllib.parse import urlencode

EPOCH = datetime(2024, 1, 1, tzinfo=timezone.utc)
EPOCH_NS = 1704067200000000000
STEP_NS = 100000000


def tag(value):
    value = str(value)
    if any(c in value for c in "\r\n\\"):
        raise ValueError("Unsupported tag characters")
    return value.replace(" ", "\\ ").replace(",", "\\,").replace("=", "\\=")


def string_field(value):
    value = str(value)
    if "\n" in value or "\r" in value:
        raise ValueError("Newline in string field")
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


def number(value):
    value = float(value)
    if not math.isfinite(value):
        raise ValueError("Non-finite field value")
    return repr(value)


def timestamp_ns(value):
    if value.tzinfo is None:
        raise ValueError("Timezone-aware timestamp required")
    delta = value.astimezone(timezone.utc) - EPOCH
    return EPOCH_NS + ((delta.days * 86400 + delta.seconds) * 1000000
                       + delta.microseconds) * 1000


def parse_csv(text):
    """Handle repeated Flux table headers and HTTP-200 query error tables."""
    header = None
    rows = []
    for values in csv.reader(io.StringIO(text)):
        if not values or values[0].startswith("#"):
            continue
        if "error" in values and "reference" in values:
            raise RuntimeError("InfluxDB returned a Flux error table")
        if "result" in values and "table" in values:
            header = values
            continue
        if header is None or len(values) != len(header):
            raise RuntimeError("Unexpected Flux CSV structure")
        rows.append(dict(zip(header, values)))
    return rows


class InfluxBackend:
    def __init__(self, target, read_only=False):
        self.target = dict(target)
        self.read_only = read_only
        self.conn = None

    def connect(self):
        if self.target['host'] != '127.0.0.1':
            raise ValueError("Pilot server must use loopback")
        self.conn = http.client.HTTPConnection(
            self.target['host'], self.target['port'],
            timeout=self.target.get('http_timeout', 30))
        self.health()

    def request(self, path, body=None, content_type='application/json',
                auth=True, expected=200):
        headers = {'Content-Type': content_type, 'Accept': (
            'application/csv' if path.startswith('/api/v2/query') else 'application/json')}
        if auth:
            headers['Authorization'] = 'Token ' + self.target['token']
        self.conn.request('POST' if body is not None else 'GET', path,
                          body=body, headers=headers)
        response = self.conn.getresponse()
        payload = response.read()
        if response.status != expected:
            # Avoid putting request headers or credentials in saved exceptions.
            detail = payload.decode(errors='replace').replace(self.target['token'], '[REDACTED]')[:500]
            raise RuntimeError(f'InfluxDB HTTP status {response.status}; expected {expected}: {detail}')
        return payload

    def health(self):
        health = json.loads(self.request('/health', auth=False))
        if health.get('status') != 'pass' or health.get('version', '').lstrip('v') != '2.9.1':
            raise RuntimeError('Unexpected InfluxDB version or health')
        return health

    def close(self):
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def write(self, lines):
        if self.read_only:
            raise RuntimeError('Write attempted through read-only pilot adapter')
        params = urlencode({'org': self.target['org'], 'bucket': self.target['bucket'],
                            'precision': 'ns'})
        self.request('/api/v2/write?' + params, '\n'.join(lines).encode(),
                     'text/plain; charset=utf-8', expected=204)

    def insert_scientific_metadata_batch(self, records):
        self.write([
            f'scientific_metadata,variable={tag(variable)},location={tag(location)} '
            f'record_id={record_id}i,timestep={timestep}i,value={number(value)} '
            f'{EPOCH_NS + record_id * STEP_NS}'
            for record_id, timestep, variable, location, value in records
        ])

    def insert_telemetry_batch(self, records):
        self.write([
            f'telemetry,metric={tag(r["metric"])} value={number(r["value"])},'
            f'tags_json={string_field(json.dumps(r.get("tags", {})))} '
            f'{timestamp_ns(r["ts"])}'
            for r in records
        ])

    def query(self, flux):
        body = json.dumps({'query': flux, 'dialect': {
            'header': True, 'annotations': [], 'delimiter': ','}}).encode()
        params = urlencode({'org': self.target['org']})
        return parse_csv(self.request('/api/v2/query?' + params, body).decode())

    def source(self, measurement, first_ns, last_ns):
        return (f'from(bucket: {json.dumps(self.target["bucket"])})'
                f' |> range(start: time(v: {first_ns}), stop: time(v: {last_ns}))'
                f' |> filter(fn: (r) => r._measurement == {json.dumps(measurement)})')

    def query_scientific_metadata_range(self, first, last):
        flux = self.source('scientific_metadata', EPOCH_NS + first * STEP_NS,
                           EPOCH_NS + last * STEP_NS)
        flux += (' |> pivot(rowKey: ["_time"],'
                 ' columnKey: ["_field"], valueColumn: "_value")'
                 ' |> group()'
                 ' |> sort(columns: ["_time"])'
                 ' |> keep(columns: ["record_id", "timestep", "variable", "location", "value"])')
        return [(int(r['record_id']), int(r['timestep']), r['variable'],
                 r['location'], float(r['value'])) for r in self.query(flux)]

    def query_telemetry_range(self, start, end):
        flux = self.source('telemetry', timestamp_ns(start), timestamp_ns(end) + 1)
        flux += (' |> pivot(rowKey: ["_time"],'
                 ' columnKey: ["_field"], valueColumn: "_value")'
                 ' |> group()'
                 ' |> sort(columns: ["_time"])'
                 ' |> keep(columns: ["_time", "metric", "value", "tags_json"])')
        return [(datetime.fromisoformat(r['_time'].replace('Z', '+00:00')),
                 r['metric'], float(r['value']), r['tags_json']) for r in self.query(flux)]

    def validate_records(self, workload, expected):
        measurement = 'scientific_metadata' if workload == 'metadata' else 'telemetry'
        # Use the full supported positive-time range so stray points also count.
        base = (self.source(measurement, 0, 9223372036854775807)
                + ' |> filter(fn: (r) => r._field == "value") |> group()')
        def count(suffix):
            rows = self.query(base + suffix + ' |> count(column: "_value")'
                              ' |> keep(columns: ["_value"])')
            return sum(int(r['_value']) for r in rows)
        stored = count('')
        unique = count(' |> map(fn: (r) => ({_value: int(v: r._time)}))'
                       ' |> distinct(column: "_value")')
        if (stored, unique) != (expected, expected):
            raise RuntimeError(f'InfluxDB count/unique-time mismatch: {(stored, unique)}')
        return stored
