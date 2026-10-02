"""Explicit 50/50 metadata/telemetry composition; separate write/read phases.

A logical write batch contains half of each record type. It performs two
independent backend calls, NOT one atomic transaction. Every worker handles
both types. Read queries are assigned in metadata/telemetry pairs so every
worker also reads both types. IDs are contiguous within each table.
"""
MIXED_FIELDS = [
    'mixed_policy', 'logical_write_batches', 'backend_write_calls',
    'stored_metadata_records', 'stored_telemetry_records',
    'metadata_read_queries', 'telemetry_read_queries',
]


def validate_mixed_config(cfg):
    if cfg['workload'] != 'mixed':
        return
    if cfg['records'] % 2 or cfg['batch_size'] % 2:
        raise ValueError('Mixed records and batch_size must both be even')
    if cfg['query_window'] > cfg['records'] // 2:
        raise ValueError('Mixed query_window exceeds records per table')
    queries = cfg['read_queries']
    if queries % 2 or (queries and queries < 2 * max(cfg['clients'])):
        raise ValueError('Mixed reads require an even count and one query pair per client')


def query_ids(cfg, worker_id):
    if cfg['workload'] != 'mixed':
        yield from range(worker_id, cfg['read_queries'], cfg['clients'])
    else:
        for pair in range(worker_id, cfg['read_queries'] // 2, cfg['clients']):
            yield 2 * pair
            yield 2 * pair + 1


def query_workload(cfg, query_id):
    return ('metadata' if query_id % 2 == 0 else 'telemetry') if cfg['workload'] == 'mixed' else cfg['workload']


def table_specs(cfg):
    specs = {'metadata': ('scientific_metadata', 'record_id'),
             'telemetry': ('telemetry', 'ts')}
    if cfg['workload'] == 'mixed':
        return [(name, *specs[name], cfg['records'] // 2)
                for name in ('metadata', 'telemetry')]
    return [(cfg['workload'], *specs[cfg['workload']], cfg['records'])]


def validate_sql_counts(conn, cfg):
    counts = {}
    for name, table, key, expected in table_specs(cfg):
        count, unique = conn.execute(
            f'SELECT COUNT(*), COUNT(DISTINCT {key}) FROM {table}'
        ).fetchone()
        if (count, unique) != (expected, expected):
            raise RuntimeError(f'{name} count/unique mismatch: {(count, unique)}, expected {expected}')
        counts[name] = count
    return counts


def mixed_result(cfg, counts, logical_batches):
    if cfg['workload'] != 'mixed':
        return {}
    return {
        'mixed_policy': '50_percent_records_each_type; two_nonatomic_calls_per_logical_batch; paired_warm_reads',
        'logical_write_batches': logical_batches,
        'backend_write_calls': 2 * logical_batches,
        'transactions': (None if cfg.get('database') == 'influxdb' else 2 * logical_batches),
        'write_batches': 2 * logical_batches,
        # Existing transaction latency fields refer to logical batches here.
        'write_latency_unit': 'logical_mixed_batch_two_nonatomic_backend_calls',
        'stored_metadata_records': counts['metadata'],
        'stored_telemetry_records': counts['telemetry'],
    }


class MixedBatchWorkload:
    def __init__(self, backend, batch_size, seed):
        from pilot_workloads import MetadataBatchWorkload, TelemetryBatchWorkload
        self.metadata = MetadataBatchWorkload(backend, batch_size // 2, seed)
        self.telemetry = TelemetryBatchWorkload(backend, batch_size // 2, seed)

    def generate_batch(self, first, last, base_ts, rng):
        if first % 2 or last % 2:
            raise ValueError('Mixed batch boundaries must be even')
        lo, hi = first // 2, last // 2
        return (self.metadata.generate_batch(lo, hi, base_ts, rng)
                + self.telemetry.generate_batch(lo, hi, base_ts, rng))

    def insert_batch(self, records):
        half = len(records) // 2
        self.metadata.insert_batch(records[:half])
        self.telemetry.insert_batch(records[half:])
