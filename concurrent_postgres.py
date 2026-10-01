"""PostgreSQL coordinator reusing the pilot's synchronized write/read workers."""
import multiprocessing as mp
import queue
import time
from pathlib import Path
import psycopg
from psycopg import pq
from concurrent_sqlite import worker, percentile
from backend_factory import connect_backend
from read_phase import run_read_phase
from process_metrics import summarize_resources
from storage import StorageManager
from pg_server import PostgresServer


def write_phase(cfg, target):
    ctx = mp.get_context('spawn')
    event, messages = ctx.Event(), ctx.Queue()
    processes = []
    deadline = time.monotonic() + cfg['timeout']

    def receive(kind):
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError('PostgreSQL write phase exceeded timeout')
            try:
                message = messages.get(timeout=min(1, remaining))
            except queue.Empty:
                if any(p.exitcode not in (None, 0) for p in processes):
                    raise RuntimeError('PostgreSQL write worker crashed')
                continue
            if message['kind'] != kind:
                raise RuntimeError(f'Unexpected worker message: {message}')
            return message

    try:
        for worker_id in range(cfg['clients']):
            p = ctx.Process(target=worker, args=(worker_id, cfg, target, event, messages))
            p.start()
            processes.append(p)
        for _ in processes:
            receive('ready')
        started = time.perf_counter()
        event.set()
        done = [receive('done') for _ in processes]
        for p in processes:
            p.join(timeout=max(0, deadline - time.monotonic()))
            if p.is_alive() or p.exitcode != 0:
                raise RuntimeError('PostgreSQL write worker did not exit successfully')
        return started, done
    finally:
        for p in processes:
            if p.is_alive():
                p.terminate()
                p.join(timeout=5)
                if p.is_alive():
                    p.kill()
                    p.join()
        messages.close()


def run(cfg, result, log_path):
    storage = StorageManager({'lustre': '/pscratch/sd/m/makhatri', 'tmpfs': '/tmp'}).prepare(cfg['storage'])
    result.update(storage_info=storage, server_profiling=cfg.get('server_profiling', False), client_runtime='native', server_runtime='shifter',
                  server_image_id=cfg['server_image_id'], server_log=Path(log_path).name,
                  driver_version=psycopg.__version__, libpq_version=pq.version(),
                  worker_metrics_scope='client_processes_only',
                  server_resource_metrics_status='not_collected',
                  insertion_method='psycopg_executemany_one_transaction_per_batch')
    server = PostgresServer(storage, cfg, log_path)
    result['server_socket_directory'] = server.socket_dir
    backend = None
    try:
        server.start()
        backend = connect_backend(cfg, server.connection)
        backend.initialize_schema()
        conn = backend.conn
        result['postgresql_version'] = conn.execute('SHOW server_version').fetchone()[0]
        settings = conn.execute("""SELECT name,setting,unit FROM pg_settings WHERE name IN
            ('fsync','synchronous_commit','full_page_writes','shared_buffers','max_connections',
             'autovacuum','checkpoint_timeout','max_wal_size','data_directory',
             'listen_addresses','unix_socket_directories') ORDER BY name""").fetchall()
        result['postgresql_settings'] = {name: {'value': value, 'unit': unit} for name,value,unit in settings}
        for name in ('fsync', 'synchronous_commit', 'full_page_writes'):
            if result['postgresql_settings'][name]['value'] != 'on':
                raise RuntimeError(f'Unexpected PostgreSQL setting: {name}')

        started, done = write_phase(cfg, server.connection)
        elapsed = max(d['finished'] for d in done) - started
        committed = sum(d['committed'] for d in done)
        values = sorted(v for d in done for v in d['latencies_ms'])
        checkpoint_start = time.perf_counter()
        conn.execute('CHECKPOINT')
        checkpoint_end = time.perf_counter()
        table, key = ('scientific_metadata','record_id') if cfg['workload']=='metadata' else ('telemetry','ts')
        stored, unique = conn.execute(f'SELECT count(*),count(DISTINCT {key}) FROM {table}').fetchone()
        if (committed, stored, unique) != (cfg['records'],) * 3:
            raise RuntimeError(f'PostgreSQL record validation failed: {(committed, stored, unique)}')
        if len(values) != (cfg['records'] + cfg['batch_size'] - 1) // cfg['batch_size']:
            raise RuntimeError('PostgreSQL transaction count mismatch')

        result.update(summarize_resources(done, 'write', elapsed))
        if cfg['read_queries']:
            result.update(run_read_phase(cfg, server.connection))
        result.update(committed_records=committed, stored_records=stored,
            workload_wall_s=elapsed, throughput_records_s=committed/elapsed,
            transactions=len(values), transaction_latency_samples_ms=values,
            transaction_latency_mean_ms=sum(values)/len(values),
            transaction_latency_p50_ms=percentile(values,50),
            transaction_latency_p95_ms=percentile(values,95),
            transaction_latency_p99_ms=percentile(values,99),
            final_checkpoint_s=checkpoint_end-checkpoint_start,
            completion_wall_s=checkpoint_end-started,
            completion_throughput_records_s=committed/(checkpoint_end-started),
            checkpoint_policy='postgresql_automatic_plus_explicit_final_checkpoint',
            worker_committed_records=[d['committed'] for d in sorted(done,key=lambda d:d['worker'])])
    finally:
        try:
            if backend is not None:
                backend.close()
        finally:
            try:
                server.stop()
            finally:
                result.update(server.resource_metrics())
    server.remove_successful_trial()
    result['success'] = True
