"""InfluxDB coordinator using the unchanged synchronized pilot workers."""
import multiprocessing as mp
import os
import queue
from pathlib import Path
from concurrent_sqlite import worker, percentile
from backend_factory import connect_backend
from read_phase import run_read_phase
from process_metrics import summarize_resources
from storage import StorageManager
from influx_server import InfluxServer
import time


def write_phase(cfg, target):
    ctx = mp.get_context('spawn')
    event, messages = ctx.Event(), ctx.Queue()
    processes = []
    deadline = time.monotonic() + cfg['timeout']

    def receive(kind):
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError('InfluxDB write phase exceeded timeout')
            try:
                message = messages.get(timeout=min(1, remaining))
            except queue.Empty:
                if any(p.exitcode not in (None, 0) for p in processes):
                    raise RuntimeError('InfluxDB write worker crashed')
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
                raise RuntimeError('InfluxDB write worker did not exit successfully')
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
    storage = StorageManager({'lustre': os.environ['SCRATCH'], 'tmpfs': '/tmp'}).prepare(cfg['storage'])
    result.update(storage_info=storage, client_runtime='native', server_runtime='shifter',
                  server_image_id=cfg['server_image_id'], server_log=Path(log_path).name,
                  driver_version='python_stdlib_http.client',
                  worker_metrics_scope='client_processes_only',
                  server_resource_metrics_status='not_collected',
                  influx_schema_version=1,
                  write_latency_unit='HTTP_batch_request_not_SQL_transaction',
                  write_completion_policy='synchronous_HTTP_204_acknowledgements',
                  checkpoint_policy='no_explicit_checkpoint_API_used',
                  final_checkpoint_s=None,
                  server_wal_fsync_delay='0s', server_wal_flush_on_shutdown=True)
    server = InfluxServer(storage, cfg, log_path)
    backend = None
    try:
        health = server.start()
        result['influxdb_version'] = health['version'].lstrip('v')
        backend = connect_backend(cfg, server.connection)
        started, done = write_phase(cfg, server.connection)
        elapsed = max(d['finished'] for d in done) - started
        acknowledged = sum(d['committed'] for d in done)
        values = sorted(v for d in done for v in d['latencies_ms'])
        expected_batches = (cfg['records'] + cfg['batch_size'] - 1) // cfg['batch_size']
        if acknowledged != cfg['records'] or len(values) != expected_batches:
            raise RuntimeError('InfluxDB acknowledged record/batch count mismatch')
        stored = backend.validate_records(cfg['workload'], cfg['records'])
        result.update(summarize_resources(done, 'write', elapsed))
        if cfg['read_queries']:
            result.update(run_read_phase(cfg, server.connection))
        result.update(
            read_cache_state='warm_after_acknowledged_writes_and_validation_no_checkpoint',
            committed_records=acknowledged, acknowledged_records=acknowledged,
            stored_records=stored, workload_wall_s=elapsed,
            throughput_records_s=acknowledged/elapsed,
            write_batches=len(values), transactions=len(values),
            transaction_latency_samples_ms=values,
            transaction_latency_mean_ms=sum(values)/len(values),
            transaction_latency_p50_ms=percentile(values,50),
            transaction_latency_p95_ms=percentile(values,95),
            transaction_latency_p99_ms=percentile(values,99),
            completion_wall_s=elapsed,
            completion_throughput_records_s=acknowledged/elapsed,
            worker_committed_records=[d['committed'] for d in sorted(done,key=lambda d:d['worker'])])
    except Exception as exc:
        result['workload_error'] = repr(exc)
        raise
    finally:
        try:
            if backend is not None:
                backend.close()
        finally:
            try:
                server.stop()
            finally:
                result.update(server.shutdown)
    server.remove_successful_trial()
    result['success'] = True
