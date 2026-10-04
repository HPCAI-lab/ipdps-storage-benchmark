"""One bounded, diagnostic system-metrics study on an exclusive compute node.

Full single-trial lifecycle scopes; these are not write/read phase counters.
Unavailable node-wide perf events remain unavailable; no permission changes.
"""
import csv
import hashlib
import json
import math
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import threading
import time
import traceback
import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from experiment import build_plan

EVENTS = ('cycles', 'instructions', 'cache-misses', 'context-switches')


def perf_values(path):
    values = {}
    if not path.exists():
        return values
    for row in csv.reader(path.read_text(errors='replace').splitlines(), delimiter=';'):
        if len(row) < 3 or row[0].startswith('#'):
            continue
        name = row[2].strip()
        if name not in EVENTS:
            continue
        try:
            value = float(row[0].strip())
            if not math.isfinite(value) or value < 0:
                continue
            coverage = float(row[4].strip().rstrip('%')) if len(row) > 4 and row[4].strip() else None
            values[name] = {'count': value, 'running_percent': coverage}
        except ValueError:
            continue
    return values


def proc_sample():
    cpu = next(line for line in Path('/proc/stat').read_text().splitlines() if line.startswith('cpu '))
    ticks = list(map(int, cpu.split()[1:9]))
    memory = {}
    for line in Path('/proc/meminfo').read_text().splitlines():
        key, value = line.split(':', 1)
        if key in ('MemTotal', 'MemAvailable'):
            memory[key] = int(value.split()[0])
    return dict(monotonic_s=time.monotonic(), cpu_ticks=ticks, memory_kib=memory)


def summarize_node(samples):
    result = {'node_samples': len(samples)}
    if len(samples) < 2:
        return dict(result, node_cpu_status='insufficient_samples')
    delta = [b-a for a,b in zip(samples[0]['cpu_ticks'], samples[-1]['cpu_ticks'])]
    total = sum(delta)
    if len(delta) != 8 or any(x < 0 for x in delta) or total <= 0:
        result['node_cpu_status'] = 'invalid_counter_delta'
    else:
        result.update(node_cpu_status='collected', node_busy_percent=100*(total-delta[3]-delta[4])/total,
                      node_iowait_percent=100*delta[4]/total,
                      node_idle_percent=100*delta[3]/total)
    memory = [s['memory_kib'] for s in samples]
    if all('MemTotal' in m and 'MemAvailable' in m for m in memory):
        result['node_sampled_peak_used_MiB'] = max(m['MemTotal']-m['MemAvailable'] for m in memory)/1024
    return result


def collect(command, directory, perf, groups, iostat):
    env = dict(os.environ, LC_ALL='C')
    prefix = []
    if groups:
        prefix = [perf, 'stat', '-a', '-x', ';', '-o', str(directory/'perf.csv')]
        for group in groups:
            prefix += ['-e', group]
        prefix += ['--']
    if os.access('/usr/bin/time', os.X_OK):
        prefix += ['/usr/bin/time', '-v', '-o', str(directory/'time.txt')]
    stop = threading.Event()
    samples = [proc_sample()]
    errors = []
    def sample():
        while not stop.wait(0.5):
            try:
                samples.append(proc_sample())
            except Exception as exc:
                errors.append(repr(exc))
                return
    thread = threading.Thread(target=sample, daemon=True)
    monitor = None
    started = time.monotonic()
    with (directory/'iostat.txt').open('w') as io_log, (directory/'controller.log').open('w') as log:
        try:
            if iostat:
                monitor = subprocess.Popen([iostat, '-y', '-x', '1'], stdout=io_log, stderr=subprocess.STDOUT, env=env)
            thread.start()
            completed = subprocess.run(prefix+command, stdout=log, stderr=subprocess.STDOUT, env=env)
        finally:
            stop.set()
            if thread.ident:
                thread.join(timeout=3)
            samples.append(proc_sample())
            if monitor and monitor.poll() is None:
                monitor.terminate()
                try:
                    monitor.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    monitor.kill()
                    monitor.wait()
    elapsed = time.monotonic()-started
    with (directory/'node-samples.jsonl').open('w') as stream:
        for sample in samples:
            stream.write(json.dumps(sample)+'\n')
    result = dict(command_exit_code=completed.returncode, observed_lifecycle_wall_s=elapsed,
                  sampler_errors=errors, iostat_available=bool(iostat),
                  iostat_exit_code=monitor.returncode if monitor else None)
    result.update(summarize_node(samples))
    result['perf_events'] = perf_values(directory/'perf.csv')
    counts = result['perf_events']
    if all(k in counts for k in ('cycles','instructions')) and counts['cycles']['count'] > 0:
        result['node_ipc'] = counts['instructions']['count']/counts['cycles']['count']
    return result


def main():
    if not os.environ.get('SLURM_JOB_ID') or not socket.gethostname().startswith('nid'):
        raise SystemExit('Run only on an allocated compute node')
    output = ROOT/'results'/('system-metrics-'+os.environ['SLURM_JOB_ID'])
    output.mkdir(parents=True, exist_ok=False)
    print('SYSTEM_RESULTS='+str(output), flush=True)
    perf, iostat = shutil.which('perf'), shutil.which('iostat')
    caps = dict(hostname=socket.gethostname(), job_id=os.environ['SLURM_JOB_ID'],
                perf_path=perf, iostat_path=iostat, cpu_affinity=sorted(os.sched_getaffinity(0)),
                scope='exclusive_node_full_trial_lifecycle_including_setup_validation_read_shutdown_and_monitors',
                design='48 diagnostic trials; 4 deployments x 3 workloads x 2 tiers x 1/64 clients; one repetition; no warmup',
                limitations=['No phase-specific PMU attribution', 'One diagnostic observation per condition; instrumentation overhead not isolated',
                             'Node iowait is not database-specific or a direct measure of Lustre bandwidth',
                             'GNU time CPU is waited-process accounting; max RSS is not process-tree peak memory',
                             'Sampled node used memory is MemTotal minus MemAvailable, not database PSS'])
    for path in ('/proc/sys/kernel/perf_event_paranoid','/proc/cpuinfo'):
        try:
            (output/Path(path).name).write_text(Path(path).read_text())
        except OSError:
            pass
    groups = []
    caps['probes'] = []
    if perf:
        for index, (group, names) in enumerate((('{cycles,instructions}', ('cycles','instructions')),
                                               ('cache-misses', ('cache-misses',)),
                                               ('context-switches', ('context-switches',)))):
            destination = output/f'probe-{index}.csv'
            with (output/f'probe-{index}.log').open('w') as log:
                try:
                    run = subprocess.run([perf,'stat','-a','-x',';','-o',str(destination),'-e',group,'--',
                                          sys.executable,'-c','sum(i*i for i in range(500000))'],
                                         stdout=log,stderr=subprocess.STDOUT,timeout=20,env=dict(os.environ,LC_ALL='C'))
                    supported = run.returncode == 0 and all(n in perf_values(destination) for n in names)
                    caps['probes'].append(dict(group=group, exit_code=run.returncode, supported=supported))
                    if supported:
                        groups.append(group)
                except subprocess.TimeoutExpired:
                    caps['probes'].append(dict(group=group,supported=False,error='probe_timeout'))
        # Check the combined event set before launching any benchmark.
        if groups:
            destination = output/'probe-combined.csv'
            cmd=[perf,'stat','-a','-x',';','-o',str(destination)]
            for group in groups: cmd += ['-e',group]
            with (output/'probe-combined.log').open('w') as log:
                run = subprocess.run(cmd+['--',sys.executable,'-c','sum(i*i for i in range(500000))'],
                                     stdout=log,stderr=subprocess.STDOUT,timeout=20,env=dict(os.environ,LC_ALL='C'))
            expected=[n for group in groups for n in group.strip('{}').split(',')]
            caps['combined_probe_ok']=run.returncode==0 and all(n in perf_values(destination) for n in expected)
            if not caps['combined_probe_ok']: groups=[]
    caps['enabled_node_perf_groups']=groups
    (output/'capabilities.json').write_text(json.dumps(caps,indent=2)+'\n')
    print('PERF_GROUPS='+repr(groups)+' (unavailable events will remain explicitly uncollected)',flush=True)

    rows=[]
    fields=['deployment','workload','storage','clients','success','results','lifecycle_wall_s',
            'node_busy_percent','node_iowait_percent','node_sampled_peak_used_MiB',
            'cycles','instructions','cache_misses','context_switches','node_ipc','perf_status','node_cpu_status']
    with (output/'summary.csv').open('w',newline='') as stream:
        writer=csv.DictWriter(stream,fieldnames=fields,lineterminator='\n');writer.writeheader()
        for workload in ('metadata','telemetry','mixed'):
            for deployment in ('sqlite-native','sqlite-shifter','postgres','influx'):
                base=(f'configs/mixed-{deployment}-pilot.yaml' if workload=='mixed' else
                      f'configs/{workload}-influx-pilot.yaml' if deployment=='influx' else
                      f'configs/{workload}-{deployment}-read1024.yaml')
                for tier in ('lustre','tmpfs'):
                    for clients in (1,64):
                        label=f'{workload}-{deployment}-{tier}-{clients}'
                        directory=output/label;directory.mkdir()
                        cfg=yaml.safe_load((ROOT/base).read_text())
                        cfg.update(storage=[tier],clients=[clients],repetitions=1,warmup_repetitions=0,
                                   records=1_000_000,read_queries=1024,timeout=600,server_profiling=False)
                        assert len(build_plan(cfg))==1
                        config=directory/'config.yaml';config.write_text(yaml.safe_dump(cfg))
                        if deployment=='sqlite-shifter':
                            command=['shifter','--image=id:'+cfg['shifter_image_id'],'--volume='+str(ROOT)+':/app',
                                     '--env=IPDPS_RUNTIME=shifter','--env=OMP_NUM_THREADS=1','--env=OPENBLAS_NUM_THREADS=1',
                                     '--env=MKL_NUM_THREADS=1','--env=PYTHONUNBUFFERED=1',
                                     '/usr/local/bin/python3','/app/experiment.py','--config',str(config),'--output-root',str(output/'runs')]
                        else:
                            python=ROOT/('.venv/bin/python' if deployment=='sqlite-native' else '.venv-postgres/bin/python')
                            command=[str(python),str(ROOT/'experiment.py'),'--config',str(config),'--output-root',str(output/'runs')]
                        print('MEASURING='+label,flush=True)
                        summary=collect(command,directory,perf,groups,iostat)
                        paths=[Path(line.split('=',1)[1]) for line in (directory/'controller.log').read_text().splitlines() if line.startswith('RESULTS=')]
                        try:
                            assert summary['command_exit_code']==0, summary['command_exit_code']
                            assert len(paths)==1,paths
                            raw=json.loads((paths[0]/'trial-0001.json').read_text())
                            with (paths[0]/'results.csv').open() as csv_stream:
                                saved=list(csv.DictReader(csv_stream))
                            assert len(saved)==1 and saved[0]['success']=='True'
                            assert raw['success'] is True
                            assert raw['stored_records']==raw['committed_records']==1_000_000
                            assert raw['read_queries_completed']==1024 and raw['read_rows_returned']==102400
                            assert str(raw['slurm_job_id'])==os.environ['SLURM_JOB_ID']
                            for name,digest in json.loads((paths[0]/'source_hashes.json').read_text()).items():
                                assert hashlib.sha256((ROOT/name).read_bytes()).hexdigest()==digest,name
                            summary['success']=True
                        except Exception:
                            summary['success']=False;summary['error']=traceback.format_exc()
                            (directory/'metrics.json').write_text(json.dumps(summary,indent=2)+'\n')
                            raise
                        expected=[n for group in groups for n in group.strip('{}').split(',')]
                        summary['perf_status']=('unavailable' if not groups else 'collected' if all(n in summary['perf_events'] for n in expected) else 'partial')
                        summary['results']=str(paths[0])
                        (directory/'metrics.json').write_text(json.dumps(summary,indent=2)+'\n')
                        row=dict(deployment=deployment,workload=workload,storage=tier,clients=clients,
                                 success=True,results=str(paths[0]),lifecycle_wall_s=summary['observed_lifecycle_wall_s'])
                        for name in ('node_busy_percent','node_iowait_percent','node_sampled_peak_used_MiB','node_ipc','perf_status','node_cpu_status'):
                            row[name]=summary.get(name,'')
                        for name in EVENTS:
                            row[name.replace('-','_')]=summary['perf_events'].get(name,{}).get('count','')
                        writer.writerow(row);stream.flush();rows.append(row)
                        print(f'PASS: {len(rows)}/48 {label} perf={summary["perf_status"]}',flush=True)
    assert len(rows)==48
    (output/'completion.json').write_text(json.dumps(dict(success=True,trials=48,scope=caps['scope'],
                                                        hardware_counter_status=sorted(set(r['perf_status'] for r in rows))),indent=2)+'\n')
    print('SYSTEM_MEASUREMENTS_COMPLETE: 48 diagnostic trials; inspect capabilities.json for counter availability',flush=True)

if __name__=='__main__':
    main()
