"""Small InfluxDB lifecycle check; run only on an allocated compute node."""
import csv
import io
import json
import os
import secrets
import shutil
import signal
import socket
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from storage import StorageManager

IMAGE = "db0bdab1e5ad5ee899c127b8c13d9c986a3ced78cd9200dd80a1064bf1533b6e"
assert os.environ.get("SLURM_JOB_ID")
assert socket.gethostname().startswith("nid")

output = ROOT / "results" / (
    "influx-lifecycle-" + os.environ["SLURM_JOB_ID"] + "-" + secrets.token_hex(4)
)
output.mkdir(parents=True)
print("RESULTS=" + str(output), flush=True)

manager = StorageManager({
    "lustre": os.environ["SCRATCH"],
    "tmpfs": "/tmp",
})
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

for tier in ("lustre", "tmpfs"):
    info = manager.prepare(tier)
    data = Path(info["path"])
    token = secrets.token_hex(32)
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    base = f"http://127.0.0.1:{port}"
    process = None
    log = None
    success = False
    report = {
        "storage": info, "image_id": IMAGE,
        "hostname": socket.gethostname(),
        "slurm_job_id": os.environ["SLURM_JOB_ID"],
        "success": False,
    }

    def request(path, body=None, content_type="application/json", auth=True):
        headers = {"Content-Type": content_type}
        if auth:
            headers["Authorization"] = "Token " + token
        req = urllib.request.Request(
            base + path, data=body, headers=headers,
            method="POST" if body is not None else "GET",
        )
        with opener.open(req, timeout=30) as response:
            return response.read()

    def start(label):
        global process, log
        log = (output / f"{tier}-{label}.log").open("w")
        global control
        control = data / ("lifecycle-" + label)
        supervisor = r"""
set -eu
test "$SHIFTER_IMAGE" = "$1"
findmnt -n -o FSTYPE -T /influxwork
control="$3"
influxd --bolt-path /influxwork/influxd.bolt \
    --sqlite-path /influxwork/influxd.sqlite \
    --engine-path /influxwork/engine \
    --http-bind-address "$2" --reporting-disabled &
child=$!
trap 'kill -INT "$child" 2>/dev/null || true' TERM INT
while kill -0 "$child" 2>/dev/null; do
    if [ -f "${control}.stop" ]; then
        kill -INT "$child" 2>/dev/null || true
        break
    fi
    sleep 0.2
done
set +e
wait "$child"
rc=$?
set -e
printf '%s\n' "$rc" > "${control}.exit.tmp"
mv "${control}.exit.tmp" "${control}.exit"
exit "$rc"
"""
        command = [
            "shifter", "--image=id:" + IMAGE,
            "--volume=" + str(data) + ":/influxwork",
            "/bin/sh", "-c", supervisor,
            "influx-check", IMAGE, f"127.0.0.1:{port}",
            "/influxwork/" + control.name,
        ]
        process = subprocess.Popen(
            command, stdout=log, stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise RuntimeError(f"Server exited; inspect {tier}-{label}.log")
            try:
                health = json.loads(request("/health", auth=False))
                if health.get("status") == "pass":
                    assert health["version"].lstrip("v") == "2.9.1", health
                    return health
            except (urllib.error.URLError, TimeoutError, OSError):
                pass
            time.sleep(0.5)
        raise TimeoutError("InfluxDB readiness timeout")

    def stop():
        global process, log
        if process is None:
            return
        try:
            if process.poll() is not None:
                raise RuntimeError(
                    f"Server launcher exited before stop request: {process.returncode}"
                )
            Path(str(control) + ".stop").touch()
            try:
                launcher_code = process.wait(timeout=60)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(process.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    process.wait(timeout=10)
                raise RuntimeError("Graceful server shutdown timed out")

            exit_path = Path(str(control) + ".exit")
            if not exit_path.is_file():
                raise RuntimeError("Missing influxd child-exit evidence")
            server_code = int(exit_path.read_text().strip())
            report.setdefault("shutdowns", []).append({
                "launch": control.name,
                "launcher_exit_code": launcher_code,
                "influxd_exit_code": server_code,
            })
            if launcher_code != 0 or server_code != 0:
                raise RuntimeError(
                    f"Shutdown failed: launcher={launcher_code}, influxd={server_code}"
                )
            print(
                f"STOP_OK: {tier} {control.name} launcher=0 influxd=0",
                flush=True,
            )
        finally:
            if log is not None:
                log.close()
            process = None
            log = None

    def verify_points():
        flux = '''
from(bucket: "check")
  |> range(start: 2024-01-01T00:00:00Z, stop: 2024-01-01T01:00:00Z)
  |> filter(fn: (r) => r._measurement == "lifecycle" and r._field == "value")
  |> group()
  |> sort(columns: ["_time"])
  |> keep(columns: ["_time", "_value"])
'''
        payload = json.dumps({
            "query": flux,
            "dialect": {"header": True, "annotations": []},
        }).encode()
        answer = request("/api/v2/query?org=ipdps", payload).decode()
        rows = list(csv.DictReader(io.StringIO(answer)))
        assert len(rows) == 1000, f"Expected 1000 points, found {len(rows)}"
        assert [int(r["_value"]) for r in rows] == list(range(1000))
        assert len({r["_time"] for r in rows}) == 1000
        return len(rows)

    try:
        report["initial_health"] = start("initial")
        setup = {
            "username": "benchmark",
            "password": secrets.token_urlsafe(24),
            "org": "ipdps", "bucket": "check",
            "token": token, "retentionPeriodSeconds": 0,
        }
        request("/api/v2/setup", json.dumps(setup).encode(), auth=False)

        points = "\n".join(
            f"lifecycle,sensor=test value={i}i {1704067200 + i}"
            for i in range(1000)
        ).encode()
        request(
            "/api/v2/write?org=ipdps&bucket=check&precision=s",
            points, "text/plain",
        )
        report["points_before_restart"] = verify_points()
        stop()
        report["restart_health"] = start("restart")
        report["points_after_restart"] = verify_points()
        stop()
        success = True
        report["success"] = True
        print(f"PASS: {tier} — write/read/stop/restart/read/stop", flush=True)
    except Exception as exc:
        report["error"] = repr(exc)
        raise
    finally:
        try:
            stop()
        finally:
            (output / f"{tier}.json").write_text(
                json.dumps(report, indent=2) + "\n"
            )
            if success:
                shutil.rmtree(data)
            else:
                print("PRESERVED_DATA=" + str(data), flush=True)

print("INFLUX_LIFECYCLE_COMPLETE", flush=True)
