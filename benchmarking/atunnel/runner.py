# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Run `atunnel-bench load` in the benchmark pipeline's Job and upload what it
measured in the pipeline's shape.

The worker and sink run as sidecars of the same pod; pki wrote /pki before
them. Arguments this runner does not know go to `atunnel-bench load`. The exit
code is non-zero only for harness faults: load failing, a missing or
unreadable results.json, or a failed upload. Verdicts, atunnel deaths and OOM
kills are data in status.json and stats.jsonl.
"""

import argparse
import ipaddress
import json
import os
import shutil
import ssl
import subprocess
import sys
import threading
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import IO, TextIO

import output as output_mod

PKI_DIR = Path("/pki")
SHARED_DIR = Path("/shared")
WORK_ROOT = Path("/tmp")
SERVICE_ACCOUNT = Path("/var/run/secrets/kubernetes.io/serviceaccount")
SIDECARS = ("worker", "sink")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--name", required=True, help="Test name (GCS path component)")
    p.add_argument("--tag", required=True, help="Tag for this run (commit sha)")
    p.add_argument("--dest", required=True, help="Root destination (gs://bucket/path or local path)")
    # The rig's shape, recorded with every row; the worker and sink sidecars
    # apply it.
    p.add_argument("--tunnel-cpus", required=True, dest="tunnel_cpus")
    p.add_argument("--tunnel-memory", required=True, dest="tunnel_memory")
    p.add_argument("--actors", required=True, type=int)
    p.add_argument("--body-bytes", required=True, type=int, dest="body_bytes")
    args, extra = p.parse_known_args(argv)
    args.extra = extra
    return args


def tee(logs: TextIO, msg: str) -> None:
    print(msg, flush=True)
    logs.write(msg + "\n")
    logs.flush()


def pump_stream(prefix: str, stream: IO[str], sink: TextIO) -> None:
    for line in stream:
        line = line.rstrip("\n")
        print(f"[{prefix}] {line}", flush=True)
        sink.write(line + "\n")
        sink.flush()


def run_load(extra: list[str], work_dir: Path, logs: TextIO) -> int:
    """Run load with its report on stdout to results.txt and its progress on
    stderr to logs.txt. The test's own flags come first, so the runner's paths
    win when both name the same flag."""
    cmd = [
        "atunnel-bench",
        "load",
        *extra,
        "--pki-dir",
        str(PKI_DIR),
        "--shared-dir",
        str(SHARED_DIR),
        "--results-json",
        str(work_dir / "results.json"),
        "--profile-dir",
        str(work_dir / "profiles"),
    ]
    tee(logs, f"Running: {' '.join(cmd)}")
    with open(work_dir / "results.txt", "w") as results:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=1, text=True)
        out = threading.Thread(target=pump_stream, args=("load", proc.stdout, results))
        err = threading.Thread(target=pump_stream, args=("load", proc.stderr, logs))
        out.start()
        err.start()
        code = proc.wait()
        out.join()
        err.join()
    return code


def sidecar_logs(work_dir: Path, logs: TextIO) -> None:
    """Save the worker's and the sink's logs through the kube API. Best effort:
    the run's results do not depend on them."""
    try:
        token = (SERVICE_ACCOUNT / "token").read_text()
        namespace = (SERVICE_ACCOUNT / "namespace").read_text()
        ctx = ssl.create_default_context(cafile=str(SERVICE_ACCOUNT / "ca.crt"))
    except OSError as e:
        tee(logs, f"sidecar logs skipped: {e}")
        return
    host = os.environ.get("KUBERNETES_SERVICE_HOST", "kubernetes.default.svc")
    try:
        if ipaddress.ip_address(host).version == 6:
            host = f"[{host}]"
    except ValueError:
        pass  # a host name
    port = os.environ.get("KUBERNETES_SERVICE_PORT", "443")
    for container in SIDECARS:
        url = (
            f"https://{host}:{port}/api/v1/namespaces/{namespace}"
            f"/pods/{os.environ.get('HOSTNAME', '')}/log?container={container}"
        )
        req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
        try:
            with urllib.request.urlopen(req, context=ctx, timeout=60) as resp:
                (work_dir / f"{container}.log").write_bytes(resp.read())
        except Exception as e:  # noqa: BLE001 - best effort by design
            tee(logs, f"sidecar log of {container} skipped: {e}")


def upload_to_gcs(local_path: Path, gcs_uri: str) -> None:
    # Imported here so a local --dest does not need google-cloud-storage.
    from google.cloud import storage

    bucket_name, _, blob_path = gcs_uri[len("gs://"):].partition("/")
    storage.Client().bucket(bucket_name).blob(blob_path).upload_from_filename(str(local_path))


def upload(src: Path, dest: str) -> None:
    if dest.startswith("gs://"):
        upload_to_gcs(src, dest)
    else:
        dest_path = Path(dest)
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(src, dest_path)


def upload_dir(work_dir: Path, prefix: str) -> None:
    for path in sorted(p for p in work_dir.rglob("*") if p.is_file()):
        upload(path, f"{prefix}/{path.relative_to(work_dir)}")


def config(args: argparse.Namespace) -> dict:
    cpus = float(args.tunnel_cpus)
    return {
        "tunnel_cpus": int(cpus) if cpus.is_integer() else cpus,
        "tunnel_memory": args.tunnel_memory,
        "actors": args.actors,
        "body_bytes": args.body_bytes,
    }


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    now = datetime.now(timezone.utc)
    work_dir = WORK_ROOT / f"{now.strftime('%Y%m%dT%H%M%SZ')}-atunnel-runner"
    work_dir.mkdir(parents=True, exist_ok=True)
    prefix = (
        f"{args.dest.rstrip('/')}/runs/{args.name}"
        f"/run_date={now.strftime('%Y-%m-%d')}/run_ts={int(now.timestamp())}"
        f"/run_tag={args.tag}"
    )
    timestamp = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    cfg = config(args)

    with open(work_dir / "logs.txt", "w") as logs:
        tee(logs, f"atunnel runner: name={args.name} tag={args.tag} rig={output_mod.RIG} {cfg} dest={prefix}")
        try:
            load_exit = run_load(args.extra, work_dir, logs)
            tee(logs, f"load exited {load_exit}")
        except Exception as e:  # noqa: BLE001 - a harness fault, reported through the exit code
            tee(logs, f"load did not run: {e}")
            load_exit = 1

        deaths_log = SHARED_DIR / "tunnel-deaths.log"
        if deaths_log.exists():
            shutil.copy(deaths_log, work_dir / "tunnel-deaths.log")
        deaths, ooms = output_mod.death_counts(deaths_log)
        line = output_mod.death_line(deaths, ooms, args.tunnel_memory)
        tee(logs, line)
        with open(work_dir / "results.txt", "a") as results:
            results.write(line + "\n")

        # Without a usable results.json the run measured nothing it can
        # report, whatever load's exit code: a harness fault.
        reports: list = []
        results_error = ""
        try:
            reports = json.loads((work_dir / "results.json").read_text())
            rows = output_mod.rows(reports, cfg)
            output_mod.write_jsonl(work_dir / "stats.jsonl", output_mod.stats_records(rows, args.tag, args.name, timestamp))
        except Exception as e:  # noqa: BLE001 - any failure here means no rows
            results_error = f"results.json unusable: {e}"
            tee(logs, results_error)
        status = output_mod.status(reports, load_exit, deaths, ooms, cfg)
        if results_error:
            status["results_error"] = results_error
        (work_dir / "status.json").write_text(json.dumps(status, indent=2) + "\n")
        tee(logs, f"status: {json.dumps(status)}")
        sidecar_logs(work_dir, logs)

    try:
        upload_dir(work_dir, prefix)
    except Exception as e:  # noqa: BLE001 - reported, then the exit code says so
        print(f"upload to {prefix} failed: {e}", flush=True)
        return 1
    print(f"uploaded to {prefix}", flush=True)
    return 0 if load_exit == 0 and not results_error else 1


if __name__ == "__main__":
    sys.exit(main())
