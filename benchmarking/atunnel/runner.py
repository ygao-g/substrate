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

"""Kubernetes Job runner for the atunnel isolation benchmark."""

import argparse
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
from output import death_counts

SA_DIR = Path("/var/run/secrets/kubernetes.io/serviceaccount")
SHARED_DEATHS_LOG = Path("/shared/tunnel-deaths.log")


def _parse_cpus(val: str) -> int | float:
    v = float(val)
    return int(v) if v.is_integer() else v


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--name", required=True, help="Test name (GCS path component)")
    p.add_argument("--tag", required=True, help="Tag for this run (commit sha)")
    p.add_argument(
        "--dest",
        required=True,
        help="Root destination (gs://bucket/path or local path)",
    )
    p.add_argument(
        "--tunnel-cpus",
        type=_parse_cpus,
        default=1,
        dest="tunnel_cpus",
    )
    p.add_argument("--tunnel-memory", default="1g", dest="tunnel_memory")
    p.add_argument("--actors", type=int, default=10)
    args, extra = p.parse_known_args(argv)
    args.extra = extra
    return args


def tee(logs: TextIO, msg: str) -> None:
    print(msg, flush=True)
    logs.write(msg + "\n")
    logs.flush()


def pump_stream(prefix: str, stream: IO[str], logs: TextIO) -> None:
    for line in stream:
        tagged = f"[{prefix}] {line.rstrip()}"
        sys.stdout.write(tagged + "\n")
        sys.stdout.flush()
        logs.write(tagged + "\n")
        logs.flush()


def run_load(
    extra_flags: list[str],
    results_json: Path,
    profile_dir: Path,
    results_txt: Path,
    logs: TextIO,
) -> int:
    # Extra flags go before the runner's own paths so runner paths win in cobra.
    cmd = [
        "atunnel-bench",
        "load",
        *extra_flags,
        "--pki-dir",
        "/pki",
        "--shared-dir",
        "/shared",
        "--results-json",
        str(results_json),
        "--profile-dir",
        str(profile_dir),
    ]
    tee(logs, f"Running: {' '.join(cmd)}")
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=1,
        text=True,
    )
    assert proc.stdout is not None
    assert proc.stderr is not None
    err_thread = threading.Thread(
        target=pump_stream, args=("load", proc.stderr, logs), daemon=True
    )
    err_thread.start()
    with open(results_txt, "w") as out_f:
        for line in proc.stdout:
            sys.stdout.write(line)
            sys.stdout.flush()
            out_f.write(line)
            out_f.flush()
    err_thread.join(timeout=5)
    return proc.wait()


def sidecar_logs(work_dir: Path, logs: TextIO) -> None:
    token_path = SA_DIR / "token"
    ca_path = SA_DIR / "ca.crt"
    pod = os.environ.get("HOSTNAME", "")
    if not (token_path.exists() and ca_path.exists() and pod):
        tee(logs, "Skipping sidecar logs: no in-cluster serviceaccount or HOSTNAME")
        return
    ns_path = SA_DIR / "namespace"
    ns = ns_path.read_text().strip() if ns_path.exists() else "benchmarking"
    token = token_path.read_text().strip()
    host = os.environ.get("KUBERNETES_SERVICE_HOST", "kubernetes.default.svc")
    port = os.environ.get("KUBERNETES_SERVICE_PORT", "443")
    ctx = ssl.create_default_context(cafile=str(ca_path))
    for container in ("worker", "sink"):
        url = (
            f"https://{host}:{port}/api/v1/namespaces/{ns}"
            f"/pods/{pod}/log?container={container}"
        )
        req = urllib.request.Request(
            url, headers={"Authorization": f"Bearer {token}"}
        )
        try:
            with urllib.request.urlopen(req, context=ctx, timeout=30) as resp:
                (work_dir / f"{container}.log").write_bytes(resp.read())
        except Exception as e:
            tee(logs, f"Warning: could not fetch {container} sidecar log: {e}")


def upload_to_gcs(local_path: Path, gcs_uri: str) -> None:
    from google.cloud import storage

    bucket_name, _, blob_path = gcs_uri[len("gs://") :].partition("/")
    storage.Client().bucket(bucket_name).blob(blob_path).upload_from_filename(
        str(local_path)
    )


def upload(src: Path, dest: str) -> None:
    if dest.startswith("gs://"):
        upload_to_gcs(src, dest)
    else:
        dest_path = Path(dest)
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(src, dest_path)


def main() -> None:
    args = parse_args()
    now = datetime.now(timezone.utc)
    timestamp = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    work_dir = Path(f"/tmp/{now.strftime('%Y%m%dT%H%M%SZ')}-atunnel-runner")
    profile_dir = work_dir / "profiles"
    profile_dir.mkdir(parents=True, exist_ok=True)

    prefix = (
        f"{args.dest.rstrip('/')}/runs/{args.name}"
        f"/run_date={now.strftime('%Y-%m-%d')}/run_ts={int(now.timestamp())}"
        f"/run_tag={args.tag}"
    )

    logs_path = work_dir / "logs.txt"
    results_json_path = work_dir / "results.json"
    results_txt_path = work_dir / "results.txt"
    stats_path = work_dir / "stats.jsonl"
    status_path = work_dir / "status.json"
    deaths_path = work_dir / "tunnel-deaths.log"

    load_exit = 1
    deaths, ooms = 0, 0
    reports: list[dict] = []

    with open(logs_path, "w") as logs:
        tee(logs, f"Destination prefix: {prefix}")
        try:
            load_exit = run_load(
                args.extra,
                results_json_path,
                profile_dir,
                results_txt_path,
                logs,
            )
            tee(logs, f"atunnel-bench load exited with code {load_exit}")
        except Exception as e:
            tee(logs, f"atunnel-bench load failed: {e}")

        if SHARED_DEATHS_LOG.exists():
            shutil.copy(SHARED_DEATHS_LOG, deaths_path)
            deaths, ooms = death_counts(deaths_path)
            msg = (
                f"atunnel died {deaths} time(s), {ooms} of them OOM-killed "
                f"at its {args.tunnel_memory} limit"
            )
            tee(logs, msg)
            with open(results_txt_path, "a") as out_f:
                out_f.write(msg + "\n")

        if results_json_path.exists():
            reports = json.loads(results_json_path.read_text())
            config = {
                "tunnel_cpus": args.tunnel_cpus,
                "tunnel_memory": args.tunnel_memory,
                "actors": args.actors,
            }
            row_list = output_mod.rows(reports, config)
            output_mod.write_jsonl(
                output_mod.stats_records(
                    row_list, args.tag, args.name, timestamp
                ),
                stats_path,
            )

        sidecar_logs(work_dir, logs)

    status_dict = output_mod.status(
        reports,
        load_exit_code=load_exit,
        tunnel_deaths=deaths,
        oom_kills=ooms,
        tunnel_cpus=args.tunnel_cpus,
        tunnel_memory=args.tunnel_memory,
        actors=args.actors,
    )
    status_path.write_text(json.dumps(status_dict, indent=2) + "\n")

    upload_ok = True
    files: list[tuple[Path, str]] = [
        (status_path, "status.json"),
        (results_json_path, "results.json"),
        (results_txt_path, "results.txt"),
        (logs_path, "logs.txt"),
        (stats_path, "stats.jsonl"),
        (deaths_path, "tunnel-deaths.log"),
        (work_dir / "worker.log", "worker.log"),
        (work_dir / "sink.log", "sink.log"),
    ]
    for profile in sorted(profile_dir.glob("*.pb.gz")):
        files.append((profile, f"profiles/{profile.name}"))

    for src, rel_dest in files:
        if not src.exists():
            continue
        dest = f"{prefix}/{rel_dest}"
        try:
            upload(src, dest)
            print(f"Uploaded {src} -> {dest}", flush=True)
        except Exception as e:
            print(f"Upload of {src} failed: {e}", flush=True)
            upload_ok = False

    sys.exit(0 if (load_exit == 0 and upload_ok) else 1)


if __name__ == "__main__":
    main()
