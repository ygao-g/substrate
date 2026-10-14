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

"""egress-tunnels benchmark runner.

One Kubernetes Job per tests.yaml entry (submitted by
benchmarking/automation/orchestrator.py):
  1. Run the egress-tunnels driver (benchmark-egress driver run): create A actors, resume B, loop against
     C endpoints for the duration, stop, suspend, write report.json.
  2. Judge the report against the entry's success-rate threshold.
  3. Delete the actors, so the next test starts clean.
  4. Upload status.json, report.json and logs.txt to
     <dest>/runs/<name>/run_date=.../run_ts=.../run_tag=<tag>/.

Exit 0 iff the driver exited 0, the report met the threshold, and every
artifact uploaded.
"""

import argparse
import json
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, TextIO

# In-cluster addresses: the runner Job sits in the benchmarking namespace,
# which ate-system's NetworkPolicy admits by label.
DEFAULT_API_ENDPOINT = "api.ate-system.svc:443"
DEFAULT_ROUTER_URL = "http://atenet-router.ate-system.svc"
DEFAULT_TOKEN_FILE = "/run/ateapi-token/token"
DEFAULT_DRIVER = "/app/benchmark-egress"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--name", required=True, help="Test name (GCS path component)")
    p.add_argument("--tag", required=True, help="Tag for this run (commit sha)")
    p.add_argument("--dest", required=True, help="Root destination (gs://bucket/path or local path)")
    p.add_argument("--actors", required=True, type=int)
    p.add_argument("--parallel", required=True, type=int)
    p.add_argument("--endpoints", required=True, type=int)
    p.add_argument("--duration", required=True)
    p.add_argument("--scheme", default="http")
    p.add_argument("--conn-mode", default="keepalive", dest="conn_mode")
    p.add_argument("--request-interval", default="100ms", dest="request_interval")
    p.add_argument("--request-timeout", default="5s", dest="request_timeout")
    p.add_argument("--opens-budget", type=int, default=0, dest="opens_budget",
                   help="Opens per second the gateway can handshake; the driver refuses a start that asks for more. 0 = unchecked")
    p.add_argument("--atespace", default="benchmark-workloads")
    p.add_argument("--template", default="egress")
    p.add_argument("--success-rate-threshold", type=float, default=0.999, dest="success_rate")
    p.add_argument("--driver", default=DEFAULT_DRIVER)
    p.add_argument("--api-endpoint", default=DEFAULT_API_ENDPOINT, dest="api_endpoint")
    p.add_argument("--api-token-file", default=DEFAULT_TOKEN_FILE, dest="api_token_file")
    p.add_argument("--router-url", default=DEFAULT_ROUTER_URL, dest="router_url")
    args, extra = p.parse_known_args(argv)
    args.extra = extra
    return args


def run_prefix(dest: str, name: str, tag: str, now: datetime) -> str:
    """The Hive-partitioned upload prefix every benchmark runner uses."""
    return (
        f"{dest.rstrip('/')}/runs/{name}"
        f"/run_date={now.strftime('%Y-%m-%d')}/run_ts={int(now.timestamp())}"
        f"/run_tag={tag}"
    )


def cluster_args(args: argparse.Namespace) -> list[str]:
    return [
        "--api-endpoint", args.api_endpoint,
        "--api-token-file", args.api_token_file,
        "--atespace", args.atespace,
    ]


def run_args(args: argparse.Namespace, report: Path) -> list[str]:
    """The driver's `run` command line for this entry."""
    return [
        args.driver, "driver", "run",
        *cluster_args(args),
        "--router-url", args.router_url,
        "--template", args.template,
        "--actors", str(args.actors),
        "--parallel", str(args.parallel),
        "--endpoints", str(args.endpoints),
        "--duration", args.duration,
        "--scheme", args.scheme,
        "--conn-mode", args.conn_mode,
        "--request-interval", args.request_interval,
        "--request-timeout", args.request_timeout,
        "--opens-budget", str(args.opens_budget),
        "--output", str(report),
        *args.extra,
    ]


def cleanup_args(args: argparse.Namespace) -> list[str]:
    return [args.driver, "cleanup", *cluster_args(args), "--actors", str(args.actors)]


def judge(report: dict[str, Any], success_rate: float) -> dict[str, Any]:
    """What the run proved: the loop's request count, its success rate, and
    whether that met the threshold. A report without a loop (the run never
    reached steady state) fails."""
    loop = report.get("loop") or {}
    requests = int(loop.get("requests", 0))
    successes = int(loop.get("successes", 0))
    rate = successes / requests if requests else 0.0
    return {
        "requests": requests,
        "successes": successes,
        "successRate": rate,
        "successRateThreshold": success_rate,
        "interrupted": bool(report.get("interrupted")),
        "ok": requests > 0 and rate >= success_rate and not report.get("interrupted"),
    }


def tee(logs: TextIO, msg: str) -> None:
    print(msg, flush=True)
    logs.write(msg + "\n")
    logs.flush()


def run_streaming(cmd: list[str], logs: TextIO) -> int:
    tee(logs, "$ " + " ".join(cmd))
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
    assert proc.stdout is not None
    for line in proc.stdout:
        tee(logs, line.rstrip("\n"))
    return proc.wait()


def upload(src: Path, dest: str) -> None:
    if dest.startswith("gs://"):
        # Imported here so a local --dest needs no google-cloud-storage.
        from google.cloud import storage

        bucket_name, _, blob_path = dest[len("gs://"):].partition("/")
        storage.Client().bucket(bucket_name).blob(blob_path).upload_from_filename(str(src))
    else:
        dest_path = Path(dest)
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(src, dest_path)


def main() -> None:
    args = parse_args()
    now = datetime.now(timezone.utc)
    work_dir = Path(f"/tmp/{now.strftime('%Y%m%dT%H%M%SZ')}-egress-tunnels-runner")
    work_dir.mkdir(parents=True, exist_ok=True)
    prefix = run_prefix(args.dest, args.name, args.tag, now)
    logs_path = work_dir / "logs.txt"
    report_path = work_dir / "report.json"
    status_path = work_dir / "status.json"

    status: dict[str, Any] = {"driverExitCode": None, "cleanupExitCode": None, "ok": False}
    with open(logs_path, "w") as logs:
        tee(logs, f"egress-tunnels runner: name={args.name} tag={args.tag} dest={prefix}")
        try:
            status["driverExitCode"] = run_streaming(run_args(args, report_path), logs)
            if report_path.exists():
                verdict = judge(json.loads(report_path.read_text()), args.success_rate)
                status.update(verdict)
                status["ok"] = verdict["ok"] and status["driverExitCode"] == 0
                tee(logs, f"verdict: {json.dumps(verdict)}")
            else:
                tee(logs, f"no report at {report_path}")
        except Exception as e:  # noqa: BLE001 - the status file carries the failure
            tee(logs, f"run failed: {e}")
            status["error"] = str(e)
        finally:
            tee(logs, f"cleaning up {args.actors} actors")
            try:
                status["cleanupExitCode"] = run_streaming(cleanup_args(args), logs)
            except Exception as e:  # noqa: BLE001
                tee(logs, f"cleanup failed: {e}")

    status_path.write_text(json.dumps(status, indent=2))
    upload_ok = True
    for src, basename in ((status_path, "status.json"), (report_path, "report.json"), (logs_path, "logs.txt")):
        if not src.exists():
            continue
        try:
            upload(src, f"{prefix}/{basename}")
        except Exception as e:  # noqa: BLE001
            print(f"upload of {basename} failed: {e}", flush=True)
            upload_ok = False
    print(f"status: {json.dumps(status)}", flush=True)
    sys.exit(0 if status["ok"] and upload_ok else 1)


if __name__ == "__main__":
    main()
