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

"""Snapshot the atenet-router Envoy admin /stats into envoy-stats.jsonl.

Keeps server.memory_*, server.worker_*, cluster.*.upstream_cx_total,
http.*.downstream_rq_* and listener.*.downstream_cx_*. Each call appends
one record per router pod, labeled (for example before or after).

Reaching the admin port (9901, bound on the pod IP; the atenet-router
Service does not expose it):
  --via direct  GET http://<pod IP>:9901/stats. Pod IPs come from the
                atenet-router EndpointSlices, which the runner's
                ServiceAccount can already read. Works from inside the
                cluster only.
  --via proxy   GET /api/v1/namespaces/ate-system/pods/<pod>:9901/proxy/stats
                through the API server. Needs pods list and pods/proxy get
                in ate-system; works off-cluster with kubectl.

  python3 envoy_stats.py --via proxy --label before --out envoy-stats.jsonl
"""

import argparse
import json
import re
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

import kubeapi
import resources_common as rc

ADMIN_PORT = 9901
NAMESPACE = "ate-system"
SERVICE = "atenet-router"
POD_SELECTOR = "app=atenet-router"

KEEP = re.compile(
    r"server\.memory_.*"
    r"|server\.worker_.*"
    r"|cluster\.[^:]+\.upstream_cx_total"
    r"|http\.[^:]+\.downstream_rq_.*"
    r"|listener\.[^:]+\.downstream_cx_.*"
)


def parse_stats(text: str) -> dict[str, float]:
    """Kept counters and gauges from Envoy's text /stats. Histograms, whose
    values are not numbers, are skipped."""
    out = {}
    for line in text.splitlines():
        name, sep, value = line.partition(": ")
        if not sep or not KEEP.fullmatch(name):
            continue
        try:
            out[name] = float(value)
        except ValueError:
            continue
    return out


def router_endpoints(client: kubeapi.KubeClient) -> list[tuple[str, str]]:
    """(pod name, address) for each atenet-router endpoint."""
    q = urllib.parse.urlencode({"labelSelector": f"kubernetes.io/service-name={SERVICE}"})
    body = client.get_json(f"/apis/discovery.k8s.io/v1/namespaces/{NAMESPACE}/endpointslices?{q}")
    seen = {}
    for sl in body.get("items", []):
        for ep in sl.get("endpoints") or []:
            addrs = ep.get("addresses") or []
            pod = (ep.get("targetRef") or {}).get("name", "")
            if addrs and pod not in seen:
                seen[pod] = addrs[0]
    return sorted(seen.items())


def http_get(url: str, timeout_s: float = 10) -> str:
    with urllib.request.urlopen(url, timeout=timeout_s) as r:
        return r.read().decode()


def admin_url(addr: str, port: int = ADMIN_PORT) -> str:
    host = f"[{addr}]" if ":" in addr else addr
    return f"http://{host}:{port}/stats"


def snapshot(
    client: kubeapi.KubeClient,
    via: str,
    get: Callable[[str], str] = http_get,
    port: int = ADMIN_PORT,
) -> list[tuple[str, dict[str, float] | None, str]]:
    """(pod, stats or None, error) per router pod."""
    out = []
    if via == "direct":
        targets = [(pod, admin_url(addr, port)) for pod, addr in router_endpoints(client)]
        fetch = get
    else:
        pods = client.list_pods(NAMESPACE, POD_SELECTOR)
        targets = [
            (p["metadata"]["name"], f"/api/v1/namespaces/{NAMESPACE}/pods/{p['metadata']['name']}:{port}/proxy/stats")
            for p in pods
            if p.get("status", {}).get("phase") == "Running"
        ]
        fetch = client.get_raw
    for pod, url in targets:
        try:
            out.append((pod, parse_stats(fetch(url)), ""))
        except Exception as e:
            out.append((pod, None, str(e)))
    return out


def records(
    snaps: list[tuple[str, dict[str, float] | None, str]],
    label: str,
    via: str,
    tag: str,
    test_name: str,
    now: datetime,
) -> list[dict]:
    """stats.jsonl envelope; stats null when the admin port did not answer."""
    return [
        {
            "timestamp": rc.format_time(now),
            "tag": tag,
            "test_name": test_name,
            "metric": f"envoy-stats/{label}",
            "measurements": {
                "pod": pod,
                "label": label,
                "via": via,
                "error": err or None,
                "stats": stats,
            },
        }
        for pod, stats, err in snaps
    ]


def append(
    client: kubeapi.KubeClient,
    out: Path,
    label: str,
    via: str = "direct",
    tag: str = "",
    test_name: str = "",
    get: Callable[[str], str] = http_get,
    now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
) -> list[dict]:
    recs = records(snapshot(client, via, get), label, via, tag, test_name, now())
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "a") as f:
        for r in recs:
            f.write(json.dumps(r) + "\n")
    return recs


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        prog="envoy_stats",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--out", required=True, help="envoy-stats.jsonl to append to")
    p.add_argument("--label", required=True, help="Snapshot label, e.g. before or after")
    p.add_argument("--via", choices=("direct", "proxy"), default="direct")
    p.add_argument("--context", help="kubectl context (default: in-cluster, else current)")
    p.add_argument("--tag", default="")
    p.add_argument("--test-name", default="")
    args = p.parse_args(argv)
    recs = append(
        kubeapi.default_client(args.context), Path(args.out), args.label, args.via, args.tag, args.test_name
    )
    for r in recs:
        m = r["measurements"]
        n = len(m["stats"]) if m["stats"] is not None else 0
        print(f"{m['pod']}: {n} stats" + (f", error: {m['error']}" if m["error"] else ""))
    if not recs:
        print("no atenet-router pods found", file=sys.stderr)
    return 0 if recs and all(r["measurements"]["stats"] is not None for r in recs) else 1


if __name__ == "__main__":
    sys.exit(main())
