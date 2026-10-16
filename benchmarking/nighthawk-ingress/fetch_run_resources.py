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

"""fetch-run-resources: CPU and memory for one finished nighthawk-ingress run.

Reads the run's stats.jsonl (stage windows) and capacity.json (envoy_cpu),
queries Cloud Monitoring and Google Managed Prometheus for the run window,
and writes resources.jsonl plus resources-by-stage.json.

Sources, at 60 s resolution:
  otlp  Default. Substrate's own instruments through Managed Prometheus:
        ate.actor.stats.* per atelet pod (actor cgroups summed per node)
        and the router request rate from atenet.router.route.duration.
        Two installs can share one cluster label, so the run's pods are
        picked by name: the atelet DaemonSet whose version suffix carries
        the run's commit sha, and the atenet-router pod on the same nodes.
        --atelet names the DaemonSet directly when that match fails.
  k8s   With --system-metrics. kubernetes.io/container/* for router envoy
        and atenet-router, the runner Job pod and the worker pods. Returns
        nothing until the cluster exports GKE system metrics; router CPU
        and memory then come out null. For router CPU today, run the
        runner with --sample-resources.

The GCP project and the cluster label come from --project and --cluster,
or from PROJECT_ID and CLUSTER_NAME in the environment, as set by
.ate-dev-env.sh.

Example:
  python3 fetch_run_resources.py \\
    gs://<bucket>/runs/<name>/run_date=.../run_ts=.../run_tag=<sha> \\
    --project <project> --cluster <cluster> --out /tmp/res
"""

import argparse
import json
import os
import re
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable

import resources_common as rc

MONITORING = "https://monitoring.googleapis.com"
ROUTER_CONTAINERS = ("envoy", "atenet-router")
STEP_S = 60
PAD_S = 60

BY_STAGE_SERIES = (
    "cpu_cores",
    "working_set_bytes",
    "cpu_limit_utilization",
    "cpu_cores_p50",
    "cpu_cores_max",
    "working_set_bytes_p50",
    "working_set_bytes_max",
    "sampled_actors",
    "route_rps",
)

# (value name, metric type, extra metric filter, per-series aligner)
K8S_METRICS = (
    ("cpu_cores", "kubernetes.io/container/cpu/core_usage_time", "", "ALIGN_RATE"),
    (
        "cpu_limit_utilization",
        "kubernetes.io/container/cpu/limit_utilization",
        "",
        "ALIGN_MEAN",
    ),
    (
        "working_set_bytes",
        "kubernetes.io/container/memory/used_bytes",
        ' AND metric.labels.memory_type="non-evictable"',
        "ALIGN_MEAN",
    ),
)


def sanitize(name: str) -> str:
    """Same rule as benchmarking/automation/orchestrator.py."""
    return re.sub(r"[^a-z0-9-]+", "-", name.lower()).strip("-")


def tag_sha7(tag: str) -> str:
    """First 7 hex digits of the commit in a runner tag.

    Tags are the full commit sha, or run-dev.sh's quick-<sha7>-<HHMMSS>.
    """
    for part in tag.lower().split("-"):
        if re.fullmatch(r"[0-9a-f]{7,40}", part):
            return part[:7]
    return tag[:7]


def atelet_controller_regex(tag: str) -> str:
    """PromQL regex for the run's atelet DaemonSet name.

    ate-setup names it atelet-<version suffix>: the ko build version with
    every non-alphanumeric mapped to '-', so a bare sha such as 12cf471 or
    a git describe such as v0-2-0-54-g12cf471-dirty.
    """
    return f"atelet-(.*-g)?{tag_sha7(tag)}[0-9a-f]*(-dirty)?"


def runner_pod_prefix(test_name: str, tag: str) -> str:
    """Orchestrator Job pods: runner-<sanitized name>-<sha7>-<uuid6>-<suffix>.

    run-dev.sh names its Job differently; pass --runner-pod-prefix for those.
    """
    return f"runner-{sanitize(test_name)}-{tag_sha7(tag)}-"


def component_selectors(
    test_name: str,
    tag: str,
    workers_namespace: str,
    worker_container: str,
    runner_prefix: str | None = None,
) -> list[dict]:
    """Namespace, pod-name prefix, and container set per component."""
    return [
        {
            "component": "router",
            "namespace": "ate-system",
            "pod_prefix": "atenet-router-",
            "containers": ROUTER_CONTAINERS,
        },
        {
            "component": "runner",
            "namespace": "benchmarking",
            "pod_prefix": runner_prefix or runner_pod_prefix(test_name, tag),
            "containers": ("runner",),
        },
        {
            "component": "workers",
            "namespace": workers_namespace,
            "pod_prefix": "",
            "containers": (worker_container,),
        },
    ]


# ---- run artifacts -----------------------------------------------------


def read_artifact(prefix: str, name: str) -> str | None:
    if prefix.startswith("gs://"):
        res = subprocess.run(
            ["gcloud", "storage", "cat", f"{prefix.rstrip('/')}/{name}"],
            capture_output=True,
            text=True,
        )
        return res.stdout if res.returncode == 0 else None
    p = Path(prefix) / name
    return p.read_text() if p.exists() else None


def upload_outputs(prefix: str, paths: list[Path]) -> None:
    for p in paths:
        dest = f"{prefix.rstrip('/')}/{p.name}"
        if prefix.startswith("gs://"):
            subprocess.run(["gcloud", "storage", "cp", str(p), dest], check=True)
        elif Path(dest).resolve() != p.resolve():
            Path(dest).write_bytes(p.read_bytes())
        print(f"Uploaded {p} -> {dest}")


# ---- HTTP ----------------------------------------------------------------


def access_token(token_file: str | None) -> str:
    if token_file:
        return Path(token_file).read_text().strip()
    if os.environ.get("GOOGLE_OAUTH_ACCESS_TOKEN"):
        return os.environ["GOOGLE_OAUTH_ACCESS_TOKEN"].strip()
    # The plain gcloud CLI token can be bound to a client certificate, which
    # the Monitoring API rejects; the ADC token is not.
    return subprocess.run(
        ["gcloud", "auth", "application-default", "print-access-token"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()


HttpGet = Callable[[str, list[tuple[str, str]]], dict]


def make_http_get(token: str) -> HttpGet:
    def get(url: str, params: list[tuple[str, str]]) -> dict:
        full = f"{url}?{urllib.parse.urlencode(params)}"
        req = urllib.request.Request(full, headers={"Authorization": f"Bearer {token}"})
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                return json.load(resp)
        except urllib.error.HTTPError as e:
            body = e.read().decode(errors="replace")[:300]
            raise RuntimeError(f"HTTP {e.code} from {url}: {body}") from e
        except urllib.error.URLError as e:
            raise RuntimeError(f"{url}: {e.reason}") from e

    return get


# ---- Cloud Monitoring (kubernetes.io) ------------------------------------


def k8s_filter(metric_type: str, extra: str, cluster: str, sel: dict) -> str:
    containers = ",".join(f'"{c}"' for c in sel["containers"])
    f = (
        f'metric.type="{metric_type}"{extra}'
        ' AND resource.type="k8s_container"'
        f' AND resource.labels.cluster_name="{cluster}"'
        f' AND resource.labels.namespace_name="{sel["namespace"]}"'
        f" AND resource.labels.container_name=one_of({containers})"
    )
    if sel["pod_prefix"]:
        f += f' AND resource.labels.pod_name=starts_with("{sel["pod_prefix"]}")'
    return f


def list_time_series(
    get: HttpGet,
    project: str,
    flt: str,
    start: datetime,
    end: datetime,
    aligner: str,
) -> list[dict]:
    url = f"{MONITORING}/v3/projects/{project}/timeSeries"
    params = [
        ("filter", flt),
        ("interval.startTime", rc.format_time(start)),
        ("interval.endTime", rc.format_time(end)),
        ("aggregation.alignmentPeriod", f"{STEP_S}s"),
        ("aggregation.perSeriesAligner", aligner),
    ]
    series: list[dict] = []
    token = ""
    while True:
        page = get(url, params + ([("pageToken", token)] if token else []))
        series.extend(page.get("timeSeries", []))
        token = page.get("nextPageToken", "")
        if not token:
            return series


def point_value(point: dict) -> float | None:
    v = point.get("value", {})
    for k in ("doubleValue", "int64Value"):
        if k in v:
            return float(v[k])
    return None


def k8s_samples(series_by_value: dict[str, list[dict]], component: str) -> list[rc.Sample]:
    """Merge per-metric Monitoring series into one Sample per (pod, container, t)."""
    merged: dict[tuple, rc.Sample] = {}
    for value_name, series in series_by_value.items():
        for ts in series:
            labels = ts["resource"]["labels"]
            pod, container = labels["pod_name"], labels["container_name"]
            for p in ts.get("points", []):
                t = rc.parse_time(p["interval"]["endTime"])
                key = (pod, container, t)
                s = merged.get(key)
                if s is None:
                    s = merged[key] = rc.Sample(
                        timestamp=t,
                        window_s=STEP_S,
                        component=component,
                        pod=pod,
                        container=container,
                        source="k8s",
                    )
                s.values[value_name] = point_value(p)
    return list(merged.values())


# ---- GMP PromQL ------------------------------------------------------------


def promql_range(
    get: HttpGet, project: str, query: str, start: datetime, end: datetime
) -> list[dict]:
    url = f"{MONITORING}/v1/projects/{project}/location/global/prometheus/api/v1/query_range"
    res = get(
        url,
        [
            ("query", query),
            ("start", rc.format_time(start)),
            ("end", rc.format_time(end)),
            ("step", f"{STEP_S}s"),
        ],
    )
    if res.get("status") != "success":
        raise RuntimeError(f"PromQL error for {query}: {res}")
    return res["data"]["result"]


def promql_instant(get: HttpGet, project: str, query: str, at: datetime) -> list[dict]:
    url = f"{MONITORING}/v1/projects/{project}/location/global/prometheus/api/v1/query"
    res = get(url, [("query", query), ("time", rc.format_time(at))])
    if res.get("status") != "success":
        raise RuntimeError(f"PromQL error for {query}: {res}")
    return res["data"]["result"]


def matrix_samples(
    results_by_value: dict[str, tuple[list[dict], float]],
    component: str,
    source: str,
    pod_label: str = "pod",
    container_label: str = "container",
    container_default: str = "",
    pod_names: dict[str, str] | None = None,
) -> list[rc.Sample]:
    """Merge PromQL matrices into one Sample per (pod, container, t)."""
    merged: dict[tuple, rc.Sample] = {}
    for value_name, (results, window) in results_by_value.items():
        for r in results:
            metric = r["metric"]
            pod = metric.get(pod_label, "")
            if pod_names:
                pod = pod_names.get(pod, pod)
            container = metric.get(container_label, container_default)
            for ts, val in r["values"]:
                t = datetime.fromtimestamp(float(ts), timezone.utc)
                key = (pod, container, t)
                s = merged.get(key)
                if s is None:
                    s = merged[key] = rc.Sample(
                        timestamp=t,
                        window_s=window,
                        component=component,
                        pod=pod,
                        container=container,
                        source=source,
                    )
                s.window_s = max(s.window_s, window)
                s.values[value_name] = float(val)
    return list(merged.values())


def otlp_queries(cluster: str, atelet_match: str) -> dict[str, tuple[str, float]]:
    """atelet_match is the label matcher, e.g. '=~"atelet-.*"' or '="atelet-x"'."""
    m = f'cluster="{cluster}",top_level_controller_name{atelet_match}'
    return {
        "cpu_cores": (
            f'sum by (instance)(rate({{__name__="ate.actor.stats.cpu.time",{m}}}[2m]))',
            120,
        ),
        "working_set_bytes": (
            f'sum by (instance)({{__name__="ate.actor.stats.memory.working_set",{m}}})',
            STEP_S,
        ),
        "sampled_actors": (
            f'sum by (instance)({{__name__="ate.actor.stats.sampled_actors",{m}}})',
            STEP_S,
        ),
    }


def router_rps_query(cluster: str) -> str:
    return (
        "sum by (instance)(rate("
        f'{{__name__="atenet.router.route.duration_count",cluster="{cluster}",'
        'job="atenet-router"}[2m]))'
    )


def target_info_query(cluster: str, job: str) -> str:
    return f'{{__name__="target_info",cluster="{cluster}",job="{job}"}}'


def instance_map(target_info: list[dict]) -> dict[str, dict]:
    """instance uid -> {pod, node} from a target_info matrix or vector."""
    out = {}
    for r in target_info:
        m = r["metric"]
        out[m.get("instance", "")] = {
            "pod": m.get("k8s.pod.name", ""),
            "node": m.get("k8s.node.name", ""),
        }
    return out


def select_router_instances(
    atelet_instances: set[str],
    atelet_info: dict[str, dict],
    router_info: dict[str, dict],
) -> set[str] | None:
    """Router pods on the same nodes as this run's atelet pods.

    Several runs can share one cluster label; the run's atelet DaemonSet
    carries the run's commit sha, and its router runs on that install's nodes.
    Without a node mapping, every atenet-router pod is kept. None means
    target_info was empty: keep every instance.
    """
    # atenet-egress shares the atenet-router binary and job label.
    routers = {
        i: v for i, v in router_info.items() if v["pod"].startswith("atenet-router-")
    }
    nodes = {atelet_info[i]["node"] for i in atelet_instances if i in atelet_info}
    nodes.discard("")
    if not nodes:
        return set(routers) if routers else None
    return {i for i, v in routers.items() if v["node"] in nodes}


# ---- worker aggregation ------------------------------------------------------


def aggregate_workers(samples: list[rc.Sample]) -> list[rc.Sample]:
    """Per timestamp and source: p50 and max across worker pods."""
    groups: dict[tuple, list[rc.Sample]] = {}
    for s in samples:
        groups.setdefault((s.timestamp, s.source, s.container), []).append(s)
    out = []
    for (t, source, container), group in sorted(groups.items()):
        agg = rc.Sample(
            timestamp=t,
            window_s=max(g.window_s for g in group),
            component="workers",
            pod="*",
            container=container,
            source=source,
            values={"pods": float(len(group))},
        )
        for name in ("cpu_cores", "working_set_bytes"):
            vals = [g.values[name] for g in group if g.values.get(name) is not None]
            agg.values[f"{name}_p50"] = rc.percentile(vals, 50) if vals else None
            agg.values[f"{name}_max"] = max(vals) if vals else None
        out.append(agg)
    return out


# ---- driver ------------------------------------------------------------------


def fetch(
    get: HttpGet,
    project: str,
    cluster: str,
    stages: list[rc.Stage],
    tag: str,
    test_name: str,
    workers_namespace: str,
    worker_container: str,
    per_pod: bool,
    log: Callable[[str], None] = print,
    system_metrics: bool = False,
    atelet: str | None = None,
    runner_prefix: str | None = None,
) -> tuple[list[rc.Sample], list[dict]]:
    """Return samples and a log of every query with its series count.

    atelet names the run's atelet DaemonSet exactly; by default it is matched
    by the commit sha in tag. runner_prefix overrides the runner pod prefix.
    """
    start = stages[0].start - timedelta(seconds=PAD_S)
    end = stages[-1].end + timedelta(seconds=PAD_S)
    queries: list[dict] = []
    samples: list[rc.Sample] = []

    def record(source: str, component: str, value: str, query: str, n: int | str):
        queries.append(
            {"source": source, "component": component, "value": value, "query": query, "series": n}
        )
        log(f"[{source}] {component}.{value}: {n} series")

    selectors = component_selectors(
        test_name, tag, workers_namespace, worker_container, runner_prefix
    )
    for sel in selectors if system_metrics else []:
        comp = sel["component"]

        by_value: dict[str, list[dict]] = {}
        for value, metric, extra, aligner in K8S_METRICS:
            flt = k8s_filter(metric, extra, cluster, sel)
            try:
                by_value[value] = list_time_series(get, project, flt, start, end, aligner)
                record("k8s", comp, value, flt, len(by_value[value]))
            except RuntimeError as e:
                record("k8s", comp, value, flt, f"error: {e}")
        got = k8s_samples(by_value, comp)

        if comp == "workers":
            agg = aggregate_workers(got)
            samples += agg + (got if per_pod else [])
        else:
            samples += got

    atelet_match = f'="{atelet}"' if atelet else f'=~"{atelet_controller_regex(tag)}"'
    mid = stages[0].start + (stages[-1].end - stages[0].start) / 2
    try:
        atelet_info = instance_map(
            promql_instant(get, project, target_info_query(cluster, "atelet"), mid)
        )
        router_info = instance_map(
            promql_instant(get, project, target_info_query(cluster, "atenet-router"), mid)
        )
    except RuntimeError as e:
        log(f"[otlp] target_info failed: {e}")
        atelet_info, router_info = {}, {}

    otlp: dict[str, tuple[list[dict], float]] = {}
    for value, (q, window) in otlp_queries(cluster, atelet_match).items():
        try:
            otlp[value] = (promql_range(get, project, q, start, end), window)
            record("otlp", "actors", value, q, len(otlp[value][0]))
        except RuntimeError as e:
            record("otlp", "actors", value, q, f"error: {e}")
    if not any(rs for rs, _ in otlp.values()):
        log(
            f"[otlp] WARNING: no actor series for top_level_controller_name{atelet_match}; "
            "pass --atelet <daemonset name> (kubectl -n ate-system get ds)"
        )
    names = {i: v["pod"] or i for i, v in atelet_info.items()}
    samples += matrix_samples(
        otlp, "actors", "otlp", pod_label="instance",
        container_default="all-actors", pod_names=names,
    )

    atelet_instances = {
        r["metric"].get("instance", "") for rs, _ in otlp.values() for r in rs
    }
    keep = select_router_instances(atelet_instances, atelet_info, router_info)
    if keep is None:
        log("[otlp] WARNING: router rps unfiltered: no atelet node mapping, every atenet-router job instance kept")
    q = router_rps_query(cluster)
    try:
        rps = promql_range(get, project, q, start, end)
        if keep is not None:
            rps = [r for r in rps if r["metric"].get("instance") in keep]
        record("otlp", "router", "route_rps", q, len(rps))
        names = {i: v["pod"] or i for i, v in router_info.items()}
        samples += matrix_samples(
            {"route_rps": (rps, 120)}, "router", "otlp", pod_label="instance",
            container_default="atenet-router-otlp", pod_names=names,
        )
    except RuntimeError as e:
        record("otlp", "router", "route_rps", q, f"error: {e}")

    return samples, queries


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="fetch-run-resources",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("run", help="Run prefix: gs://.../run_tag=<sha> or a local dir")
    p.add_argument("--out", help="Output dir (default: the run dir if local, else ./<run_ts>)")
    p.add_argument(
        "--project",
        default=os.environ.get("PROJECT_ID"),
        help="GCP project that holds the metrics (default: $PROJECT_ID)",
    )
    p.add_argument(
        "--cluster",
        default=os.environ.get("CLUSTER_NAME"),
        help="Value of the cluster / cluster_name label (default: $CLUSTER_NAME)",
    )
    p.add_argument("--envoy-cpu", type=float, help="Override capacity.json envoy_cpu")
    p.add_argument(
        "--atelet",
        help="The run's atelet DaemonSet name (default: match the tag's commit sha)",
    )
    p.add_argument(
        "--runner-pod-prefix",
        help="Runner pod name prefix for --system-metrics (default: orchestrator naming)",
    )
    p.add_argument("--workers-namespace", default="benchmark-workloads")
    p.add_argument("--worker-container", default="ateom")
    p.add_argument("--per-pod", action="store_true", help="Also emit each worker pod")
    p.add_argument(
        "--system-metrics",
        action="store_true",
        help="Also query kubernetes.io/container/* (empty until the cluster "
        "exports GKE system metrics)",
    )
    p.add_argument("--token-file", help="OAuth access token file (default: gcloud ADC)")
    p.add_argument(
        "--upload",
        action="store_true",
        help="Copy both outputs next to the run's artifacts (default off)",
    )
    args = p.parse_args(argv)
    for flag, env in (("--project", "PROJECT_ID"), ("--cluster", "CLUSTER_NAME")):
        if not getattr(args, flag[2:]):
            p.error(f"{flag} is required when ${env} is unset")
    return args


def default_out(run: str) -> Path:
    if not run.startswith("gs://"):
        return Path(run)
    m = re.search(r"run_ts=(\d+)", run)
    return Path(m.group(1) if m else "resources")


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    stats = read_artifact(args.run, "stats.jsonl")
    if not stats:
        print(f"no stats.jsonl under {args.run}", file=sys.stderr)
        return 2
    stages, tag, test_name = rc.stages_from_stats(stats.splitlines())
    if not stages:
        print("stats.jsonl has no stages", file=sys.stderr)
        return 2
    envoy_cpu = args.envoy_cpu
    if envoy_cpu is None:
        cap = read_artifact(args.run, "capacity.json")
        if cap:
            envoy_cpu = json.loads(cap).get("envoy_cpu")

    get = make_http_get(access_token(args.token_file))
    samples, queries = fetch(
        get, args.project, args.cluster, stages, tag, test_name,
        args.workers_namespace, args.worker_container, args.per_pod,
        system_metrics=args.system_metrics,
        atelet=args.atelet,
        runner_prefix=args.runner_pod_prefix,
    )

    expected = [("actors", "all-actors"), ("router", "atenet-router-otlp")]
    if args.system_metrics:
        expected += [
            (sel["component"], c)
            for sel in component_selectors(
                test_name, tag, args.workers_namespace, args.worker_container
            )
            for c in sel["containers"]
        ]
    out = Path(args.out) if args.out else default_out(args.run)
    out.mkdir(parents=True, exist_ok=True)
    res_path = out / "resources.jsonl"
    rc.write_jsonl(rc.sample_records(samples, stages, tag, test_name), res_path)
    stage_path = out / "resources-by-stage.json"
    stage_path.write_text(
        json.dumps(
            {
                "run": args.run,
                "tag": tag,
                "test_name": test_name,
                "project": args.project,
                "cluster": args.cluster,
                "envoy_cpu": envoy_cpu,
                "missing": rc.missing_containers(samples, expected),
                "stages": rc.by_stage(samples, stages, envoy_cpu, BY_STAGE_SERIES),
                "queries": queries,
            },
            indent=2,
        )
    )
    print(f"Wrote {len(samples)} samples to {res_path} and {stage_path}")
    missing = rc.missing_containers(samples, expected)
    if missing:
        print(f"No data for: {', '.join(missing)}; see 'queries' in {stage_path}")
    if args.upload:
        upload_outputs(args.run, [res_path, stage_path])
    return 0


if __name__ == "__main__":
    sys.exit(main())
