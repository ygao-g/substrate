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

"""Adaptive load driver for the nighthawk-egress benchmark."""

import argparse
import base64
import concurrent.futures
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import grpc

import actors as actors_mod
import envoy_stats as envoy_stats_mod
import kubeapi
import output as output_mod
import resource_sampler as sampler_mod
import resources_common as rc
import spec as spec_mod

ROUTER_HOST = "atenet-router.ate-system.svc:443"


class ExponentialSearchStepController:
    """Parity with Nighthawk C++ ExponentialSearchStepController.

    Performs 2x exponential expansion during range-finding, immediate doom
    abort if Stage 0 fails, and bisection until successive RPS values are
    within < 1% of each other.
    """

    def __init__(
        self,
        initial_rps: float = 1000.0,
        exponential_factor: float = 2.0,
        convergence_threshold: float = 0.01,
    ):
        self.initial_rps = float(initial_rps)
        self.factor = float(exponential_factor) if exponential_factor > 0 else 2.0
        self.threshold = float(convergence_threshold)

        self._is_range_finding = True
        self._current_rps = self.initial_rps
        self._previous_rps = None
        self._bottom_rps = 0.0
        self._top_rps = 0.0
        self.converged = False
        self.doomed = False
        self.doom_reason = ""

    def is_doomed(self) -> bool:
        return self.doomed

    def is_converged(self) -> bool:
        return self.converged

    @property
    def best_clean_rps(self) -> float:
        return self._bottom_rps if self._bottom_rps > 0 else 0.0

    def next_rps(self) -> float:
        return self._current_rps

    def record_result(self, passed: bool) -> None:
        score = 1.0 if passed else -1.0
        if self._is_range_finding:
            if score > 0.0:
                self._previous_rps = self._current_rps
                self._bottom_rps = self._current_rps
                self._current_rps *= self.factor
            else:
                if self._previous_rps is None:
                    self.doomed = True
                    self.doom_reason = (
                        "Stage 0 failed thresholds; target cannot sustain initial RPS"
                    )
                    return
                self._is_range_finding = False
                self._bottom_rps = self._previous_rps
                self._top_rps = self._current_rps
                self._previous_rps = self._current_rps
                self._current_rps = (self._bottom_rps + self._top_rps) / 2.0
                if (
                    self._previous_rps > 0
                    and abs(self._current_rps / self._previous_rps - 1.0) < self.threshold
                ):
                    self.converged = True
        else:
            if score > 0.0:
                self._bottom_rps = self._current_rps
            else:
                self._top_rps = self._current_rps

            self._previous_rps = self._current_rps
            self._current_rps = (self._bottom_rps + self._top_rps) / 2.0
            if (
                self._previous_rps > 0
                and abs(self._current_rps / self._previous_rps - 1.0) < self.threshold
            ):
                self.converged = True


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Adaptive nighthawk-egress benchmark runner")
    p.add_argument("--mode", choices=["http", "https-mitm"], default="http")
    p.add_argument("--conn-mode", choices=["keepalive", "churn"], default="keepalive")
    p.add_argument("--actors", type=int, default=1, help="Number of traffic-driver actor instances (M)")
    p.add_argument("--client-concurrency", type=int, default=4)
    p.add_argument(
        "--connections",
        type=int,
        default=None,
        help="Connections per worker thread (default: 32 for keepalive, 250 for churn)",
    )
    p.add_argument("--max-pending-requests", type=int, default=1000)
    p.add_argument("--initial-total-rps", type=int, default=1000)
    p.add_argument("--exponential-factor", type=float, default=2.0)
    p.add_argument("--measuring-period-s", type=int, default=20)
    p.add_argument("--testing-stage-duration-s", type=int, default=60)
    p.add_argument("--convergence-deadline-s", type=int, default=600)
    p.add_argument("--success-rate-threshold", type=float, default=0.998)
    p.add_argument("--send-rate-threshold", type=float, default=0.9)
    p.add_argument("--tail-latency-slo-ms", type=float, default=25.0)
    p.add_argument(
        "--simple-warmup",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Enable simple warmup request in CommandLineOptions (default: true for keepalive, false for churn)",
    )
    p.add_argument(
        "--sample-resources",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    p.add_argument("--out-dir", type=Path, default=Path("/tmp/nighthawk-egress-out"))
    p.add_argument("--envoy-cpu", type=int, default=2)
    p.add_argument("--ext-proc-cpu", type=int, default=None)
    p.add_argument("--atespace", default=actors_mod.ATESPACE)
    p.add_argument("--dest", default="")
    p.add_argument("--tag", default="")
    p.add_argument("--run-tag", default="", dest="run_tag")
    p.add_argument("--name", default="")
    p.add_argument("--test-name", default="", dest="test_name")
    args = p.parse_args(argv)
    if args.ext_proc_cpu is None:
        args.ext_proc_cpu = args.envoy_cpu
    if args.connections is None:
        args.connections = 32 if args.conn_mode == "keepalive" else 250
    if args.simple_warmup is None:
        args.simple_warmup = (args.conn_mode == "keepalive")
    return args


def extract_stage_resources(by_stage_data: list[dict], target_stage_name: str | None) -> dict:
    if not by_stage_data:
        return {}
    target_entry = None
    if target_stage_name:
        for entry in by_stage_data:
            st = entry.get("stage", "")
            if st == target_stage_name or st == f"nighthawk_{target_stage_name}":
                target_entry = entry
                break
    if not target_entry:
        target_entry = by_stage_data[-1]

    stage_res: dict[str, float | None] = {}
    for c_entry in target_entry.get("containers", {}).values():
        comp = c_entry.get("component")
        cont = c_entry.get("container")
        pin = c_entry.get("cpu_fraction_of_pin") or {}
        if comp == "egress" and cont == "envoy":
            stage_res["envoy_cpu_util"] = pin.get("mean")
        elif comp == "egress" and cont == "ext-proc":
            stage_res["ext_proc_cpu_util"] = pin.get("mean")
        elif comp == "workers":
            val = pin.get("mean")
            if val is not None:
                stage_res["worker_max_cpu_util"] = max(stage_res.get("worker_max_cpu_util", 0.0), val)
        elif comp == "server":
            val = pin.get("mean")
            if val is not None:
                stage_res["origin_max_cpu_util"] = max(stage_res.get("origin_max_cpu_util", 0.0), val)
    return stage_res


def upload_file(src: Path, dest: str) -> None:
    if not src.exists():
        return
    if dest.startswith("gs://"):
        from google.cloud import storage

        bucket_name, _, blob_path = dest[len("gs://"):].partition("/")
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_path)
        blob.upload_from_filename(str(src))
        print(f"Uploaded {src} -> {dest}")
    else:
        dest_p = Path(dest)
        dest_p.parent.mkdir(parents=True, exist_ok=True)
        dest_p.write_bytes(src.read_bytes())
        print(f"Copied {src} -> {dest}")


def evaluate_stage_thresholds(
    row: dict,
    success_rate_threshold: float,
    send_rate_threshold: float | None = None,
    tail_latency_slo_ms: float | None = None,
) -> bool:
    succ_rate = row.get(output_mod._builtin_key("success-rate"), 0.0)
    send_rate = row.get(output_mod._builtin_key("send-rate"), 0.0)
    lat_slo = row.get("latency_mean_plus_2stdev_ms", 0.0)

    passed = True
    row["failed_thresholds"] = []
    if succ_rate < success_rate_threshold:
        passed = False
        row["failed_thresholds"].append("success-rate")
    if send_rate_threshold is not None and send_rate < send_rate_threshold:
        passed = False
        row["failed_thresholds"].append("send-rate")
    if tail_latency_slo_ms is not None and lat_slo > tail_latency_slo_ms:
        passed = False
        row["failed_thresholds"].append("latency-ns-mean-plus-2stdev")
    return passed


def run_stage_on_actor(
    router_target,
    actor_name: str,
    execution_req,
    atespace: str = actors_mod.ATESPACE,
    desc_path: str | None = None,
    timeout_s: float = 180.0,
) -> dict:
    """Execute one benchmark stage against an actor via NighthawkService.ExecutionStream."""
    from google.protobuf import json_format

    if desc_path is None:
        desc_path = os.environ.get("NIGHTHAWK_DESC", "/app/nighthawk.desc")

    if not desc_path or not os.path.exists(desc_path):
        raise FileNotFoundError(
            f"Nighthawk descriptor set not found at {desc_path!r}; "
            "set NIGHTHAWK_DESC or pass desc_path"
        )

    pool = spec_mod.load_pool(desc_path)
    resp_cls = spec_mod.message_class(pool, "nighthawk.client.ExecutionResponse")

    if isinstance(router_target, grpc.Channel) or hasattr(router_target, "stream_stream"):
        stream_rpc = router_target.stream_stream(
            "/nighthawk.client.NighthawkService/ExecutionStream",
            request_serializer=lambda r: r.SerializeToString() if hasattr(r, "SerializeToString") else r,
            response_deserializer=resp_cls.FromString,
        )
    else:
        stream_rpc = getattr(router_target, "ExecutionStream", None)

    metadata = [("ate-target-actor", f"{atespace}/{actor_name}")]
    call = stream_rpc(iter([execution_req]), metadata=metadata, timeout=timeout_s)
    responses = list(call)
    if not responses:
        code = call.code() if hasattr(call, "code") else None
        details = call.details() if hasattr(call, "details") else ""
        raise RuntimeError(
            f"actor {actor_name} ExecutionStream returned 0 responses (code={code}, details={details})"
        )

    resp = responses[0]
    if hasattr(resp, "HasField") and resp.HasField("error_detail"):
        code = resp.error_detail.code
        msg = resp.error_detail.message
        if code != 0:
            raise RuntimeError(f"actor {actor_name} ExecutionStream error {code}: {msg}")
    elif hasattr(resp, "error_detail") and getattr(resp.error_detail, "code", 0) != 0:
        raise RuntimeError(
            f"actor {actor_name} ExecutionStream error {resp.error_detail.code}: {resp.error_detail.message}"
        )

    if hasattr(resp, "output"):
        return json_format.MessageToDict(
            resp.output,
            preserving_proto_field_name=True,
            descriptor_pool=pool,
        )
    return {}


def main(argv=None) -> int:
    args = parse_args(argv)
    args.out_dir.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc)
    tag = args.tag or args.run_tag or "local"
    name = args.name or args.test_name or f"nighthawk-egress-{args.mode}"
    if args.dest:
        if "run_tag=" in args.dest or "runs/" in args.dest:
            dest_prefix = args.dest.rstrip("/")
        else:
            dest_prefix = (
                f"{args.dest.rstrip('/')}/runs/{name}"
                f"/run_date={now.strftime('%Y-%m-%d')}/run_ts={int(now.timestamp())}"
                f"/run_tag={tag}"
            )
    else:
        dest_prefix = ""

    logs_path = args.out_dir / "logs.txt"
    log_file = open(logs_path, "w")

    def log(msg: str) -> None:
        print(msg, flush=True)
        log_file.write(msg + "\n")
        log_file.flush()

    log("==== Nighthawk Egress Benchmark ====")
    log(f"  mode:                   {args.mode}")
    log(f"  conn_mode:              {args.conn_mode}")
    log(f"  actors:                 {args.actors}")
    log(f"  envoy_cpu:              {args.envoy_cpu}")
    log(f"  ext_proc_cpu:           {args.ext_proc_cpu}")
    log(f"  client_concurrency:     {args.client_concurrency}")
    log(f"  connections:            {args.connections}")
    log(f"  max_pending_requests:   {args.max_pending_requests}")
    log(f"  initial_total_rps:      {args.initial_total_rps}")
    log(f"  exponential_factor:     {args.exponential_factor}")
    log(f"  measuring_period_s:     {args.measuring_period_s}")
    log(f"  testing_stage_duration: {args.testing_stage_duration_s}")
    log(f"  convergence_deadline:   {args.convergence_deadline_s}")
    log(f"  success_rate_threshold: {args.success_rate_threshold}")
    log(f"  send_rate_threshold:    {args.send_rate_threshold}")
    log(f"  tail_latency_slo_ms:    {args.tail_latency_slo_ms}")
    log(f"  simple_warmup:          {args.simple_warmup}")
    log(f"  atespace:               {args.atespace}")
    log(f"  tag:                    {tag}")
    log(f"  name:                   {name}")
    log(f"  dest_prefix:            {dest_prefix or '(none)'}")
    log("====================================")

    auth_mode = os.environ.get("ATE_ATEAPI_CLIENT_AUTH", "cert")
    stub = actors_mod.ateapi_stub(auth_mode)
    k8s = kubeapi.default_client()
    desc_path = os.environ.get("NIGHTHAWK_DESC", "/app/nighthawk.desc")

    created_actors = []
    sampler = None
    if args.sample_resources:
        sampler = sampler_mod.Sampler(k8s, interval_s=2.0, log=log)
        sampler.start()

    boundary_records = []
    all_stage_rows = []
    converged = False
    run_err = None
    router_ch = None

    try:
        router_ch = actors_mod.router_grpc_channel(ROUTER_HOST)
        log(f"Creating and warming {args.actors} traffic-driver actor(s)...")
        actor_names = actors_mod.create_and_warm(
            stub,
            router_ch,
            args.actors,
            created=created_actors,
            atespace=args.atespace,
            warm_deadline_s=args.convergence_deadline_s,
            log=log,
        )

        controller = ExponentialSearchStepController(
            initial_rps=args.initial_total_rps,
            exponential_factor=args.exponential_factor,
        )

        stage_idx = 0
        search_start = time.time()
        while not controller.converged:
            if time.time() - search_start >= args.convergence_deadline_s:
                log(f"Convergence deadline ({args.convergence_deadline_s}s) exceeded.")
                break

            if controller.is_doomed():
                log(f"Search aborted: {controller.doom_reason}")
                break

            stage_idx += 1
            total_rps = controller.next_rps()
            total_workers = args.actors * args.client_concurrency
            per_worker_rps = max(1, round(total_rps / total_workers))
            attempted_total_rps = float(per_worker_rps * total_workers)
            stage_id = f"adjusting_{stage_idx:03d}"
            log(
                f"Executing stage {stage_id} at {attempted_total_rps:.1f} RPS "
                f"({per_worker_rps * args.client_concurrency} RPS/actor, {per_worker_rps} RPS/worker)..."
            )

            cmd_opts = spec_mod.build_command_line_options_dict(
                mode=args.mode,
                conn_mode=args.conn_mode,
                concurrency=args.client_concurrency,
                connections=args.connections,
                max_pending_requests=args.max_pending_requests,
                rps=per_worker_rps,
                duration_s=args.measuring_period_s,
                simple_warmup=args.simple_warmup,
            )
            exec_req = spec_mod.build_execution_request(cmd_opts, desc_path)

            try:
                boundary_records.append(envoy_stats_mod.scrape_boundary(k8s, stage_id, label="start"))
            except Exception as e:
                log(f"scrape boundary start error: {e}")

            stage_start_t = rc.format_time(datetime.now(timezone.utc))
            with concurrent.futures.ThreadPoolExecutor(max_workers=args.actors) as pool:
                futures = [
                    pool.submit(
                        run_stage_on_actor,
                        router_ch,
                        actor,
                        exec_req,
                        atespace=args.atespace,
                        desc_path=desc_path,
                        timeout_s=args.measuring_period_s + 60.0,
                    )
                    for actor in actor_names
                ]
                actor_outputs = [f.result() for f in futures]
            stage_end_t = rc.format_time(datetime.now(timezone.utc))

            try:
                boundary_records.append(envoy_stats_mod.scrape_boundary(k8s, stage_id, label="end"))
            except Exception as e:
                log(f"scrape boundary end error: {e}")

            agg_row = output_mod.aggregate_multi_actor_stage(
                stage_id,
                actor_outputs,
                total_attempted_rps=attempted_total_rps,
                start_time=stage_start_t,
                end_time=stage_end_t,
            )

            passed = evaluate_stage_thresholds(
                agg_row,
                success_rate_threshold=args.success_rate_threshold,
                send_rate_threshold=args.send_rate_threshold,
                tail_latency_slo_ms=args.tail_latency_slo_ms,
            )

            controller.record_result(passed)
            all_stage_rows.append(agg_row)

            if controller.is_doomed():
                log(f"Aborting search immediately: {controller.doom_reason}")
                break

        best_rps = controller.best_clean_rps
        if controller.converged and best_rps > 0:
            total_workers = args.actors * args.client_concurrency
            test_per_worker_rps = max(1, round(best_rps / total_workers))
            test_attempted_rps = float(test_per_worker_rps * total_workers)
            log(
                f"Converged! Running testing stage at {test_attempted_rps:.1f} RPS "
                f"({test_per_worker_rps * args.client_concurrency} RPS/actor, {test_per_worker_rps} RPS/worker) "
                f"for {args.testing_stage_duration_s}s..."
            )
            testing_stage_id = "testing"
            test_cmd_opts = spec_mod.build_command_line_options_dict(
                mode=args.mode,
                conn_mode=args.conn_mode,
                concurrency=args.client_concurrency,
                connections=args.connections,
                max_pending_requests=args.max_pending_requests,
                rps=test_per_worker_rps,
                duration_s=args.testing_stage_duration_s,
                simple_warmup=args.simple_warmup,
            )
            test_exec_req = spec_mod.build_execution_request(test_cmd_opts, desc_path)

            try:
                boundary_records.append(envoy_stats_mod.scrape_boundary(k8s, testing_stage_id, label="start"))
            except Exception as e:
                log(f"scrape boundary start error: {e}")

            test_start_t = rc.format_time(datetime.now(timezone.utc))
            with concurrent.futures.ThreadPoolExecutor(max_workers=args.actors) as pool:
                futures = [
                    pool.submit(
                        run_stage_on_actor,
                        router_ch,
                        actor,
                        test_exec_req,
                        atespace=args.atespace,
                        desc_path=desc_path,
                        timeout_s=args.testing_stage_duration_s + 60.0,
                    )
                    for actor in actor_names
                ]
                test_outputs = [f.result() for f in futures]
            test_end_t = rc.format_time(datetime.now(timezone.utc))

            try:
                boundary_records.append(envoy_stats_mod.scrape_boundary(k8s, testing_stage_id, label="end"))
            except Exception as e:
                log(f"scrape boundary end error: {e}")

            test_row = output_mod.aggregate_multi_actor_stage(
                testing_stage_id,
                test_outputs,
                total_attempted_rps=test_attempted_rps,
                start_time=test_start_t,
                end_time=test_end_t,
            )
            evaluate_stage_thresholds(
                test_row,
                success_rate_threshold=args.success_rate_threshold,
                send_rate_threshold=args.send_rate_threshold,
                tail_latency_slo_ms=args.tail_latency_slo_ms,
            )
            all_stage_rows.append(test_row)
            converged = True
        else:
            log(f"Did not converge or best_clean_rps is 0 (converged={controller.converged}, best_rps={best_rps})")

        stats_records = output_mod.stats_records(all_stage_rows, tag=tag, test_name=name)
        output_mod.write_jsonl(stats_records, args.out_dir / "stats.jsonl")

        with open(args.out_dir / "results.json", "w") as f:
            json.dump({"stages": all_stage_rows}, f, indent=2)

    except Exception as e:
        run_err = e
        log(f"Benchmark run encountered error: {e}")

    finally:
        by_stage_data = []
        if sampler:
            samples = sampler.stop()
            if samples:
                sampler_mod.write_raw_samples(samples, args.out_dir / "resources-raw.jsonl")
                stages, _, _ = rc.stages_from_stats(
                    (json.dumps(r) for r in output_mod.stats_records(all_stage_rows, tag=tag, test_name=name))
                ) if all_stage_rows else ([], "", "")
                sampler_mod.write_resources_by_stage(
                    samples,
                    stages,
                    float(args.envoy_cpu),
                    args.out_dir / "resources-by-stage.json",
                    ext_proc_cpu=float(args.ext_proc_cpu),
                )
                by_stage_data = rc.by_stage(
                    samples, stages, float(args.envoy_cpu), ext_proc_cpu=float(args.ext_proc_cpu)
                )

        if boundary_records:
            envoy_stats_mod.write_envoy_stats_by_stage(boundary_records, args.out_dir / "envoy-stats.json")

        testing_passed = any(r.get("stage") == "testing" and not r.get("failed_thresholds") for r in all_stage_rows)
        clean_rows = [r for r in all_stage_rows if not r.get("failed_thresholds")]
        best_clean = max(
            clean_rows, key=lambda r: r.get(output_mod._builtin_key("achieved-rps"), 0.0), default=None
        )
        if testing_passed:
            target_stage = "testing"
        elif best_clean:
            target_stage = best_clean["stage"]
        else:
            target_stage = None

        stage_res = extract_stage_resources(by_stage_data, target_stage)

        cap = output_mod.capacity_summary(
            all_stage_rows,
            envoy_cpu=args.envoy_cpu,
            ext_proc_cpu=args.ext_proc_cpu,
            actors=args.actors,
            client_concurrency=args.client_concurrency,
            tail_latency_slo_ms=args.tail_latency_slo_ms,
            stage_resources=stage_res,
        )
        with open(args.out_dir / "capacity.json", "w") as f:
            json.dump(cap, f, indent=2)

        status_obj = {
            "converged": converged,
            "verdict": cap.get("verdict", "fail"),
            "actors_created": len(created_actors),
            "stages_executed": len(all_stage_rows),
            "error": str(run_err) if run_err else None,
        }
        with open(args.out_dir / "status.json", "w") as f:
            json.dump(status_obj, f, indent=2)

        if created_actors:
            log("Cleaning up actors...")
            actors_mod.cleanup(stub, created_actors, atespace=args.atespace, log=log)

        if router_ch is not None:
            try:
                router_ch.close()
            except Exception as e:
                log(f"Error closing router gRPC channel: {e}")

        log_file.close()

    upload_ok = True
    if dest_prefix:
        files = [
            "capacity.json",
            "stats.jsonl",
            "results.json",
            "resources-raw.jsonl",
            "resources-by-stage.json",
            "envoy-stats.json",
            "status.json",
            "logs.txt",
        ]
        for fname in files:
            p = args.out_dir / fname
            if p.exists():
                try:
                    upload_file(p, f"{dest_prefix}/{fname}")
                except Exception as e:
                    print(f"Failed to upload {p}: {e}")
                    upload_ok = False
    else:
        print("Capacity summary:")
        print(json.dumps(cap, indent=2))

    artifact_files = [
        "capacity.json",
        "stats.jsonl",
        "resources-by-stage.json",
        "envoy-stats.json",
        "status.json",
    ]
    for fname in artifact_files:
        p = args.out_dir / fname
        if p.exists():
            encoded = base64.b64encode(p.read_bytes()).decode("ascii")
            print(f"===ARTIFACT:{fname}:{encoded}===", flush=True)

    passed_verdict = cap.get("verdict") == "pass"
    return 0 if (converged and passed_verdict and upload_ok and run_err is None) else 1


if __name__ == "__main__":
    sys.exit(main())
