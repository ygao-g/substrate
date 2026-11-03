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

"""Unit tests for the nighthawk-egress runner and adaptive step controller."""

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

for mod_name in [
    "common",
    "common.ateapi_pb2",
    "common.ateapi_pb2_grpc",
    "common.ateapi_channel",
]:
    if mod_name not in sys.modules:
        sys.modules[mod_name] = MagicMock()

sys.modules["common.ateapi_channel"].CA_FILE = "/run/servicedns-ca/ca.crt"
sys.modules["common.ateapi_channel"].CRED_BUNDLE = (
    "/run/podidentity.podcert.ate.dev/credential-bundle.pem"
)
sys.modules["common.ateapi_channel"].SERVER_NAME = "api.ate-system.svc"
sys.modules["common.ateapi_channel"]._split_cred_bundle = MagicMock(
    return_value=(b"key", b"cert")
)
sys.modules["common.ateapi_channel"].ateapi_channel = MagicMock()

import runner as runner_mod


def test_uses_initial_rps():
    controller = runner_mod.ExponentialSearchStepController(initial_rps=500.0)
    assert controller.next_rps() == 500.0
    assert not controller.converged
    assert not controller.is_doomed()


def test_increases_rps_exponentially_if_within_threshold():
    controller = runner_mod.ExponentialSearchStepController(initial_rps=100.0, exponential_factor=2.0)
    controller.record_result(True)
    assert controller.next_rps() == 200.0
    controller.record_result(True)
    assert controller.next_rps() == 400.0


def test_stage0_failure_dooms_immediately():
    controller = runner_mod.ExponentialSearchStepController(initial_rps=1000.0)
    controller.record_result(False)
    assert controller.is_doomed() is True
    assert "Stage 0 failed thresholds" in controller.doom_reason
    assert controller.best_clean_rps == 0.0


def test_performs_binary_search_after_exceeding_threshold():
    controller = runner_mod.ExponentialSearchStepController(initial_rps=100.0, exponential_factor=2.0)
    controller.record_result(True)   # Next: 200.0
    controller.record_result(False)  # Fails at 200.0 -> binary search between 100.0 and 200.0
    assert controller.next_rps() == 150.0
    assert not controller.is_doomed()


def test_binary_search_converges_after_many_steps():
    controller = runner_mod.ExponentialSearchStepController(initial_rps=100.0, exponential_factor=2.0)
    controller.record_result(True)
    controller.record_result(False)
    for _ in range(100):
        if controller.converged:
            break
        controller.record_result(False)
    assert controller.converged is True
    assert controller.is_converged() is True
    # Converged to bottom (100.0)
    assert abs(controller.best_clean_rps - 100.0) < 1.0


def test_binary_search_finds_midpoint():
    controller = runner_mod.ExponentialSearchStepController(initial_rps=100.0, exponential_factor=2.0)
    controller.record_result(True)   # 200
    controller.record_result(False)  # 150
    controller.record_result(True)   # pass at 150 -> range is [150, 200], next 175
    # Subsequent tests fail, pushing back toward 150
    for _ in range(100):
        if controller.converged:
            break
        controller.record_result(False)
    assert controller.converged is True
    assert abs(controller.best_clean_rps - 150.0) < 1.5


def test_exponential_step_controller_cpp_step_by_step_parity():
    # Exact reproduction of C++ test/adaptive_load/step_controller_test.cc:
    # 1. IncreasesRpsExponentiallyIfWithinThresholdUsingCustomExponent (factor=1.5)
    c1 = runner_mod.ExponentialSearchStepController(initial_rps=100.0, exponential_factor=1.5)
    assert not c1.is_doomed()
    assert not c1.is_converged()
    c1.record_result(True)
    assert c1.next_rps() == 150.0

    # 2. PerformsBinarySearchAfterExceedingThreshold
    c2 = runner_mod.ExponentialSearchStepController(initial_rps=100.0, exponential_factor=2.0)
    c2.record_result(True)  # within threshold -> 200.0
    assert c2.next_rps() == 200.0
    c2.record_result(False) # exceeds threshold -> midpoint 150.0
    assert c2.next_rps() == 150.0
    assert not c2.is_converged()

    # 3. BinarySearchFindsBottomOfRange
    for _ in range(100):
        if c2.is_converged():
            break
        c2.record_result(False)
    assert c2.is_converged() is True
    assert c2.best_clean_rps == 100.0

    # 4. Range-finding -> binary transition convergence with factor=1.01 (abs(100.5/101.0 - 1.0) < 0.01)
    c3 = runner_mod.ExponentialSearchStepController(initial_rps=100.0, exponential_factor=1.01)
    c3.record_result(True)  # next is 101.0
    assert abs(c3.next_rps() - 101.0) < 1e-6
    c3.record_result(False) # binary midpoint is 100.5 -> within 1% of 101.0
    assert c3.is_converged() is True


def test_parse_args_custom_and_defaults():
    args = runner_mod.parse_args([
        "--atespace", "custom-bench",
        "--ext-proc-cpu", "4",
        "--envoy-cpu", "2",
        "--connections", "64",
        "--dest", "gs://my-bucket/runs",
        "--run-tag", "tag123",
        "--test-name", "my-test",
    ])
    assert args.atespace == "custom-bench"
    assert args.ext_proc_cpu == 4
    assert args.envoy_cpu == 2
    assert args.connections == 64
    assert args.tail_latency_slo_ms == 25.0
    assert args.dest == "gs://my-bucket/runs"

    # Default connections: 32 for keepalive, 250 for churn
    args_keepalive = runner_mod.parse_args(["--conn-mode", "keepalive"])
    assert args_keepalive.connections == 32
    assert args_keepalive.simple_warmup is True

    args_churn = runner_mod.parse_args(["--conn-mode", "churn"])
    assert args_churn.connections == 250
    assert args_churn.simple_warmup is False


def test_evaluate_stage_thresholds():
    row = {
        "metric_nighthawk.builtin_success_rate": 0.999,
        "metric_nighthawk.builtin_send_rate": 0.95,
        "latency_mean_plus_2stdev_ms": 20.0,
    }
    assert runner_mod.evaluate_stage_thresholds(row, 0.998, 0.90, 25.0) is True
    assert row["failed_thresholds"] == []

    # Latency exceeds 25ms SLO
    row_high_lat = {
        "metric_nighthawk.builtin_success_rate": 0.999,
        "metric_nighthawk.builtin_send_rate": 0.95,
        "latency_mean_plus_2stdev_ms": 30.0,
    }
    assert runner_mod.evaluate_stage_thresholds(row_high_lat, 0.998, 0.90, 25.0) is False
    assert "latency-ns-mean-plus-2stdev" in row_high_lat["failed_thresholds"]


def test_main_execution_stream_success(tmp_path, monkeypatch):
    import actors as actors_mod

    mock_stub = MagicMock()
    monkeypatch.setattr(actors_mod, "ateapi_stub", lambda auth_mode=None: mock_stub)
    monkeypatch.setattr(actors_mod, "create_and_warm", lambda stub, ch, count, created, **kwargs: ["actor-001", "actor-002"])
    monkeypatch.setattr(actors_mod, "cleanup", lambda stub, actors, **kwargs: None)

    mock_channel = MagicMock()
    monkeypatch.setattr(actors_mod, "router_grpc_channel", lambda host=None, options=None: mock_channel)

    # Mock ExecutionStream response
    def fake_run_stage(router_ch, actor_name, exec_req, **kwargs):
        return {
            "results": [
                {
                    "name": "global",
                    "execution_duration": "20s",
                    "counters": [
                        {"name": "upstream_rq_total", "value": 10000},
                        {"name": "benchmark.http_2xx", "value": 10000},
                    ],
                    "statistics": [
                        {
                            "id": "benchmark_http_client.request_to_response",
                            "mean": "0.005s",
                            "pstdev": "0.001s",
                            "percentiles": [{"percentile": 0.99, "duration": "0.010s"}],
                        }
                    ],
                }
            ]
        }

    monkeypatch.setattr(runner_mod, "run_stage_on_actor", fake_run_stage)

    mock_k8s = MagicMock()
    monkeypatch.setattr("kubeapi.default_client", lambda: mock_k8s)
    monkeypatch.setattr("envoy_stats.scrape_boundary", lambda k8s, stage, label="start": {"stage": stage, "label": label, "pods": {}})
    monkeypatch.setattr("spec.build_execution_request", lambda opts, desc_path=None: MagicMock())

    out_dir = tmp_path / "out"
    code = runner_mod.main([
        "--actors", "2",
        "--out-dir", str(out_dir),
        "--initial-total-rps", "1000",
        "--convergence-deadline-s", "60",
        "--no-sample-resources",
    ])
    assert code == 0
    assert (out_dir / "capacity.json").exists()
    cap = json.loads((out_dir / "capacity.json").read_text())
    assert cap["verdict"] == "pass"
    assert cap["testing_passed"] is True
    assert cap["converged"] is True


def test_main_marginal_testing_stage_still_exits_zero(tmp_path, monkeypatch):
    import actors as actors_mod

    mock_stub = MagicMock()
    monkeypatch.setattr(actors_mod, "ateapi_stub", lambda auth_mode=None: mock_stub)
    monkeypatch.setattr(actors_mod, "create_and_warm", lambda stub, ch, count, created, **kwargs: ["actor-001"])
    monkeypatch.setattr(actors_mod, "cleanup", lambda stub, actors, **kwargs: None)
    monkeypatch.setattr(actors_mod, "router_grpc_channel", lambda host=None, options=None: MagicMock())

    stage_calls = 0

    def fake_run_stage(router_ch, actor_name, exec_req, **kwargs):
        nonlocal stage_calls
        stage_calls += 1
        latency = "0.005s" if stage_calls == 1 else "0.030s"
        dur_s = 60 if stage_calls > 2 else 20
        rq = 1000 * dur_s
        return {
            "results": [
                {
                    "name": "global",
                    "execution_duration": f"{dur_s}s",
                    "counters": [
                        {"name": "upstream_rq_total", "value": rq},
                        {"name": "benchmark.http_2xx", "value": rq},
                    ],
                    "statistics": [
                        {
                            "id": "benchmark_http_client.request_to_response",
                            "mean": latency,
                            "pstdev": "0.001s",
                            "percentiles": [{"percentile": 0.99, "duration": latency}],
                        }
                    ],
                }
            ]
        }

    monkeypatch.setattr(runner_mod, "run_stage_on_actor", fake_run_stage)
    monkeypatch.setattr("kubeapi.default_client", lambda: MagicMock())
    monkeypatch.setattr("envoy_stats.scrape_boundary", lambda k8s, stage, label="start": {"stage": stage, "label": label, "pods": {}})
    monkeypatch.setattr("spec.build_execution_request", lambda opts, desc_path=None: MagicMock())

    out_dir = tmp_path / "out_marginal"
    code = runner_mod.main([
        "--actors", "1",
        "--out-dir", str(out_dir),
        "--initial-total-rps", "1000",
        "--exponential-factor", "1.01",
        "--tail-latency-slo-ms", "25.0",
        "--no-sample-resources",
    ])
    assert code == 0
    cap = json.loads((out_dir / "capacity.json").read_text())
    assert cap["verdict"] == "pass"
    assert cap["converged"] is True
    assert cap["testing_passed"] is False
    assert cap["slo_max_rps"] > 0


def test_main_stage0_doomed_aborts(tmp_path, monkeypatch):
    import actors as actors_mod

    mock_stub = MagicMock()
    monkeypatch.setattr(actors_mod, "ateapi_stub", lambda auth_mode=None: mock_stub)
    monkeypatch.setattr(actors_mod, "create_and_warm", lambda stub, ch, count, created, **kwargs: ["actor-001"])
    monkeypatch.setattr(actors_mod, "cleanup", lambda stub, actors, **kwargs: None)
    monkeypatch.setattr(actors_mod, "router_grpc_channel", lambda host=None, options=None: MagicMock())

    # Simulate Stage 0 failure (high latency)
    def fake_run_stage_fail(router_ch, actor_name, exec_req, **kwargs):
        return {
            "results": [
                {
                    "name": "global",
                    "execution_duration": "20s",
                    "counters": [
                        {"name": "upstream_rq_total", "value": 1000},
                        {"name": "benchmark.http_2xx", "value": 1000},
                    ],
                    "statistics": [
                        {
                            "id": "benchmark_http_client.request_to_response",
                            "mean": "0.050s",  # 50ms exceeds 25ms SLO
                            "pstdev": "0.010s",
                        }
                    ],
                }
            ]
        }

    monkeypatch.setattr(runner_mod, "run_stage_on_actor", fake_run_stage_fail)
    monkeypatch.setattr("kubeapi.default_client", lambda: MagicMock())
    monkeypatch.setattr("envoy_stats.scrape_boundary", lambda k8s, stage, label="start": {"stage": stage, "label": label, "pods": {}})

    out_dir = tmp_path / "out_doomed"
    code = runner_mod.main([
        "--actors", "1",
        "--out-dir", str(out_dir),
        "--initial-total-rps", "1000",
        "--tail-latency-slo-ms", "25.0",
        "--no-sample-resources",
    ])
    assert code == 1
    cap = json.loads((out_dir / "capacity.json").read_text())
    assert cap["verdict"] == "fail"
    assert cap["converged"] is False


def _make_runner_test_descriptor(path: Path) -> Path:
    from google.protobuf import descriptor_pb2

    fds = descriptor_pb2.FileDescriptorSet()
    file_proto = fds.file.add()
    file_proto.name = "nighthawk/client.proto"
    file_proto.package = "nighthawk.client"
    file_proto.syntax = "proto3"

    msg_err = file_proto.message_type.add()
    msg_err.name = "ErrorDetail"
    f_code = msg_err.field.add()
    f_code.name = "code"
    f_code.number = 1
    f_code.label = descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
    f_code.type = descriptor_pb2.FieldDescriptorProto.TYPE_INT32
    f_msg = msg_err.field.add()
    f_msg.name = "message"
    f_msg.number = 2
    f_msg.label = descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
    f_msg.type = descriptor_pb2.FieldDescriptorProto.TYPE_STRING

    msg_out = file_proto.message_type.add()
    msg_out.name = "Output"
    f_title = msg_out.field.add()
    f_title.name = "title"
    f_title.number = 1
    f_title.label = descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
    f_title.type = descriptor_pb2.FieldDescriptorProto.TYPE_STRING

    msg_resp = file_proto.message_type.add()
    msg_resp.name = "ExecutionResponse"
    f_err = msg_resp.field.add()
    f_err.name = "error_detail"
    f_err.number = 1
    f_err.label = descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
    f_err.type = descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE
    f_err.type_name = ".nighthawk.client.ErrorDetail"
    f_out = msg_resp.field.add()
    f_out.name = "output"
    f_out.number = 2
    f_out.label = descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
    f_out.type = descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE
    f_out.type_name = ".nighthawk.client.Output"

    desc_file = path / "nighthawk.desc"
    desc_file.write_bytes(fds.SerializeToString())
    return desc_file


def test_run_stage_on_actor_missing_descriptor_raises(tmp_path):
    import pytest
    missing_desc = tmp_path / "nonexistent.desc"
    mock_ch = MagicMock()
    with pytest.raises(FileNotFoundError, match="descriptor set not found"):
        runner_mod.run_stage_on_actor(
            mock_ch, "actor-1", MagicMock(), desc_path=str(missing_desc)
        )


def test_run_stage_on_actor_with_descriptor(tmp_path):
    import pytest
    import spec as spec_mod

    desc_file = _make_runner_test_descriptor(tmp_path)
    pool = spec_mod.load_pool(str(desc_file))
    resp_cls = spec_mod.message_class(pool, "nighthawk.client.ExecutionResponse")

    # 1. Non-zero error_detail in proto response raises RuntimeError
    err_resp = resp_cls()
    err_resp.error_detail.code = 2
    err_resp.error_detail.message = "bad options"
    mock_ch1 = MagicMock()
    mock_ch1.stream_stream.return_value = lambda *args, **kwargs: [err_resp]
    with pytest.raises(RuntimeError, match="actor actor-1 ExecutionStream error 2: bad options"):
        runner_mod.run_stage_on_actor(
            mock_ch1, "actor-1", MagicMock(), desc_path=str(desc_file)
        )

    # 2. Valid output proto response returns parsed dict
    ok_resp = resp_cls()
    ok_resp.output.title = "bench-success"
    mock_ch2 = MagicMock()
    mock_ch2.stream_stream.return_value = lambda *args, **kwargs: [ok_resp]
    out = runner_mod.run_stage_on_actor(
        mock_ch2, "actor-1", MagicMock(), desc_path=str(desc_file)
    )
    assert out == {"title": "bench-success"}


def test_main_router_ch_closed_in_finally(tmp_path, monkeypatch):
    import actors as actors_mod

    mock_stub = MagicMock()
    monkeypatch.setattr(actors_mod, "ateapi_stub", lambda auth_mode=None: mock_stub)
    monkeypatch.setattr(actors_mod, "create_and_warm", lambda stub, ch, count, created, **kwargs: ["actor-001"])
    monkeypatch.setattr(actors_mod, "cleanup", lambda stub, actors, **kwargs: None)

    mock_channel = MagicMock()
    monkeypatch.setattr(actors_mod, "router_grpc_channel", lambda host=None, options=None: mock_channel)

    monkeypatch.setattr(runner_mod, "run_stage_on_actor", lambda ch, name, req, **kwargs: {"results": []})
    monkeypatch.setattr("kubeapi.default_client", lambda: MagicMock())
    monkeypatch.setattr("envoy_stats.scrape_boundary", lambda k8s, stage, label="start": {"stage": stage, "label": label, "pods": {}})

    out_dir = tmp_path / "out_close"
    runner_mod.main([
        "--actors", "1",
        "--out-dir", str(out_dir),
        "--initial-total-rps", "1000",
        "--no-sample-resources",
    ])
    mock_channel.close.assert_called_once()
