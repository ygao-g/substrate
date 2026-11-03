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

"""Unit tests for the Nighthawk egress spec builder and CommandLineOptions."""

import sys
from pathlib import Path

from google.protobuf import descriptor_pb2
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import spec as spec_mod


def _make_test_descriptor(path: Path) -> Path:
    fds = descriptor_pb2.FileDescriptorSet()
    file_proto = fds.file.add()
    file_proto.name = "nighthawk/client.proto"
    file_proto.package = "nighthawk.client"
    file_proto.syntax = "proto3"

    msg_clo = file_proto.message_type.add()
    msg_clo.name = "CommandLineOptions"
    f_rps = msg_clo.field.add()
    f_rps.name = "requests_per_second"
    f_rps.number = 1
    f_rps.label = descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
    f_rps.type = descriptor_pb2.FieldDescriptorProto.TYPE_UINT32

    f_uri = msg_clo.field.add()
    f_uri.name = "uri"
    f_uri.number = 2
    f_uri.label = descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
    f_uri.type = descriptor_pb2.FieldDescriptorProto.TYPE_STRING

    msg_sr = file_proto.message_type.add()
    msg_sr.name = "StartRequest"
    f_opt = msg_sr.field.add()
    f_opt.name = "options"
    f_opt.number = 1
    f_opt.label = descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
    f_opt.type = descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE
    f_opt.type_name = ".nighthawk.client.CommandLineOptions"

    msg_er = file_proto.message_type.add()
    msg_er.name = "ExecutionRequest"
    f_sr = msg_er.field.add()
    f_sr.name = "start_request"
    f_sr.number = 1
    f_sr.label = descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
    f_sr.type = descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE
    f_sr.type_name = ".nighthawk.client.StartRequest"

    desc_file = path / "nighthawk.desc"
    desc_file.write_bytes(fds.SerializeToString())
    return desc_file


def test_command_line_options_http_plaintext_defaults():
    opts = spec_mod.build_command_line_options_dict(
        mode="http",
        conn_mode="keepalive",
        concurrency=4,
        rps=2000,
        duration_s=20,
    )
    assert opts["uri"] == "http://nighthawk-server.nighthawk.svc:80/"
    assert opts["requests_per_second"] == 2000
    assert opts["duration"] == "20s"
    assert opts["concurrency"] == "4"
    assert opts["connections"] == 32
    assert opts["open_loop"] is True
    assert opts["h2"] is False
    assert opts["sequencer_idle_strategy"] == {"value": "POLL"}
    assert opts["no_default_failure_predicates"] is True
    assert opts["simple_warmup"] is True
    assert "max_requests_per_connection" not in opts
    assert "transport_socket" not in opts


def test_command_line_options_https_mitm_transport_socket():
    opts = spec_mod.build_command_line_options_dict(
        mode="https-mitm",
        conn_mode="keepalive",
    )
    assert opts["uri"] == "https://nighthawk-server.nighthawk.svc:443/"
    ts = opts.get("transport_socket")
    assert ts is not None
    assert ts["name"] == "envoy.transport_sockets.tls"
    cfg = ts["typed_config"]
    assert cfg["sni"] == "nighthawk-server.nighthawk.svc"
    assert cfg["common_tls_context"]["alpn_protocols"] == ["http/1.1"]
    val = cfg["common_tls_context"]["validation_context"]
    assert val["trusted_ca"]["filename"] == "/run/ate/egress-trust-bundle/trust-bundle.pem"


def test_command_line_options_churn_mode():
    opts = spec_mod.build_command_line_options_dict(
        mode="https-mitm",
        conn_mode="churn",
    )
    assert opts["connections"] == 250
    assert opts["max_requests_per_connection"] == 1
    assert "simple_warmup" not in opts or opts.get("simple_warmup") is False


def test_command_line_options_custom_connections():
    opts = spec_mod.build_command_line_options_dict(
        mode="http",
        conn_mode="keepalive",
        connections=100,
    )
    assert opts["connections"] == 100


def test_invalid_mode_or_conn_mode():
    with pytest.raises(ValueError, match="mode"):
        spec_mod.build_command_line_options_dict(mode="invalid")
    with pytest.raises(ValueError, match="conn_mode"):
        spec_mod.build_command_line_options_dict(conn_mode="invalid")


def test_build_execution_request_missing_descriptor_raises(tmp_path):
    missing_desc = tmp_path / "nonexistent.desc"
    with pytest.raises(FileNotFoundError, match="descriptor file not found"):
        spec_mod.build_execution_request({"requests_per_second": 1000}, desc_path=str(missing_desc))


def test_build_execution_request_with_valid_descriptor(tmp_path):
    desc_path = _make_test_descriptor(tmp_path)
    opts = {"requests_per_second": 1000, "uri": "http://example.com:80/"}
    req = spec_mod.build_execution_request(opts, desc_path=str(desc_path))
    assert req is not None
    assert req.start_request.options.requests_per_second == 1000
    assert req.start_request.options.uri == "http://example.com:80/"
