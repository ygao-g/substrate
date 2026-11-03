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

"""Specification builder for Nighthawk egress capacity benchmarks.

Builds nighthawk.client.CommandLineOptions dicts and ExecutionRequest protos
driven directly over NighthawkService.ExecutionStream against stock
traffic-driver actors.
"""

import os

EXECUTION_REQUEST_MESSAGE = "nighthawk.client.ExecutionRequest"
COMMAND_LINE_OPTIONS_MESSAGE = "nighthawk.client.CommandLineOptions"

SERVER_HOST = "nighthawk-server.nighthawk.svc"
TRUST_BUNDLE_PATH = "/run/ate/egress-trust-bundle/trust-bundle.pem"

_POOL_CACHE: dict = {}


def target_uri(mode: str) -> str:
    if mode == "http":
        return f"http://{SERVER_HOST}:80/"
    elif mode == "https-mitm":
        return f"https://{SERVER_HOST}:443/"
    else:
        raise ValueError(f"unknown mode {mode!r}; want 'http' or 'https-mitm'")


def _tls_transport_socket() -> dict:
    return {
        "name": "envoy.transport_sockets.tls",
        "typed_config": {
            "@type": "type.googleapis.com/envoy.extensions.transport_sockets.tls.v3.UpstreamTlsContext",
            "sni": SERVER_HOST,
            "common_tls_context": {
                "alpn_protocols": ["http/1.1"],
                "validation_context": {
                    "trusted_ca": {"filename": TRUST_BUNDLE_PATH},
                    "match_typed_subject_alt_names": [
                        {
                            "san_type": "DNS",
                            "matcher": {"exact": SERVER_HOST},
                        },
                    ],
                },
            },
        },
    }


def build_command_line_options_dict(
    *,
    mode: str = "http",
    conn_mode: str = "keepalive",
    concurrency: int = 4,
    connections: int | None = None,
    max_pending_requests: int = 1000,
    rps: int = 1000,
    duration_s: int = 20,
    simple_warmup: bool | None = None,
) -> dict:
    """Build a nighthawk.client.CommandLineOptions message dict for one stage."""
    uri = target_uri(mode)
    if conn_mode not in ("keepalive", "churn"):
        raise ValueError(f"unknown conn_mode {conn_mode!r}; want 'keepalive' or 'churn'")

    if connections is None:
        connections = 32 if conn_mode == "keepalive" else 250

    if simple_warmup is None:
        simple_warmup = (conn_mode == "keepalive")

    opts: dict = {
        "requests_per_second": rps,
        "duration": f"{duration_s}s",
        "concurrency": str(concurrency),
        "connections": connections,
        "max_pending_requests": max_pending_requests,
        "open_loop": True,
        "h2": False,
        "sequencer_idle_strategy": {"value": "POLL"},
        "no_default_failure_predicates": True,
        "uri": uri,
    }

    if simple_warmup:
        opts["simple_warmup"] = True

    if conn_mode == "churn":
        opts["max_requests_per_connection"] = 1

    if mode == "https-mitm":
        opts["transport_socket"] = _tls_transport_socket()

    return opts


def load_pool(desc_path: str):
    if desc_path in _POOL_CACHE:
        return _POOL_CACHE[desc_path]
    from google.protobuf import descriptor_pb2
    from google.protobuf import descriptor_pool

    pool = descriptor_pool.DescriptorPool()
    fds = descriptor_pb2.FileDescriptorSet()
    with open(desc_path, "rb") as f:
        fds.ParseFromString(f.read())
    for file_proto in fds.file:
        pool.Add(file_proto)
    _POOL_CACHE[desc_path] = pool
    return pool


def message_class(pool, full_name: str):
    from google.protobuf import message_factory

    return message_factory.GetMessageClass(pool.FindMessageTypeByName(full_name))


def build_execution_request(
    options_dict: dict,
    desc_path: str | None = None,
):
    """Build ExecutionRequest proto message from options dict."""
    if desc_path is None:
        desc_path = os.environ.get("NIGHTHAWK_DESC", "/app/nighthawk.desc")

    if not desc_path or not os.path.exists(desc_path):
        raise FileNotFoundError(f"nighthawk descriptor file not found at {desc_path!r}")

    from google.protobuf import json_format

    pool = load_pool(desc_path)
    req_cls = message_class(pool, EXECUTION_REQUEST_MESSAGE)
    req = req_cls()
    json_format.ParseDict(
        {"start_request": {"options": options_dict}},
        req,
        descriptor_pool=pool,
    )
    return req
