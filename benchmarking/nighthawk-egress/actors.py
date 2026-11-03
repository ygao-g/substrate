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

"""Actor fleet lifecycle for nighthawk-egress benchmark."""

import concurrent.futures
import time

import grpc

from common import ateapi_pb2
from common import ateapi_pb2_grpc
from common.ateapi_channel import (
    CA_FILE,
    CRED_BUNDLE,
    SERVER_NAME,
    _split_cred_bundle,
    ateapi_channel,
)

ATEAPI_HOST = "api.ate-system.svc.cluster.local:443"
ROUTER_HOST = "atenet-router.ate-system.svc:443"
TOKEN_FILE = "/run/ateapi-token/token"

ATESPACE = "benchmark-workloads"
TEMPLATE_ATESPACE = "benchmark-workloads"
TEMPLATE_NAME = "traffic-driver"


def _token_channel(host: str) -> grpc.Channel:
    with open(CA_FILE, "rb") as f:
        ca_cert = f.read()
    with open(TOKEN_FILE) as f:
        token = f.read().strip()
    creds = grpc.composite_channel_credentials(
        grpc.ssl_channel_credentials(root_certificates=ca_cert),
        grpc.access_token_call_credentials(token),
    )
    return grpc.secure_channel(
        host, creds, options=[("grpc.ssl_target_name_override", SERVER_NAME)]
    )


def ateapi_stub(auth_mode: str = "cert"):
    if auth_mode == "token":
        channel = _token_channel(ATEAPI_HOST)
    else:
        channel = ateapi_channel(ATEAPI_HOST)
    return ateapi_pb2_grpc.ControlStub(channel)


def router_grpc_channel(
    router_host: str = ROUTER_HOST,
    options=None,
) -> grpc.Channel:
    """Open an authenticated mTLS gRPC channel to atenet-router."""
    target = router_host.replace("http://", "").replace("https://", "")
    with open(CA_FILE, "rb") as f:
        ca_cert = f.read()
    with open(CRED_BUNDLE, "rb") as f:
        private_key, cert_chain = _split_cred_bundle(f.read())

    creds = grpc.ssl_channel_credentials(
        root_certificates=ca_cert,
        private_key=private_key,
        certificate_chain=cert_chain,
    )
    channel_options = [("grpc.ssl_target_name_override", "atenet-router.ate-system.svc")]
    if options:
        channel_options.extend(options)
    return grpc.secure_channel(target, creds, options=channel_options)


def ensure_atespace(stub, name: str = ATESPACE) -> None:
    try:
        stub.CreateAtespace(
            ateapi_pb2.CreateAtespaceRequest(
                atespace=ateapi_pb2.Atespace(
                    metadata=ateapi_pb2.ResourceMetadata(name=name)
                )
            )
        )
    except grpc.RpcError as e:
        if e.code() != grpc.StatusCode.ALREADY_EXISTS:
            raise


def ensure_egress_policy(stub, actor_name: str, atespace: str = ATESPACE) -> None:
    req = ateapi_pb2.CreateActorEgressPolicyRequest(
        actor=ateapi_pb2.ObjectRef(name=actor_name, atespace=atespace),
        egress_policy=ateapi_pb2.EgressPolicy(
            metadata=ateapi_pb2.ResourceMetadata(name="default", atespace=atespace),
            rules=[
                ateapi_pb2.EgressRule(
                    http=ateapi_pb2.HttpRule(
                        hostnames=["*"],
                        ports=ateapi_pb2.Ports(numbers=[80]),
                    )
                ),
                ateapi_pb2.EgressRule(
                    https=ateapi_pb2.HttpsRule(
                        hostnames=["*"],
                        ports=ateapi_pb2.Ports(numbers=[443]),
                    )
                ),
            ],
        ),
    )
    try:
        stub.CreateActorEgressPolicy(req)
    except grpc.RpcError as e:
        if e.code() != grpc.StatusCode.ALREADY_EXISTS:
            raise


def create_actor(stub, name: str, atespace: str = ATESPACE) -> None:
    try:
        stub.CreateActor(
            ateapi_pb2.CreateActorRequest(
                actor=ateapi_pb2.Actor(
                    metadata=ateapi_pb2.ResourceMetadata(
                        atespace=atespace, name=name
                    ),
                    actor_template=ateapi_pb2.ObjectRef(
                        atespace=TEMPLATE_ATESPACE,
                        name=TEMPLATE_NAME,
                    ),
                )
            )
        )
    except grpc.RpcError as e:
        if e.code() != grpc.StatusCode.ALREADY_EXISTS:
            raise
    ensure_egress_policy(stub, name, atespace)


def _warm_actor(
    stub,
    router_channel_or_stub,
    name: str,
    atespace: str,
    deadline: float,
    log,
) -> None:
    ref = ateapi_pb2.ObjectRef(atespace=atespace, name=name)
    if isinstance(router_channel_or_stub, grpc.Channel):
        stream_rpc = router_channel_or_stub.stream_stream(
            "/nighthawk.client.NighthawkService/ExecutionStream",
            request_serializer=lambda req: req.SerializeToString() if hasattr(req, "SerializeToString") else (req if isinstance(req, bytes) else b""),
            response_deserializer=lambda b: b,
        )
    else:
        stream_rpc = router_channel_or_stub.ExecutionStream

    last_err: str = "not attempted"
    while time.time() < deadline:
        if stub:
            try:
                stub.ResumeActor(ateapi_pb2.ResumeActorRequest(actor=ref))
            except grpc.RpcError as e:
                last_err = f"ResumeActor: {e.code().name}"
        try:
            call = stream_rpc(
                iter(()),
                metadata=[("ate-target-actor", f"{atespace}/{name}")],
                timeout=30.0,
            )
            _ = list(call)
            if call.code() is None or call.code() == grpc.StatusCode.OK:
                return
            last_err = f"ExecutionStream -> {call.code().name}: {call.details()}"
        except grpc.RpcError as e:
            last_err = f"ExecutionStream: {e.code().name}: {e.details()}"
        except Exception as e:
            last_err = f"ExecutionStream: {e}"
        time.sleep(2)
    raise RuntimeError(f"actor {name} not warm by deadline ({last_err})")


def create_and_warm(
    stub,
    router_target,
    count: int,
    created: list[str],
    atespace: str = ATESPACE,
    warm_deadline_s: int = 600,
    parallelism: int = 16,
    log=print,
) -> list[str]:
    ensure_atespace(stub, atespace)
    names = [f"nh-egress-{i:03d}" for i in range(count)]
    for name in names:
        create_actor(stub, name, atespace)
        created.append(name)
    log(f"Created {count} actors from {TEMPLATE_ATESPACE}/{TEMPLATE_NAME}")

    if isinstance(router_target, (str, bytes)):
        router_ch = router_grpc_channel(str(router_target))
    else:
        router_ch = router_target

    deadline = time.time() + warm_deadline_s
    failures: list[str] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=parallelism) as ex:
        futures = {
            ex.submit(
                _warm_actor, stub, router_ch, name, atespace, deadline, log
            ): name
            for name in names
        }
        warmed = 0
        for fut in concurrent.futures.as_completed(futures):
            name = futures[fut]
            try:
                fut.result()
                warmed += 1
                if warmed % 10 == 0 or warmed == count:
                    log(f"Warmed {warmed}/{count} actors")
            except Exception as e:
                failures.append(name)
                log(f"Warm failed: {e}")
    if failures:
        raise RuntimeError(
            f"{len(failures)}/{count} actors failed to warm "
            f"within {warm_deadline_s}s: {failures[:5]}"
        )
    return names


def cleanup(stub, names: list[str], atespace: str = ATESPACE, log=print) -> None:
    """Best-effort suspend and delete in separate try/except blocks."""
    for name in names:
        ref = ateapi_pb2.ObjectRef(atespace=atespace, name=name) if ateapi_pb2 else None
        if stub and ref:
            try:
                stub.SuspendActor(ateapi_pb2.SuspendActorRequest(actor=ref))
            except Exception as e:
                log(f"SuspendActor({name}) failed: {e}")
            try:
                stub.DeleteActor(ateapi_pb2.DeleteActorRequest(actor=ref))
            except Exception as e:
                log(f"DeleteActor({name}) failed: {e}")
