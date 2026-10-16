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

"""Import-time stand-ins so actors.py and runner.py import without grpc,
requests, or the generated ateapi modules, none of which the unit tests
exercise. The generated modules are always replaced; grpc and requests only
when they are not installed. install() is idempotent, so every test module
that needs the stand-ins can call it."""

import importlib.util
import sys
import types


class Msg:
    """Keyword-constructed stand-in for a protobuf message."""

    def __init__(self, **fields):
        self.__dict__.update(fields)

    def __eq__(self, other):
        return type(other) is type(self) and self.__dict__ == other.__dict__

    def __repr__(self):
        return f"{type(self).__name__}({self.__dict__})"


def _module(name: str, **attrs) -> types.ModuleType:
    mod = types.ModuleType(name)
    mod.__dict__.update(attrs)
    return mod


def _missing(name: str) -> bool:
    """True when `name` is neither imported nor importable. find_spec
    raises on an already-installed stand-in, hence the sys.modules check."""
    return name not in sys.modules and importlib.util.find_spec(name) is None


def install() -> None:
    if "common.ateapi_pb2" in sys.modules:
        return
    if _missing("grpc"):
        sys.modules["grpc"] = _module(
            "grpc",
            Channel=object,
            RpcError=type("RpcError", (Exception,), {}),
            StatusCode=types.SimpleNamespace(ALREADY_EXISTS="ALREADY_EXISTS"),
        )
    if _missing("requests"):
        sys.modules["requests"] = _module(
            "requests",
            Session=object,
            RequestException=type("RequestException", (Exception,), {}),
        )
    pb2 = _module(
        "common.ateapi_pb2",
        **{
            name: type(name, (Msg,), {})
            for name in (
                "CreateAtespaceRequest",
                "Atespace",
                "ResourceMetadata",
                "CreateActorRequest",
                "Actor",
                "ObjectRef",
                "ResumeActorRequest",
                "DeleteActorRequest",
            )
        },
    )
    sys.modules["common"] = _module("common", ateapi_pb2=pb2)
    sys.modules["common.ateapi_pb2"] = pb2
    sys.modules["common.ateapi_pb2_grpc"] = _module(
        "common.ateapi_pb2_grpc", ControlStub=object
    )
    sys.modules["common.ateapi_channel"] = _module(
        "common.ateapi_channel",
        CA_FILE="",
        SERVER_NAME="",
        ateapi_channel=lambda host: None,
    )
