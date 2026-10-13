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

"""Unit tests for the actor fleet helpers: template, name prefix, probe.
Runs anywhere (`python3 tests/test_actors.py` or pytest); see
ateapi_fakes.py for how actors.py imports without its runtime deps."""

import sys
from pathlib import Path
from unittest import mock

import ateapi_fakes

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
ateapi_fakes.install()

import actors  # noqa: E402
import requests  # noqa: E402


class FakeStub:
    def __init__(self):
        self.atespaces = []
        self.created = []
        self.resumed = []

    def CreateAtespace(self, req):
        self.atespaces.append(req.atespace.metadata.name)

    def CreateActor(self, req):
        self.created.append(req.actor)

    def ResumeActor(self, req):
        self.resumed.append(req.actor)


class FakeClock:
    """Replaces actors.time so warm deadlines pass without waiting."""

    def __init__(self):
        self.now = 0.0

    def time(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


class FakeResponse:
    def __init__(self, status_code: int):
        self.status_code = status_code


class FakeSession:
    def __init__(self, result):
        self.result = result
        self.calls = []

    def post(self, url, **kwargs):
        self.calls.append((url, kwargs))
        if isinstance(self.result, Exception):
            raise self.result
        return self.result


def warm_probe(session, router_url, atespace, name):
    return None


def test_create_and_warm_uses_template_prefix_and_probe():
    stub = FakeStub()
    probes = []
    logs = []

    def probe(session, router_url, atespace, name):
        probes.append((router_url, atespace, name))
        return None

    created = []
    names = actors.create_and_warm(
        stub,
        "http://router",
        3,
        created,
        atespace="eg",
        parallelism=2,
        log=logs.append,
        template=("tpl-ns", "tpl"),
        name_prefix="eg-",
        probe=probe,
    )
    assert names == ["eg-000", "eg-001", "eg-002"]
    assert created == names
    assert stub.atespaces == ["eg"]
    assert [(a.metadata.atespace, a.metadata.name) for a in stub.created] == [
        ("eg", name) for name in names
    ]
    assert {
        (a.actor_template.atespace, a.actor_template.name) for a in stub.created
    } == {("tpl-ns", "tpl")}
    assert sorted(probes) == [("http://router", "eg", name) for name in names]
    assert "Created 3 actors from tpl-ns/tpl" in logs


def test_create_and_warm_defaults_to_glutton_and_nh_prefix():
    stub = FakeStub()
    logs = []
    names = actors.create_and_warm(
        stub, "http://router", 2, [], probe=warm_probe, log=logs.append
    )
    assert names == ["nh-000", "nh-001"]
    assert {
        (a.actor_template.atespace, a.actor_template.name) for a in stub.created
    } == {("benchmark-workloads", "glutton")}
    assert "Created 2 actors from benchmark-workloads/glutton" in logs


def test_warm_retries_until_the_probe_answers():
    stub = FakeStub()
    answers = iter(["POST /ping -> 503", None])
    with mock.patch.object(actors, "time", FakeClock()):
        names = actors.create_and_warm(
            stub, "http://router", 1, [], probe=lambda *a: next(answers)
        )
    assert names == ["nh-000"]
    assert len(stub.resumed) == 2


def test_warm_failure_reports_the_last_probe_reason():
    stub = FakeStub()
    logs = []
    with mock.patch.object(actors, "time", FakeClock()):
        try:
            actors.create_and_warm(
                stub,
                "http://router",
                1,
                [],
                warm_deadline_s=10,
                probe=lambda *a: "GET /readyz -> 502",
                log=logs.append,
            )
        except RuntimeError as e:
            assert "1/1 actors failed to warm" in str(e)
        else:
            raise AssertionError("create_and_warm did not raise")
    assert (
        "Warm failed: actor nh-000 not warm by deadline (GET /readyz -> 502)"
        in logs
    )


def test_ping_probe():
    session = FakeSession(FakeResponse(200))
    assert actors.ping_probe(session, "http://router/", "ns", "nh-000") is None
    url, kwargs = session.calls[0]
    assert url == "http://router/ping"
    assert kwargs["headers"] == {"ate-target-actor": "ns/nh-000"}

    session = FakeSession(FakeResponse(503))
    assert (
        actors.ping_probe(session, "http://router", "ns", "nh-000")
        == "POST /ping -> 503"
    )

    session = FakeSession(requests.RequestException("refused"))
    assert (
        actors.ping_probe(session, "http://router", "ns", "nh-000")
        == "POST /ping: refused"
    )


if __name__ == "__main__":
    for fn_name, fn in sorted(dict(globals()).items()):
        if fn_name.startswith("test_") and callable(fn):
            fn()
            print(f"PASS {fn_name}")
