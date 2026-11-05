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

"""Unit tests for runner.py's exit code and what it uploads, with
atunnel-bench load, the sidecar log fetch and the work directory replaced."""

import io
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import runner as runner_mod
from tests.test_output import REPORTS

ARGS = ["--name", "atunnel_1cpu_1g", "--tag", "abc1234", "--tunnel-cpus", "1", "--tunnel-memory", "1g", "--actors", "10", "--body-bytes", "256"]


@pytest.fixture
def rig(tmp_path, monkeypatch):
    """A fake pod: a shared dir, a work root, a local dest, and a load that
    the test scripts."""
    shared = tmp_path / "shared"
    shared.mkdir()
    monkeypatch.setattr(runner_mod, "SHARED_DIR", shared)
    monkeypatch.setattr(runner_mod, "WORK_ROOT", tmp_path / "work")
    sidecars = []

    def fake_sidecar_logs(work_dir, logs):
        sidecars.append(work_dir)
        (work_dir / "worker.log").write_text("worker\n")

    monkeypatch.setattr(runner_mod, "sidecar_logs", fake_sidecar_logs)

    class Rig:
        dest = tmp_path / "dest"
        shared_dir = shared
        sidecar_calls = sidecars

        def load(self, exit_code=0, results=REPORTS, text="table\n"):
            def fake_run_load(extra, work_dir, logs):
                self.extra = extra
                (work_dir / "results.txt").write_text(text)
                if results is not None:
                    (work_dir / "results.json").write_text(results if isinstance(results, str) else json.dumps(results))
                    (work_dir / "profiles").mkdir()
                    (work_dir / "profiles" / "IngressH1-1000.cpu.pb.gz").write_bytes(b"prof")
                return exit_code

            monkeypatch.setattr(runner_mod, "run_load", fake_run_load)

        def run(self, *flags):
            return runner_mod.main([*ARGS, "--dest", str(self.dest), *flags])

        def uploaded(self):
            runs = list(self.dest.rglob("status.json"))
            assert len(runs) == 1
            run_dir = runs[0].parent
            assert run_dir.name == "run_tag=abc1234" and run_dir.parent.name.startswith("run_ts=")
            return run_dir, json.loads(runs[0].read_text())

    return Rig()


def test_success(rig):
    rig.shared_dir.joinpath("tunnel-deaths.log").write_text("12:00:00 atunnel exited (signal: killed); OOM kills: 1\n")
    rig.load()
    assert rig.run("--scenarios", "IngressH1", "--rungs", "1000") == 0
    assert rig.extra == ["--scenarios", "IngressH1", "--rungs", "1000"]
    run_dir, status = rig.uploaded()
    assert sorted(p.relative_to(run_dir).as_posix() for p in run_dir.rglob("*") if p.is_file()) == [
        "logs.txt",
        "profiles/IngressH1-1000.cpu.pb.gz",
        "results.json",
        "results.txt",
        "stats.jsonl",
        "status.json",
        "tunnel-deaths.log",
        "worker.log",
    ]
    assert status["load_exit_code"] == 0
    assert status["body_bytes"] == 256
    assert status["tunnel_deaths"] == 1 and status["oom_kills"] == 1
    assert "results_error" not in status
    assert (run_dir / "results.txt").read_text() == "table\natunnel died 1 time(s), 1 of them OOM-killed at its 1g limit\n"
    records = [json.loads(line) for line in (run_dir / "stats.jsonl").read_text().splitlines()]
    assert next(r for r in records if r["metric"] == "atunnel_IngressH1_4000")["measurements"]["body_bytes"] == 256
    assert all(r["tag"] == "abc1234" and r["test_name"] == "atunnel_1cpu_1g" for r in records)


def test_load_failure_still_uploads(rig):
    rig.load(exit_code=1, results=None)
    assert rig.run() == 1
    run_dir, status = rig.uploaded()
    assert status["load_exit_code"] == 1
    assert status["scenarios"] == 0
    assert (run_dir / "logs.txt").exists() and (run_dir / "worker.log").exists()
    assert rig.sidecar_calls


def test_load_that_does_not_start(rig, monkeypatch):
    def boom(*a, **kw):
        raise FileNotFoundError("atunnel-bench")

    monkeypatch.setattr(runner_mod, "run_load", boom)
    assert rig.run() == 1
    run_dir, status = rig.uploaded()
    assert status["load_exit_code"] == 1
    assert "atunnel-bench" in (run_dir / "logs.txt").read_text()


@pytest.mark.parametrize("results", [None, "{corrupt", '[{"rungs": [{"verdict": "OK"}]}]'])
def test_unusable_results_is_a_harness_fault(rig, results):
    rig.load(exit_code=0, results=results)
    assert rig.run() == 1
    run_dir, status = rig.uploaded()
    assert status["load_exit_code"] == 0
    assert status["results_error"].startswith("results.json unusable")
    assert not (run_dir / "stats.jsonl").exists()
    assert (run_dir / "logs.txt").exists() and (run_dir / "worker.log").exists()


@pytest.mark.parametrize(
    "host,port,want",
    [
        ("10.0.0.1", "443", "https://10.0.0.1:443/"),
        ("fd00::1", "6443", "https://[fd00::1]:6443/"),
        ("kubernetes.default.svc", "443", "https://kubernetes.default.svc:443/"),
    ],
)
def test_sidecar_logs_url(tmp_path, monkeypatch, host, port, want):
    sa = tmp_path / "sa"
    sa.mkdir()
    (sa / "token").write_text("tok")
    (sa / "namespace").write_text("benchmarking")
    (sa / "ca.crt").write_text("")
    monkeypatch.setattr(runner_mod, "SERVICE_ACCOUNT", sa)
    monkeypatch.setattr(runner_mod.ssl, "create_default_context", lambda cafile: None)
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", host)
    monkeypatch.setenv("KUBERNETES_SERVICE_PORT", port)
    monkeypatch.setenv("HOSTNAME", "pod-1")
    urls = []

    class Resp(io.BytesIO):
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    def fake_urlopen(req, context=None, timeout=None):
        urls.append(req.full_url)
        return Resp(b"log\n")

    monkeypatch.setattr(runner_mod.urllib.request, "urlopen", fake_urlopen)
    runner_mod.sidecar_logs(tmp_path, io.StringIO())
    assert urls == [f"{want}api/v1/namespaces/benchmarking/pods/pod-1/log?container={c}" for c in ("worker", "sink")]
    assert (tmp_path / "worker.log").read_bytes() == b"log\n"


def test_upload_failure(rig, monkeypatch):
    rig.load()

    def fail(src, dest):
        raise OSError("bucket gone")

    monkeypatch.setattr(runner_mod, "upload", fail)
    assert rig.run() == 1
    assert not rig.dest.exists()
