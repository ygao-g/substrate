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

"""Golden tests pinning what the ingress benchmark builds and reports.

testdata/spec-golden-ingress.json holds named goldens, each the
build_spec_dict kwargs runner.py derives from a tests.yaml entry with a
fixed five-actor fleet, and the dict they produced, plus an `entries` map
from every `type: nighthawk-ingress` entry name to its golden. Each entry
and run-dev.sh's defaults map onto their golden through the real path:
job_subs into the Job template, the template's args through
runner.parse_args, and runner.spec_kwargs. envoyCpu and the fleet size
never enter the spec, so today every entry shares one golden; an entry
with different knobs gets its own.

testdata/output-session.json is a real 2 CPU session from the results
bucket, reduced to the fields output.py reads and to four stages (the
first ramp step, the overshoot, the converged step, and the testing
stage), with the rows and capacity summary the parser produced from it.
stats_records is a fixed reshaping of the rows, checked in code.

Needs PyYAML and benchmarking/automation, so unlike the other tests here it
does not run inside the runner image.
"""

import json
import sys
from pathlib import Path
from unittest import mock

import yaml

import ateapi_fakes

HERE = Path(__file__).resolve().parent
INGRESS_DIR = HERE.parent
AUTOMATION_DIR = INGRESS_DIR.parent / "automation"
sys.path.insert(0, str(INGRESS_DIR))
sys.path.insert(0, str(AUTOMATION_DIR))
ateapi_fakes.install()

import orchestrator  # noqa: E402
import output as output_mod  # noqa: E402
import runner  # noqa: E402
import spec as spec_mod  # noqa: E402
from testtypes import nighthawk_ingress  # noqa: E402

TESTDATA = HERE / "testdata"
ACTOR_NAMES = [f"nh-{i:03d}" for i in range(5)]


def load(name: str):
    return json.loads((TESTDATA / name).read_text())


def spec_kwargs_for(test: dict) -> dict:
    """What runner.py passes to build_spec_dict for a tests.yaml entry:
    the rendered Job's args through runner's own argparse."""
    nighthawk_ingress.validate(test)
    subs = {
        "JOB_NAME": "golden",
        "IMAGE": "golden",
        "TAG": "golden",
        "NAME": test["name"],
        "DEST": "/tmp/golden",
        "ATE_ATEAPI_CLIENT_AUTH": "cert",
        **nighthawk_ingress.job_subs(test),
    }
    rendered = orchestrator.render_template(
        nighthawk_ingress.job_tmpl(str(AUTOMATION_DIR / "manifests")), subs
    )
    job = next(
        d for d in yaml.safe_load_all(rendered) if d and d.get("kind") == "Job"
    )
    container = job["spec"]["template"]["spec"]["containers"][0]
    assert container["command"] == ["python3", "/app/runner.py"]
    with mock.patch.object(sys, "argv", ["runner.py", *container["args"]]):
        args = runner.parse_args()
    assert args.extra == []
    return runner.spec_kwargs(args, list(ACTOR_NAMES))


def test_spec_golden_rebuilds():
    goldens = load("spec-golden-ingress.json")["goldens"]
    assert goldens
    for name, golden in goldens.items():
        assert spec_mod.build_spec_dict(**golden["kwargs"]) == golden["spec"], name


def test_every_tests_yaml_entry_maps_onto_its_golden():
    golden_file = load("spec-golden-ingress.json")
    tests = yaml.safe_load((AUTOMATION_DIR / "tests.yaml").read_text())["tests"]
    entries = [t for t in tests if t.get("type") == nighthawk_ingress.TEST_TYPE]
    assert entries
    for test in entries:
        name = test["name"]
        assert name in golden_file["entries"], (
            f"{name} has no golden; add it to entries in spec-golden-ingress.json"
        )
        golden = golden_file["goldens"][golden_file["entries"][name]]
        assert spec_kwargs_for(test) == golden["kwargs"], name


def test_run_dev_defaults_map_onto_the_default_golden():
    # The test dict run-dev.sh renders from its defaults.
    test = {
        "name": "ingress_routercap_envoy_2cpu",
        "type": "nighthawk-ingress",
        "targetCluster": "dev",
        "duration": "30m",
        "workerCount": 100,
        "nighthawk-ingress": {
            "envoyCpu": 2,
            "atespace": "ingress-benchmark",
            "tailLatencySloMs": 25.0,
        },
    }
    golden = load("spec-golden-ingress.json")["goldens"]["default"]
    assert spec_kwargs_for(test) == golden["kwargs"]


def test_output_goldens():
    session = load("output-session.json")
    rows = output_mod.stage_rows(session)
    assert rows == load("output-golden-rows.json")
    records = output_mod.stats_records(
        rows, tag="golden", test_name="ingress_routercap_envoy_2cpu"
    )
    assert [r["metric"] for r in records] == [f"nighthawk_{r['stage']}" for r in rows]
    for record, row in zip(records, rows):
        assert record["timestamp"] == row["start_time"]
        assert record["tag"] == "golden"
        assert record["test_name"] == "ingress_routercap_envoy_2cpu"
        assert record["measurements"] == {
            k: v for k, v in row.items() if k not in ("stage", "start_time")
        }
    summary = output_mod.capacity_summary(
        session,
        envoy_cpu=2,
        actors=50,
        client_concurrency=16,
        tail_latency_slo_ms=25.0,
    )
    assert summary == load("output-golden-summary.json")


if __name__ == "__main__":
    for fn_name, fn in sorted(dict(globals()).items()):
        if fn_name.startswith("test_") and callable(fn):
            fn()
            print(f"PASS {fn_name}")
