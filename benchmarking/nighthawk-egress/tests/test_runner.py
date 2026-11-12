# Copyright 2026 The Agent Substrate Authors
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

"""Unit tests for benchmarking/nighthawk-egress/runner.py."""

import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import runner

ARGS = ["--name", "test", "--tag", "tag1", "--dest", "gs://b/p", "--envoy-cpu", "1"]


def _stage(attempted_rps: float, achieved_rps: float, failed: str | None = None) -> dict:
    evals = [
        {"metric_id": "nighthawk.builtin/attempted-rps", "metric_value": attempted_rps},
        {"metric_id": "nighthawk.builtin/achieved-rps", "metric_value": achieved_rps},
    ]
    if failed:
        evals.append({"metric_id": f"nighthawk.builtin/{failed}", "metric_value": 1.0, "threshold_score": -1.0})
    return {
        "metric_evaluations": evals,
        "nighthawk_service_output": {"results": [{"name": "global", "execution_duration": "20s"}]},
    }


ADJUSTING = [_stage(400.0, 399.0), _stage(800.0, 750.0), _stage(1600.0, 900.0, failed="send-rate")]
SAMPLE_OUTPUT = {
    "session_status": {"code": 0},
    "adjusting_stage_results": ADJUSTING,
    "testing_stage_result": _stage(800.0, 760.0),
}
NO_CLEAN_OUTPUT = {
    "session_status": {"code": 0},
    "adjusting_stage_results": ADJUSTING[2:],
    "testing_stage_result": _stage(800.0, 700.0, failed="send-rate"),
}


def _run_main(work_dir: Path, mode: str, output: dict | None, client_exit: int = 0, upload_error=None):
    """Runs runner.main with Nighthawk mocked out.

    Returns the exit code, the names of the files passed to upload, and the
    spec_dict_to_textproto mock.
    """
    proc = MagicMock(stdout=["[adaptive] stage done\n"])
    proc.wait.return_value = client_exit
    if output is not None:
        (work_dir / "output.textproto").write_text("fake output textproto")
    with (
        patch("runner.Path", return_value=work_dir),
        patch("runner.upload", side_effect=upload_error) as upload,
        patch("subprocess.Popen", return_value=proc),
        patch("socket.create_connection"),
        patch("runner.parse_output_textproto", return_value=output),
        patch("runner.spec_dict_to_textproto", return_value="fake textproto") as spec,
    ):
        got = runner.main([*ARGS, "--mode", mode])
    return got, {c.args[0].name for c in upload.call_args_list}, spec


class TestRunner(unittest.TestCase):
    def test_main_exit_code_and_artifacts(self):
        client_failed = "nighthawk_adaptive_load_client exited with code 3"
        cases = [
            # label, Nighthawk output, client exit code, upload error, want exit code, want failures
            ("clean run", SAMPLE_OUTPUT, 0, None, 0, []),
            ("client fails", SAMPLE_OUTPUT, 3, None, 1, [client_failed]),
            ("no Nighthawk output", None, 3, None, 1, [client_failed, "no Nighthawk output file"]),
            ("no testing stage", {"adjusting_stage_results": ADJUSTING}, 0, None, 1, ["no testing stage result"]),
            ("no stage passed", NO_CLEAN_OUTPUT, 0, None, 1, ["no slo_max_rps"]),
            ("upload fails", SAMPLE_OUTPUT, 0, OSError("bucket gone"), 1, []),
        ]
        for label, output, client_exit, upload_error, want, want_failures in cases:
            with self.subTest(label=label), tempfile.TemporaryDirectory() as tmp:
                work_dir = Path(tmp)
                got, uploaded, _ = _run_main(work_dir, "mitm", output, client_exit, upload_error)
                self.assertEqual(got, want)

                want_uploaded = {"logs.txt", "spec.textproto", "status.json"}
                if output is not None:
                    want_uploaded |= {"output.textproto", "results.json", "stats.jsonl", "capacity.json"}
                self.assertEqual(uploaded, want_uploaded)

                status = json.loads((work_dir / "status.json").read_text())
                self.assertEqual(status["adaptive_client_exit_code"], client_exit)
                self.assertEqual(status["failures"], want_failures)
                if output is None:
                    self.assertFalse((work_dir / "capacity.json").exists())
                    self.assertIsNone(status["slo_max_rps"])
                    continue
                cap = json.loads((work_dir / "capacity.json").read_text())
                self.assertEqual(cap["failures"], want_failures)
                self.assertEqual(cap["slo_max_rps"], status["slo_max_rps"])

    def test_capacity_json_fields(self):
        with tempfile.TemporaryDirectory() as tmp:
            _run_main(Path(tmp), "http", SAMPLE_OUTPUT)
            cap = json.loads((Path(tmp) / "capacity.json").read_text())
        want = {
            "mode": "http",
            "envoy_cpu": 1,
            "actors": 1,
            "client_concurrency": 16,
            "tail_latency_slo_ms": 25.0,
            "slo_max_rps": 750.0,
            "slo_max_stage": "adjusting_001",
            "binding_threshold": ["send-rate"],
            "converged": True,
            "failures": [],
        }
        self.assertEqual({k: cap.get(k) for k in want}, want)

    def test_main_passthrough_servicedns_ca_reaches_spec(self):
        pem = "-----BEGIN CERTIFICATE-----\nMIIBfake\n-----END CERTIFICATE-----\n"
        with tempfile.TemporaryDirectory() as tmp:
            ca = Path(tmp) / "ca.crt"
            ca.write_text(pem)
            with patch("runner.SERVICEDNS_CA_CERT", str(ca)):
                _, _, spec = _run_main(Path(tmp), "passthrough", None)
        tpl = spec.call_args[0][0]["nighthawk_traffic_template"]
        trusted_ca = tpl["transport_socket"]["typed_config"]["common_tls_context"]["validation_context"]["trusted_ca"]
        self.assertEqual(trusted_ca, {"inline_string": pem})


if __name__ == "__main__":
    unittest.main()
