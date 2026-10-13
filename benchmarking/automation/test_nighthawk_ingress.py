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

"""Unit tests for testtypes/nighthawk_ingress.py:
python3 benchmarking/automation/test_nighthawk_ingress.py"""

import unittest
from unittest import mock

from testtypes import nighthawk_ingress

# The kubectl calls pre_test made for envoyCpu 2 before the pin moved into
# util.pin_deployment, recorded with util.run mocked.
RECORDED_PIN_ARGV = [
    [
        "kubectl",
        "-n",
        "ate-system",
        "patch",
        "deployment",
        "atenet-router",
        "--type=strategic",
        "-p",
        '{"spec": {"template": {"spec": {"containers": ['
        '{"name": "atenet-router", "resources": '
        '{"requests": {"cpu": "2"}, "limits": {"cpu": "2"}}}, '
        '{"name": "envoy", "command": ["/usr/local/bin/envoy", "-c", '
        '"/etc/envoy/envoy.yaml", "--concurrency", "2"], "resources": '
        '{"requests": {"cpu": "2"}, "limits": {"cpu": "2"}}}]}}}}',
    ],
    [
        "kubectl",
        "-n",
        "ate-system",
        "rollout",
        "status",
        "deployment/atenet-router",
        "--timeout=300s",
    ],
]


class PreTestTest(unittest.TestCase):
    @mock.patch("util.run")
    def test_pins_the_router_with_the_recorded_argv(self, run_mock):
        nighthawk_ingress.pre_test({"nighthawk-ingress": {"envoyCpu": 2}})

        self.assertEqual(
            [c.args[0] for c in run_mock.call_args_list], RECORDED_PIN_ARGV
        )


if __name__ == "__main__":
    unittest.main()
