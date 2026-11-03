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

"""Read-only Kubernetes API GETs, in-cluster or through kubectl.

In a pod, requests go to the API server with the ServiceAccount token. Off
cluster, `kubectl get --raw` reuses the caller's kubeconfig. Only GET is
exposed.
"""

import json
import os
import ssl
import subprocess
import urllib.parse
import urllib.request
from pathlib import Path

SA_DIR = Path("/var/run/secrets/kubernetes.io/serviceaccount")


class KubeClient:
    def get_raw(self, path: str, timeout_s: float = 20) -> str:
        raise NotImplementedError

    def get_json(self, path: str, timeout_s: float = 20) -> dict:
        return json.loads(self.get_raw(path, timeout_s))

    def list_pods(self, namespace: str, selector: str) -> list[dict]:
        q = urllib.parse.urlencode({"labelSelector": selector}) if selector else ""
        path = f"/api/v1/namespaces/{namespace}/pods" + (f"?{q}" if q else "")
        return self.get_json(path).get("items", [])


class InClusterClient(KubeClient):
    def __init__(self, sa_dir: Path = SA_DIR):
        host = os.environ["KUBERNETES_SERVICE_HOST"]
        port = os.environ.get("KUBERNETES_SERVICE_PORT", "443")
        if ":" in host:
            host = f"[{host}]"
        self.base = f"https://{host}:{port}"
        self.sa_dir = sa_dir
        self.ctx = ssl.create_default_context(cafile=str(sa_dir / "ca.crt"))

    def get_raw(self, path: str, timeout_s: float = 20) -> str:
        # Re-read each call: projected tokens rotate.
        token = (self.sa_dir / "token").read_text().strip()
        req = urllib.request.Request(
            self.base + path, headers={"Authorization": f"Bearer {token}"}
        )
        with urllib.request.urlopen(req, timeout=timeout_s, context=self.ctx) as r:
            return r.read().decode()


class KubectlClient(KubeClient):
    def __init__(self, context: str | None = None):
        self.context = context

    def get_raw(self, path: str, timeout_s: float = 20) -> str:
        cmd = ["kubectl", "get", "--raw", path, f"--request-timeout={int(timeout_s)}s"]
        if self.context:
            cmd += ["--context", self.context]
        res = subprocess.run(cmd, capture_output=True, text=True)
        if res.returncode != 0:
            raise RuntimeError(f"kubectl get --raw {path}: {res.stderr.strip()}")
        return res.stdout


def default_client(context: str | None = None) -> KubeClient:
    if context is None and os.environ.get("KUBERNETES_SERVICE_HOST") and SA_DIR.exists():
        return InClusterClient()
    return KubectlClient(context)
