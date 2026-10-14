// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/yaml"

	"github.com/agent-substrate/substrate/internal/egresstunnels"
)

// logShim logs its name and arguments to $SHIM_LOG and drains stdin; it
// stands in for hack/run-tool.sh.
const logShim = `#!/bin/bash
echo "$(basename "$0") $*" >>"${SHIM_LOG}"
cat >/dev/null
`

// kubectlShim is logShim for kubectl, answering the reads deploy.sh branches
// on. With $SHIM_NO_SECRET set, the first existence check of a Secret fails.
// It prints $SHIM_ENVOY_IMAGE as the gateway's envoy image and shimCA as the
// target's CA, refuses a --from-file that does not exist, and copies a
// --patch-file to $SHIM_LOG.patch.
const kubectlShim = `#!/bin/bash
echo "$(basename "$0") $*" >>"${SHIM_LOG}"
cat >/dev/null
case " $* " in
  *" get secret "*go-template*)
    printf 'test-ca\n'
    ;;
  *" get secret "*)
    if [[ -n "${SHIM_NO_SECRET:-}" && ! -e "${SHIM_LOG}.secret" ]]; then
      : >"${SHIM_LOG}.secret"
      exit 1
    fi
    ;;
  *" get deployment atenet-egress "*)
    echo "${SHIM_ENVOY_IMAGE:-}"
    ;;
  *" create secret "*)
    for arg in "$@"; do
      if [[ "${arg}" == --from-file=* && ! -f "${arg#*=*=}" ]]; then
        echo "no such file: ${arg}" >&2
        exit 1
      fi
    done
    ;;
esac
while [[ "$#" -gt 0 ]]; do
  if [[ "$1" == --patch-file ]]; then
    cp "$2" "${SHIM_LOG}.patch"
  fi
  shift
done
`

// shimCA is what kubectlShim prints as the target's CA.
const shimCA = "test-ca\n"

// goShim is logShim for go, writing the files certs would into its --out
// directory.
const goShim = `#!/bin/bash
echo "$(basename "$0") $*" >>"${SHIM_LOG}"
while [[ "$#" -gt 0 ]]; do
  if [[ "$1" == --out ]]; then
    mkdir -p "$2"
    for f in ca.crt tls.crt tls.key; do
      echo "${f}" >"$2/${f}"
    done
  fi
  shift
done
`

// deployToolsRequired says whether a missing tool fails TestDeployScript
// rather than skipping it. A skip and a pass share an exit code, so in CI
// the test must fail, or the script would go untested unnoticed.
func deployToolsRequired() bool {
	return os.Getenv("CI") == "true" || os.Getenv("REQUIRE_DEPLOY_TOOLS") == "true"
}

// scriptDir is where deploy.sh and its manifests live, relative to the
// repository root; the tests read them from there and run the copy from the
// same place in a scratch repository.
const scriptDir = "benchmarking/egress-tunnels"

// scriptPath is scriptDir from this package's directory.
const scriptPath = "../../../" + scriptDir

// TestDeployScript runs deploy.sh in a scratch repository with kubectl, go
// and hack/run-tool.sh replaced by logging shims, no .ate-dev-env.sh, and a
// TMPDIR with a space in it.
func TestDeployScript(t *testing.T) {
	t.Parallel()
	for _, tool := range []string{"bash", "git"} {
		if _, err := exec.LookPath(tool); err != nil {
			if deployToolsRequired() {
				t.Fatalf("%s not found: %v", tool, err)
			}
			t.Skipf("%s not found: %v", tool, err)
		}
	}
	for _, tc := range []struct {
		name    string
		args    []string
		env     []string
		want    []string
		notWant []string
		// dir, when set, is the directory under the repository the script
		// runs from.
		dir string
		// cluster, when set, must reach every kubectl and ko call. "{repo}"
		// stands for the scratch repository's path.
		cluster string
		// patch holds what the gateway patch deploy.sh applied must contain.
		patch []string
	}{
		{
			name: "delete without the env file",
			args: []string{"--delete"},
			want: []string{"kubectl delete namespace egress-tests-targets --ignore-not-found"},
		},
		{
			name: "deploy",
			args: []string{"--deploy", "--endpoints", "2"},
			env:  []string{"KO_DOCKER_REPO=r"},
			want: []string{
				"run-tool.sh ko apply -f -",
				"kubectl rollout status deployment/egress-target --namespace=egress-tests-targets --timeout=300s",
			},
			notWant: []string{"kubectl-ate", "actor-template", "workerpool", "certs", "patch deployment"},
		},
		{
			name: "deploy issues the target certificate once",
			args: []string{"--deploy", "--endpoints", "2"},
			env:  []string{"KO_DOCKER_REPO=r", "SHIM_NO_SECRET=1"},
			want: []string{
				"go run ./cmd/benchmarking/egress-tunnels certs --out ",
				"kubectl -n egress-tests-targets create secret generic egress-target-tls --type=kubernetes.io/tls",
				"kubectl -n egress-tests-targets rollout restart deployment/egress-target",
			},
		},
		{
			name: "deploy with https patches the gateway",
			args: []string{"--deploy", "--https", "--endpoints", "2"},
			env:  []string{"KO_DOCKER_REPO=r", "SHIM_ENVOY_IMAGE=envoy:test"},
			want: []string{
				"kubectl -n ate-system create configmap egress-tests-target-ca --from-file=ca.crt=",
				"kubectl -n ate-system patch deployment atenet-egress --type strategic --patch-file ",
				"kubectl -n ate-system rollout status deployment/atenet-egress --timeout=300s",
			},
			patch: []string{"image: envoy:test", fmt.Sprintf("target-ca-sha256: %q", sha256Hex(shimCA))},
		},
		{
			name:    "deploy to a named cluster",
			args:    []string{"--deploy", "--endpoints", "2", "--kubeconfig", "/k/c 2", "--context", "ctx"},
			env:     []string{"KO_DOCKER_REPO=r"},
			want:    []string{"run-tool.sh ko apply -f - -- --kubeconfig /k/c 2 --context ctx"},
			cluster: "--kubeconfig /k/c 2 --context ctx",
		},
		{
			name:    "delete from a named context",
			args:    []string{"--delete", "--context=ctx"},
			want:    []string{"kubectl --context ctx delete namespace egress-tests-targets --ignore-not-found"},
			cluster: "--context ctx",
		},
		{
			name:    "unpatch the gateway in a named context",
			args:    []string{"--unpatch-gateway", "--context", "ctx"},
			want:    []string{"kubectl --context ctx -n ate-system delete configmap egress-tests-target-ca --ignore-not-found"},
			cluster: "--context ctx",
		},
		{
			// The script changes to the repository root, so a relative
			// kubeconfig must be resolved against the caller's directory.
			name:    "relative kubeconfig from a subdirectory",
			args:    []string{"--delete", "--kubeconfig", "kc.yaml"},
			dir:     scriptDir,
			cluster: "--kubeconfig {repo}/" + scriptDir + "/kc.yaml",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			logged, patch, tmpdir, repo := runDeployScript(t, tc.dir, tc.args, tc.env)
			tc.cluster = strings.ReplaceAll(tc.cluster, "{repo}", repo)
			for _, want := range tc.want {
				if !strings.Contains(logged, want) {
					t.Errorf("deploy.sh did not run %q; ran:\n%s", want, logged)
				}
			}
			for _, want := range tc.patch {
				if !strings.Contains(patch, want) {
					t.Errorf("gateway patch lacks %q:\n%s", want, patch)
				}
			}
			for _, nw := range tc.notWant {
				if strings.Contains(logged, nw) {
					t.Errorf("deploy.sh ran %q; ran:\n%s", nw, logged)
				}
			}
			if tc.cluster != "" {
				for _, line := range strings.Split(strings.TrimSpace(logged), "\n") {
					tool, rest, _ := strings.Cut(line, " ")
					switch {
					case tool == "kubectl":
						if !strings.HasPrefix(rest, tc.cluster+" ") {
							t.Errorf("%s call without %q first: %s", tool, tc.cluster, line)
						}
					case strings.HasPrefix(line, "run-tool.sh ko"):
						if !strings.HasSuffix(line, " "+tc.cluster) {
							t.Errorf("ko call without %q last: %s", tc.cluster, line)
						}
					}
				}
			}
			left, err := os.ReadDir(tmpdir)
			if err != nil {
				t.Fatal(err)
			}
			if len(left) != 0 {
				t.Errorf("TMPDIR %q holds %d entries after exit, want the scratch dir removed", tmpdir, len(left))
			}
		})
	}
}

// runDeployScript runs deploy.sh with args from dir under a scratch
// repository and returns the shims' log, the gateway patch it applied if any,
// the TMPDIR it used and the repository's path.
func runDeployScript(t *testing.T, dir string, args, env []string) (logged, patch, tmpdir, repo string) {
	t.Helper()
	tmp := t.TempDir()
	// The script's $PWD has symlinks resolved, as on macOS's /var.
	tmp, err := filepath.EvalSymlinks(tmp)
	if err != nil {
		t.Fatal(err)
	}
	repo = filepath.Join(tmp, "repo")
	shims := filepath.Join(tmp, "shims")
	tmpdir = filepath.Join(tmp, "with space")
	log := filepath.Join(tmp, "shim.log")

	writeFile(t, filepath.Join(repo, scriptDir, "deploy.sh"), readFile(t, filepath.Join(scriptPath, "deploy.sh")), 0o755)
	manifests, err := filepath.Glob(filepath.Join(scriptPath, "manifests/*"))
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range manifests {
		writeFile(t, filepath.Join(repo, scriptDir, "manifests", filepath.Base(m)), readFile(t, m), 0o644)
	}
	writeFile(t, filepath.Join(repo, "hack/run-tool.sh"), logShim, 0o755)
	writeFile(t, filepath.Join(shims, "kubectl"), kubectlShim, 0o755)
	writeFile(t, filepath.Join(shims, "go"), goShim, 0o755)
	if err := os.MkdirAll(tmpdir, 0o755); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("git", "init", "-q", repo).CombinedOutput(); err != nil {
		t.Fatalf("git init: %v\n%s", err, out)
	}
	cmd := exec.Command("bash", append([]string{filepath.Join(repo, scriptDir, "deploy.sh")}, args...)...)
	cmd.Dir = filepath.Join(repo, dir)
	cmd.Env = append([]string{
		"PATH=" + shims + ":" + filepath.Dir(mustLookPath(t, "git")) + ":/usr/bin:/bin",
		"HOME=" + tmp,
		"TMPDIR=" + tmpdir,
		"SHIM_LOG=" + log,
	}, env...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("deploy.sh %v: %v\n%s", args, err, out)
	}
	if b, err := os.ReadFile(log + ".patch"); err == nil {
		patch = string(b)
	} else if !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	return readFile(t, log), patch, tmpdir, repo
}

func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func writeFile(t *testing.T, path, content string, mode os.FileMode) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), mode); err != nil {
		t.Fatal(err)
	}
}

func mustLookPath(t *testing.T, name string) string {
	t.Helper()
	p, err := exec.LookPath(name)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// TestDeployMaxEndpointsMatchesTheDriver pins deploy.sh's MAX_ENDPOINTS to
// egresstunnels.MaxEndpoints: the script and the driver must refuse the same
// --endpoints, or a deploy the driver accepts fails at preflight.
func TestDeployMaxEndpointsMatchesTheDriver(t *testing.T) {
	t.Parallel()
	m := regexp.MustCompile(`(?m)^MAX_ENDPOINTS=(\d+)$`).FindStringSubmatch(readFile(t, filepath.Join(scriptPath, "deploy.sh")))
	if m == nil {
		t.Fatal("deploy.sh has no MAX_ENDPOINTS=<n> line")
	}
	if got, _ := strconv.Atoi(m[1]); got != egresstunnels.MaxEndpoints {
		t.Errorf("deploy.sh MAX_ENDPOINTS = %d, want egresstunnels.MaxEndpoints = %d", got, egresstunnels.MaxEndpoints)
	}
}

// TestPrintTargets renders the target Deployment and Services with
// --print-targets, which must not need a cluster: kubectl is not on PATH.
func TestPrintTargets(t *testing.T) {
	t.Parallel()
	// Every Service gets its own target ports, and the target listens on
	// the whole MAX_ENDPOINTS range whatever --endpoints is, so a Service
	// left from an earlier, larger deploy still reaches an open port.
	wantArgs := []string{
		fmt.Sprintf("--listen-ports=10000-%d", 10000+egresstunnels.MaxEndpoints-1),
		fmt.Sprintf("--tls-listen-ports=12000-%d", 12000+egresstunnels.MaxEndpoints-1),
	}
	for _, tc := range []struct {
		name      string
		args      []string
		wantImage string
	}{
		{
			name:      "the ko image",
			args:      []string{"--endpoints", "3"},
			wantImage: "ko://github.com/agent-substrate/substrate/cmd/benchmarking/egress-target",
		},
		{
			name:      "another image",
			args:      []string{"--endpoints", "3", "--target-image", "gcr.io/p/target@sha256:0"},
			wantImage: "gcr.io/p/target@sha256:0",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			repo := filepath.Join(t.TempDir(), "repo")
			writeFile(t, filepath.Join(repo, scriptDir, "deploy.sh"), readFile(t, filepath.Join(scriptPath, "deploy.sh")), 0o755)
			for _, m := range []string{"targets.yaml.tmpl", "target-service.yaml.tmpl"} {
				writeFile(t, filepath.Join(repo, scriptDir, "manifests", m), readFile(t, filepath.Join(scriptPath, "manifests", m)), 0o644)
			}
			if out, err := exec.Command("git", "init", "-q", repo).CombinedOutput(); err != nil {
				t.Fatalf("git init: %v\n%s", err, out)
			}
			cmd := exec.Command("bash", append([]string{scriptDir + "/deploy.sh", "--print-targets"}, tc.args...)...)
			cmd.Dir = repo
			cmd.Env = []string{"PATH=" + filepath.Dir(mustLookPath(t, "git")) + ":/usr/bin:/bin", "HOME=" + repo}
			out, err := cmd.Output()
			if err != nil {
				t.Fatalf("deploy.sh --print-targets: %v\n%s", err, out)
			}
			var services []corev1.Service
			var dep *appsv1.Deployment
			for _, doc := range strings.Split(string(out), "\n---\n") {
				var meta struct{ Kind string }
				if err := yaml.Unmarshal([]byte(doc), &meta); err != nil {
					t.Fatalf("decoding %q: %v", doc, err)
				}
				switch meta.Kind {
				case "Deployment":
					dep = &appsv1.Deployment{}
					if err := yaml.UnmarshalStrict([]byte(doc), dep); err != nil {
						t.Fatal(err)
					}
				case "Service":
					var svc corev1.Service
					if err := yaml.UnmarshalStrict([]byte(doc), &svc); err != nil {
						t.Fatal(err)
					}
					services = append(services, svc)
				}
			}
			if dep == nil || len(services) != 3 {
				t.Fatalf("got a Deployment %v and %d Services, want one and 3", dep != nil, len(services))
			}
			c := dep.Spec.Template.Spec.Containers[0]
			if c.Image != tc.wantImage {
				t.Errorf("target image = %q, want %q", c.Image, tc.wantImage)
			}
			for _, a := range wantArgs {
				if !slices.Contains(c.Args, a) {
					t.Errorf("target args %v lack %q", c.Args, a)
				}
			}
			for i, svc := range services {
				wantHTTP, wantHTTPS := intstr.FromInt32(int32(10000+i)), intstr.FromInt32(int32(12000+i))
				if p := svc.Spec.Ports; len(p) != 2 || p[0].TargetPort != wantHTTP || p[1].TargetPort != wantHTTPS {
					t.Errorf("Service %s ports %v, want targetPorts %v and %v", svc.Name, p, wantHTTP.String(), wantHTTPS.String())
				}
			}
		})
	}
}
