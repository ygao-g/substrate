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

package networking

import (
	"context"
	"io"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/agent-substrate/substrate/internal/e2e"
)

// probeImage resolves from the node's cache: the cluster's own bring-up runs a
// busybox probe on the same tag, so this is not a fresh registry pull.
const probeImage = "busybox:1.36"

// TestPodResolvesExternalNameOnIPv6Only asserts a plain pod can resolve an
// external name.
//
// kind's DNS translation is IPv4-only. CoreDNS runs dnsPolicy: Default and
// inherits the node's IPv4 resolver, which no pod on an IPv6-only cluster can
// reach, so nothing resolves and no actor boots -- every suite goes red at once
// and none of them names the cause. hack/create-kind-cluster.sh repoints
// CoreDNS at an IPv6 upstream; this is the assertion that the cluster under
// test actually got that treatment.
//
// Deliberately a bare pod rather than an Actor: atenet-egress does not go Ready
// on IPv6-only, for an unrelated Envoy bind, so an Actor-based assertion would
// be a standing red rather than a regression guard.
func TestPodResolvesExternalNameOnIPv6Only(t *testing.T) {
	ctx := t.Context()

	families := e2e.ClusterIPFamilies(t, ctx)
	if !families[corev1.IPv6Protocol] || families[corev1.IPv4Protocol] {
		t.Skip("cluster is not IPv6-only; kind's IPv4 DNS translation covers the other families")
	}

	namespace := e2e.CreateNamespace(t).Name
	clients := e2e.GetClients()

	// A name with an AAAA record: an IPv6-only pod that gets only an A back has
	// resolved nothing it can use.
	const name = "storage.googleapis.com"
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "dns-probe", Namespace: namespace},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Containers: []corev1.Container{{
				Name:    "probe",
				Image:   probeImage,
				Command: []string{"nslookup", "-type=AAAA", name},
			}},
		},
	}
	if _, err := clients.K8s.CoreV1().Pods(namespace).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating the DNS probe pod: %v", err)
	}

	phase := waitForPodToFinish(t, ctx, namespace, pod.Name)
	logs := podLogs(t, ctx, namespace, pod.Name)
	if phase != corev1.PodSucceeded {
		t.Fatalf("a pod cannot resolve %s, so CoreDNS is pointed at a resolver this cluster cannot reach; probe %s:\n%s", name, phase, logs)
	}
	t.Logf("resolved %s from a pod:\n%s", name, logs)
}

// waitForPodToFinish returns the pod's terminal phase, or fails the test if it
// has not reached one in time.
func waitForPodToFinish(t *testing.T, ctx context.Context, namespace, name string) corev1.PodPhase {
	t.Helper()

	var phase corev1.PodPhase
	err := wait.PollUntilContextTimeout(ctx, time.Second, 2*time.Minute, true, func(ctx context.Context) (bool, error) {
		pod, err := e2e.GetClients().K8s.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		phase = pod.Status.Phase
		return phase == corev1.PodSucceeded || phase == corev1.PodFailed, nil
	})
	if err != nil {
		t.Fatalf("waiting for pod %s/%s to finish (last phase %q): %v", namespace, name, phase, err)
	}
	return phase
}

// podLogs reads a finished pod's log. Read once it has exited: an attach drops
// lines.
func podLogs(t *testing.T, ctx context.Context, namespace, name string) string {
	t.Helper()

	stream, err := e2e.GetClients().K8s.CoreV1().Pods(namespace).GetLogs(name, &corev1.PodLogOptions{}).Stream(ctx)
	if err != nil {
		t.Fatalf("opening the log of pod %s/%s: %v", namespace, name, err)
	}
	defer stream.Close()

	out, err := io.ReadAll(stream)
	if err != nil {
		t.Fatalf("reading the log of pod %s/%s: %v", namespace, name, err)
	}
	return string(out)
}
