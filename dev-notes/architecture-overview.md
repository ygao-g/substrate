# Architecture Overview

First-pass orientation on [agent-substrate/substrate](https://github.com/agent-substrate/substrate).
Baseline: `main` @ `55f16fa9`, read 2026-08-03. ~96k lines of Go across 409
non-vendored files. Google-originated, Apache 2.0, explicitly pre-production.

## Core thesis

Agents are idle most of the time. So map a large set of **actors** (agent
instances) onto a small set of pre-warmed **workers** (K8s Pods), suspending and
resuming actors via sandbox checkpoint/restore.

Kubernetes handles infrastructure provisioning. A purpose-built control plane
handles the per-actor hot path, because kube-apiserver cannot hold millions of
records or sustain 1000 resume/sec against a 100ms p95 activation target.

## The split that defines the architecture

| | Where it lives | Why |
|---|---|---|
| **Config** — `WorkerPool`, `ActorTemplate`, `SandboxConfig` | K8s CRDs (`pkg/api/v1alpha1/`) | low-frequency; gets RBAC and audit for free |
| **Instance state** — `Actor`, `Worker`, `ActorSnapshot`, `Atespace` | Valkey/Redis behind `ateapi` | high-frequency, latency-critical |

Consequence worth internalizing: **`Actor` is not a CRD.** Don't go looking for
one in `pkg/api`.

## Components

One binary per `cmd/`:

- **`cmd/ateapi`** — the control plane. gRPC `Control` service
  (`CreateActor` / `ResumeActor` / `SuspendActor`, snapshot tagging, atespaces)
  defined in `pkg/proto/ateapipb/ateapi.proto`. Its `internal/` holds the state
  store, scheduler, worker cache, and an `ActorIdentity` service that mints
  per-actor JWTs and certs — actor identity is decoupled from whichever host the
  actor lands on.
- **`cmd/atelet`** — node-level DaemonSet supervisor. Drives checkpoint/restore
  on its node's worker pods and streams snapshots to/from GCS/S3.
- **`cmd/ateom-gvisor`, `cmd/ateom-microvm`** — the in-pod "sandbox herder", one
  per sandbox class. gVisor shells out to `runsc checkpoint`/`restore`; the
  microVM path is a full Kata + Cloud Hypervisor implementation using
  `userfaultfd` demand-paging for fast restore. Most of the interesting systems
  code is here.
- **`cmd/atenet`** — data plane. DNS
  (`<actor>.<atespace>.actors.resources.substrate.ate.dev`) plus an Envoy router
  whose `ext_proc` filter pulls the actor name out of the `Host` header, calls
  `ResumeActor`, then opens an mTLS tunnel to `atunnel` on the assigned worker.
  **A cold actor is woken by the request itself.**
- **`cmd/atecontroller`** — reconciles the CRDs into Deployments of worker pods.
- **`cmd/kubectl-ate`** — the CLI (`kubectl ate create actor …`).
- **`cmd/podcertcontroller`**, **`cmd/benchmarking`** — supporting binaries;
  not yet read.

## Layout conventions

From `AGENTS.md`:

- `cmd/<bin>/internal/` for single-binary code
- `internal/` for cross-binary code
- `pkg/` for external consumers
- Public protos in `pkg/proto`; internal ones (atelet↔ateom) in `internal/proto`
- `make build` / `make test` / `make verify`. `make verify` gates on boilerplate
  headers, licenses, and `go mod tidy` — run it before review. Tests are
  required for new code.

## Reading order

1. `docs/architecture.md` — good mermaid sequence and state diagrams.
2. `docs/glossary.md` — the `ate*` prefixes are opaque until you know
   atespace / atelet / ateom / atenet.
3. The `ResumeActor` path end-to-end: `ext_proc` → ateapi scheduler → atelet →
   ateom. This is the spine of the system.

`hack/create-kind-cluster.sh` + `hack/install-ate-kind.sh` gets a local cluster
without GCP.

## Caveat

The README and `docs/architecture.md` describe a fair amount of **intent, not
shipped behavior** — `docs/architecture.md:3` says so outright. GC of deleted
actors' snapshots, for example, is explicitly not implemented. Verify against
code before relying on any doc claim.
