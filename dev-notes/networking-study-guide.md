# Networking Study Guide

A ground-up path to understanding Substrate's networking, written for someone
who does **not** have a networking background. Companion to
[architecture-overview.md](architecture-overview.md).

Baseline: `main` @ `55f16fa9`, planned 2026-08-03/04.

---

## The core idea of this guide

Don't study "networking." It's an enormous field and most of it is irrelevant
here. Instead, learn to **fully explain one command** — the one from
`README.md:113`:

```bash
curl -X POST -H "Host: my-counter-1.demo.actors.resources.substrate.ate.dev" \
  -i http://localhost:8000/
```

That line contains the whole thesis: a name, a header, a port, a proxy, and a
sleeping actor that wakes because you asked for it. Everything in Substrate's
networking is either *inside* that command or *downstream* of it. The
curriculum is: understand each piece, then follow it inward.

### Explicitly skip

You will not need these: BGP and routing protocols, subnetting arithmetic
beyond "what is a /30", IPv6, TCP congestion control, load-balancer algorithms,
service-mesh theory, TLS cipher internals.

### The four planes

Networking here is not one system, it's four, meeting at the worker pod:

| Plane | Code | Size | What it does |
|---|---|---|---|
| North-south ingress | `cmd/atenet/` | ~5.5k lines | DNS → Envoy → ext_proc → `ResumeActor` → mTLS to worker |
| In-pod actor netns | `internal/ateomnet/net.go` | 597 lines | veth pair + nftables inside the sandbox |
| Actor egress | `internal/atunnel/` | ~740 lines | transparent intercept → CONNECT tunnel out |
| Policy & identity | `cmd/atecontroller/internal/controllers/networkpolicy_controller.go`, `podcertcontroller`, `ActorIdentity` | — | who may talk to whom, with what cert |

---

## Phase 0 — Run it once (½ day) — **not recommended on macOS**

The intent is a motivating win: stand up a local kind cluster, curl an actor,
watch it wake, *before* understanding any of it.

> **Skip this on an 8 GB Intel Mac.** The ROI is poor and the failure modes are
> expensive. Phase 0 teaches almost nothing that Phase 1 doesn't teach better,
> and Phase 1 needs no infrastructure at all.
>
> Full reasoning, the exact install sequence, and the ranked risk list are in
> [Appendix A](#appendix-a--phase-0-on-macos). Read it before deciding.

**If you have a 16 GB Linux box or cloud VM**, do it there — it runs natively,
KVM is actually available, and the microVM path works too:

```bash
hack/create-kind-cluster.sh
hack/install-ate-kind.sh --deploy-demo-counter
kubectl ate create atespace demo
kubectl ate create actor my-counter-1 -a demo --template=ate-demo-counter/counter
kubectl port-forward -n ate-system svc/atenet-router 8000:80
# then the curl above
```

**Otherwise: start at Phase 1.** Nothing downstream depends on Phase 0.

---

## Phase 1 — The six concepts (1 week)

These six unlock everything. Each has a hands-on experiment that runs natively
on macOS.

### 1. IP + port = an address. A TCP connection = a pipe between two addresses.

Not "a message" — a persistent two-way byte pipe. Everything else builds on it.

```bash
nc -l 9000        # terminal 1
nc localhost 9000 # terminal 2 — type in both directions
```

That's TCP. There is nothing else to it.

### 2. DNS turns a name into an IP. That is its entire job.

It does not route, load balance, or know anything about HTTP.

```bash
dig google.com
```

Later, against a running cluster, notice Substrate's DNS returns **the same IP
for every actor name** — it's a wildcard. **DNS carries zero per-actor
information here.** That realization matters more than the experiment.

### 3. HTTP is text over TCP, and `Host` is how one IP serves many names.

*The* concept for Substrate. A server can't tell which site you wanted from the
IP alone — the client must say so in the `Host` header.

```bash
curl -v http://example.com   # read the raw request block
```

Re-run the Phase 0 curl with a different `Host` and it routes to a different
actor: same IP, same port, different destination. **That is Substrate's routing
key.**

### 4. A proxy accepts one TCP connection and opens another.

It reads your request, decides where it goes, connects there, shuttles bytes.
Envoy is a very sophisticated version of exactly this.

> **Write a 20-line TCP proxy in Go.** Highest-value hour in this guide. After
> it, Envoy stops being magic.

### 5. TLS encrypts; mTLS also proves *both* sides' identity.

Normal HTTPS: client verifies server. mTLS: server also verifies client, via a
certificate. Substrate uses this so a worker only accepts traffic from the real
router. Concept only for now — hold "a cert is a verifiable name badge."

### 6. A network namespace is a private copy of the whole network stack.

**TL;DR**

- **Network namespaces** — a Linux netns is a fully isolated replica of the
  network stack: its own interfaces, routing tables, firewall rules, and socket
  space.
- **`veth` pairs** — namespaces are isolated by default, so a virtual Ethernet
  pair acts as a two-ended patch cable joining a private namespace back to the
  host.
- **The sandbox boundary** — a fresh namespace (`ip netns add test`) is empty:
  no routes, and only a loopback interface that starts `DOWN`. That emptiness
  *is* the boundary.
- **Why the host namespace is privileged** — it owns the physical NICs, hosts
  the bridges and forwarding rules that let namespaces reach each other, and is
  the only vantage point with the kernel handles to wire new sandboxes up.

**macOS cannot do any of this** — netns is a Linux kernel feature. Get a Linux
shell cheaply:

```bash
docker run --rm -it --privileged ubuntu bash
apt update && apt install -y iproute2
ip netns add test
ip netns exec test ip addr   # sees nothing — no eth0, no routes
```

That isolation is the actor sandbox boundary.

> **Full treatment in [linux-netns-primer.md](linux-netns-primer.md)** — what the
> kernel clones, `veth` wiring by hand, bridges, and the command reference.
> Read §1–§3 now; the rest is Phase 5 material.

**One Substrate-specific thing to carry forward.** The primer's diagram shows the
*conventional* layout: a bridge fanning one uplink out to many namespaces, which
is roughly what a CNI does for pods. Substrate's actor netns does **not** use a
bridge — `ateom0` ↔ `eth0` is a point-to-point `169.254.17.0/30` link with no
switch in the middle (Phase 5). Learn the bridge model because it's the ambient
convention you'll read everywhere, then notice Substrate declined it.

> **Two unrelated things here are called "host":** the HTTP `Host:` header
> (concept 3, Substrate's routing key) and the host *network namespace*
> (primer §5). They share a word and nothing else.

**Checkpoint:** explain in your own words why the HTTP `Host:` header is
required for Substrate to work at all — and, separately, why the daemon that
builds an actor's netns cannot itself live inside one.

---

## Phase 2 — Kubernetes networking, minimum viable (2-3 days)

Four things:

| Concept | One-line version |
|---|---|
| Pod IP | every pod gets its own IP; containers in it share a netns |
| Service / ClusterIP | a stable virtual IP load-balancing to a changing set of pods |
| CoreDNS | resolves `svc.namespace.svc.cluster.local` to a ClusterIP |
| NetworkPolicy | a firewall rule expressed as "which pods may talk to which" |

**Then the part that matters most — the negative space.** Substrate uses almost
none of this for actors. There is **no Service per actor, no Endpoint per
actor, and actors are not Pods.** Kubernetes provides exactly one ClusterIP
(the router), one CoreDNS stub domain, and one NetworkPolicy per WorkerPool.
The per-actor hot path is entirely custom.

Internalize the negative space and the architecture stops looking weird.

---

## Phase 3 — Read the ingress path (1 week)

> **Full treatment in [phase3-ingress-path.md](phase3-ingress-path.md)** — the
> ten-hop map, a day-by-day reading order, the twelve steps of
> `handleRequestHeaders`, exercises against the existing tests, and a ten-question
> checkpoint. Use the list below as the index; use that document as the guide.

Follow your curl inward:

1. `cmd/atenet/README.md`, `cmd/atenet/internal/router/README.md`,
   `cmd/atenet/internal/dns/README.md` — short, plain-language.
2. `cmd/atenet/internal/dns/corefile.go` — the wildcard regex from experiment 2,
   in code.
3. `cmd/atenet/internal/router/extproc_in.go` — where `Host` becomes an actor
   name. **The heart of the system.** Start here if you read only one file.
4. `cmd/atenet/internal/router/resumer.go` — the call that wakes the actor, plus
   its cache. Cache invalidation during rescheduling is where the interesting
   bugs live (`docs/threat-model.md:81`).
5. `internal/atunnel/server.go` — the worker end accepting the mTLS connection,
   which refuses traffic for an actor not currently assigned there.

Defer `cmd/atenet/internal/router/xds.go` (828 lines) to Phase 4 — it's Envoy
config, unreadable without Envoy vocabulary.

**Checkpoint:** narrate every hop from `curl` to actor, out loud, without notes.

---

## Phase 3.5 — Request parking (½ day)

`docs/request-parking.md` + `cmd/atenet/internal/router/parking.go`. The density
thesis meeting reality: what happens when the worker pool is saturated. Small,
self-contained, explains a constraint Phase 3 leaves implicit.

---

## Phase 4 — Envoy vocabulary (1 week)

Three terms, in this order:

1. **xDS** — Envoy's config API. Instead of a config file, a server streams
   config to it. That server is `cmd/atenet/internal/router/xds.go`.
2. **ext_proc** — a filter handing each request to *your* gRPC service, which
   rewrites headers before Envoy routes. This is how Substrate injects a
   destination.
3. **`ORIGINAL_DST` cluster** — a cluster whose destination is read from the
   request at runtime rather than from a config list. Combined with ext_proc,
   **the routing table is one RPC deep.**

Now `xds.go` is readable. Also skim
`cmd/atenet/internal/router/envoyrunner.go` for how Envoy is supervised.

**Checkpoint:** explain how Envoy sends traffic to an IP appearing in no config
file.

---

## Phase 5 — Inside the sandbox (1 week)

Hardest and most interesting. `internal/ateomnet/net.go`, 597 lines.

- The veth pair `ateom0` ↔ `eth0` on the point-to-point `169.254.17.0/30` link
  (link-local by design).
- The `ateom_actor` nftables table — the actor's firewall.
- **The question to hold throughout:** the netns is rebuilt per activation but
  the snapshot is not. What does a restored actor believe about its network?

**Prerequisite: all of [linux-netns-primer.md](linux-netns-primer.md)** — not
just the §1–§3 you read at Phase 1. Do §3.1 (wire a `veth` pair by hand) and
keep §6's command reference open. Reading `net.go` without having built a
namespace yourself is the way to bounce off this phase.

Note that §4's bridge layout is *not* what happens here: the `/30` above is
point-to-point, no switch. Ask why as you read.

---

## Phase 6 — Egress and policy (3-4 days)

- `internal/atunnel/egress.go` — how an actor's outbound connection is
  intercepted and tunneled. Note `egressActivation`: the proxy is long-lived
  across activations but only carries traffic while an actor is assigned. That
  lifecycle mismatch is exactly the "state leaks across worker reuse" risk at
  `docs/threat-model.md:109`.
- `internal/atunnel/original_dst_linux.go` — `SO_ORIGINAL_DST`, how a
  transparently redirected socket recovers its pre-NAT destination.
- `cmd/atecontroller/internal/controllers/networkpolicy_controller.go:119` —
  egress is deliberately **not** managed by Kubernetes NetworkPolicy yet.
  Compare against `docs/threat-model.md:99`, which requires default-deny egress.
  **A live gap, and a good first contribution area.**

---

## Critical dependencies

**Hard, load-bearing:**

- `github.com/envoyproxy/go-control-plane` v0.14.0 + `/envoy` v1.37.0 — xDS server
- Envoy itself, run as a sidecar process
- `github.com/google/nftables` — actor firewall rules
- `github.com/vishvananda/netlink` + `netns` — veth and namespace manipulation
- CoreDNS, run as a managed Deployment
- gRPC — every control hop

**Soft / at the edges:** the cluster CNI (Substrate makes few assumptions, but
NetworkPolicy *enforcement* depends on it), kube-dns or GKE DNS for stub-domain
integration.

**Watch item:** `--atenet-router=agentgateway` is a second, static-ConfigMap
dataplane alternative to Envoy, added recently (commit `9e74b1d7`). Study the
Envoy path first — agentgateway is newer and less exercised.

---

## Key concepts, ranked

1. **Request-driven activation** — routing and scheduling are the same operation.
2. **Late-bound destination** — `ORIGINAL_DST` plus an ext_proc-written header
   means the routing table is one RPC deep, not preconfigured.
3. **Host-decoupled identity** — the cert belongs to the actor, not the worker.
   mTLS is what makes actor mobility safe.
4. **Two-tier state** — DNS and Envoy config are static; all per-actor state
   sits in Valkey behind one RPC.
5. **Namespace-per-actor inside a pod** — a second isolation layer *below* the
   Kubernetes pod boundary. The part with no Kubernetes analogue.

---

## Timeline

**5-6 weeks part-time.** Phases 1-2 (fundamentals) are ~2 weeks and are the ones
people skip and then flounder. Don't skip them: the Substrate code isn't hard
once you have the vocabulary, and is nearly impossible before.

Compressed path: Phases 1 and 3 alone (~2 weeks) make you able to discuss the
ingress path competently. Phases 4-6 are what you need to change code.

---

## Appendix A — Phase 0 on macOS

Why the local cluster is not recommended on an 8 GB Intel Mac, recorded so the
decision doesn't have to be re-derived.

### The nesting problem

macOS has no Linux kernel, so the stack is six layers deep:

```
macOS
 └─ Lima VM (Linux, via Virtualization.framework)   ← Colima manages this
     └─ dockerd
         └─ kind node container ("kind-control-plane")
             └─ containerd + kubelet + etcd + apiserver
                 └─ worker Pod
                     └─ gVisor sandbox (runsc)
                         └─ actor
```

- **Colima** = "Containers on Lima". Boots a Linux VM, runs dockerd inside,
  exposes the socket at `~/.colima/default/docker.sock` and registers a
  `docker context`. `kind` uses that socket like any other Docker client and has
  no idea a VM is involved.
- **kind** = "Kubernetes IN Docker". Each *node* is a Docker container, not a VM.

Consequences:

1. **`/dev/kvm` is unavailable.** `hack/create-kind-cluster.sh:53` probes for it;
   KVM inside a Lima VM needs nested virtualization, which Intel Macs under
   Virtualization.framework don't provide. `ateom-microvm` stays disabled; gVisor
   still works (the script says so explicitly). Expected, not a failure.
2. **Port forwarding is automatic** — the registry's `127.0.0.1:5001` inside the
   VM is forwarded to macOS, so `ko` can push to `localhost:5001`.
3. **`docker exec kind-control-plane bash` is your Linux shell** — how you'd run
   `ip netns` / `nft` for Phase 5. (A plain `docker run --privileged` container
   does this far more cheaply.)
4. **`kubectl port-forward` is unaffected** — it tunnels over the Kubernetes API,
   not Docker's port machinery.

### What the install actually does

| Stage | Time | What |
|---|---|---|
| 1. Colima boot | 2-5 min | ~1.5 GB Lima image, boots VM + dockerd |
| 2. `create-kind-cluster.sh` | 3-6 min | local `registry:3`; KVM probe; kind node image (~1 GB); cluster with `ClusterTrustBundle`, `ClusterTrustBundleProjection`, `PodCertificateRequest` feature gates; `proxy_arp=1`; registry wiring |
| 3. PKI generation | 1-2 min | **four CAs** — valkey certs, JWT authority pool, actor-ID CA pool, podcert controller CAs |
| 4. Build + deploy | **15-30 min** | `ko` compiles all 9 `cmd/` binaries for `linux/amd64`, pushes to local registry; applies kind overlay |
| 5. Counter demo | 5-10 min | ActorTemplate + golden snapshot; `atelet` downloads `runsc` from `gs://gvisor/releases/release/20260622/x86_64/runsc` |

**Realistic total: 30-50 minutes if nothing goes wrong.**

Two things worth noting even if you never run it:

- The **`proxy_arp=1`** line exists because gVisor's network stack needs proxy
  ARP for pod-to-pod loopback inside kind. A real Phase 5 detail surfacing on
  day one.
- The **four CAs in stage 3** show that mTLS isn't bolted on — it's a
  precondition for the system booting at all.

The kind overlay (`manifests/ate-install/kind/kustomization.yaml`) deploys
ate-api-server, ate-controller, atelet, atenet-dns, atenet-router+Envoy, valkey,
pod-certificate-controller, **rustfs** (local S3 stand-in for snapshots, plus an
`aws-cli` job to create the bucket), **otel-collector**, **Prometheus**, and
**Jaeger** — roughly 15 container images totaling several GB.

### Ranked risks

1. **Memory — 8 GB host. The likely killer.**
   Budget: macOS + editor ~3 GB; Colima VM 5 GB, inside which the kind control
   plane takes ~1.5 GB and the 12 workloads another ~2-3 GB, before any worker
   pods. Concurrently, `ko` compiles 9 Go binaries *on the host* at 2-4 GB peak.
   *Symptom:* OOM-killed pods, `CrashLoopBackOff`, or the demo missing its 300s
   readiness timeout.
   *Aggravating factor:* the manifests set almost no memory requests (only two
   `memory:` lines in all of `manifests/`), so Kubernetes won't refuse to
   schedule — it will just thrash.
   *Mitigation:* drop `prometheus.yaml` and `otel-collector.yaml` (which also
   carries Jaeger) from the kind overlay. Neither is needed for networking.

2. **Nested checkpoint/restore is unproven.** runsc inside a container inside a
   VM on an Intel Mac. Without KVM, runsc falls back to its systrap/ptrace
   platform — functional but slow. Substrate's entire premise is
   checkpoint/restore of those sandboxes, and the project's paved path is GKE.
   *Symptom:* actors create but never resume; snapshot operations hang.
   *This risk is unsized.*

3. **Disk.** ~61 GB free vs. a 40 GB sparse VM disk + ~15 images + Go build
   cache. Probably fits, not comfortably.

4. **Network egress.** Pulls from docker.io, gcr.io, and
   `storage.googleapis.com` (runsc). Any proxy or filtering breaks stage 4 or 5.

5. **Time-to-signal.** Failure arrives ~25 minutes in, during the slowest stage.

### Better options

- **A 16 GB Linux cloud VM.** Runs natively, no nested virtualization, KVM
  actually available so the microVM path works too. The recommended route.
- **`docker run --rm -it --privileged ubuntu bash`** for the Phase 1 and Phase 5
  Linux experiments — a fraction of the resources, no kind, no 30-minute build.

### Tooling notes (macOS)

- `kind` and `ko` **self-bootstrap** via `go tool` from `hack/tools/` — see
  `hack/run-tool.sh`. No manual install needed.
- `kubectl` is **not** bootstrapped; the scripts call it bare. Install it.
- `jq` is required (`hack/install-ate.sh:499`).
- Teardown: `hack/delete-kind-cluster.sh`, then `colima stop` / `colima delete`.
