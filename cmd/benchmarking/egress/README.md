# benchmark-egress

`benchmark-egress` is the workload of the egress scale test. It holds many
CONNECT tunnels open through the egress gateway at a fixed pace and reports what
each tunnel saw, so that a run can price what one tunnel costs the gateway.

## 1. What the benchmark measures

The benchmark sizes one `atenet-egress` replica for a target fleet: A actors,
each reaching C destinations. It reports what one tunnel and one request cost
the gateway, so that the replica count for a fleet follows from fleet size,
node shape, and these costs.

Three terms:

- **Tunnel.** One kept-alive CONNECT from one actor to one destination
  Service, opened by the actor's first request and reused by every later one.
  It is the unit the gateway's cost scales with: a fleet of A actors over C
  Services holds A × C tunnels.
- **Blended.** The window from the first actor's start until the last actor
  has opened its last tunnel. New tunnels are being set up while tunnels
  already open carry repeat requests, so the gateway's CPU mixes setup with
  ordinary requests.
- **Settled.** The period, after the blended window, in which every tunnel
  has been reused and every request reuses one. Nothing is set up; the
  gateway's cost is per request.

The settled period gives the cost of one request directly. The blended window
gives the cost of setting up one tunnel once its requests are priced at the
settled rate.

A cell is one fleet size and one protocol. It reports:

- **Settled req/s per core.** Throughput per gateway core once every tunnel
  exists. Its inverse is the CPU cost of one request.
- **Gateway CPU per tunnel opened.** CPU milliseconds to set up one tunnel, the
  cost of its requests taken out.
- **Envoy heap per tunnel.** KiB of heap one held tunnel adds.
- **Settled p50, p99, p99.9 and success rate.** Latency of repeat requests as
  the actor sees it, and the share that got HTTP 200. Flat across fleet sizes
  means the gateway was not saturated while the costs were read.

The costs price held tunnels, not tunnels that reconnect. Section 5 lists the
checks a cell must pass before its numbers count.

Per-core and per-tunnel figures do not depend on the node size or the replica
count. Memory sizes as tunnel count times heap per tunnel, against one node's
memory. CPU sizes as request rate times CPU per request, plus the rate of
tunnel opens times CPU per tunnel opened, against one node's cores. The
replica count for a fleet is the larger of the two.

## 2. Topology

```mermaid
flowchart LR
  D[driver] --> A[actor]
  A --> G[egress gateway]
  G --> S0[Service 0]
  G --> S1[Service 1]
  G --> S2[Service 2]
  S0 --> T[target Deployment]
  S1 --> T
  S2 --> T
  G --> M[collector]
  M --> P[Prometheus]
  D --> P
```

- **`benchmark-egress actor`** runs inside each Actor. The driver controls it
  through the atenet router on port 80: `/readyz`, `/start`, `/stats`,
  `/stop`. Once started, it holds one CONNECT tunnel per endpoint through the
  gateway and sends a `GET` on each at a fixed pace, and it exports what it
  measures as OpenTelemetry metrics over OTLP, through the same gateway, to the
  collector named by its environment. The actor's egress policy must name every
  endpoint hostname and the collector. Section 3 has the workflow, section 4
  the metrics.
- **Egress gateway.** One `atenet-egress` replica in its sdsmint deployment,
  the system under test. It carries one CONNECT tunnel per actor and
  destination, plus one per actor to the collector, terminates the actor's TLS
  on HTTPS runs, and checks the egress policy on every request. On HTTPS runs
  the actor trusts the gateway's MITM CA through `SSL_CERT_FILE`, and the
  gateway re-originates TLS to the target and must trust the target CA.
- **Endpoints.** C ClusterIP Services, `egress-target-0` to
  `egress-target-<C-1>`, in namespace `egress-tests-targets`. Each is a
  distinct hostname, so each is its own tunnel.
- **`benchmark-egress target`** runs as one Deployment behind all C Services.
  It answers every `GET /` with a 200 and a small fixed body, over plain HTTP
  and, given a certificate, HTTPS. Each Service maps to its own backend port,
  set with `--listen-ports` and `--tls-listen-ports`. The driver issues the
  HTTPS certificate for `*.egress-tests-targets.svc.cluster.local` from a
  throwaway CA that is name-constrained to
  `egress-tests-targets.svc.cluster.local`.
- **Collector and Prometheus.** An OpenTelemetry collector that receives the
  actors' OTLP on port 4317 and serves it as a Prometheus exposition, and a
  Prometheus that scrapes it and the gateway node's cAdvisor. Both belong to
  the benchmark's deployment, not to the actor. The actor only needs the
  collector's address.
- **Driver.** Sets up the run, starts the A actors, posts `/start`, polls
  `/stats` for the gates, records the instants that bound each window, and
  reads every result from Prometheus with the queries of section 5.
