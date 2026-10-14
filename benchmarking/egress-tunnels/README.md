# egress-tunnels

A scale benchmark for actor egress. It creates **A** actors, resumes **B** of
them, and has each resumed actor run a loop that sends HTTP requests to **C**
in-cluster endpoints. Every request takes the actor egress path:

```
actor ─▶ sandbox nftables redirect ─▶ atunnel (per-actor mTLS CONNECT, multiplexed over HTTP/2)
      ─▶ atenet-egress (Envoy + ext_proc policy check) ─▶ egress-target-<i> Service
```

The unit of measurement is the tunnel: one per actor and endpoint in keep-alive
mode. `benchmarking/nighthawk-egress` measures one actor's throughput; this
measures how many tunnels a gateway carries and what each one costs.

| Piece | What it is |
|---|---|
| `cmd/benchmarking/egress-actor` | The workload. `POST /start` starts the loop, `GET /stats` reports progress, `POST /stop` stops the loop and returns its final stats. |
| `cmd/benchmarking/egress-target` | A small HTTP and HTTPS server. One Deployment of it backs every endpoint Service. |
| `cmd/benchmarking/egress-tunnels` | The driver: `run` creates the actors, gives each one an egress policy, resumes B of them, runs the loops, and prints a report; `cleanup` deletes the actors; `certs` issues the target's certificate. |
| `internal/egresstunnels` | Endpoint naming, the actor's stats and histogram, and the target certificate, shared by all three. |
| `benchmarking/egress-tunnels/deploy.sh` | Deploys the target server, the endpoint Services and, for HTTPS, the gateway trust patch. |

## Endpoints and the egress policy

Endpoint `i` is the Service DNS name
`egress-target-<i>.egress-tests-targets.svc.cluster.local`, on port 80 for HTTP
and 443 for HTTPS. Each endpoint has its own Service, so it has its own name and
ClusterIP. All the Services select the same five `egress-target` pods.

Each Service `i` forwards to its own target ports, 10000+i and 12000+i,
never to a port another Service uses. On GKE Dataplane V2 a pod reaching a
ClusterIP is DNATed per packet, so the gateway picks source ports per
Service IP, and two Services forwarding to one pod port can hand it two
connections with the same 4-tuple. The target then resets the established
one, which the gateway reports as a 503. The target listens on the whole range
up to the 1024-endpoint cap (`--listen-ports`, `--tls-listen-ports`), so a
Service left from an earlier, larger deploy still reaches an open port.
`deploy.sh --print-targets` prints the Deployment and Services without
touching a cluster; `--target-image` replaces the ko reference, for
`--print-targets` and `--deploy` alike.

The naming scheme is fixed in code, in
[internal/egresstunnels/endpoints.go](../../internal/egresstunnels/endpoints.go).
The driver sends each actor only the count C in the `POST /start` body
(`{"endpoints": C, ...}`). The actor then holds a tunnel to each of endpoints
0 to C−1 and requests each of them once per period, on its own goroutine. The
driver builds the egress policy from the same names, so
the two always match.

An actor without an egress policy gets no egress at all. Right after creating
each actor, the driver gives it this policy:

```yaml
rules:
- http:          # no ports means port 80
    hostnames:
    - egress-target-0.egress-tests-targets.svc.cluster.local
    # ... up to egress-target-<C-1>
- https:         # no ports means port 443
    hostnames:
    - egress-target-0.egress-tests-targets.svc.cluster.local
    # ... the same names
```

Both schemes are always allowed, so switching `--scheme` changes no policy. A
rerun with a different C updates the existing policies.

## Prerequisites

- A cluster with Substrate installed, including the `atenet-egress` gateway.
- `source .ate-dev-env.sh`. `benchmarking/workloads/deploy.sh` reads
  `BUCKET_NAME` and `KO_DOCKER_REPO` from it, and this directory's `deploy.sh`
  reads `KO_DOCKER_REPO`. On macOS, `workloads/deploy.sh` needs bash 4 or
  newer and takes no `--context`.
- `kubectl` pointed at the cluster.

## Deploy

The actors run on the shared benchmark pool, with the `egress` actor template:

```bash
WORKLOAD_TEMPLATES=egress ./benchmarking/workloads/deploy.sh --deploy --worker-count 4 --worker-memory 8Gi
./benchmarking/egress-tunnels/deploy.sh --deploy --endpoints 100
```

The first command creates the `benchmark-worker` WorkerPool and the
`benchmark-workloads/egress` actor template from
`benchmarking/workloads/manifests/egress-template.yaml.tmpl`, and waits for its
golden snapshot. The second issues the target's HTTPS certificate into Secret
`egress-target-tls` the first time only, deploys the target server, and creates
the endpoint Services `egress-target-0` up to `egress-target-<C-1>` in namespace
`egress-tests-targets`. Add `--https` to also patch the egress gateway for HTTPS
runs (see [HTTPS](#https)).

Deploy at least as many endpoints as the largest `--endpoints` you plan to run.

Size the pool for B running actors. A worker hosts up to `--worker-memory` /
`--actor-memory` actors, and at most 1000. When the pool is full, `ResumeActor`
returns ResourceExhausted and the driver keeps retrying until `--resume-timeout`.

## In CI

The benchmark orchestrator (`benchmarking/automation`) runs an entry of
`type: egress-tunnels` end to end: it installs Substrate on the target
cluster, deploys the endpoints, adds the `egress` template to the shared pool,
runs the driver in a Job from the `egress-tunnels-test` image, judges the
report, and tears everything down. `tests.yaml` carries one smoke entry:

```yaml
- name: egress_tunnels_smoke_http
  type: egress-tunnels
  targetCluster: dev
  duration: 5m          # the loop time; the orchestrator adds its own margin
  workerCount: 10       # the parallel actors need somewhere to run
  egress-tunnels:
    actors: 100
    parallel: 100
    endpoints: 100
```

Knobs of the `egress-tunnels:` block, all optional:

| Knob | Default | Meaning |
|---|---|---|
| `actors`, `parallel`, `endpoints` | 100, 100, 100 | A, B and C above. B × C is the tunnel count; the shipped gateway caps it at 16,384 per replica, and the entry is rejected above that. |
| `scheme` | `http` | `http` or `https`; HTTPS makes the orchestrator patch the gateway's trust. |
| `connMode` | `keepalive` | `keepalive` or `new-conn`. |
| `requestInterval`, `requestTimeout` | `100ms`, `5s` | The driver's pacing and timeout. |
| `successRateThreshold` | 0.999 | The run fails below this fraction of successful requests. |
| `atespace`, `template` | `benchmark-workloads`, `egress` | Where the actors live. |
| `runnerCpu`, `runnerMemory` | `2`, `2Gi` | The runner pod's requests. |

The Job runs `runner.py`, which drives the binary with in-cluster addresses
and a projected ateapi token, writes `status.json` with the verdict, deletes
the actors, and uploads `status.json`, `report.json` and `logs.txt` to
`<dest>/runs/<name>/run_date=.../run_ts=.../run_tag=<commit>/`. The verdict
is the success rate against the threshold; latency and resource verdicts come
with the sampler in a later change.

## Run

```bash
go run ./cmd/benchmarking/egress-tunnels run --actors 1000 --parallel 10 --endpoints 10 --duration 5m
```

| Flag | Default | Meaning |
|---|---|---|
| `--actors` | 1000 | **A**: actors to create. Actors are named `egress-<i>`. A rerun reuses the ones that already exist. |
| `--parallel` | 1 | **B**: actors to resume, all running their loops at the same time: `egress-0` up to `egress-<B-1>`, so one warm-up run gives them all their own snapshots. |
| `--endpoints` | 10 | **C**: endpoints each loop holds a tunnel to, each requested once per period of C × `--request-interval`. At most 1024. |
| `--duration` | 5m | How long the loops run once they have all started. |
| `--scheme` | `http` | `http`, or `https` through the gateway's TLS interception. HTTPS needs `deploy.sh --https`; see [HTTPS](#https). |
| `--conn-mode` | `keepalive` | `keepalive` keeps one connection per endpoint. `new-conn` opens a new connection for every request. |
| `--request-timeout` | 5s | Timeout of one request. |
| `--request-interval` | 100ms | Each actor's pacing: one request per interval, spread over its endpoints, so each endpoint is requested once per C intervals. 0 sends back to back at full speed. |
| `--create-concurrency` | 32 | How many actors are created at the same time. |
| `--resume-timeout` | 5m | Per actor: how long to keep resuming, then waiting for it to answer. |
| `--progress-interval` | 30s | How often to print progress. 0 turns it off. |
| `--start-batch` | off | Start the loops in batches of this many actors, `--start-gap` apart, so one batch's opens fit the gateway's handshake budget; see [Opening throughput](#opening-throughput). 0 starts them all at once. |
| `--start-gap` | C × `--request-interval` | Pause between batches of loop starts, by default one opening window. Needs `--start-batch`. |
| `--stream-idle-timeout` | 5m | The gateway's CONNECT stream idle timeout. A `keepalive` run is refused when the per-endpoint period, C × `--request-interval`, is more than half of it; see [Pacing](#pacing). 0 skips the check. |
| `--settle` | off | Start the measured `--duration` only once every loop has opened all its endpoints with a success each and requested each of them twice, and no tunnel has been redialed for this long. `keepalive` only; needs `--progress-interval`; must cover one period, C × `--request-interval`. |
| `--settle-timeout` | 30m | How long to wait for `--settle` before measuring anyway. The report then says `NOT settled`. |
| `--atespace`, `--template` | `benchmark-workloads`, `egress` | Where the actors live and what they are created from. |
| `--output` | | Write the full report, including per-actor stats, as JSON to this file. Durations are in nanoseconds. |
| `--kubeconfig`, `--context` | | Which cluster to use. |
| `--api-endpoint`, `--router-url` | | When left empty, the driver port-forwards to the `api` and `atenet-router` Services. |
| `--api-token-file` | | With `--api-endpoint`, authenticate to ateapi with the token in this file, re-read on every call, such as a projected ServiceAccount token with audience `api.ate-system.svc`. When left empty, the driver mints an `ate-client` token. `cleanup` takes it too. |

### Pacing

Each loop runs one goroutine per endpoint, with at most one request in flight
per endpoint. Endpoint i sends its first request i intervals after the loop
starts, and every endpoint is then requested once per period of
C × `--request-interval`, so an actor sends one request per interval: with the
default 100ms, about 10 requests per second, and B=100 about 1,000 req/s in
total. With `--request-interval 0`, every endpoint is requested as fast as
responses come back. Use that only to find the gateway's limit, raising B
until latency and errors climb.

The gateway resets a CONNECT tunnel that stays quiet for its stream idle
timeout, 5 minutes by default, so the per-endpoint period, C ×
`--request-interval`, must stay under half of it: 1000 endpoints at 100ms is
100s, which fits. The driver refuses a `keepalive` run that does not. A
request that outlasts the period delays only its own endpoint's next request,
so how quiet a tunnel goes depends on that tunnel alone, not on how long the
rest of the fleet takes to open. A tunnel the gateway did reset shows up as a
redial: one more new connection in the actor's stats, with no error, because
the client retries the GET on a fresh connection.

With `--settle`, the measured window begins only once every loop reports all
C endpoints opened with at least as many successes, every endpoint requested
twice (`rounds` of 2 or more), and no redial for `--settle`. A redial is a
connection opened beyond one per endpoint, counted per loop. The report's
`settle` line gives when all loops had opened, when all had their second
round, how many redials happened before the window, and the longest gap any
endpoint saw; it flags a gap within 10% of the timer, since the period rule
counts no request latency. The `steady` line summarizes the measured window
alone: requests, rate, success, redials and latency quantiles between the
window's first and last complete poll. The `whole run` line above it still
covers everything from the first open. The loop timeline carries the same
counters per poll.

### Opening throughput

Every tunnel begins with a TLS handshake through the gateway, and the
gateway's handshake throughput, not the actors' pacing, bounds how fast a
fleet opens. B actors at interval I ask for B / I opens per second, for C × I:
1000 actors at 100ms ask for 10,000 opens/s, and one 88-thread c3 gateway on
main sustained about 2,800, so that wave took six minutes with the gateway
saturated throughout. Read a gateway's budget from a smoke run: the opens per
second while `opened` in the progress log is still below B × C, or B × C over
the length of the report's `opening` line.

`--start-batch` keeps the demand inside the budget: the driver starts the
loops in batches of that many actors, `--start-gap` apart, which defaults to
one opening window, C × I, so one batch has opened before the next starts.
With `--start-batch 250` at 100ms and C=1000, each batch asks for 2,500
opens/s over a 100s window, and four batches open 1000 actors in about 400s.
The first batch's window holds opens only, and later ones overlap with the
earlier batches' steady traffic. On a 44-thread gateway, halve the batch. The
report records each batch, and the `opening` line gives how long the whole
opening took, from the first batch's start until the last loop had its first
answer from its last endpoint, how many tunnels it opened, the redials polled
right after it, and the pure window of the first batch alone; the `opening`
and `opening-pure` phase marks in the JSON report cover the same spans.

The run goes through these phases: preflight (the C endpoint Services exist),
create (the A actors and their policies), resume (the B actors, until each one
answers through the router), start (all the loops at once, or in batches with
`--start-batch`), settle (only with `--settle`), steady (`--duration`, printing
progress), stop (collect the loops' stats), suspend. The opening is not a
phase the driver waits for: its marks are computed from the loops' stats once
they have stopped.
After Ctrl-C, the loops that started are still stopped and the actors
suspended.

### HTTPS

`--scheme https` sends every request over TLS, through the gateway's TLS
interception path: the gateway terminates the actor's TLS with a certificate
it mints for each name, checks each decrypted request against the policy, and
opens its own TLS connection to the target. Most agent egress is HTTPS, so this
path is the one that matters at scale.

| | TLS #1: actor → gateway | TLS #2: gateway → target |
|---|---|---|
| Certificate | minted for the name by the gateway's sdsmint, signed by the gateway's MITM CA | the target's own `*.egress-tests-targets.svc.cluster.local`, from Secret `egress-target-tls` |
| Why it's trusted | the actor template projects the `egress-mitm.ate.dev` trust bundle and points `SSL_CERT_FILE` at it | `deploy.sh --https` patches the gateway so its root bundle includes the target's CA |

**The gateway patch.** The gateway verifies every certificate it gets from a
destination against the public roots in its Envoy image, and the install has no
way to add a CA. `--patch-gateway`, or `--deploy --https`, works around that on
test clusters by patching the `atenet-egress` Deployment with an init container
that appends the target's CA to that root bundle. It changes no `envoy.yaml`,
and it rolls the gateway. The CA can vouch only for the endpoint names: it is
name-constrained to their domain, and its private key was thrown away after it
signed the target's certificate. Reinstalling Substrate drops the patch; run
`--patch-gateway` again afterwards, and `--unpatch-gateway` or `--delete` to
remove it. `run --scheme https` checks the patch first and stops with a hint
if it is missing, stale or still rolling out.

**The actor deliberately does not trust the target's CA.** If the gateway ever
passed TLS straight through instead of intercepting it, the actor would fail
with `tls: unknown authority`. So a run with no errors shows that every request
went through decrypt, policy check and re-encrypt.

### Why the loop waits for `/start`

Every actor restores from the template's golden snapshot, which is taken after
the actor boots. If the loop started at boot, it would run in the golden actor,
which has no egress policy, so every request would fail and those failures
would be frozen into every actor's snapshot. Starting on request keeps the
snapshot idle, lets the driver start all B loops together, and lets C and the
connection mode change without rebuilding the template.

## Reading the report

```
== egress-tunnels: actors=100 parallel=1 endpoints=10 scheme=http conn-mode=keepalive request-interval=0s duration=2m0s
create     100/100 ok in 386.4ms (0 actors reused, 0 policies updated)
resume     1/1 ok in 396.4ms
           resume p50 298.8ms p99 298.8ms max 298.8ms; ready p50 97.6ms p99 97.6ms max 97.6ms
start      1/1 ok in 32.9ms
loop       2m0.1s: 99142 requests, 825.7 req/s, 100.000% success, 10 new connections
latency    p50 1.26ms p90 1.41ms p99 2ms p99.9 3.98ms max 42.9ms (mean 1.2ms)
dns        10 lookups, p50 17.8ms p99 32.2ms max 32.2ms
           req/s per actor: min 825.7 median 825.7 max 825.7
stop       1/1 ok in 33.9ms
suspend    1/1 ok in 398.9ms
```

When requests fail, the report adds an `errors` line with counts per class,
and a line naming the endpoints with the most errors. Once every loop has
opened, an `opening` line follows `start`; with `--settle`, a `settle` line
and a `steady` line bracket the `whole run` line (see [Pacing](#pacing)).

- **Latency** is measured inside the actor, for successful requests only. It
  includes the DNS lookup when the request needed one.
- **New connections** tell the two modes apart. In keepalive mode the count is
  about B×C. In new-conn mode it is about one per request.
- **DNS** counts lookups. Go does not cache DNS, so every new connection
  resolves the name through the sandbox DNS relay and the cluster DNS.
- **TLS** (HTTPS only) counts completed handshakes between the actor and the
  gateway, with their times.
- **Errors** are grouped into classes: `timeout`, `connection refused`,
  `connection reset`, `EOF`, `dns: …`, `HTTP <code>`, and `other: …`. An
  `HTTP 403` means the gateway denied the request. In HTTPS mode, `tls: …`
  classes name what failed verification, a `tls handshake: ` prefix marks a
  failure partway through a handshake, and `HTTP 503` usually means the
  gateway could not verify the target: check that the gateway is patched.

Resume latency runs from the first `ResumeActor` attempt until it succeeds,
retries included; ready latency from then until the actor first answers through
the router. Actors that never ran restore from the template's golden snapshot,
and actors an earlier run suspended restore from their own, so one warm-up run
of B actors makes every later run of at most B restore from own snapshots.

## Limits to expect

- **Keep-alive mode holds one tunnel per actor and endpoint.** atunnel carries
  the tunnels as streams of a few HTTP/2 connections to the gateway, but inside
  the gateway each tunnel is still one connection in the `mitm_internal`
  cluster, whose circuit breakers cap tunnels per replica (16,384 as shipped;
  `max_connections` and `max_pending_requests` in the `atenet-egress`
  ConfigMap). Past the limit, new CONNECTs wait in the pending queue until
  atunnel's tunnel-open deadline withdraws them and the actor sees a timeout;
  when the queue is full too, the gateway answers 503 and the actor sees `EOF`
  or `connection reset`. `cluster.mitm_internal.upstream_cx_overflow` and
  `upstream_rq_pending_overflow` on the gateway's admin port say which limit
  applied.
- **The destination clusters have their own limit.** `egress_forward_proxy` and
  `egress_forward_proxy_cleartext` use Envoy's default of 1,024 connections
  unless the manifest says otherwise; bursts of simultaneous requests to many
  destinations push those counts up, and the actor sees `HTTP 503`.
- **HTTPS costs more per connection.** Session resumption is off, so new-conn
  mode does a full handshake with the gateway on every request, and the
  gateway's minted certificates last 15 minutes, so longer runs mint them
  again. Expect more gateway CPU and memory than the same HTTP run.
- **New-conn mode loads the control plane.** Every CONNECT makes the gateway
  authenticate the actor through ateapi.
- **Confirming the gateway carried the traffic.** `kubectl -n ate-system logs
  deploy/atenet-egress -c envoy` shows JSON lines with the actor's SPIFFE ID,
  an `egress-target-<i>` authority, and `"leg":"cleartext"` (HTTP) or
  `"leg":"mitm"` (HTTPS). The gateway samples those logs, so at high rates
  only failures and a sample of successes appear.

## Cleanup

```bash
go run ./cmd/benchmarking/egress-tunnels cleanup --actors 10000   # the largest --actors any run used
./benchmarking/egress-tunnels/deploy.sh --delete
WORKLOAD_TEMPLATES=egress ./benchmarking/workloads/deploy.sh --delete
```

`cleanup` deletes `egress-0` up to `egress-<actors-1>` in any state, each with
its egress policy. `deploy.sh --delete` removes the gateway patch and the
endpoints; `--unpatch-gateway` removes only the patch. The workloads script
removes the template and the `benchmark-worker` pool, which the other workload
templates share; skip that step while other benchmarks use the pool.
