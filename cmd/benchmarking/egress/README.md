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
```

- **`benchmark-egress actor`** runs inside each Actor. The driver controls it
  through the atenet router on port 80: `/readyz`, `/start`, `/stats`,
  `/collect`, `/stop`. Once started, it holds one CONNECT tunnel per endpoint
  through the gateway and sends a `GET` on each at a fixed pace. The actor's
  egress policy must name every endpoint hostname. Section 3 has the workflow.
- **Egress gateway.** One `atenet-egress` replica in its sdsmint deployment,
  the system under test. It carries one CONNECT tunnel per actor and
  destination, terminates the actor's TLS on HTTPS runs, and checks the egress
  policy on every request. On HTTPS runs the actor trusts the gateway's MITM
  CA through `SSL_CERT_FILE`, and the gateway re-originates TLS to the target
  and must trust the target CA.
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
- **Driver.** Sets up the run, starts the A actors, posts `/start`, polls
  `/stats`, collects `/collect` and the gateway's stats, and merges the answers
  into the results of section 5.

## 3. What the actor does

The actor is idle or running. It boots idle and sends nothing until `/start`:
every actor restores from a golden snapshot, and a loop started at boot would
have run in the golden actor, which has no egress policy. A restarted actor is
idle again with nothing kept.

| Route | Idle | Running |
|---|---|---|
| `GET /readyz` | 200 | 200 |
| `POST /start` | 204 | 409 |
| `GET /stats` | 404 | 200, section 4 |
| `POST /collect` | 404 | 200, section 4 |
| `POST /stop` | 404 | 200 with the final report once every goroutine has exited |

Every answer carries `Benchmark-Egress-Protocol: 1`; the driver refuses an
actor without it as an image from before this contract.

`POST /start` takes one JSON object, at most 1 MiB; unknown fields are ignored.

| Field | Meaning | 400 when |
|---|---|---|
| `endpoints` | C, the number of endpoints. | Missing, under 1, or over 1024. |
| `intervalMs` | Pacing. Endpoint *i* opens *i* intervals after `/start`; each endpoint is then requested once per period of C × `intervalMs`. Zero sends every endpoint's requests back to back. | Negative. |
| `offsetMs` | Delay before endpoint 0 opens; endpoint *i* opens `offsetMs` + *i* × `intervalMs` after `/start`. Lets a driver give each actor its own phase, so a fleet started at once does not send in lockstep; see below for how a driver picks it. | Negative, or more than C × `intervalMs`. |
| `scheme` | `http` or `https`; empty means `http`. | Any other value. |
| `requestTimeoutMs` | Bound on one request, handshake included; zero means 5000. | Negative. |

```json
{"endpoints": 1000, "intervalMs": 100, "scheme": "https"}
```

204 means the goroutines exist, not that any request has been sent. A 400
carries a one-line reason. `/start` does not probe the endpoints: a missing
egress policy or Service shows up as errors in `/stats`, not as a failed
`/start`. The instruments are created at `/start` and released at `/stop`, so
every counter counts from `/start` and a later `/start` begins from zero.

The loop then runs one goroutine per endpoint:

1. Wait offset + *i* × interval, so C handshakes do not reach the gateway at
   once.
2. Send the first `GET` to the endpoint. When it completes, success or not, the
   endpoint counts as opened. When the last endpoint opens, the actor stamps
   `openingDone`.
3. Send one `GET` per period on a ticker started when the open starts. At most
   one request is in flight per endpoint. A request that outlasts the period
   delays only its own endpoint, whose next request starts as soon as the slow
   one returns.
4. Stop on `POST /stop`, which waits for every goroutine and answers with the
   final report of section 4. A request cut off by the stop is not counted.

The client keeps one connection per endpoint and never closes it for idling,
so a redial, one more in `newConns`, means the gateway cut the tunnel or a
request on it timed out. The gateway resets a tunnel that stays quiet for its stream idle
timeout, 5 minutes by default, so keep the per-endpoint period, C ×
`intervalMs` between two requests on the same endpoint, under it.

Only an actor's phase modulo `intervalMs` matters to the fleet: once started
it sends at `/start` + `offsetMs` + k × `intervalMs`. A driver gives actor k
of A `offsetMs` = ⌊k × `intervalMs` / A⌋, which spreads the phases evenly; a
uniform draw from [0, `intervalMs`) does about as well but needs its seed
recorded. Four actors with `intervalMs` 100:

| Offsets | Fleet sends in each interval |
|---|---|
| 0, 0, 0, 0 | 4 at 0 ms |
| 0, 25, 50, 75 | 1 at 0, 25, 50, 75 ms |

The offset lives on the actor because the router's fan-out of `/start` sets
each actor's real phase, and the driver does not control it.

With these rules an actor sends one request per interval throughout the run,
and holds C tunnels.

## 4. What the actor reports

The actor answers every read with one JSON object, `Stats`. `GET /stats`
carries the counters alone, cumulative since `/start`, a few hundred bytes:
poll it. `POST /collect` carries the counters and the four distributions
recorded since the previous `/collect`, or since `/start` for the first one.
Each sample is served once, so the driver is the only collector; a second one
would steal windows. `POST /stop` answers like a `/collect` for what is left,
then the loop is gone. Durations are Go `time.Duration` values, nanoseconds.
The per-endpoint list comes only with `?endpoints=1`, on any of the three.

| Field | What it holds | Used for |
|---|---|---|
| `scheme` | `http` or `https`. | The scheme in use. |
| `elapsed` | Time since `/start`. | Rates between two polls. |
| `requests`, `successes` | Requests finished; HTTP 200 with no error is a success. | Request rate and success rate. |
| `newConns` | Requests sent on a freshly dialed connection. | One per endpoint is the tunnel. Any more is a reconnect after the gateway cut a tunnel or a request on it timed out. |
| `opened` | Endpoints whose first request has completed. | Progress of this actor's opens. |
| `openingDone` | Elapsed when the last endpoint opened; zero until then. | Added to this actor's `/start` time, the latest across actors ends the blended window. |
| `rounds` | Requests completed by the least-visited endpoint. | Two means every tunnel has been reused. |
| `longestGap` | Longest time between two consecutive requests on one endpoint. | Whether any tunnel went quiet long enough to be reset. |
| `errors` | Failures by class, such as `timeout`, `connection reset`, `HTTP 403`; one entry per class seen. | One class per failure mode. |
| `endpoints` | Per endpoint, by number: `requests`, `errors`, `longestGap`. `?endpoints=1` only. | Naming a failing or quiet tunnel. |
| `distributions.opening` | Successful first requests. `/collect` and `/stop` only. | Tunnel setup latency as the actor sees it, apart from the steady state. |
| `distributions.latency` | Successful repeat requests. `/collect` and `/stop` only. | Settled quantiles, no handshakes in them. |
| `distributions.dns`, `distributions.tls` | Lookup and handshake times, when the request did one. `/collect` and `/stop` only. | Setup costs. Both are absent from a settled window. |

Each distribution is the OTel histogram data point of its window:

| Field | What it holds |
|---|---|
| `bounds` | Upper bounds of the finite buckets, seconds. |
| `bucketCounts` | One count per bucket, plus the overflow bucket last. Per bucket, not cumulative. |
| `count`, `sum` | Samples in the window and their total, seconds. |
| `min`, `max` | Smallest and largest sample in the window. |

A distribution with no sample in the window is absent. The bounds are the
same in every actor, 20 per decade from 1 µs to 63.1 s, 157 values, and they
travel in the payload so a driver checks them before it adds. Neighboring
bounds differ by about 12%, which sets the resolution: a quantile read from the
buckets is the upper edge of the bucket holding its rank, so it is within about
12% above the true value, and two runs whose quantiles fall within one bucket
step are indistinguishable. `max` is exact. At C = 1024 a `/collect` is about
7 KB, a `/stats` about 200 bytes, and the endpoint list about 60 KB more.

```json
{
  "scheme": "https", "elapsed": 612000000000,
  "requests": 61440, "successes": 61437, "newConns": 1024, "opened": 1024,
  "openingDone": 102400000000, "rounds": 60, "longestGap": 102600000000,
  "errors": {"timeout": 3},
  "distributions": {
    "latency": {
      "bounds": [0.000001, 0.000001122, "... 157 values ...", 63.1],
      "bucketCounts": [0, 0, "... 158 values ...", 0],
      "count": 60413, "sum": 181.2, "min": 0.00091, "max": 2.31
    }
  }
}
```

## 5. What a run aggregates

The driver is the next PR. This section is the contract it implements.

The driver polls every actor's `/stats` for the gates, collects every actor's
`/collect` at the two instants that bound a window, and pairs the merged
answers with the gateway's own metrics read at the same instants. Per field,
the rule is:

| Rule | Fields |
|---|---|
| sum | `requests`, `successes`, `newConns`, `opened`, `rounds`, `errors` per class, `endpoints` per endpoint |
| max | `elapsed`, `openingDone`, `longestGap`, `endpoints[].longestGap` |
| bucket-wise add, `Point.Merge` | each of `distributions`: bounds must be equal; `bucketCounts`, `count` and `sum` are added, `min` and `max` taken |
| first | `scheme` |

`Point.Merge` refuses a point whose `bounds` are not the sum's, or whose counts
do not fit its bounds; the driver names the actor and leaves the sum untouched.
A quantile is read from the merged buckets with `Point.Quantile`: the upper
bound of the bucket that holds the rank, capped at `max`; a rank in the
overflow bucket reads as `max`. A window is one `/collect` across every actor,
and the first collect of a run covers everything since `/start`. A `/collect`
that fails, or a `requests` that went down between two `/stats` polls, means
the actor restarted or was out of reach: that window is dropped and the next
starts from a fresh collect.

The results of a cell come from two windows. The blended window runs from the
first `/start` to the latest `openingDone`, each actor's shifted by its own
`/start` time. An actor's `openingDone` counts from its `/start`, so each
actor's window also covers its `offsetMs`. The settled window starts once
every actor has opened, every actor's `rounds` has reached two, checked per
actor before the sum, and `newConns` is flat between two polls. Two actors and
two endpoints, 100 ms apart, both started at the same time, actor 2 with a
50 ms offset and a slow second open:

```mermaid
sequenceDiagram
  autonumber
  participant A1 as Actor 1
  participant A2 as Actor 2
  participant S0 as Service 0
  participant S1 as Service 1
  rect rgb(240, 248, 255)
    A1->>+S0: open (t=0ms)
    S0-->>-A1: done (t=20ms)
    A2->>+S0: open (t=50ms, offset)
    S0-->>-A2: done (t=70ms)
    A1->>+S1: open (t=100ms)
    S1-->>-A1: done (t=120ms)
    A2->>+S1: open, SLOW (t=150ms)
    Note over A1,S1: Repeat requests begin while the slow open is still pending
    A1->>S0: repeat (t=200ms)
    A2->>S0: repeat (t=250ms)
    A1->>S1: repeat (t=300ms)
    S1-->>-A2: latest openingDone (t=320ms)
  end
  Note over A1,S1: Settled, after the blended window
  A2->>S1: repeat (t=350ms)
  A1->>S0: repeat (t=400ms)
  A2->>S0: repeat (t=450ms)
  A1->>S1: repeat (t=500ms)
```

Both actors' first repeats fall inside the blended window, because it ends
when the slowest actor finishes its last open. The offset puts actor 2's sends
halfway between actor 1's, so the two together send one request every 50 ms
rather than two every 100 ms; without it every send of actor 2 would coincide
with one of actor 1's. The gateway's CPU in that window
paid for four tunnel setups and for every request in it; the settled rate prices
every request, the first on each tunnel included, and what is left is the
setups.

| Result | Source | How it is computed |
|---|---|---|
| Settled req/s per gateway core | actors + gateway | Requests completed in the settled window divided by the gateway pod's CPU-seconds in the same window: the kubelet's cAdvisor metric `container_cpu_usage_seconds_total`, summed over the envoy, ext-proc and sdsmint containers. |
| Gateway CPU per tunnel opened | actors + gateway | The blended window's gateway CPU, minus every request in it priced at the settled per-request cost, divided by the tunnels opened. What remains is setup alone, independent of pacing, on the assumption that a request on an open tunnel costs the same in either window. Computed after the settled window, since it needs the settled rate. |
| Envoy heap | gateway | The maximum of `server.memory_allocated` in the cell; per-tunnel cost is the slope across fleet sizes. |
| Settled p50, p99, p99.9 | actors | `Point.Quantile` on the settled window's merged `latency`. |
| Settled maximum | actors | `max` of the settled window's merged `latency`. |
| Success rate | actors | `successes` over `requests` in the window. |
| Run validity | actors + gateway | `newConns` equals `opened` and stays flat, `longestGap` stays under the stream idle timeout, `dns` and `tls` are absent from the settled window, `cluster.mitm_internal.upstream_cx_total` equals A × C and stays flat, and `http.egress_connect.downstream_rq_idle_timeout`, `downstream_cx_idle_timeout` and `downstream_cx_protocol_error` do not move. |
