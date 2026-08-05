# Phase 3 — Reading the ingress path

The detailed version of Phase 3 in
[networking-study-guide.md](networking-study-guide.md). One week, five days of
reading, ~1,300 lines of Go across three packages.

Baseline: `main` @ `55f16fa9`.

**Prerequisites:** Phase 1 concepts 1–5 (TCP, DNS, `Host`, proxy, mTLS). Netns
is *not* needed here — that's Phase 5. If you skipped Phase 1 concept 4 (write a
20-line TCP proxy), do it now; day 3 assumes you know what a proxy is doing.

---

## The thesis of this phase

Substrate's routing table does not exist. There is no map from actor name to
worker IP held anywhere in the dataplane. Every request re-derives its own
destination by making one RPC to the control plane — and that same RPC is what
*starts* the actor. Routing and scheduling are the same operation.

Everything you read this week is machinery for making that idea safe and fast:
deduplicating the RPC, bounding its wait, and proving to the worker that the
answer it got is still true.

---

## §0 — The answer, up front

Read the hops before you read the code. Then each file has a slot to land in.

| # | Where | What happens | Anchor |
|---|---|---|---|
| 1 | client | `curl` resolves `my-counter-1.demo.actors.resources.substrate.ate.dev` | — |
| 2 | CoreDNS | wildcard template answers with the **router Service ClusterIP** — same IP for every actor | [corefile.go:44-50](../cmd/atenet/internal/dns/corefile.go#L44-L50) |
| 3 | Envoy | accepts :80, matches the one catch-all route, calls ext_proc before routing | [xds.go:583-606](../cmd/atenet/internal/router/xds.go#L583-L606) |
| 4 | router ext_proc | `Host` → `ActorRef{atespace, name}` | [extproc_in.go:66](../cmd/atenet/internal/router/extproc_in.go#L66) |
| 5 | router | admits to parking lot, then `ResumeActor` to ateapi | [extproc.go:164-171](../cmd/atenet/internal/router/extproc.go#L164-L171) |
| 6 | ateapi | assigns a free worker from the pool, restores the snapshot, returns worker pod IP | (Phase 3 boundary — treat as a black box) |
| 7 | router | writes `x-ate-original-dst: <workerIP>:443` and `x-ate-original-host: <actor DNS name>` | [extproc_out.go:62](../cmd/atenet/internal/router/extproc_out.go#L62) |
| 8 | Envoy | `ORIGINAL_DST` cluster dials the address from that header, over mTLS | [xds.go:546](../cmd/atenet/internal/router/xds.go#L546) |
| 9 | worker `atunnel` | verifies router's client cert, re-parses the actor name, checks it matches the actor *currently assigned here* | [server.go:236-279](../internal/atunnel/server.go#L236-L279) |
| 10 | actor | reverse-proxied to `:80` inside the sandbox, sees its own DNS name as `Host` | [server.go:274](../internal/atunnel/server.go#L274) |

Note what hop 2 does *not* carry: nothing per-actor. The name is resolved by DNS
and then immediately thrown away as an IP; the actual identity travels the whole
way as an HTTP header. That is why Phase 1 concept 3 was the important one.

---

## Day 1 — Orientation and the name

### Read

1. `cmd/atenet/README.md` — 40 lines. The deployment shape: one Deployment
   holding *both* Envoy and the router process, one Service exposing 80/443.
2. `cmd/atenet/internal/router/README.md` — the router's five jobs. Note it
   admits ext_proc is co-located "for easier debugging" and will be split.
3. `cmd/atenet/internal/dns/README.md` — the stub-resolver design.
4. [`internal/resources/actorref.go`](../internal/resources/actorref.go) — 91
   lines, the whole identity type. `DNSName()` at :52 and `ParseActorDNSName()`
   at :75 are exact inverses; the rest is conversion to the wire `ObjectRef`.
5. [`cmd/atenet/internal/dns/corefile.go`](../cmd/atenet/internal/dns/corefile.go)
   — 64 lines, builds the CoreDNS `template` block by string concatenation.
6. [`cmd/atenet/internal/dns/dns.go:69-113`](../cmd/atenet/internal/dns/dns.go#L69-L113)
   — the reconcile loop.

### What to notice

- **The Corefile is generated, not authored.** `buildTemplate()` runs in `init()`
  and interpolates `resources.ActorDNSSuffix` and `ResourceNameRegexPattern` —
  the same constants `ParseActorDNSName` validates against. The regex and the
  parser cannot drift.
- **`%s` survives into the template.** Line 49 leaves the answer's IP as a
  format verb; `makeCoreFile(routerIP)` fills it at :62. That is the *only*
  per-cluster variable in DNS.
- **The reconcile is a polling ticker, not a watch** (`dns.go:53-67`). It reads
  two Services (`atenet-router` and `dns`), writes the Corefile to a shared
  volume, and patches the kube-dns ConfigMap. Nothing here is per-actor, so a
  slow loop is fine — this is the "static tier" of the two-tier state model.
- **60-second TTL** on the A record. Harmless, because the answer never changes.

### Questions to answer before moving on

1. Why is a wildcard DNS record *safe* here, when in most systems returning the
   same IP for unknown names would be a bug?
2. An actor is deleted. How long until DNS stops resolving it? (Trick question.)
3. `ParseActorDNSName("foo.bar.baz.actors.resources.substrate.ate.dev")` — what
   happens, and why? Read [actorref.go:80](../internal/resources/actorref.go#L80)
   carefully: `strings.Cut` splits on the *first* dot.

### Exercise

```bash
go test ./internal/resources/ -run TestParseActorDNSName -v
go test ./cmd/atenet/internal/dns/ -run TestMakeCoreFile -v
```

Then add a table case to `TestParseActorDNSName` for an uppercase actor name and
predict the result before running it.

---

## Day 2 — Envoy as a black box, and the ext_proc contract

You are deferring `xds.go` to Phase 4, but you cannot read the ext_proc server
without knowing what is on the other end of the gRPC stream. Learn exactly this
much and no more.

### The contract

Envoy is configured to, for every request, **pause before routing** and send the
request headers to a gRPC service the router runs. That service replies with a
set of header mutations, or with an immediate response that short-circuits the
request. Envoy applies the mutations and then routes.

The route it picks is a single catch-all (`prefix: "/"`, `domains: ["*"]`) to
one cluster, `actor_original_dst`. That cluster's type is `ORIGINAL_DST` with
`use_http_header: true, http_header_name: x-ate-original-dst`
([xds.go:546-560](../cmd/atenet/internal/router/xds.go#L546-L560)) — meaning:
*don't look up an endpoint, read the destination out of that header and dial it.*

So the complete Envoy config, as far as Phase 3 is concerned, is:

> Send everything to the router's ext_proc. Then dial whatever IP:port the
> router wrote into `x-ate-original-dst`, using the router's client certificate.

That is the "one RPC deep routing table." Peek at
[xds.go:583-606](../cmd/atenet/internal/router/xds.go#L583-L606) to confirm the
route really is a single wildcard, and then close the file until Phase 4.

### Read

- [`extproc.go:86-136`](../cmd/atenet/internal/router/extproc.go#L86-L136) —
  `Process`, the stream loop.

### What to notice

- It is a **bidirectional stream**, but this implementation only ever handles
  `ProcessingRequest_RequestHeaders` and logs an error for anything else
  (:121-129). Response headers, bodies, trailers — all unhandled by design. The
  router touches the request once, at the very start, and then gets out of the
  way. The proxying itself is 100% Envoy.
- The **error path returns 200-shaped success to gRPC**. `handleRequestHeaders`
  returning an error does not fail the stream; it becomes an `ImmediateResponse`
  with an HTTP status (:107-112). Failures are HTTP-level, not gRPC-level.
- Every request records both a **metric** (`recordRouteDuration`) and a **ring
  buffer entry** (`s.recorder.AddRouterRequest`, last 100, served at `/statusz`).
  The `/statusz` page is your primary debugging surface in a live cluster.

### Question

Why does `handleRequestHeaders` return *seven* values? Look at what the extra
ones (`tmplNs`, `tmplName`, `resumeOutcome`) are used for and where they must be
available even on the error path. This is a small window into how much of this
code exists for observability rather than function.

---

## Day 3 — The heart: `Host` becomes an actor

This is the file to understand completely. It is 75 + 264 lines.

### Read

- [`extproc_in.go`](../cmd/atenet/internal/router/extproc_in.go) — all 75 lines.
- [`extproc.go:138-214`](../cmd/atenet/internal/router/extproc.go#L138-L214) —
  `handleRequestHeaders`, the twelve steps below.

### The twelve steps of `handleRequestHeaders`

| Step | Line | Note |
|---|---|---|
| 1 | :142 | `newRequestMetadata` lowercases every header key, folds `RawValue` into `Value` |
| 2 | :149 | extract `traceparent` **from HTTP headers**, not gRPC metadata — see the comment; Envoy doesn't propagate trace context into the ext_proc stream |
| 3 | :153 | `parseActorRef(metadata.host)` — **the routing decision** |
| 4 | :156 | invalid host → 404, before any RPC |
| 5 | :164 | `parking.enter` — admission control *before* the resume |
| 6 | :166 | lot full → 503, shed immediately |
| 7 | :170 | `ResumeActor` — the one RPC |
| 8 | :171 | `release(...)` — slot freed as soon as the resume returns, not when the request completes |
| 9 | :181-190 | worker IP from the response, validated with `net.ParseIP` |
| 10 | :199 | `targetAddr = workerIP:443` — the port is **hardcoded** |
| 11 | :207 | `addRoutingMutations` |
| 12 | :209 | return the mutation; Envoy does the rest |

### `parseActorRef` — 10 lines that decide everything

```go
if strings.Contains(host, ":") {          // strip an optional :port
    h, _, err := net.SplitHostPort(host)
    ...
}
return resources.ParseActorDNSName(host)
```

That's it. `newRequestMetadata` accepts **either** `:authority` (HTTP/2) or
`host` (HTTP/1.1) at [extproc_in.go:49](../cmd/atenet/internal/router/extproc_in.go#L49);
whichever arrives last wins, which is fine because Envoy normalizes them.

### The three things worth arguing with

1. **Port 80 only.** `net.JoinHostPort(workerIP, "443")` at :199 is the *worker's*
   atunnel port; the actor is always reached on its `:80` inside the sandbox.
   There is a `TODO(bowei)` at :198 acknowledging it. An actor cannot expose a
   second port today.
2. **`:authority` is deliberately not rewritten** in the Envoy path (:203-205).
   The Host header must survive all the way to the worker, because the worker
   authorizes on it. Compare the agentgateway path, which *must* rewrite it —
   [extproc_out.go:71-79](../cmd/atenet/internal/router/extproc_out.go#L71-L79)
   — and therefore needs `X-Ate-Original-Host` to carry the real name instead.
   That header exists purely because one dataplane can't do what the other can.
3. **`OVERWRITE_IF_EXISTS_OR_ADD` is a security control, not a style choice.**
   Read the comment at
   [extproc_out.go:41-44](../cmd/atenet/internal/router/extproc_out.go#L41-L44):
   nothing strips `x-ate-original-dst` from the *inbound* request, so a client
   could set it. Overwriting is what makes that harmless. Ask yourself what
   would happen with `APPEND_IF_EXISTS_OR_ADD` — this is the single most
   security-critical line in the file.

### Exercise

```bash
go test ./cmd/atenet/internal/router/ -run 'TestExtractMetadata|TestParseActorRef|TestExtProcHeadersEvaluation' -v
```

`TestExtProcHeadersEvaluation` ([extproc_test.go:97](../cmd/atenet/internal/router/extproc_test.go#L97))
runs the whole handler against a fake `ateapipb.ControlClient`. Read it as the
executable spec for this day. Then add a case: a request that arrives already
carrying `x-ate-original-dst: 10.0.0.1:443`, and assert the mutation replaces it.

---

## Day 4 — `resumer.go`: making one RPC survive contact with reality

265 lines, and the densest reasoning in the package. Read the comments — they are
unusually good and several of them record a bug that was fixed.

### The four mechanisms, in order

**1. Singleflight** ([resumer.go:170](../cmd/atenet/internal/router/resumer.go#L170)).
Keyed on `actorRef.String()` (`"atespace/name"`). A thousand concurrent requests
for one cold actor produce **one** `ResumeActor` RPC. This is the cache the
study guide mentions — note that it is not a TTL cache of results, it is a
deduplicator of *in-flight* calls. Nothing is retained after the flight ends.

**2. Context detachment** ([resumer.go:180](../cmd/atenet/internal/router/resumer.go#L180)).
The flight runs on `context.Background()` with its own timeout, not on the first
caller's context. If caller 1 hangs up, callers 2 and 3 still get their answer.
The consequence is stated explicitly at :176-179: **the budget is per-flight,
not per-caller.** A late joiner can be told "budget exhausted" after waiting
200ms. That is a deliberate trade.

**3. The retry classification** ([resumer.go:149-158](../cmd/atenet/internal/router/resumer.go#L149-L158)).
Three lines of `switch` that encode the entire parking policy:

| Code | Meaning | Retried? |
|---|---|---|
| `Aborted` | another resume in progress for this actor | always |
| `FailedPrecondition` | "no free workers available" | only if parking enabled |
| `Unavailable` | ateapi rolling restart | only if parking enabled |
| everything else | NotFound, PermissionDenied, … | never |

**4. Budget exhaustion** ([resumer.go:204-221](../cmd/atenet/internal/router/resumer.go#L204-L221)).
When the clock runs out mid-retry, returning the deadline error would show the
client a misleading `504`. `budgetExhaustedError` wraps the *last retryable
error* so the HTTP boundary still maps it to `503 "no free workers available"`.

Read the comment at :211-216 twice. It records a real bug: gRPC surfaces a
*status* error with code `DeadlineExceeded` that does **not** satisfy
`errors.Is(err, context.DeadlineExceeded)`, so the obvious check was wrong. The
fix gates on `bgCtx.Err()` instead. This is the kind of thing you only learn by
reading the comments.

### Also read

- `resumeBackoff` ([resumer.go:49-56](../cmd/atenet/internal/router/resumer.go#L49-L56))
  and its comment on why `Cap` is deliberately unset — `wait.Backoff` zeroes
  `Steps` when the delay reaches `Cap`, which would silently end retries early.
- `ResumeOutcome` (:68-75, :250-261) — `none` / `triggered` / `joined`, the
  metric label that tells you whether a request caused a cold activation, joined
  someone else's, or hit an already-running actor. The `leaderID == reqID`
  comparison at :256 is how a joiner is distinguished from the leader.
- [`errors.go:79-139`](../cmd/atenet/internal/router/errors.go#L79-L139) —
  `mapResumeError`, the gRPC→HTTP table. Note :107-118: `FailedPrecondition` and
  `Aborted` deliberately *preserve* the server's message ("no free workers
  available") because it's actionable and not sensitive; everything unrecognized
  collapses to a generic 500 to avoid leaking internals.

### Exercise

```bash
go test ./cmd/atenet/internal/router/ -run 'TestActorResumer' -v
```

`TestActorResumer_CallerCancelDoesNotAbortFlight`
([resumer_test.go:423](../cmd/atenet/internal/router/resumer_test.go#L423)) is
mechanism 2 as an executable assertion. Read it before the others.

### Question

The threat model flags this exact code: *"Cache invalidation during Actor
rescheduling must avoid stale routing"* (`docs/threat-model.md`, DoS row and DNS
misconfiguration row). Given that singleflight retains nothing after a flight
completes, where is the stale-routing window actually located? Hold your answer
until tomorrow.

---

## Day 5 — The far end: `atunnel` and the answer to yesterday's question

[`internal/atunnel/server.go`](../internal/atunnel/server.go), 304 lines. This
runs *inside the worker pod*, next to the sandbox.

### Read in this order

1. `NewServer` :77-143 — the TLS config.
2. `Activate` / `Deactivate` :182-227 — the lifecycle.
3. `ServeHTTP` :236-279 — the per-request check.

### The two independent gates

**Gate 1 — is the caller the router?** `ClientAuth: tls.RequireAndVerifyClientCert`
plus a `VerifyConnection` hook (:130-140) that walks the peer certificate's URI
SANs looking for one exact string, `cfg.AllowedClientID` (a SPIFFE-style URI).
Not "signed by our CA" — *this specific identity*. Anyone else who reaches
port 443 on the worker pod is rejected at the TLS handshake.

**Gate 2 — is this actor assigned here right now?** :252-258:

```go
active := s.active
if active == nil || active.ref != ref {
    s.reject(w)   // 421 Misdirected Request
}
```

`s.active` is set by `Activate` and cleared by `Deactivate`, called from
[`cmd/ateom-gvisor/main.go:616`](../cmd/ateom-gvisor/main.go#L616) (and the
microvm equivalent) as part of the sandbox lifecycle. **Exactly one actor per
worker at a time** (:188-190 refuses a second).

### Now answer yesterday's question

The stale-routing window is between the router receiving a worker IP and the
worker still holding that actor. It is closed *at the far end*, not by cache
management: the worker independently re-derives the actor identity from the Host
header and compares it against what it is currently running. A stale route
doesn't reach the wrong actor — it gets `421 Misdirected Request` with
`X-Ate-Assignment-Stale: true` ([server.go:281-284](../internal/atunnel/server.go#L281-L284)).

That header exists so a 421 from the routing layer can be told apart from a 421
the actor's own application emitted. Retryable, and diagnosable.

**This is the design principle to carry out of Phase 3:** the dataplane holds no
authoritative state, so correctness is enforced by re-checking at the boundary
rather than by invalidating a cache.

### Three more details

- **Host restoration** (:270-278). `X-Ate-Original-Host` is *deleted* before
  proxying (:273) — actor code must never see the router's internal header — and
  `r.Host` is set back to the actor's DNS name (:274). The actor always observes
  its own stable name regardless of which worker it landed on. That is
  host-decoupled identity made concrete.
- **Deactivate is synchronous** (:202-227). It cancels in-flight requests via
  `active.ctx`, waits on a `WaitGroup`, then closes idle upstream connections.
  A suspend does not race with a request still writing into the sandbox.
- **A live TODO at :124-128.** `ClientCAs` is frozen at process start while
  `GetCertificate` reloads per connection. After a CA rotation, a long-lived
  worker rejects the router until its pod restarts. Real, acknowledged,
  unfixed — a plausible first contribution.

### Exercise

```bash
go test ./internal/atunnel/ -run 'TestServeHTTP|TestInactive|TestMutualTLSClientIdentity' -v
```

`TestMutualTLSClientIdentity` ([server_test.go:175](../internal/atunnel/server_test.go#L175))
is gate 1; `TestInactive` (:147) is gate 2.

---

## Checkpoint

Narrate all ten hops out loud, no notes. You have passed when you can answer all
of these without looking:

1. What does DNS contribute to routing this request? (Correct answer: an IP that
   is the same for every actor, and nothing else.)
2. At which line does an HTTP header become an actor identity?
3. Why must Envoy's route config contain no actor names?
4. A client sends `x-ate-original-dst: 10.1.2.3:443` with its request. What
   happens, and which line prevents it from mattering?
5. 500 requests arrive for one suspended actor in the same 10ms. How many
   `ResumeActor` RPCs does ateapi see? Which mechanism, and what is its key?
6. The pool is momentarily full. Trace the request from `FailedPrecondition`
   through to the HTTP status the client sees, naming every function it passes.
7. The actor is rescheduled between the router's resume and Envoy's dial. What
   does the client get, and which component decided that?
8. Why does the worker re-parse the actor DNS name instead of trusting the
   router that just authenticated to it with mTLS?
9. Which two headers does the actor application never see?
10. Name the one piece of per-actor state anywhere in the dataplane. (There
    isn't one. Say why that's the point.)

---

## Deliberately out of scope this week

| Deferred to | What |
|---|---|
| Phase 3.5 | `parking.go` — you used `parking.enter` today as a black box |
| Phase 4 | `xds.go` (828 lines), `envoyrunner.go` — all Envoy config |
| Phase 5 | how the request crosses into the sandbox netns; `ateom0`/`eth0` |
| Phase 6 | `atunnel/egress.go`, `client.go` — the *outbound* direction |
| never (this guide) | `ateapi`'s `AssignWorkerStep`, snapshot restore, `atelet` |

Two files in the router package you will bump into and can skip: `status.go`
(the `/statusz` page — worth a two-minute skim for debugging), `health.go` and
`atstore.go` (ActorTemplate watching, not on the request path).

---

## Traps

- **"The router load balances."** It doesn't. It resolves. There is exactly one
  destination and it comes from the control plane.
- **"The resume cache could serve stale IPs."** There is no result cache — only
  in-flight deduplication. Staleness is handled at the worker, not here.
- **Confusing the two `443`s.** The router Service exposes 443 (client-facing
  TLS); the worker atunnel listens on 443 (router-facing mTLS). Different
  connections, different certs, different trust roots.
- **Confusing `x-ate-original-dst` with `SO_ORIGINAL_DST`.** The first is an
  HTTP header this codebase invented, read by Envoy's `ORIGINAL_DST` cluster.
  The second is a Linux socket option used on the *egress* path in Phase 6.
  Related name, unrelated mechanism.
- **Assuming agentgateway works like Envoy.** The `routeViaAuthority` flag exists
  because it doesn't. Read the Envoy path first; treat every `routeViaAuthority`
  branch as a footnote.
