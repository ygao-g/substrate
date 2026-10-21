# The benchmark-egress driver: contract

What `benchmark-egress driver run` promises, refuses and leaves behind, as
clauses a test can cite. The operator guide is [README.md](README.md); the
actor's own contract is
[cmd/benchmarking/egress/README.md](../../cmd/benchmarking/egress/README.md).
Clause ids are `G<n>` for a guarantee, `R<n>` for a refusal; the appendix maps
each to the test that holds it, and the test carries `// CONTRACT <id>` above
its name.

## For agents

You are changing or running the benchmark-egress driver. This file is the fixed
part of your brief. A change that alters a clause updates the clause in the same
commit and the test named in the appendix. A clause with no test is a bug in
the contract: add the test or remove the clause, in that commit; the appendix
names the ones open today, and a change that touches one closes it. Where the code
and this file disagree, stop and report which clause and which line; do not
pick a side. An item marked open is unknown, not implied: do not fill it in
from memory. Standing rules for a run: no push and no PR edit without a
go-ahead; every `kubectl` with an explicit `--kubeconfig` and `--context`;
gateway counters are cumulative per pod, so read them as deltas around the
window; the target Deployment is Ready before a Job or a `run` starts, because
preflight does not check it (G4).

## 1. Purpose

A run creates A actors, resumes B of them, has each resumed actor hold a tunnel
to each of C in-cluster endpoints through the egress gateway at a fixed pace for
a measured window, and collects what every actor counted. It produces one text
report on stdout and, with `--output`, the same run as JSON with every actor's
final scrape. When it returns, for any reason, every actor it resumed is
suspended again and every loop it started is stopped; what it leaves behind is
the actors and their policies, which the next run reuses and `cleanup` deletes.

## 2. Inputs

Flags of `run`. The last column is the refusal given before anything is
created (G1): `validate` for the run flags, and the connection for the
`--api-token-file` rule, which runs before the kubeconfig is loaded.

| Flag | Default | Meaning | Refused when |
|---|---|---|---|
| `--actors` | 1000 | A, the actors named `egress-0` to `egress-<A-1>` | below 1; there is no upper bound |
| `--parallel` | 1 | B, the actors resumed and run | below 1 or above A |
| `--endpoints` | 10 | C, endpoints 0 to C-1 each loop holds a tunnel to | below 1 or above 1024 |
| `--duration` | 5m | the measured window | not positive |
| `--conn-mode` | `keepalive` | `keepalive` or `new-conn` | anything else |
| `--request-timeout` | 5s | one request's timeout | not a positive whole number of ms |
| `--request-interval` | 100ms | one request per interval per actor; 0 is unpaced | negative or not whole ms |
| `--create-concurrency` | 32 | actors created at once | below 1 |
| `--resume-timeout` | 5m | per actor: resume retries plus the wait for `/readyz` | not positive |
| `--progress-interval` | 30s | the poll period; 0 turns polling off | negative |
| `--start-batch` | 0 | loops started together; 0 is all of B | negative or above B |
| `--start-gap` | one period | pause between batches | negative; set without `--start-batch` |
| `--opens-budget` | 0 | opens per second the gateway can handshake; 0 skips the check | negative; demand above it (R1) |
| `--stream-idle-timeout` | 5m | the gateway's CONNECT stream idle timeout; 0 skips the check | negative; keep-alive period over half of it (R2) |
| `--settle` | 0 | settle wait, see section 6 | negative; with `new-conn` (R3); without a progress interval (R4); shorter than one period (R5) |
| `--settle-timeout` | 30m | bound on the settle wait | not positive while `--settle` is set |
| `--scheme` | `http` | `http`, or `https` through the gateway's interception | anything else |
| `--atespace`, `--template` | `benchmark-workloads`, `egress` | where the actors live and what creates them | empty |
| `--output` | | JSON report path | never |
| `--kubeconfig`, `--context`, `--api-endpoint`, `--api-token-file`, `--router-url` | | cluster access; empty endpoint and router URL port-forward to the `api` and `atenet-router` Services | `--api-token-file` without `--api-endpoint` |

Derived terms. The period is C x `--request-interval`: how long one loop takes
to open all its endpoints and how often it requests each of them. The opening
demand is the actors started together, `--start-batch` or all of B, times the
batches whose openings overlap, ceil(period / gap), divided by the interval, in
opens per second; it is zero when the interval is zero. Open, a code gap: an
explicit `--start-gap 0` with `--start-batch` is accepted, the demand check
counts one period as the gap and sees one batch, and the loops then start back
to back, so the real demand is all of B over the interval and R1 does not hold
for that input.

Environment: a cluster with Substrate and the `atenet-egress` gateway; the
actor template in the atespace; the endpoint Services from `deploy.sh`; for
HTTPS, the target's TLS Secret and the gateway trust patch. The driver reaches
ateapi with a minted `ate-client` token or the token file, and the actors
through the atenet router with the `ate-target-actor` header.

## 3. Preflight

Before any actor is created the driver lists the Services in
`egress-tests-targets` and requires `egress-target-0` to `egress-target-<C-1>`
(G2); over HTTPS each must have port 443 (G3) and the gateway must trust the
target's CA: Secret `egress-target-tls` exists, ConfigMap
`egress-tests-target-ca` holds the same certificate, the `atenet-egress`
Deployment has the `egress-tests-trust` init container, its annotation is the
SHA-256 of that CA, and its rollout is complete (G3). Each failure names the
missing or stale piece; the Service and trust failures name the `deploy.sh`
flag that fixes it, except a failed read of the gateway Deployment, which
names the Deployment, and a rollout still in progress, which points at
`kubectl rollout status`. A preflight failure returns an error and no report.

Non-promise (G4): preflight checks Services, not endpoints or pods. A target
Deployment at zero replicas passes preflight and the run then measures
connection failures and redials; a 2026-10-19 run did exactly that. Check
`egress-target` is Ready before a run until this clause is replaced by a check.

## 4. Phases

The phases run in this order, each recorded in the report's `phases` with its
start and end on the driver's clock.

### 4.1 Create or reuse

Entry: preflight passed. The driver creates actors `egress-0` to `egress-<A-1>`
from the template, `--create-concurrency` at a time, two minutes per actor, and
gives each one the egress policy named `default`: an `http` rule and an `https`
rule over endpoints 0 to C-1 by DNS name, in chunks of 256 hostnames (G5). An
actor that exists is reused and counted; a policy that exists is read and
replaced only when its rules differ, ports aside, and counted as updated (G5).
A create whose answer was lost and then reports AlreadyExists is not counted as
reused (G6). Exit: per actor, created with its policy or failed; a failed actor
is left out of the resume set. Failure: counted by kind with three examples; the
run goes on with the actors that succeeded. An interrupt here returns the
report as interrupted. Leaves behind: the actors and policies.

### 4.2 Resume

Entry: the first B created actors in index order (G7), so a warm-up run of B
gives every later run of at most B its own snapshots. Each actor is read once,
before the burst, to record what it will resume from: `own`, `tag`, `none`,
`running` or `unknown`. All are then resumed at once. A resume is retried under
`--resume-timeout` while the pool is full, the server unavailable or slow, or a
version conflict is reported (G8); an Aborted that says the actor crashed is not
retried (G8). A resume whose outcome is unknown marks the actor as possibly
running, so suspend covers it (G13). Exit: resumed, or failed with its error in
the report.

### 4.3 Wait ready

Entry: the actor resumed. The driver polls `GET /readyz` through the router
every 200 ms until it answers 2xx or `--resume-timeout` runs out from the first
resume attempt (G9). Resume latency runs from the first `ResumeActor` attempt to
its success; ready latency from then to the first 2xx. Exit: ready, or failed
with the last error.

### 4.4 Start loops in batches under the opens budget

Entry: the ready actors. First, every ready actor's `/readyz` is probed for the
header `Benchmark-Egress-Protocol: 1`; one actor without it, or with another
version, means the fleet's golden snapshot is stale: no loop starts, every
ready actor is marked with that error, the actors are still suspended and the
run returns the error (R6). Then the loops start, all at once or in batches of
`--start-batch` with `--start-gap` between them, each batch recorded with its
size and time (G10). `POST /start` carries C, the connection mode, the request
timeout and interval in ms, and the scheme. A 409, a loop left running by an
earlier run, is answered by `POST /stop` and a second start (G11). An interrupt
during a gap leaves the remaining batches unstarted (G12). The start log prints
the opening demand against the budget, or marks it unchecked. Exit: the started
loops.

### 4.5 Poll and scrape

Entry: started loops and a progress interval above zero; with zero the driver
only waits. Every interval the driver scrapes `GET /metrics` of each started
loop, sums the scrapes, appends the sum to the loop timeline, and logs the rate
since the last poll with the cumulative p99. A loop that does not answer is
counted missing and left out of that point; a loop whose scrape cannot be added
is counted unmerged and left out; the log names up to three of each (G14). A
poll with fewer answers than loops undercounts the fleet and says so in its
`answered` and `polled` fields. A missing loop does not fail the run.

### 4.6 Settle

Entry: `--settle` above zero and at least one loop started. Section 6 is the
predicate. Exit: settled, or not settled after `--settle-timeout` or an
interrupt with the reason recorded; the run measures in either case (G15).

### 4.7 Windows

Entry: the settle wait ended, or there was none. The driver polls once as the
window opens, then every interval for `--duration` (G16). Section 7 defines the
windows the report derives.

### 4.8 Stop

Entry: always reached, after an interrupt too, under a fresh three-minute
deadline that ignores the run's cancellation (G17). Every ready actor, whether
its start was confirmed or not, is scraped once more with
`GET /metrics?endpoints=1` and then sent `POST /stop` (G18). A 404 on an actor
whose start was never confirmed is a success; a 404, any other non-2xx or no
answer from one that did start is a stop failure, so an actor that crashed
mid-run makes the run return the G20 error. A final scrape that is lost costs
that actor's share of the report, not the run: the stop still counts, the
entry's `error` says `final scrape`, and its counts are absent from `loop`
(G19). A final scrape the driver cannot add, other bucket bounds, is kept raw in
the entry's `metrics`, the entry's `error` says `metrics cannot be added up`,
the stop counts as a success, and its counts are absent from `loop`; the
`opening.opens` figure and the per-actor rate line still include it (G19).
Exit: the merged final scrapes as the report's `loop`, and one entry per actor.

### 4.9 Suspend

Entry: after stop, under the same deadline. Every actor that resumed, or whose
resume may have committed, is suspended, with retries; suspending an actor that
is already suspended succeeds (G13). A suspend answered with Aborted naming a
crash is not retried and counts as a suspend failure. Exit: counted; a run with
any stop or suspend failure returns an error that says how many loops may still
be sending traffic (G20). After an interrupt the run returns the interruption
instead, and those counts are in the report only.

### 4.10 Report

The text report is printed on stdout whenever a report exists, which is every
run that passed preflight, after an interrupt too, headed `!! interrupted` then
(G21). With `--output` the JSON report is written, and a write failure is joined
to the run's error (G22). The opening marks are derived after stop from the
loops' final scrapes. The command exits non-zero when the run returns an error:
a stop or suspend failure, the protocol refusal (R6), or the interruption
itself.

## 5. The scrape protocol

- `GET /readyz` through the router must answer 2xx with
  `Benchmark-Egress-Protocol: 1`; no header is an actor from before the
  exposition, another value is another version; both stop the run before any
  loop starts (R6).
- `GET /metrics` is requested as `application/vnd.google.protobuf;
  proto=io.prometheus.client.MetricFamily; encoding=delimited` with
  `text/plain` accepted at lower preference, and decoded in whichever the actor
  serves (G23). Polls ask for the fleet families; the final scrape before stop
  asks `?endpoints=1` for the per-endpoint families too.
- A histogram whose finite bucket bounds are not the fleet's is refused with
  the actor and family named, and the sum is left as it was (R7). A protobuf
  scrape omits the `+Inf` bucket; its count is the sample count (G24). A
  histogram family with no samples is absent and reads as empty.
- A window is a later scrape minus an earlier one, bucket by bucket; a count
  that went down, or bounds that changed, is a reset, and that window yields no
  quantiles (G25).
- An actor that stops answering is named in the poll log and left out of that
  poll; the run continues; the settle wait cannot complete while any loop is
  missing; its final scrape is attempted at stop and, if lost, its entry
  carries the error and its counts are absent from `loop` (G14, G19).

## 6. Settle criteria

With `--settle S`, the measured window begins at the first poll on which all of
these hold, or after `--settle-timeout` (G15, G26):

1. every started loop answered and its scrape was added;
2. every loop has `endpoints_opened_total` at least C and at least C successful
   requests;
3. every loop has `rounds` at least 2;
4. the fleet redial count, the sum over loops of
   `connections_opened_total - endpoints_opened_total` clamped at zero, has not
   changed for S, with half a poll interval of slack.

The report records when all loops had opened and when all had swept, the redial
count and the longest gap at the end of the wait, and flags a longest gap within
10 % of `--stream-idle-timeout`. When the wait ends unsettled the reason is one
of: loops did not answer, loops answered metrics the driver cannot add up, not
every loop opened with a success each, not every loop requested each endpoint
twice, redials kept changing, interrupted. The run measures anyway and the text
report says `NOT settled`.

## 7. Windows

- Opening, the blended window: from the first batch's start to the latest
  `startedAt + opening_done_seconds` over the started loops (G27). It is marked
  only once every started loop has reported `opening_done_seconds`; until then
  the report has no `opening` and no opening phase mark.
- Opening-pure: from the first batch's start to the latest opening end among
  the first batch's loops, only when there was more than one batch (G27).
- Steady, the settled window: from the first poll taken as the measured window
  opened to the last poll in it, counting only complete polls; a poll is
  complete when every started loop answered and its scrape was added. It needs
  two such polls or it is absent (G16, G28). After an interrupt, or once a loop
  stopped answering, it is built from the complete polls taken until then, so a
  loop that crashes before the window leaves no steady summary at all.

## 8. Aggregation

Per family, the rule when one actor's scrape is added to the fleet's (G29):

| Rule | Families |
|---|---|
| sum, per label set | `requests_total`, `request_errors_total`, `connections_opened_total`, `endpoints_opened_total`, `endpoint_requests_total`; the gauge `rounds` |
| max | `opening_done_seconds`, `longest_gap_seconds`, `loop_elapsed_seconds`, `endpoint_longest_gap_seconds` |
| bucket-wise add | `opening_seconds`, `latency_seconds`, `dns_seconds`, `tls_seconds`: every finite bucket, the count and the sum |
| first | `loop_info` |

The sum does not depend on the order the scrapes arrive in (G30). A quantile
is the upper bound of the bucket holding rank ceil(q x count); a rank past the
last finite bound reads as `+Inf` and prints as `>60s`; an empty histogram
reads 0 (G31). One sample lands in one bucket, so a fleet quantile is the bound
of the bucket that one actor's recorder would have answered (G29). There is no
cap at the observed maximum. The bounds are `bucketBounds` in the package's
histogram.go: 10^(k/20) microseconds, rounded to four significant digits, from
1 µs up to the first bound at or past 60 s, 20 per decade, so neighboring
bounds differ by about 12 %, which is the resolution of every quantile here.
Every actor records with these bounds; that is what makes scrapes add bucket
by bucket and what R7 refuses otherwise.

## 9. Report

JSON fields, with the source:

| Field | Meaning and source |
|---|---|
| `config` | the validated `runConfig` |
| `started`, `finished`, `interrupted` | driver clock; `interrupted` when the context ended before suspend returned |
| `create` | attempted A, succeeded, failed, duration, errors by kind, up to three examples; `actorsReused`, `policiesUpdated` |
| `resume` | the same counts over the picked actors; `resumeLatency` and `readyLatency` as nearest-rank p50, p99, max over the ready actors; `sources` by resume source |
| `start` | counts over the ready actors; `batches[]` with index, actors, time |
| `stop`, `suspend` | counts over the ready actors, and over the resumed-or-possibly-resumed actors |
| `loop` | the sum of every final `?endpoints=1` scrape |
| `opening` | `start`, `end`, `pureEnd`; `opens` = `endpoints_opened_total` summed over every started loop's final scrape, an unmergeable one included; `failedOpens` = opens minus the summed `opening_seconds` count, so an unmergeable scrape inflates it; `openP99` from the summed `opening_seconds`; `timeouts` = the `timeout` error class and `redials` at the first poll at or after `end` |
| `settle` | `settled`, `waited`, `allOpened`, `allSwept`, `redials`, `longestGap`, `longestGapNearTimer`, `reason`, `unanswered`, `unmergeable` |
| `steady` | `start`, `end`, `polls`, `requests`, `successes`, `redials` as last minus first complete poll; `reqPerS` = requests / (end - start); `p50`, `p99`, `p999` from the latency delta |
| `actors[]` | name, resume and ready latency, resume source, `startedAt`, `batch`, `error`, `metrics` = the final scrape |
| `phases[]` | create, resume, start, settle, steady, stop, suspend, and after stop opening and opening-pure, each with start and end |
| `loopTimeline[]` | per poll: time, requests, successes, newConns, opened, openingDone, rounds, longestGap, redials, errors by class, the summed latency histogram, polled, answered |

Durations in the JSON are nanoseconds.

Text report lines, in order:

| Line | Formula |
|---|---|
| `== egress-tunnels: ...` | A, B, C, scheme, mode, interval, duration |
| `!! interrupted` | present when `interrupted` |
| `create n/A ok in d (r actors reused, u policies updated)` | plus `failed:` kinds and `e.g.` examples when any failed |
| `resume n/B ok in d`, `resume p50 .. p99 .. max ..; ready ...`, `resumed from: ...` | nearest-rank over ready actors; sources sorted by name |
| `start n/ready ok in d` | |
| `opening d: o opens, f failed, open p99 q, t timeouts, r redials (pure window d, n actors)` | section 7; redials only when a poll came after the end; pure window only with batches |
| `settle settled after d: all opened at .., all swept at .., r redials, longest gap g` or `settle NOT settled after d: reason` | section 6; after `NOT settled`, `all opened at ..` when all had opened and `all swept at .., r redials, longest gap g` when all had swept |
| `whole run d: n requests, x req/s, s% success, c new connections` | from `loop`: `loop_elapsed_seconds` (max), `requests_total`, `requests_total{result="success"}`, `connections_opened_total` |
| `steady d window, p polls: ...; p50 p99 p99.9` | from `steady`, printed only when the window has requests |
| `n endpoints opened, r rounds in all, longest gap g` | `endpoints_opened_total`, `rounds` (sum), `longest_gap_seconds` (max); only when at least one endpoint opened |
| `latency p50 p90 p99 p99.9 (mean)` | quantile over the summed `latency_seconds`; mean = sum / count |
| `dns n lookups, p50 p99`, `tls n handshakes, p50 p99` | when the family has samples |
| `errors class=n ...` | `request_errors_total` by class, largest first |
| `req/s per actor: min median max` | per actor `requests_total` / `loop_elapsed_seconds`, over every entry with a scrape, unmergeable ones included |
| `most errors: egress-target-i (e/n), ...` | up to five endpoints from `endpoint_requests_total` |
| `loop no requests measured` | instead of the block above when `loop` has no requests |
| `stop n/ready ok in d`, `suspend n/m ok in d` | |

Validity, from the driver's own figures: tunnels held means fleet
`connections_opened_total` equals `endpoints_opened_total` in `loop` and the
steady window's `redials` is zero; `settle.settled` true with `redials` zero
says no tunnel was cut before the window. Idle cuts on the gateway side are not
read by the driver: see fork 56 (section 14).

## 10. Guarantees and refusals

Guarantees:

- G1 Every flag refusal in section 2 happens before the driver connects to the
  cluster or creates anything.
- G2 No actor is created while an endpoint Service in 0 to C-1 is missing; the
  error names up to five and the `deploy.sh` command.
- G3 Over HTTPS, no actor is created while a Service lacks port 443 or the
  gateway trust check fails; each error names what is missing or stale.
- G4 Non-promise: preflight does not detect endpoint Services with no ready
  target pods.
- G5 Every created or reused actor has the policy of section 4.1 when the
  create phase ends, and an existing policy is updated only when its rules
  differ; a rerun with the same C updates none.
- G6 A create whose answer was lost is not reported as a reused actor.
- G7 The resumed actors are the first B created, in index order.
- G8 A resume is retried while the pool is full and gives up on an actor that
  crashed; a crashed actor is not suspended.
- G9 Ready means `/readyz` answered 2xx through the router; the wait is bounded
  by `--resume-timeout` from the first resume attempt.
- G10 Loops start in batches of `--start-batch`, `--start-gap` apart, each
  batch recorded; without batches there is one batch.
- G11 A loop left running by an earlier run is stopped and started again, not
  reported as a start failure.
- G12 An interrupt between batches starts no further batch, and the loops that
  started are still stopped and the actors suspended.
- G13 Every actor that resumed, or whose resume may have committed, is
  suspended before the run returns, on success, failure and interrupt alike.
- G14 A loop that does not answer a poll, or answers metrics the driver cannot
  add, is named and left out of that poll; the run continues.
- G15 The settle wait ends at `--settle-timeout` with the reason recorded and
  the run measures anyway; the report says `NOT settled`.
- G16 The measured window is `--duration` long and summarized from complete
  polls only.
- G17 Stop and suspend run under their own three-minute deadline that starts
  after the loops, whatever the run's duration and even after an interrupt.
- G18 Every ready actor is scraped with endpoint families and stopped, those
  whose start failed included.
- G19 A lost or unmergeable final scrape marks that actor's entry and leaves
  its counts out of `loop`; the stop counts and the run does not fail for it.
- G20 A run with a loop not stopped or an actor not suspended returns an error
  with both counts; a started loop that does not answer `/stop` is a loop not
  stopped. After an interrupt the error is the interruption and the counts are
  in the report only.
- G21 A report is printed for every run that passed preflight, interrupted
  runs included, and the JSON report is written when `--output` is set.
- G22 A JSON write failure is reported together with the run's own error.
- G23 Scrapes request protobuf and accept text; both decode to the same
  families.
- G24 A missing `+Inf` bucket is read as the sample count.
- G25 A window whose later count is below the earlier one is a reset and
  yields no quantiles.
- G26 Settled requires every loop answering, opened with a success per
  endpoint, two rounds, and redials flat for `--settle`.
- G27 The opening window and its pure part are marked only once every started
  loop reported its opening done, and the pure part only with more than one
  batch.
- G28 The steady summary is absent with fewer than two complete polls.
- G29 The fleet sum follows the per-family rules of section 8 and equals what
  one recorder fed every actor's samples would answer, to the bucket.
- G30 The fleet sum is independent of the order scrapes are added in.
- G31 A quantile is the bucket upper bound at the rank; past the last finite
  bound it is `+Inf`, printed `>60s`.
- G32 The driver never writes the gateway's Deployment or ConfigMap; its only
  gateway access is the trust check's reads.
- G33 `cleanup` sends one `DeleteActor` per name `egress-0` to
  `egress-<n-1>`, in any state; the server removes the policy with the actor,
  the driver sends no policy delete. An actor that does not exist counts as
  deleted.

Refusals:

- R1 A start whose opening demand is above `--opens-budget` is refused before
  anything is created, with a `--start-batch` or `--start-gap` that fits;
  open for an explicit `--start-gap 0`, see section 2.
- R2 A keep-alive run whose period is more than half of `--stream-idle-timeout`
  is refused.
- R3 `--settle` with `new-conn` is refused.
- R4 `--settle` without a progress interval is refused.
- R5 `--settle` shorter than one period is refused.
- R6 An actor without `Benchmark-Egress-Protocol: 1` on `/readyz` stops the run
  before any loop starts; the actors are still suspended.
- R7 A scrape whose histogram bounds are not the fleet's is refused, the actor
  named, and the sum left unchanged.
- R8 `cleanup` refuses `--actors` or `--concurrency` below 1, and `run` and
  `cleanup` refuse `--api-token-file` without `--api-endpoint`.

Nobody deletes a policy during a run; see G33 and section 12.

## 11. Limits

- A: no bound in the driver. Create runs `--create-concurrency` at a time with
  two minutes per actor.
- C: 1024, shared with `deploy.sh` and the target's port range.
- Opens budget: not measured by the driver; `--opens-budget` is an operator
  input. Observed: about 2,800 opens/s on one 88-thread c3 gateway, about half
  on 44 threads (README, Opening throughput).
- Poll interval: any positive duration; there is no floor in the code. Open:
  the interval below which A x B scrapes do not complete inside one interval.
- Memory of the final `?endpoints=1` scrape at B x C: open. The per-actor
  scrapes are kept in the report; the fleet merge is quadratic in the number
  of label sets it searches. No figure is promised.
- Router client: one 30 s timeout per request, idle connections per host equal
  to B, at least 2.

## 12. Non-goals

- No latency SLO: the driver reports quantiles and refuses nothing on them.
- No gateway tuning or reading: no counters, CPU or heap; see fork 56.
- No e2e suite: the driver is measurement tooling and is tested in package
  with fakes.
- No multi-gateway runs, no cross-cluster runs, no concurrent runs on one
  atespace. The driver holds no lock and detects no overlap: two runs that
  overlap share actor names, the second one's create replaces policies whose
  C differs, its start stops and restarts the first one's loops (G11), and its
  suspend puts the first one's actors to sleep mid-window.
- No cleanup of endpoint Services or the gateway patch: that is
  `deploy.sh --delete`.

## 13. Clause-to-test map

All tests are in `cmd/benchmarking/egress`, package `main`, run with
`go test -race ./cmd/benchmarking/egress/...`. A test carries
`// CONTRACT <ids>` above its declaration.

| Clause | Test |
|---|---|
| G1, R1, R2, R3, R4, R5 | `TestConfigValidate`, `TestConfigValidateAccepts`, `TestConfigValidateMessages`, `TestParseRunFlagsOpensBudget`, `TestParseRunFlagsDefaultsTheStartGap` |
| G2 | `TestPreflightFailsOnMissingServices` |
| G3 | `TestPreflightHTTPS`, `TestRunHTTPS` |
| G4 | none: a non-promise; replaced when a readiness check lands |
| G5 | `TestRunReusesActorsAndUpdatesPolicies`, `TestRunUpgradesHTTPOnlyPolicies`, `TestBuildPolicy` |
| G6 | `TestLostAnswers` |
| G7 | `TestPickActorsFirst` |
| G8 | `TestResumeRetriesFullPool`, `TestResumeGivesUpOnCrash` |
| G9 | `TestRun` (indirect: every resumed loop starts, which needs ready); a test of the `/readyz` poll alone: open |
| G10 | `TestStartLoopsInBatches` |
| G11 | `TestStartRecoversLeftoverLoop` |
| G12 | `TestInterruptBetweenBatchesStillStopsAndSuspends` |
| G13, G17 | `TestRunInterruptedStillStopsAndSuspends`, `TestCleanupDeadlineStartsAfterTheLoops`, `TestLostAnswers` (a resume whose answer was lost is suspended) |
| G14 | `TestRunSettleNamesLoopsThatDoNotAnswer`, `TestStatsTheDriverCannotMergeAreNamedAndLeftOut` |
| G15 | `TestRunMeasuresAnywayWhenSettleTimesOut` |
| G16, G28 | `TestRunSettlesBeforeMeasuring` |
| G18 | `TestStopReachesLoopsWhoseStartFailed` |
| G19 | `TestStatsTheDriverCannotMergeAreNamedAndLeftOut` (the unmergeable case); a final scrape lost in transit: open, no test |
| G20 | `TestRunFailsWhenActorsAreLeftRunning` |
| G21, G22 | `TestRunHTTPS` (the printed report), `TestRunInterruptedStillStopsAndSuspends` (the report comes back after an interrupt); the print after an interrupt and the JSON write: open, no test |
| G23 | `TestBothEncodingsAgreeAcrossTheSeam` |
| G24 | `TestMissingInfBucketIsSampleCount` |
| G25 | `TestWindowAndReset` |
| G26 | `TestRunSettleNeedsSuccessfulOpens`, `TestRunSettleNeedsTwoRounds`, `TestRunSettleWaitsForRedialsToStop` |
| G27 | `TestRunMarksTheOpeningPhase`, `TestRunWithoutBatchesHasNoPureWindow`, `TestRunOpeningUnknownUntilEveryLoopOpened` |
| G29 | `TestAggregateEqualsOneRecorder`, `TestContractCycleMatchesTheTarget` |
| G30 | `TestAggregationIsOrderIndependent` |
| G31 | `TestQuantileTable` |
| G32 | none: open, no test |
| G33 | `TestCleanup` |
| R6 | `TestOldActorFailsFast`, `TestLaterProtocolRefused` |
| R7 | `TestAggregateRefusesOtherLe`, `TestLeStringsAgree` |
| R8 | `TestConnectPassesTheEndpointAndTokenFile`; the `cleanup` bounds: open, no test |

## 14. The usage sampler and the orchestrator

- Usage sampler (fork 56, `egress-tunnels-usage`): samples the gateway pod's
  CPU, heap and Envoy counters around the windows and computes the settled
  cores per krps and the per-tunnel opening cost. Its clauses join this file
  when it lands; until then its README section is the reference.
- Orchestrator entry (fork 53, `egress-tunnels-ci`): runs the driver from an
  in-cluster Job through `benchmarking/automation`, test type
  `egress-tunnels`. Its pass criteria join this file when it lands.
