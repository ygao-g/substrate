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
| `benchmark-egress actor` | The workload, one of the subcommands of `cmd/benchmarking/egress`. `POST /start` starts the loop, `GET /stats` reports progress, `POST /stop` stops the loop and returns its final stats. |
| `benchmark-egress target` | A small HTTP and HTTPS server. One Deployment of it backs every endpoint Service. |
| `benchmark-egress driver` | The driver: `run` creates the actors, gives each one an egress policy, resumes B of them, runs the loops, and prints a report; `cleanup` deletes the actors; `certs` issues the target's certificate. |
| `cmd/benchmarking/egress` | Endpoint naming, the actor's stats and histogram, and the target certificate, shared by the three subcommands of the one binary. |
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
[cmd/benchmarking/egress/endpoints.go](../../cmd/benchmarking/egress/endpoints.go).
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

## Cleanup

```bash
./benchmarking/egress-tunnels/deploy.sh --delete
WORKLOAD_TEMPLATES=egress ./benchmarking/workloads/deploy.sh --delete
```

`deploy.sh --delete` removes the gateway patch and the
endpoints; `--unpatch-gateway` removes only the patch. The workloads script
removes the template and the `benchmark-worker` pool, which the other workload
templates share; skip that step while other benchmarks use the pool.

## Code map

The driver is these files of `cmd/benchmarking/egress`; the actor and the
target own the rest of the package, and the three share endpoint naming, the
start contract, the stats and the certificate from it.

| File | Responsibility |
|---|---|
| `driver.go` | The `driver` command and its `run`, `cleanup` and `certs` subcommands, the cluster flags, and the ateapi and Kubernetes connection. |
| `targetcert.go` | The CA and the target's certificate that `certs` writes and deploy.sh puts in the Secret. |
