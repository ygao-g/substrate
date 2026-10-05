# nighthawk-actor

`nighthawk-actor` is the load-generating actor for the Nighthawk egress
benchmark. It runs inside an actor sandbox, so the load it generates leaves
through the actor's egress path and the egress gateway.

It serves a control API on port 80, the inbound port atunnel forwards into the
sandbox:

| Method and path | Behavior |
|---|---|
| `GET /readyz` | `200 ok`. Used to warm the actor. |
| `POST /run` | Body `{"runId": "...", "spec": "<AdaptiveLoadSessionSpec textproto>"}`. Starts `nighthawk_service` on loopback, then `nighthawk_adaptive_load_client` with the spec. Returns `202`, or `409` while a run is active. |
| `GET /status` | State of the current or last run: `idle`, `running`, `done` or `failed`, plus exit code and byte counts. |
| `GET /results` | The last run's `AdaptiveLoadSessionOutput` textproto and both process logs. `409` while running, `404` before the first run. |

The image builds with `ko` on the `envoyproxy/nighthawk-dev` base pinned in
`.ko.yaml`, the same digest as `benchmarking/nighthawk-ingress/Dockerfile`.
