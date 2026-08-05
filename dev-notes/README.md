# substrate Dev Notes

Personal development notes for
[agent-substrate/substrate](https://github.com/agent-substrate/substrate) — a
Kubernetes-based runtime that multiplexes many agent-like workloads ("actors")
onto a smaller pool of pre-warmed worker pods, using sandbox checkpoint/restore
to suspend and resume them.

> These files are **not** part of the repo. Keep them here so they don't get
> mixed into a PR.

**Baseline for everything below:** `main` @ `55f16fa9` (committed 2026-08-03),
analyzed 2026-08-03. Go 1.26.3, dependencies vendored under `vendor/`.

Unlike [../kcc](../kcc), this is **not** a fork — `origin` is the real upstream
(`https://github.com/agent-substrate/substrate.git`), so there is no
fork/upstream remote split and no sync step.

Sibling notes: [../kcc](../kcc) and [../tpu-operator](../tpu-operator) — both
KRM-object → cloud-resource controllers. Substrate shares the CRD-plus-controller
shape (`atecontroller` reconciles `WorkerPool` → `Deployment`) but diverges
sharply in that its hot path deliberately **bypasses** the Kubernetes control
plane; see the two-tier state model below.

## Orientation

Two facts unlock most of the codebase:

1. **The `ate` prefix is the project namespace.** `ateapi` (control plane),
   `atelet` (node DaemonSet), `ateom` (in-pod sandbox driver), `atenet`
   (DNS + router), `atespace` (isolation boundary / first half of an actor's
   identity). See `docs/glossary.md` in the repo.
2. **State is split two ways.** Config (`WorkerPool`, `ActorTemplate`,
   `SandboxConfig`) lives in Kubernetes CRDs; instance state (`Actor`, `Worker`,
   `ActorSnapshot`, `Atespace`) lives in Valkey/Redis behind `ateapi` because it
   changes too fast for etcd. So **`Actor` is not a CRD** — don't go looking for
   one in `pkg/api`.

Repo docs are worth reading but treat them as intent, not truth:
`docs/architecture.md:3` states outright that much of it is aspirational and
unimplemented (snapshot garbage collection, for example).

## Tooling

Two repo-local Claude Code agents are wired up in `.claude/agents/`
(gitignored, local only):

- `substrate-explorer` — broad "where is X / who calls Y" navigation
- `substrate-flow-tracer` — traces an RPC across the
  router → ateapi → atelet → ateom process hops

Both are unexercised as of 2026-08-03; correct their embedded repo map as it
proves wrong.

## Index

- [architecture-overview.md](architecture-overview.md) — first-pass map: the
  config/instance-state split, the per-binary component tour, and where to start
  reading.
- [networking-study-guide.md](networking-study-guide.md) — ground-up curriculum
  for the networking stack, assuming no networking background. Appendix A
  records why the local kind cluster isn't worth running on macOS.
- [phase3-ingress-path.md](phase3-ingress-path.md) — Phase 3 of the above in
  detail: the ten hops from `curl` to actor, a five-day reading order through
  `atenet`'s ext_proc/resumer and the worker-side `atunnel`, and a checkpoint.
- [linux-netns-primer.md](linux-netns-primer.md) — general-Linux prerequisite for
  the above: namespaces, `veth` pairs, bridges, and the `ip`/`nft` command
  reference. Read §1–§3 at Phase 1, all of it before Phase 5.
