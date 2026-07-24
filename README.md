# sojourn

sojourn turns a remote batch cluster into an abstract compute-and-data site for a
local Scala application: batch execution (one scheduler job per task), leased
persistent compute (a pilot pool acquired from the queue, with an honest lease —
deadline, degradation, revocation), and a reference-passing remote data plane.
The name is queueing theory's *sojourn time*: the total time a job spends in the
system — the fact this library refuses to hide.

The scheduler-specific substrate is [scala-slurm](../scala-slurm); sojourn's API
is scheduler-neutral and Slurm words never appear in it. The two truths a batch
backend cannot abstract away — the queue and the walltime lease — are modeled in
the types instead (`LeaseState`, total `TaskOutcome`, `Unknown` as a case rather
than an exception).

Backend-neutrality is a **tested property, not an intention**: a published
conformance kit (`sojourn-tck`) states the laws any `Site` implementation must
satisfy, and two backends pass them — a scheduler-free local backend (all 25
laws: store, batch, pool, batch/pool parity) and the exemplary Slurm backend
(store + batch laws, run against a real Slurm controller when
`SOJOURN_SLURM_TCK_WORKSPACE`/`_JAR` are set; pool wiring is the next
milestone).

Design: `docs/architecture/0001-site-layer.md` (site layer) and
`docs/architecture/0002-spool-runtime.md` (spool runtime semantics: the reclaim
evidence ladder, epoch fence, clock-free heartbeat staleness, drain, and lease
machine). Program plan: `docs/plans/2026-07-24-remote-executor.md`.

## Modules

| Module | Boundary |
| --- | --- |
| `sojourn-core` | Pure SPI (`Site`, `SiteStore`, `TaskRunner`, `LeasedPool`, `LeaseState`, total `TaskOutcome`), validated identifiers, spool wire protocol v1 with canonical codecs + golden fixtures |
| `sojourn-runtime` | Scheduler-neutral effectful machinery: content-addressed shared-FS store, operation registry, site preflight probes, the one worker binary (one-shot + pilot modes), pilot loop, pool dispatcher (reclaim ladder, epoch fence, lease governance) |
| `sojourn-local` | Scheduler-free backend: batch on supervised fibers, pool as in-process pilots over a real filesystem spool — the neutrality proof and dev-mode |
| `sojourn-slurm` | The exemplary Slurm backend: durable managed submission, typed staging, strict digest-verified result attachment over a shared workspace |
| `sojourn-tck` | Published conformance suites (`StoreTck`, `BatchTck`, `PoolTck`, `ParityTck`) any backend instantiates over a `TckHarness` |
| `sojourn-demo` | Unpublished demo operations + the assembled worker binary used by tests and acceptance |

Known coupling: sojourn borrows scala-slurm-core's identifier/evidence
vocabulary (`SubmissionKey`, `ContentDigest`, `Freshness`, `Diagnostics`, …) and
scala-slurm-worker's `AtomicFiles` kernel. Both are pure data / scheduler-free —
no scheduler runtime crosses the boundary — but true provider-neutrality would
require extracting that vocabulary; recorded as a pre-1.0 phase.

## Build

JDK 17+, Scala 3.7.4. Until scala-slurm publishes artifacts, resolve it locally.
`scala-slurm.sha` pins the exact scala-slurm commit this tree is built and tested
against — bump it deliberately (it is a reviewed diff), republish, and re-test:

```shell
cd ../scala-slurm && git checkout "$(cat ../sojourn/scala-slurm.sha)" \
  && sbt 'set ThisBuild/version := "0.1.0-SNAPSHOT"' +publishLocal
cd ../sojourn && sbt test
```

When both repos have Git remotes, CI consumes the pin the same way: clone
scala-slurm, check out the pinned sha, `+publishLocal`, then build sojourn.
