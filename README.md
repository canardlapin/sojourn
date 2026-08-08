# sojourn

sojourn turns a remote batch cluster into an abstract compute-and-data site for a
local Scala application: batch execution (one scheduler job per task), leased
persistent compute (a pilot pool acquired from the queue, with an honest lease —
deadline, degradation, revocation), and a reference-passing remote data plane.
The name is queueing theory's *sojourn time*: the total time a job spends in the
system — the fact this library refuses to hide.

```scala
import cats.effect.{IO, IOApp}
import io.github.bbuchsbaum.sojourn.dsl.*

object Quickstart extends IOApp.Simple:
  val shout = Op[String, String]("shout")(s => IO.pure(s.toUpperCase))

  def run = Sojourn.local("dev", shout).use { site =>
    site.run(shout, "hello").flatMap(IO.println)      // HELLO
  }
```

Swap `Sojourn.local` for `Sojourn.slurm(...)` (local Slurm CLI) or
`Sojourn.slurmSsh(...)` (negotiated SSH agent) and the same code runs each task
as a scheduler job on a cluster. `site.run` is a documented convenience that
collapses the total outcome into a typed exception carrying full evidence; the
honest surface — `submit`/`outcome` with `Succeeded | PublicationFailed | Failed
| Interrupted | Unknown`, keyed idempotent resubmission, `RemoteRef`
reference-passing, durable declared file artifacts, leased
pools with `awaitGranted` — is one method away, and `site.raw` exposes the
entire SPI. Operations default to at-most-once (`RetrySafety.Unknown`);
`.retrySafe` is an explicit opt-in to automatic requeue.

The scheduler-specific substrate is [slurm4s](https://github.com/canardlapin/slurm4s); sojourn's API
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
machine). `docs/architecture/0004-compositional-slurm.md` records the exact
Sojourn/slurm4s ownership boundary and local/SSH capability assembly.
`docs/architecture/0005-durable-artifacts.md` defines file-producing operation,
promotion, and pipeline-composition semantics.
1.0 contract plan: `docs/plans/2026-08-04-sojourn-1-0-contract.md`.

## Modules

| Module | Boundary |
| --- | --- |
| `sojourn-core` | SPI (`Site` / `PoolCapableSite`, `PoolRequest`, `BatchDriver`, store/batch algebras, total `TaskOutcome`), durable artifact contracts, `RequestFingerprint`, spool wire protocol v1 |
| `sojourn-worker` | Program / `OperationRegistry` execution — backend-neutral; no slurm4s dependency |
| `sojourn-runtime` | Effectful machinery: FS store, site preflight, pilot loop, pool dispatcher. Still hosts `SojournEntryPoint`/`WorkerBridge` (slurm4s-worker) until those finish migrating into `sojourn-slurm` |
| `sojourn-local` | Scheduler-free `PoolCapableSite`: batch on supervised fibers; pools as in-process pilots over a real filesystem spool |
| `sojourn-slurm` | Exemplary Slurm **batch** `Site` today (`slurm4sBatch`); the only module that may depend on slurm4s scheduler artifacts long-term. Slurm `PoolCapableSite` is required before `1.0.0` |
| `sojourn-dsl` | Ergonomics only: `Wire`, `Op`, `Program`, `SimpleSite` — **no** backend or slurm4s dependency |
| `sojourn-all` | Convenience constructors: `Sojourn.local` / `slurm4sBatch` / `slurmSsh` |
| `sojourn-tck` | Published conformance suites (`StoreTck`, `BatchTck`, `PoolTck`, `ParityTck`) |
| `sojourn-demo` | Unpublished demo operations + assembled worker binary |

Provider-neutral contracts and atomic filesystem mechanics come from
`remote-exec-kernel`; both Sojourn and slurm4s depend downward on that
artifact. `sojourn-core`, `sojourn-worker`, and `sojourn-dsl` have no slurm4s
dependency (enforced by `checkModuleBoundaries`). The ownership rule is
recorded in `docs/architecture/0003-provider-neutral-kernel.md`; the remaining
composition boundary is recorded in `docs/architecture/0004-compositional-slurm.md`.

## Build

JDK 17+, Scala 3.7.4. Depends on published `remote-exec-kernel` and `slurm4s-*`
artifacts at the exact version in `project/Dependencies.scala` (currently
`0.1.0`). Normal development and CI resolve those coordinates from the
configured resolvers — not via sibling `publishLocal`.

```shell
sbt test
```

`slurm4s.sha` records the slurm4s commit used to produce the pinned `0.1.0`
artifacts for the optional sibling integration job only:

```shell
cd ../slurm4s && git checkout "$(cat ../sojourn/slurm4s.sha)" \
  && sbt 'set ThisBuild/version := "0.1.0"' 'set ThisBuild/isSnapshot := false' publishLocal
cd ../sojourn && sbt test
```

Until `0.1.0` is on a public resolver, publish that immutable version once
locally (or to your org's repository) from the SHA above. CI's default job
must not clone the sibling; a separate workflow may.
