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

Design: `docs/architecture/0001-site-layer.md`. Master plan:
`../scala-slurm/docs/plans/2026-07-23-site-layer.md` (phases 3b–3e land here).

## Modules

| Module | Boundary |
| --- | --- |
| `sojourn-core` | Typed site/lease/task surface, validated identifiers, spool wire protocol v1 with canonical codecs |

Known coupling: `sojourn-core` depends on `scala-slurm-core` for the shared
identifier/evidence vocabulary (`SubmissionKey`, `ContentDigest`, `Freshness`,
`Diagnostics`, …). That module is pure data — no scheduler runtime crosses the
boundary — but true provider-neutrality would require extracting that vocabulary;
revisit before 1.0.

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
