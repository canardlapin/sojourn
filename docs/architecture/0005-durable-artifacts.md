# Durable task artifacts

Status: accepted

## Decision

File-producing pipelines are a Sojourn capability assembled over backend-owned output staging.
Portable APIs never expose an absolute path on a submit host, login node, compute node, or shared
filesystem.

| Owner | Responsibility |
| --- | --- |
| `sojourn-core` | Logical artifact paths, declarations, byte schemas, media types, durable references, complete artifact sets, and publication failures |
| `slurm4s` | Worker output workspace, declared-path enforcement, bounded writes, sealing, size/digest manifests, attempt fencing, and verified result attachment |
| `sojourn-slurm` | Lower declarations, independently observe sealed files, import them into `SiteStore`, and compare the imported size/digest |
| `sojourn-runtime` | Content-addressed promotion and all-or-nothing publication of the artifact set |

`ArtifactPath` and slurm4s's `RelativeOutputPath` are structurally similar but have different
authority. The former is a pipeline contract; the latter is a path capability scoped to one worker
workspace. The adapter performs the explicit lowering.

## Execution and publication

```text
SiteOperation.artifacts
        |
        v
ResultContract.Structured(declared outputs)
        |
        v
TaskContext.outputs.write + seal
        |
        v
ResultAttachment verifies envelope manifest against independently observed files
        |
        v
ArtifactPublisher streams each file into SiteStore
        |
        +-- compare size and digest with verified OutputEntry
        |
        v
TaskOutcome.Succeeded(result RemoteRef, complete ArtifactSet)
```

The structured result is stored before artifact-set publication. If file promotion fails,
`TaskOutcome.PublicationFailed` retains that result reference together with the typed publication
failure. A caller can therefore recover or retry publication without inferring that the workload
failed or blindly executing it again.

Content-addressed objects written before a later failure may remain unreferenced, but a partial
`ArtifactSet` is never published. A successful task exposes either every declared artifact exactly
once or none of them.

## Operation contract

An operation declares the complete output set:

```scala
val outputs = ArtifactDeclarations.from(
  Vector(
    ArtifactDeclaration.from(
      ArtifactPath.from("results/model.nii.gz").toOption.get,
      SchemaId.from("nifti-1.gzip.v1").toOption.get,
      ByteLimit.from(512 * 1024 * 1024).toOption.get,
      ArtifactMediaType.from("application/gzip").toOption
    )
  )
).toOption.get

val fit = Op.contextual[Input, Summary]("fit-model") { (input, context) =>
  context.artifacts.write(outputs.entries.head.path, modelBytes).flatMap {
    case Left(failure) => IO.raiseError(new RuntimeException(failure.toString))
    case Right(_)      => IO.pure(summary(input))
  }
}.produces(outputs)
```

Artifact declarations are part of the operation's semantic version. Changing paths, schemas,
media types, or limits requires an operation version bump. The registry rejects a submitted
`SiteOperation` whose declarations differ from its executable registration.

`ArtifactRef.asInput(codec)` is the typed pipeline edge: it retags the immutable stored bytes only
when the artifact schema matches the downstream input codec.

## Failure and capability boundaries

- Missing, duplicate, undeclared, oversized, non-regular, size-mismatched, or digest-mismatched
  outputs cannot produce success.
- Physical worker paths are backend-private and are never part of `ArtifactRef` identity.
- The local batch backend and Slurm batch backend implement the same terminal artifact semantics.
- Pilot pools currently reject artifact-producing operations at submission with a typed
  `InvalidInput`; the spool protocol does not yet transport artifact manifests.
- The current worker output API and store ceiling are explicitly bounded by `ByteLimit`. The public
  operation capability accepts an FS2 stream so a later chunked worker/store implementation does
  not require changing operation code.

