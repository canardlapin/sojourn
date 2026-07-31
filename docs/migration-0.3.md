# Migration notes for the compositional API

## Operation registrations

Operation identity now has one provider-neutral owner:
`io.github.bbuchsbaum.remoteexec.kernel.OperationDescriptor`. A typed `OperationRef[I, O]` wraps
that descriptor without copying its fields.

Sojourn's `SiteOperation[I, O]` now carries the typed input/result codecs and retry declaration
derived from the executable registration. Construct it with codecs:

```scala
SiteOperation(id, version, inputCodec, resultCodec, RetrySafety.Unknown)
```

The former constructor `(id, version, inputSchema, resultSchema)` is removed. Likewise,
`OperationRegistry.entry` now accepts the operation and implementation only:

```scala
OperationRegistry.entry(operation)(run)
```

The former separate codec/retry arguments are removed because they allowed the registration and
public handle to drift. `WorkerBridge.operationRef` is removed; use `operation.reference`.
`OperationRegistry.typedEntry` is removed; typed input encoding comes directly from
`SiteOperation.input`, while registry lookup uses the portable descriptor.

scala-slurm retains pre-1.0 source names `OperationRef` and `RegisteredOperation`, but both are
aliases of the neutral kernel definitions.

## Verified result storage

Sojourn's Slurm backend now consumes `ResultAttachment.attachFileVerified`. On success it receives
a `VerifiedResultPayload` containing the typed value, the exact validated value bytes, all durable
attempt/operation/release bindings, the validated output manifest, and bounded envelope evidence.
The content-addressed store writes those immutable bytes directly. It no longer reopens or decodes
the result envelope after attachment, so post-attachment path replacement cannot affect the stored
result. Advanced integrations may retain `payload.evidence` as their successful audit surface.

## Slurm construction

The low-level Slurm site is now:

```scala
SlurmSite.fromCapabilities(siteName, catalog, store, managedBatchExecutor)
```

It does not instantiate a scheduler transport or a managed controller. `SlurmSite.local` and
`SlurmSite.overSsh` acquire different `Scheduler[IO]` interpreters and feed the same managed
assembly. The DSL exposes the corresponding `Sojourn.slurm` and `Sojourn.slurmSsh` facades.

Submission identity, conflicts, scheduler observations, freshness, and cancellation evidence now
come from the durable managed attempt. The in-process task map only shares result attachment among
callers. A restarted process can therefore resubmit the same key/request and attach without a
second `sbatch`; a different durable request remains a typed `SubmitRejection.Conflict`.

## Durable file artifacts

`SiteOperation` now carries `ArtifactDeclarations` with an empty default, so existing value-only
construction remains unchanged. Contextual operations can write declared files without receiving a
host path:

```scala
val render = Op.contextual[Model, Summary]("render") { (model, context) =>
  context.artifacts.write(outputPath, renderBytes(model)).flatMap {
    case Left(failure) => IO.raiseError(new RuntimeException(failure.toString))
    case Right(_)      => IO.pure(summary(model))
  }
}.produces(declarations)
```

`TaskOutcome.Succeeded` now has two fields:

```scala
case TaskOutcome.Succeeded(resultRef, artifacts) => ...
```

Existing one-field pattern matches must add the artifact field (use `_` when it is not needed).
`TaskOutcome.PublicationFailed(resultRef, failure, diagnostics)` is new: workload execution and
structured-result storage succeeded, but the complete declared artifact set did not publish. Do
not treat it as workload failure or automatically rerun the operation.

Use `ArtifactRef.asInput(codec)` to feed an artifact to a downstream operation as
`TaskInput.Stored`; it verifies the declared artifact schema before retagging the remote reference.
Artifact-producing operations currently run through local or Slurm batch. Pilot-pool submission
rejects them explicitly until the spool protocol carries artifact manifests.
