# Compositional Slurm assembly

Status: accepted

## Decision

Sojourn and scala-slurm are compositional layers, not competing scheduler libraries.

| Owner | Capability |
| --- | --- |
| `remote-exec-kernel` | Provider-neutral identities, operation descriptors, codecs, diagnostics, freshness, retry declarations, and atomic-file publication |
| `scala-slurm` | Slurm requests and observations, local CLI and SSH-agent transports, durable submission/cancellation journal, attempt epochs, registered-worker staging, and verified result attachment |
| `sojourn` | Scheduler-neutral `Site`/`TaskRunner`/`TaskHandle`/store semantics, operation registry, reference-passing results, and assembly of scala-slurm capabilities into a site |

The overlap that previously existed in identifiers, operation descriptors, atomic files, result
decoding, idempotency, and lifecycle tracking has one owner after this change.

## Batch data flow

```text
SiteOperation + TaskInput
        |
        v
RegisteredTaskLauncher -- lowers once --> JobRequest[NoResult]
        |
        v
ManagedController -- durable intent/epoch/conflict --> Scheduler
        |                                      local CLI | SSH agent | future REST
        v
managed observation/cancellation record
        |
        v
ResultAttachment.attachFileVerified
        |
        v
VerifiedResultPayload.encodedValue --> Sojourn SiteStore --> RemoteRef[O]
```

`ManagedBatchExecutor` is the only Slurm lifecycle capability visible to the thin `Site` wrapper.
Its process-local map is only a shared attachment cache. Request identity and conflicts are decided
by the durable managed digest; phase, freshness, scheduler uncertainty, and cancellation evidence
are read from the managed attempt. Reopening and submitting the same request attaches to that
attempt. A different request is a typed `SubmitRejection.Conflict`.

Failed observation produces `Freshness.Unknown`; a vanished job without a result remains
`TaskOutcome.Unknown` unless durable cancellation evidence justifies interruption. A verified
result that wins a cancellation race is preserved. Result attachment fences attempt epochs, so an
old result cannot settle a newer attempt.

## Constructors and resource ownership

- `SlurmSite.fromCapabilities` creates a site from an already acquired store and
  `ManagedBatchExecutor`. It knows no transport.
- `SlurmSite.local` acquires `SlurmLocal` and feeds its `Scheduler[IO]` to shared assembly.
- `SlurmSite.overSsh` acquires `RemoteSlurm`, uses its transport-neutral `scheduler` view, and feeds
  the same assembly.
- `Sojourn.slurm` and `Sojourn.slurmSsh` are convenience facades over those constructors.

The closed admission flag is finalized before supervised task fibers; fibers settle before the
managed controller and scheduler resources finalize. Callers composing `fromCapabilities` retain
ownership of their resource order.

The SSH adapter records a transport disconnect after request write as acceptance uncertainty.
It never implies that Slurm rejected the job or that resubmission is safe. Authentication,
protocol, transport, remote CLI, and scheduler failures retain distinct diagnostic codes.

## Consequence

A future REST transport implements or adapts to `Scheduler[IO]` and calls the shared assembly.
It does not change `Site`, task semantics, durable control, staging, attachment, or storage.
